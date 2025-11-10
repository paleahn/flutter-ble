import CoreBluetooth
import Foundation

/// Wrapper around `CBL2CAPChannel` that integrates with the documented Input/Output stream APIs.
/// Reads and writes are serialized per CoreBluetooth guidance and processed on the run loop servicing the streams.
public class L2CAPChannel: NSObject, StreamDelegate {
    private let runLoopMode: RunLoop.Mode = .default
    private let maxWriteBytesPerCycle = 1024

    public var onClose: (() throws -> Void)
    public var channel: CBL2CAPChannel

    private var isClosed = false
    var isClosedLock: NSLock = NSLock()

    private struct QueuedWrite {
        let data: Data
        let continuation: CheckedContinuation<Void, Error>
    }
    private var writeContinuation: CheckedContinuation<Void, Error>?
    private var pendingWriteData: Data?
    private var pendingWriteOffset = 0
    private var writeQueue: [QueuedWrite] = []

    private struct QueuedRead {
        let length: Int
        let continuation: CheckedContinuation<Data?, Error>
    }
    private var readContinuation: CheckedContinuation<Data?, Error>?
    private var pendingReadLength = 0
    private var readQueue: [QueuedRead] = []
    private var bufferedReadData = Data()
    private var inputStreamAtEnd = false
    private var readScratchBuffer = [UInt8](repeating: 0, count: 1024)

    private var streamError: Error?

    init(channel: CBL2CAPChannel, onClose: @escaping (() throws -> Void)) {
        self.channel = channel
        self.onClose = onClose
        super.init()
        configureStreams()
    }

    private func configureStreams() {
        let schedule = {
            self.channel.inputStream.delegate = self
            self.channel.outputStream.delegate = self
            let currentRunLoop = RunLoop.current
            self.channel.inputStream.schedule(in: currentRunLoop, forMode: self.runLoopMode)
            self.channel.outputStream.schedule(in: currentRunLoop, forMode: self.runLoopMode)
            self.channel.inputStream.open()
            self.channel.outputStream.open()
        }

        if Thread.isMainThread {
            schedule()
        } else {
            DispatchQueue.main.sync(execute: schedule)
        }
    }

    private func teardownStreams() {
        let teardown = {
            let currentRunLoop = RunLoop.current
            self.channel.inputStream.remove(from: currentRunLoop, forMode: self.runLoopMode)
            self.channel.outputStream.remove(from: currentRunLoop, forMode: self.runLoopMode)
            self.channel.inputStream.close()
            self.channel.outputStream.close()
            self.channel.inputStream.delegate = nil
            self.channel.outputStream.delegate = nil
        }

        if Thread.isMainThread {
            teardown()
        } else {
            DispatchQueue.main.sync(execute: teardown)
        }
    }

    public func write(data: Data) async throws {
        guard (isClosedLock.withLock { !isClosed }) else {
            throw RuntimeError("channel closed")
        }
        if data.isEmpty {
            return
        }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            self.performOnStreamQueue {
                if let error = self.streamError {
                    continuation.resume(throwing: error)
                    return
                }
                self.enqueueWrite(data: data, continuation: continuation)
            }
        }
    }

    public func read(maxRead: Int) async throws -> Data? {
        guard (isClosedLock.withLock { !isClosed }) else {
            throw RuntimeError("channel closed")
        }
        if maxRead <= 0 {
            return nil
        }
        return try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Data?, Error>) in
            self.performOnStreamQueue {
                if let error = self.streamError {
                    continuation.resume(throwing: error)
                    return
                }
                self.enqueueRead(length: maxRead, continuation: continuation)
            }
        }
    }

    private func enqueueWrite(data: Data, continuation: CheckedContinuation<Void, Error>) {
        if writeContinuation == nil {
            pendingWriteData = data
            pendingWriteOffset = 0
            writeContinuation = continuation
            flushPendingWrite()
        } else {
            writeQueue.append(QueuedWrite(data: data, continuation: continuation))
        }
    }

    private func enqueueRead(length: Int, continuation: CheckedContinuation<Data?, Error>) {
        if inputStreamAtEnd && bufferedReadData.isEmpty {
            continuation.resume(returning: nil)
            return
        }

        if readContinuation == nil {
            pendingReadLength = length
            readContinuation = continuation
            accumulateAvailableBytes()
            deliverPendingRead()
        } else {
            readQueue.append(QueuedRead(length: length, continuation: continuation))
        }
    }

    private func flushPendingWrite() {
        if let error = streamError {
            failPendingWrite(error)
            return
        }
        guard let data = pendingWriteData, let continuation = writeContinuation else {
            return
        }

        guard let outputStream = channel.outputStream else {
            handleStreamError(RuntimeError("output stream unavailable"))
            return
        }
        var flushedThisCycle = 0
        while outputStream.hasSpaceAvailable && pendingWriteOffset < data.count {
            if flushedThisCycle >= maxWriteBytesPerCycle {
                break
            }
            let bytesWritten = data.withUnsafeBytes { (rawBufferPointer: UnsafeRawBufferPointer) -> Int in
                let bufferPointer = rawBufferPointer.bindMemory(to: UInt8.self)
                guard let baseAddress = bufferPointer.baseAddress else {
                    return -1
                }
                let remaining = data.count - self.pendingWriteOffset
                let chunk = min(remaining, self.maxWriteBytesPerCycle - flushedThisCycle)
                return outputStream.write(
                    baseAddress.advanced(by: self.pendingWriteOffset),
                    maxLength: chunk)
            }

            if bytesWritten < 0 {
                let error = outputStream.streamError ?? RuntimeError("write stream error")
                failPendingWrite(error)
                return
            }
            if bytesWritten == 0 {
                break
            }

            pendingWriteOffset += bytesWritten
            flushedThisCycle += bytesWritten
        }

        if pendingWriteOffset >= data.count {
            completeCurrentWrite(successContinuation: continuation)
        }
    }

    private func completeCurrentWrite(successContinuation: CheckedContinuation<Void, Error>) {
        pendingWriteData = nil
        pendingWriteOffset = 0
        writeContinuation = nil
        successContinuation.resume()
        activateNextWrite()
    }

    private func activateNextWrite() {
        guard writeContinuation == nil else {
            return
        }
        guard !writeQueue.isEmpty else {
            return
        }
        let next = writeQueue.removeFirst()
        pendingWriteData = next.data
        pendingWriteOffset = 0
        writeContinuation = next.continuation
        flushPendingWrite()
    }

    private func accumulateAvailableBytes() {
        guard let inputStream = channel.inputStream else {
            handleStreamError(RuntimeError("input stream unavailable"))
            return
        }
        while inputStream.hasBytesAvailable {
            let bytesRead = readScratchBuffer.withUnsafeMutableBytes { rawBufferPointer -> Int in
                let bufferPointer = rawBufferPointer.bindMemory(to: UInt8.self)
                guard let baseAddress = bufferPointer.baseAddress else {
                    return -1
                }
                return inputStream.read(baseAddress, maxLength: rawBufferPointer.count)
            }

            if bytesRead < 0 {
                let error = inputStream.streamError ?? RuntimeError("read stream error")
                handleStreamError(error)
                return
            }
            if bytesRead == 0 {
                break
            }

            readScratchBuffer.withUnsafeBytes { rawBufferPointer in
                let bufferPointer = rawBufferPointer.bindMemory(to: UInt8.self)
                if let baseAddress = bufferPointer.baseAddress {
                    bufferedReadData.append(baseAddress, count: bytesRead)
                }
            }
        }
    }

    private func deliverPendingRead() {
        guard let continuation = readContinuation else {
            return
        }

        if let error = streamError {
            readContinuation = nil
            pendingReadLength = 0
            continuation.resume(throwing: error)
            return
        }

        if inputStreamAtEnd && bufferedReadData.isEmpty {
            readContinuation = nil
            pendingReadLength = 0
            continuation.resume(returning: nil)
            activateNextRead()
            return
        }

        guard !bufferedReadData.isEmpty else {
            return
        }

        let bytesToReturn = min(pendingReadLength, bufferedReadData.count)
        let result = bufferedReadData.subdata(in: 0..<bytesToReturn)
        bufferedReadData.removeSubrange(0..<bytesToReturn)
        readContinuation = nil
        pendingReadLength = 0
        continuation.resume(returning: result)
        activateNextRead()
    }

    private func activateNextRead() {
        guard readContinuation == nil else {
            return
        }
        guard !readQueue.isEmpty else {
            return
        }
        let next = readQueue.removeFirst()
        pendingReadLength = next.length
        readContinuation = next.continuation
        if inputStreamAtEnd && bufferedReadData.isEmpty {
            readContinuation = nil
            pendingReadLength = 0
            next.continuation.resume(returning: nil)
            activateNextRead()
            return
        }
        deliverPendingRead()
    }

    private func failPendingWrite(_ error: Error) {
        pendingWriteData = nil
        pendingWriteOffset = 0
        if let continuation = writeContinuation {
            writeContinuation = nil
            continuation.resume(throwing: error)
        }
        if !writeQueue.isEmpty {
            writeQueue.forEach { queued in
                queued.continuation.resume(throwing: error)
            }
            writeQueue.removeAll()
        }
    }

    private func failPendingRead(_ error: Error) {
        bufferedReadData.removeAll(keepingCapacity: true)
        inputStreamAtEnd = true
        pendingReadLength = 0
        if let continuation = readContinuation {
            readContinuation = nil
            continuation.resume(throwing: error)
        }
        if !readQueue.isEmpty {
            readQueue.forEach { queued in
                queued.continuation.resume(throwing: error)
            }
            readQueue.removeAll()
        }
    }

    private func handleStreamError(_ error: Error) {
        if streamError == nil {
            streamError = error
        }
        failPendingWrite(error)
        failPendingRead(error)
    }

    public func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
        switch eventCode {
        case .hasBytesAvailable:
            accumulateAvailableBytes()
            deliverPendingRead()
        case .hasSpaceAvailable:
            flushPendingWrite()
        case .endEncountered:
            if aStream === channel.inputStream {
                inputStreamAtEnd = true
                deliverPendingRead()
            } else {
                handleStreamError(RuntimeError("output stream closed"))
            }
        case .errorOccurred:
            let error = aStream.streamError ?? RuntimeError("stream error")
            handleStreamError(error)
        default:
            break
        }
    }

    public func close() throws {
        if (isClosedLock.withLock {
            if isClosed {
                return true
            }
            isClosed = true
            return false
        }) {
            return
        }

        let closeError = RuntimeError("channel closed")
        performSyncOnStreamQueue {
            if self.streamError == nil {
                self.streamError = closeError
            }
            self.failPendingWrite(closeError)
            if !(self.inputStreamAtEnd && self.bufferedReadData.isEmpty) {
                self.failPendingRead(closeError)
            } else {
                if let continuation = self.readContinuation {
                    self.readContinuation = nil
                    continuation.resume(returning: nil)
                }
                if !self.readQueue.isEmpty {
                    self.readQueue.forEach { queued in
                        queued.continuation.resume(returning: nil)
                    }
                    self.readQueue.removeAll()
                }
            }
        }

        teardownStreams()
        try onClose()
    }

    private func performOnStreamQueue(_ block: @escaping () -> Void) {
        if Thread.isMainThread {
            block()
        } else {
            DispatchQueue.main.async(execute: block)
        }
    }

    private func performSyncOnStreamQueue(_ block: () -> Void) {
        if Thread.isMainThread {
            block()
        } else {
            DispatchQueue.main.sync(execute: block)
        }
    }
}
