package com.viam.ble

import android.bluetooth.BluetoothAdapter
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.content.Context
import android.os.ParcelUuid
import android.util.Log
import com.viam.ble.BlePlugin.Companion.TAG
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.lang.ref.WeakReference
import java.util.concurrent.TimeoutException
import kotlin.coroutines.resumeWithException
import kotlin.math.max

// CentralManager is our Android based BLE Central manager.
// It is a singleton so that we don't double up on any bluetooth operations that would cause an
// incorrect or unexpected program state.
class CentralManager(
    context: WeakReference<Context>,
) : BluetoothManager(context) {
    private val peripherals: MutableMap<String, Peripheral> = mutableMapOf()
    private val peripheralsMutex = Mutex()

    suspend fun reset() {
        withContext(Dispatchers.IO) {
            if (state == BluetoothAdapter.STATE_ON) {
                stopScanningForPeripherals()
            }
            peripheralsMutex.withLock {
                peripherals.values.forEach {
                    it.close()
                }
                peripherals.clear()
            }
        }
    }

    private var isScanning = false
    private var lastNScans = mutableListOf<Long>()
    private val scanMutex = Mutex()
    private val excludeFromScan = mutableSetOf<String>()

    @Throws(SecurityException::class)
    suspend fun scanForPeripherals(serviceIds: List<String> = listOf()) {
        mustBePoweredOn()
        withContext(Dispatchers.IO) {
            scanMutex.withLock {
                // Android excludes bonded devices from showing up in advertisements. So we need
                // to connect to it in order to check out its services. We'll disconnect if it's
                // of no use to us.
                excludeFromScan.clear()

                if (isScanning) {
                    return@withContext
                }

                isScanning = true

                Log.d(TAG, "requesting scan of $serviceIds")
                btMan.adapter.bluetoothLeScanner.startScan(
                    serviceIds.map {
                        ScanFilter
                            .Builder()
                            .setServiceUuid(ParcelUuid.fromString(it))
                            .build()
                    },
                    ScanSettings.Builder()
                        .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
                        .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
                        .setMatchMode(ScanSettings.MATCH_MODE_AGGRESSIVE)
                        .setNumOfMatches(ScanSettings.MATCH_NUM_MAX_ADVERTISEMENT)
                        .setReportDelay(0)
                        .build(),
                    leScanCallback,
                )
            }
        }
    }

    @Throws(SecurityException::class)
    suspend fun stopScanningForPeripherals() {
        mustBePoweredOn()
        withContext(Dispatchers.IO) {
            scanMutex.withLock {
                if (!isScanning) {
                    return@withContext
                }
                isScanning = false
                btMan.adapter.bluetoothLeScanner.stopScan(leScanCallback)
            }
        }
    }

    @Throws(SecurityException::class)
    suspend fun connectToDevice(macAddress: String): List<Map<String, Any>> {
        mustBePoweredOn()
        return withContext(Dispatchers.IO) {
            peripheralsMutex.withLock {
                if (peripherals.containsKey(macAddress)) {
                    return@withContext peripherals[macAddress]!!.discoveredServices
                }
            }
            val periph = Peripheral(btMan, macAddress)
            peripheralsMutex.withLock {
                peripherals[macAddress] = periph
            }
            try {
                suspendCancellableCoroutine { cont: CancellableContinuation<Unit> ->
                    CoroutineScope(Dispatchers.Main).launch {
                        val contextStrong = context.get()
                        if (contextStrong == null) {
                            cont.resumeWithException(Exception("application context no longer available"))
                            return@launch
                        }
                        periph.connect(contextStrong, cont)
                    }
                }
            } catch (e: Throwable) {
                peripheralsMutex.withLock {
                    peripherals.remove(macAddress)
                }
                throw e
            }
            return@withContext periph.discoveredServices
        }
    }

    suspend fun disconnectFromDevice(macAddress: String) {
        mustBePoweredOn()
        withContext(Dispatchers.IO) {
            peripheralsMutex.withLock {
                val periph = peripherals[macAddress] ?: throw Exception("peripheral $macAddress not found")
                peripherals.remove(macAddress)
                periph.close()
            }
        }
    }

    suspend fun getPeripheral(macAddress: String): Peripheral? {
        mustBePoweredOn()
        return withContext(Dispatchers.IO) {
            peripheralsMutex.withLock {
                return@withContext peripherals[macAddress]
            }
        }
    }

    private val _scanForPeripheralFlow = MutableSharedFlow<Result<Map<String, Any?>>>(replay = 0)
    val scanForPeripheralFlow: SharedFlow<Result<Map<String, Any?>>> = _scanForPeripheralFlow

    private val leScanCallback: ScanCallback =
        object : ScanCallback() {
            @Throws(SecurityException::class)
            override fun onScanResult(
                callbackType: Int,
                result: ScanResult,
            ) {
                super.onScanResult(callbackType, result)
                CoroutineScope(Dispatchers.IO).launch {
                    scanMutex.withLock {
                        if (excludeFromScan.contains(result.device.address)) {
                            return@launch
                        }
                    }
                    _scanForPeripheralFlow.emit(
                        Result.success(
                            hashMapOf(
                                "id" to result.device.address,
                                "name" to result.device.name,
                                "service_ids" to result.device.uuids?.map { toString() },
                            ),
                        ),
                    )
                }
            }

            override fun onScanFailed(errorCode: Int) {
                Log.e(TAG, "onScanFailed $errorCode")
                CoroutineScope(Dispatchers.IO).launch {
                    _scanForPeripheralFlow.emit(Result.failure(Exception("scan failed: $errorCode")))
                }
            }
        }
}
