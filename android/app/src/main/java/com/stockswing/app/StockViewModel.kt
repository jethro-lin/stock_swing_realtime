package com.stockswing.app

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.stockswing.app.data.TwseApiService
import com.stockswing.app.engine.StrategyEngine
import com.stockswing.app.model.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.io.File
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

private val Context.dataStore by preferencesDataStore("settings")

private val SELECTED_PRESETS_KEY  = stringPreferencesKey("selected_presets_v3")
private val SCAN_MODE_KEY         = stringPreferencesKey("scan_mode_v1")       // "preset" | "custom"
private val CUSTOM_SIGNALS_KEY    = stringPreferencesKey("custom_signals_v1")  // comma-separated

private const val TAG = "StockApp"

class StockViewModel(app: Application) : AndroidViewModel(app) {

    private val twseApi = TwseApiService(
        cacheDir       = app.filesDir,
        legacyCacheDir = app.cacheDir,
    )

    private val scanCacheFile = File(app.filesDir, "last_scan.json")

    private val jsonSerde = Json { ignoreUnknownKeys = true }

    companion object {
        private const val WORKERS    = 3
        private const val TOP_STOCKS = 300
    }

    private val DEFAULT_PRESETS = Preset.entries.toSet()

    // ── 持久化設定 ────────────────────────────────────────────────────
    val selectedPresets: StateFlow<Set<Preset>> = app.dataStore.data
        .map { prefs ->
            val saved = prefs[SELECTED_PRESETS_KEY] ?: ""
            if (saved.isBlank()) DEFAULT_PRESETS
            else saved.split(",")
                .mapNotNull { k -> Preset.entries.find { it.key == k } }
                .toSet()
                .ifEmpty { DEFAULT_PRESETS }
        }
        .stateIn(viewModelScope, SharingStarted.Eagerly, DEFAULT_PRESETS)

    val lastScanMode: StateFlow<String> = app.dataStore.data
        .map { prefs -> prefs[SCAN_MODE_KEY] ?: "preset" }
        .stateIn(viewModelScope, SharingStarted.Eagerly, "preset")

    val selectedCustomSignals: StateFlow<Set<String>> = app.dataStore.data
        .map { prefs ->
            val saved = prefs[CUSTOM_SIGNALS_KEY] ?: ""
            if (saved.isBlank()) emptySet() else saved.split(",").toSet()
        }
        .stateIn(viewModelScope, SharingStarted.Eagerly, emptySet())

    // ── UI 狀態 ───────────────────────────────────────────────────────
    private val _isLoading    = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading

    private val _loadingMsg   = MutableStateFlow("")
    val loadingMsg: StateFlow<String> = _loadingMsg

    private val _scanProgress = MutableStateFlow(0 to 0)
    val scanProgress: StateFlow<Pair<Int, Int>> = _scanProgress

    private val _error        = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error

    private val _scanResults  = MutableStateFlow<List<SignalResult>>(emptyList())
    val scanResults: StateFlow<List<SignalResult>> = _scanResults

    private val _lastScanTime = MutableStateFlow("")
    val lastScanTime: StateFlow<String> = _lastScanTime

    private val _signalDate   = MutableStateFlow("")
    val signalDate: StateFlow<String> = _signalDate

    private var scanJob: Job? = null

    // ── 日K圖表 ───────────────────────────────────────────────────────
    private val _chartTarget = MutableStateFlow<SignalResult?>(null)
    val chartTarget: StateFlow<SignalResult?> = _chartTarget

    private val _chartBars   = MutableStateFlow<List<HistoricalBar>>(emptyList())
    val chartBars: StateFlow<List<HistoricalBar>> = _chartBars

    fun openChart(result: SignalResult) {
        _chartTarget.value = result
        viewModelScope.launch(Dispatchers.IO) {
            _chartBars.value = twseApi.loadCachedBars(result.code) ?: emptyList()
        }
    }

    fun closeChart() {
        _chartTarget.value = null
        _chartBars.value   = emptyList()
    }

    // ── 初始化 ────────────────────────────────────────────────────────
    init {
        setupNotificationChannel()
        loadLastScan()
    }

    // ── 持久化上次掃描結果 ────────────────────────────────────────────

    private fun loadLastScan() {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                if (!scanCacheFile.exists()) return@launch
                val cache = jsonSerde.decodeFromString<ScanCache>(scanCacheFile.readText())
                _scanResults.value  = cache.results
                _lastScanTime.value = cache.scanTime
                _signalDate.value   = cache.signalDate
                Log.i(TAG, "loadLastScan: ${cache.results.size} results, signalDate=${cache.signalDate}")
            } catch (e: Exception) {
                Log.w(TAG, "loadLastScan: failed ($e), ignoring")
                scanCacheFile.delete()
            }
        }
    }

    private fun saveLastScan(results: List<SignalResult>, scanTime: String, signalDate: String) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                scanCacheFile.writeText(jsonSerde.encodeToString(ScanCache(results, scanTime, signalDate)))
                Log.i(TAG, "saveLastScan: ${results.size} results saved")
            } catch (e: Exception) {
                Log.w(TAG, "saveLastScan: failed ($e)")
            }
        }
    }

    // ── 公開操作 ──────────────────────────────────────────────────────

    fun stopScan() { scanJob?.cancel() }

    /**
     * 執行全市場掃描。
     * @param presets       預設模式：要套用的策略組合（customSignals 為空時生效）
     * @param customSignals 自選模式：指定的個別訊號代號，如 "B", "KS"（非空時優先）
     * @param overrideDate  null = 自動判斷；非 null = 指定訊號日
     */
    fun scan(
        presets:       Set<Preset>  = selectedPresets.value,
        customSignals: Set<String>  = emptySet(),
        overrideDate:  LocalDate?   = null,
    ) {
        val isCustomMode = customSignals.isNotEmpty()
        if (!isCustomMode && presets.isEmpty()) return

        scanJob?.cancel()
        scanJob = viewModelScope.launch {
            try {
                _isLoading.value    = true
                _error.value        = null
                _scanProgress.value = 0 to 0

                getApplication<Application>().dataStore.edit { prefs ->
                    prefs[SELECTED_PRESETS_KEY] = presets.joinToString(",") { it.key }
                    prefs[SCAN_MODE_KEY]         = if (isCustomMode) "custom" else "preset"
                    prefs[CUSTOM_SIGNALS_KEY]    = customSignals.joinToString(",")
                }

                // 1. 取得前 TOP_STOCKS 支（依成交量排序）
                _loadingMsg.value = "取得上市/上櫃股票清單…"
                val codeNames = twseApi.fetchAllCodesWithNames(limit = TOP_STOCKS)
                if (codeNames.isEmpty()) {
                    _error.value      = "無法取得股票清單，請確認網路狀況"
                    _isLoading.value  = false
                    _loadingMsg.value = ""
                    return@launch
                }
                val allCodes = codeNames.keys.toList()
                Log.i(TAG, "scan: ${allCodes.size} codes, customMode=$isCustomMode")

                // 2. 決定訊號日截止點與下載月數
                val tpe      = java.time.ZoneId.of("Asia/Taipei")
                val nowTpe   = java.time.ZonedDateTime.now(tpe)
                val todayTpe = nowTpe.toLocalDate()
                val LOOKBACK = 3

                val signalCutoff: LocalDate
                val monthsToDownload: Int
                if (overrideDate != null) {
                    signalCutoff = overrideDate.plusDays(1)
                    val lookbackStart   = overrideDate.minusMonths(LOOKBACK.toLong())
                    val monthsFromToday = (todayTpe.year - lookbackStart.year) * 12 +
                                          (todayTpe.monthValue - lookbackStart.monthValue)
                    monthsToDownload = (monthsFromToday + 1).coerceIn(LOOKBACK, 12)
                } else {
                    val isIntraday = nowTpe.hour in 9..13 ||
                                     (nowTpe.hour == 14 && nowTpe.minute < 30)
                    signalCutoff     = if (isIntraday) todayTpe else todayTpe.plusDays(1)
                    monthsToDownload = LOOKBACK
                }

                // 3. 並行下載歷史 K 棒
                data class EodEntry(val base: StrategyBase, val quote: RealtimeQuote)
                val eodMap       = ConcurrentHashMap<String, EodEntry>()
                val doneCount    = AtomicInteger(0)
                val failEmpty    = AtomicInteger(0)
                val failTooShort = AtomicInteger(0)
                val firstFails   = java.util.concurrent.CopyOnWriteArrayList<String>()
                val semaphore    = Semaphore(WORKERS)
                val fmt          = DateTimeFormatter.ofPattern("MM/dd")
                _scanProgress.value = 0 to allCodes.size

                coroutineScope {
                    allCodes.map { code ->
                        async {
                            semaphore.withPermit {
                                ensureActive()
                                try {
                                    val raw  = twseApi.fetchHistorical(code, months = monthsToDownload)
                                    val bars = raw.filter { it.date < signalCutoff }
                                    if (bars.size >= 22) {
                                        val today   = bars.last()
                                        val prevDay = bars[bars.size - 2]
                                        val base    = StrategyEngine.buildBase(bars) ?: return@withPermit
                                        val chgPct  = if (prevDay.close > 0)
                                            (today.close - prevDay.close) / prevDay.close * 100.0
                                        else 0.0
                                        eodMap[code] = EodEntry(
                                            base  = base,
                                            quote = RealtimeQuote(
                                                code         = code,
                                                name         = codeNames[code] ?: code,
                                                price        = today.close,
                                                open         = today.open,
                                                high         = today.high,
                                                low          = today.low,
                                                prevClose    = prevDay.close,
                                                chgPct       = chgPct,
                                                totalVolLots = today.volumeLots,
                                                updateTime   = today.date.format(fmt),
                                            )
                                        )
                                    } else if (raw.isEmpty()) {
                                        failEmpty.incrementAndGet()
                                        if (firstFails.size < 5) firstFails += "EMPTY:$code"
                                    } else {
                                        failTooShort.incrementAndGet()
                                        if (firstFails.size < 5)
                                            firstFails += "SHORT:$code(raw=${raw.size},filtered=${bars.size})"
                                    }
                                } catch (e: Exception) {
                                    Log.w(TAG, "kbar[$code] exception: $e")
                                }
                                val done = doneCount.incrementAndGet()
                                _scanProgress.value = done to allCodes.size
                                _loadingMsg.value   = "載入中… ($done/${allCodes.size})"
                            }
                        }
                    }.awaitAll()
                }
                Log.i(TAG, "scan: eodMap=${eodMap.size}/${allCodes.size} " +
                           "failEmpty=${failEmpty.get()} failShort=${failTooShort.get()}")
                if (firstFails.isNotEmpty()) Log.w(TAG, "scan: first failures: $firstFails")

                if (eodMap.isEmpty()) {
                    _error.value      = "無法計算技術指標，請確認網路狀況後重新掃描"
                    _isLoading.value  = false
                    _loadingMsg.value = ""
                    return@launch
                }

                // 4. 計算訊號、篩選
                _loadingMsg.value = "計算策略訊號…"
                val results = eodMap.entries.mapNotNull { (code, entry) ->
                    val (base, quote) = entry
                    val sigs = StrategyEngine.checkSignals(
                        base   = base,
                        price  = quote.price,
                        openP  = quote.open,
                        chgPct = quote.chgPct,
                        highP  = quote.high,
                        lowP   = quote.low,
                    )

                    val hitPresets: Map<String, List<String>>
                    if (isCustomMode) {
                        val hit = sigs.activeSignals.filter { it in customSignals }
                        if (hit.isEmpty()) return@mapNotNull null
                        hitPresets = mapOf("custom" to hit)
                    } else {
                        val hitMap = presets
                            .associateWith { sigs.matchedComboLabels(it) }
                            .filter       { it.value.isNotEmpty() }
                        if (hitMap.isEmpty()) return@mapNotNull null
                        hitPresets = hitMap.mapKeys { it.key.key }
                    }

                    SignalResult(
                        code       = code,
                        name       = quote.name,
                        quote      = quote,
                        signals    = sigs,
                        hitPresets = hitPresets,
                        ma5        = base.ma5,
                        ma10       = base.ma10,
                        ma20       = base.ma20,
                    )
                }.sortedWith(
                    compareByDescending<SignalResult> { it.totalComboHits }
                        .thenByDescending { it.quote.totalVolLots }
                )

                Log.i(TAG, "scan: results=${results.size}")
                val dateFmt  = DateTimeFormatter.ofPattern("MM/dd")
                val sigDate  = overrideDate?.format(dateFmt)
                    ?: results.firstOrNull()?.quote?.updateTime
                    ?: signalCutoff.minusDays(1).format(dateFmt)
                val scanTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("MM/dd HH:mm"))

                _signalDate.value   = sigDate
                _scanResults.value  = results
                _lastScanTime.value = scanTime
                _loadingMsg.value   = ""
                _isLoading.value    = false

                saveLastScan(results, scanTime, sigDate)

                if (results.isNotEmpty()) {
                    val bodyText = if (isCustomMode)
                        customSignals.sorted().joinToString(" ")
                    else
                        presets.joinToString(" + ") { it.label }
                    postNotification("選股完成：找到 ${results.size} 支", bodyText)
                }
            } finally {
                _isLoading.value  = false
                _loadingMsg.value = ""
            }
        }
    }

    // ── 通知 ─────────────────────────────────────────────────────────

    private fun setupNotificationChannel() {
        val nm = getApplication<Application>()
            .getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        nm.createNotificationChannel(
            NotificationChannel("stock_signals", "選股訊號", NotificationManager.IMPORTANCE_HIGH)
        )
    }

    private fun postNotification(title: String, body: String) {
        val ctx   = getApplication<Application>()
        val notif = NotificationCompat.Builder(ctx, "stock_signals")
            .setSmallIcon(android.R.drawable.ic_dialog_info)
            .setContentTitle(title)
            .setContentText(body)
            .setPriority(NotificationCompat.PRIORITY_HIGH)
            .setAutoCancel(true)
            .build()
        try { NotificationManagerCompat.from(ctx).notify(System.currentTimeMillis().toInt(), notif) }
        catch (_: SecurityException) {}
    }
}
