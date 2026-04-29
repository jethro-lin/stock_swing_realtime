package com.stockswing.app

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

private val Context.dataStore by preferencesDataStore("settings")

private val SELECTED_PRESETS_KEY = stringPreferencesKey("selected_presets_v2")

class StockViewModel(app: Application) : AndroidViewModel(app) {

    private val twseApi = TwseApiService()

    companion object {
        private const val WORKERS = 5  // 並行下載數；避免 TWSE rate-limit
    }

    // ── 持久化：選擇的 Preset ─────────────────────────────────────────
    val selectedPresets: StateFlow<Set<Preset>> = app.dataStore.data
        .map { prefs ->
            val saved = prefs[SELECTED_PRESETS_KEY] ?: ""
            if (saved.isBlank()) setOf(Preset.LONG3_LEAN)
            else saved.split(",")
                .mapNotNull { k -> Preset.entries.find { it.key == k } }
                .toSet()
                .ifEmpty { setOf(Preset.LONG3_LEAN) }
        }
        .stateIn(viewModelScope, SharingStarted.Eagerly, setOf(Preset.LONG3_LEAN))

    // ── UI 狀態 ───────────────────────────────────────────────────────
    private val _isLoading   = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading

    private val _loadingMsg  = MutableStateFlow("")
    val loadingMsg: StateFlow<String> = _loadingMsg

    private val _scanProgress = MutableStateFlow(0 to 0)  // done to total
    val scanProgress: StateFlow<Pair<Int, Int>> = _scanProgress

    private val _error       = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error

    private val _scanResults = MutableStateFlow<List<SignalResult>>(emptyList())
    val scanResults: StateFlow<List<SignalResult>> = _scanResults

    private val _lastScanTime = MutableStateFlow("")
    val lastScanTime: StateFlow<String> = _lastScanTime

    private var scanJob: Job? = null

    // ── 初始化 ────────────────────────────────────────────────────────
    init {
        setupNotificationChannel()
    }

    // ── 公開操作 ──────────────────────────────────────────────────────

    fun togglePreset(preset: Preset) {
        viewModelScope.launch {
            val cur = selectedPresets.value.toMutableSet()
            if (preset in cur) cur.remove(preset) else cur.add(preset)
            if (cur.isEmpty()) return@launch          // 至少保留一個
            getApplication<Application>().dataStore.edit { prefs ->
                prefs[SELECTED_PRESETS_KEY] = cur.joinToString(",") { it.key }
            }
        }
    }

    fun stopScan() { scanJob?.cancel() }

    fun scan() {
        scanJob?.cancel()
        scanJob = viewModelScope.launch {
            try {
            _isLoading.value    = true
            _error.value        = null
            _scanProgress.value = 0 to 0

            // 1. 取得全市場代號 + 公司名稱
            _loadingMsg.value = "取得上市/上櫃股票清單…"
            val codeNames = twseApi.fetchAllCodesWithNames()
            if (codeNames.isEmpty()) {
                _error.value      = "無法取得股票清單，請確認網路狀況"
                _isLoading.value  = false
                _loadingMsg.value = ""
                return@launch
            }
            val allCodes = codeNames.keys.toList()

            // 2. 並行下載歷史 K 棒（EOD 模式）
            //    每支：buildBase(bars[:-1])  →  checkSignals 用 bars[-1] 的實際 OHLCV
            //    這樣 gapPct = (今開 - 昨收)/昨收，chgPct = 今收 vs 昨收，與 Python 一致
            data class EodEntry(
                val base: StrategyBase,
                val quote: RealtimeQuote,
            )
            val eodMap    = ConcurrentHashMap<String, EodEntry>()
            val doneCount = AtomicInteger(0)
            val semaphore = Semaphore(WORKERS)
            _scanProgress.value = 0 to allCodes.size

            val fmt = java.time.format.DateTimeFormatter.ofPattern("MM/dd")

            coroutineScope {
                allCodes.map { code ->
                    async {
                        semaphore.withPermit {
                            ensureActive()
                            try {
                                val bars = twseApi.fetchHistorical(code, months = 2)
                                // 需至少 22 根：20 根建基準 + 1 根前日 + 1 根訊號日
                                if (bars.size >= 22) {
                                    val baseBars = bars.dropLast(1)   // 不含今日
                                    val today    = bars.last()        // 訊號日（最近完成交易日）
                                    val prevDay  = bars[bars.size - 2]

                                    val base = StrategyEngine.buildBase(baseBars) ?: return@withPermit
                                    // 用今日量取代 base.volYesterday（爆量基準更準確）
                                    val baseAdj = base.copy(
                                        volYesterday = today.volumeLots.toDouble()
                                    )
                                    val chgPct = if (prevDay.close > 0)
                                        (today.close - prevDay.close) / prevDay.close * 100.0
                                    else 0.0

                                    val displayQuote = RealtimeQuote(
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
                                    eodMap[code] = EodEntry(baseAdj, displayQuote)
                                }
                            } catch (_: Exception) {}
                            val done = doneCount.incrementAndGet()
                            _scanProgress.value = done to allCodes.size
                            _loadingMsg.value   = "載入中… ($done/${allCodes.size})"
                        }
                    }
                }.awaitAll()
            }
            _scanProgress.value = doneCount.get() to allCodes.size

            if (eodMap.isEmpty()) {
                _error.value      = "無法計算技術指標，請確認網路狀況後重新掃描"
                _isLoading.value  = false
                _loadingMsg.value = ""
                return@launch
            }

            // 3. 計算訊號、按選擇的 preset combo 篩選（純 CPU，不再需要即時行情 API）
            _loadingMsg.value = "計算策略訊號…"
            val presets = selectedPresets.value
            val results = eodMap.entries.mapNotNull { (code, entry) ->
                val (base, quote) = entry
                val sigs = StrategyEngine.checkSignals(
                    base    = base,
                    price   = quote.price,
                    openP   = quote.open,
                    chgPct  = quote.chgPct,
                    highP   = quote.high,
                    lowP    = quote.low,
                )
                val hitMap = presets
                    .associateWith { sigs.matchedComboLabels(it) }
                    .filter       { it.value.isNotEmpty() }
                if (hitMap.isEmpty()) return@mapNotNull null

                SignalResult(
                    code       = code,
                    name       = quote.name,
                    quote      = quote,
                    signals    = sigs,
                    hitPresets = hitMap.mapKeys { it.key.key },
                )
            }.sortedWith(
                compareByDescending<SignalResult> { it.totalComboHits }
                    .thenByDescending { it.quote.totalVolLots }
            )

            _scanResults.value  = results
            _lastScanTime.value = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("MM/dd HH:mm"))
            _loadingMsg.value   = ""
            _isLoading.value    = false

            if (results.isNotEmpty()) {
                postNotification(
                    title = "選股完成：找到 ${results.size} 支",
                    body  = presets.joinToString(" + ") { it.label },
                )
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
