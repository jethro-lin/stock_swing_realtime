package com.stockswing.app

import android.app.Application
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.intPreferencesKey
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import com.stockswing.app.data.TwseApiService
import com.stockswing.app.engine.StrategyEngine
import com.stockswing.app.model.SignalResult
import com.stockswing.app.model.StrategyBase
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private val Context.dataStore by preferencesDataStore("settings")

private val CODES_KEY   = stringPreferencesKey("codes")
private val MIN_HIT_KEY = intPreferencesKey("min_hit")

class StockViewModel(app: Application) : AndroidViewModel(app) {

    private val twseApi = TwseApiService()

    // ── 持久化設定 ────────────────────────────────────────────────────
    val stockCodes: StateFlow<List<String>> = app.dataStore.data
        .map { it[CODES_KEY]?.split(",")?.filter(String::isNotBlank) ?: emptyList() }
        .stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    val minHit: StateFlow<Int> = app.dataStore.data
        .map { it[MIN_HIT_KEY] ?: 1 }
        .stateIn(viewModelScope, SharingStarted.Eagerly, 1)

    // ── UI 狀態 ───────────────────────────────────────────────────────
    private val _isLoading  = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading

    private val _loadingMsg = MutableStateFlow("")
    val loadingMsg: StateFlow<String> = _loadingMsg

    private val _error = MutableStateFlow<String?>(null)
    val error: StateFlow<String?> = _error

    private val _signalResults = MutableStateFlow<List<SignalResult>>(emptyList())
    val signalResults: StateFlow<List<SignalResult>> = _signalResults

    private val _lastUpdate = MutableStateFlow("")
    val lastUpdate: StateFlow<String> = _lastUpdate

    // ── 記憶體快取 ────────────────────────────────────────────────────
    private val bases = mutableMapOf<String, StrategyBase>()
    private var loadJob: Job? = null

    // ── 初始化 ────────────────────────────────────────────────────────
    init {
        setupNotificationChannel()

        // 股票清單變動時重新載入歷史資料
        viewModelScope.launch {
            stockCodes
                .distinctUntilChanged()
                .collect { codes ->
                    if (codes.isNotEmpty()) reloadAll(codes)
                }
        }

        // 交易時段每 60 秒自動更新即時行情
        viewModelScope.launch {
            while (true) {
                delay(60_000)
                if (isMarketOpen() && bases.isNotEmpty()) refreshPrices()
            }
        }
    }

    // ── 公開操作 ──────────────────────────────────────────────────────

    fun addCode(code: String) {
        val c = code.trim()
        if (c.isBlank()) return
        viewModelScope.launch {
            val current = stockCodes.value
            if (c !in current) saveCodes(current + c)
        }
    }

    fun removeCode(code: String) {
        viewModelScope.launch { saveCodes(stockCodes.value - code) }
    }

    fun setMinHit(n: Int) {
        viewModelScope.launch {
            getApplication<Application>().dataStore.edit { it[MIN_HIT_KEY] = n }
        }
    }

    fun refresh() {
        viewModelScope.launch {
            val codes = stockCodes.value
            if (codes.isNotEmpty()) reloadAll(codes)
        }
    }

    // ── 內部邏輯 ──────────────────────────────────────────────────────

    private suspend fun saveCodes(codes: List<String>) {
        getApplication<Application>().dataStore.edit { it[CODES_KEY] = codes.joinToString(",") }
    }

    /**
     * 重新下載歷史資料並預算 base，然後抓一次即時行情。
     * 取消任何在途舊任務。
     */
    private fun reloadAll(codes: List<String>) {
        loadJob?.cancel()
        loadJob = viewModelScope.launch {
            _isLoading.value = true
            _error.value     = null
            bases.clear()

            var done = 0
            for (code in codes) {
                ensureActive()
                _loadingMsg.value = "載入 $code（${done + 1}/${codes.size}）"
                try {
                    val bars = twseApi.fetchHistorical(code)
                    if (bars.size >= 20) {
                        StrategyEngine.buildBase(bars)?.let { bases[code] = it }
                    }
                } catch (_: Exception) { }
                done++
            }

            _loadingMsg.value = ""
            _isLoading.value  = false

            if (bases.isEmpty()) {
                _error.value = "無法載入歷史資料，請確認代號正確或網路狀況"
            } else {
                refreshPrices()
            }
        }
    }

    private suspend fun refreshPrices() {
        val codes = bases.keys.toList()
        if (codes.isEmpty()) return
        try {
            val quotes  = twseApi.fetchRealtimePrice(codes)
            val prevHits = _signalResults.value.associate { it.code to it.signals.hit }

            val results = codes.mapNotNull { code ->
                val base  = bases[code]  ?: return@mapNotNull null
                val quote = quotes[code] ?: return@mapNotNull null
                val sigs  = StrategyEngine.checkSignals(
                    base   = base,
                    price  = quote.price,
                    openP  = quote.open,
                    chgPct = quote.chgPct,
                    highP  = quote.high,
                    lowP   = quote.low,
                )
                SignalResult(code = code, name = quote.name, quote = quote, signals = sigs)
            }.sortedWith(
                compareByDescending<SignalResult> { it.signals.hit }
                    .thenByDescending { it.signals.hitLong }
            )

            _signalResults.value = results
            _lastUpdate.value    = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))

            // 推通知：命中數增加的股票
            results.filter { it.signals.hit > 0 }.forEach { r ->
                if (r.signals.hit > (prevHits[r.code] ?: 0)) {
                    postNotification(
                        title = "${r.code} ${r.name}  ${r.signals.direction}方訊號",
                        body  = "命中 ${r.signals.hit} 個  現價 ${"%.2f".format(r.quote.price)}" +
                                "  ${r.signals.activeSignals.joinToString(" ")}",
                    )
                }
            }
        } catch (_: Exception) { }
    }

    private fun isMarketOpen(): Boolean {
        val now = LocalTime.now()
        return now.isAfter(LocalTime.of(9, 0)) && now.isBefore(LocalTime.of(13, 30))
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
        catch (_: SecurityException) { }
    }
}
