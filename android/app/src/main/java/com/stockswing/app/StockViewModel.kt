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
import java.time.LocalTime
import java.time.format.DateTimeFormatter

private val Context.dataStore by preferencesDataStore("settings")

private val SELECTED_PRESETS_KEY = stringPreferencesKey("selected_presets_v2")

class StockViewModel(app: Application) : AndroidViewModel(app) {

    private val twseApi = TwseApiService()

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

    fun scan() {
        scanJob?.cancel()
        scanJob = viewModelScope.launch {
            _isLoading.value   = true
            _error.value       = null
            _scanProgress.value = 0 to 0

            // 1. 取得全市場代號
            _loadingMsg.value = "取得上市/上櫃股票清單…"
            val allCodes = twseApi.fetchAllCodes()
            if (allCodes.isEmpty()) {
                _error.value     = "無法取得股票清單，請確認網路狀況"
                _isLoading.value = false
                _loadingMsg.value = ""
                return@launch
            }

            // 2. 逐支下載 2 個月歷史 K 棒，buildBase 會過濾低流動性
            val bases = mutableMapOf<String, StrategyBase>()
            var done  = 0
            _scanProgress.value = 0 to allCodes.size

            for (code in allCodes) {
                ensureActive()
                _loadingMsg.value   = "載入 $code (${done + 1}/${allCodes.size})"
                _scanProgress.value = done to allCodes.size
                try {
                    val bars = twseApi.fetchHistorical(code, months = 2)
                    if (bars.size >= 20) {
                        StrategyEngine.buildBase(bars)?.let { bases[code] = it }
                    }
                } catch (_: Exception) {}
                done++
            }
            _scanProgress.value = done to allCodes.size

            if (bases.isEmpty()) {
                _error.value     = "無法計算技術指標，請確認網路狀況後重新掃描"
                _isLoading.value = false
                _loadingMsg.value = ""
                return@launch
            }

            // 3. 批次抓即時行情
            _loadingMsg.value = "抓取即時行情（${bases.size} 支）…"
            val quotes = try { twseApi.fetchRealtimePrice(bases.keys.toList()) }
                         catch (_: Exception) { emptyMap() }

            // 4. 計算訊號、按選擇的 preset combo 篩選
            val presets = selectedPresets.value
            val results = bases.keys.mapNotNull { code ->
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
            _lastScanTime.value = LocalTime.now()
                .format(DateTimeFormatter.ofPattern("MM/dd HH:mm"))
            _loadingMsg.value   = ""
            _isLoading.value    = false

            if (results.isNotEmpty()) {
                postNotification(
                    title = "選股完成：找到 ${results.size} 支",
                    body  = presets.joinToString(" + ") { it.label },
                )
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
