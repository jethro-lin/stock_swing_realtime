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
import com.stockswing.app.model.StockUpdate
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json

private val Context.dataStore by preferencesDataStore("settings")
private val WS_URL_KEY = stringPreferencesKey("ws_url")
private const val CHANNEL_ID = "stock_alerts"

class StockViewModel(app: Application) : AndroidViewModel(app) {

    private val wsManager = WebSocketManager()
    private val json = Json { ignoreUnknownKeys = true }

    // ── 持久化 WS URL ──────────────────────────────────────────────
    val savedUrl: StateFlow<String> = app.dataStore.data
        .map { it[WS_URL_KEY] ?: "ws://192.168.1.100:8765" }
        .stateIn(viewModelScope, SharingStarted.Eagerly, "ws://192.168.1.100:8765")

    fun saveUrl(url: String) {
        viewModelScope.launch {
            getApplication<Application>().dataStore.edit { it[WS_URL_KEY] = url }
        }
    }

    // ── WebSocket 狀態 ─────────────────────────────────────────────
    val wsState: StateFlow<WsState> = wsManager.state

    // ── 最新推播資料 ───────────────────────────────────────────────
    private val _update = MutableStateFlow<StockUpdate?>(null)
    val update: StateFlow<StockUpdate?> = _update

    // ── 警報追蹤（用於推播通知去重）──────────────────────────────────
    private var lastAlertCount = 0

    // ── 自動重連 ──────────────────────────────────────────────────
    private var reconnectJob: Job? = null

    init {
        setupNotificationChannel()
        collectMessages()
        collectStateForReconnect()
    }

    private fun collectMessages() {
        viewModelScope.launch {
            wsManager.messages.collect { msg ->
                try {
                    val u = json.decodeFromString<StockUpdate>(msg)
                    _update.value = u
                    notifyNewAlerts(u.alerts)
                } catch (_: Exception) { }
            }
        }
    }

    private fun collectStateForReconnect() {
        viewModelScope.launch {
            wsManager.state.collect { state ->
                if (state == WsState.ERROR || state == WsState.DISCONNECTED) {
                    scheduleReconnect()
                } else {
                    reconnectJob?.cancel()
                }
            }
        }
    }

    private fun scheduleReconnect() {
        reconnectJob?.cancel()
        reconnectJob = viewModelScope.launch {
            delay(5_000)
            val url = savedUrl.value
            if (url.isNotBlank()) wsManager.connect(url)
        }
    }

    fun connect(url: String) {
        reconnectJob?.cancel()
        wsManager.connect(url)
    }

    fun disconnect() {
        reconnectJob?.cancel()
        wsManager.disconnect()
    }

    override fun onCleared() {
        super.onCleared()
        wsManager.disconnect()
    }

    // ── 本地通知 ──────────────────────────────────────────────────
    private fun setupNotificationChannel() {
        val nm = getApplication<Application>()
            .getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "選股警報", NotificationManager.IMPORTANCE_HIGH)
        )
    }

    private fun notifyNewAlerts(alerts: List<String>) {
        if (alerts.size <= lastAlertCount) {
            lastAlertCount = alerts.size
            return
        }
        val newOnes = alerts.subList(lastAlertCount, alerts.size)
        lastAlertCount = alerts.size

        val ctx = getApplication<Application>()
        val nm  = NotificationManagerCompat.from(ctx)
        newOnes.forEachIndexed { i, msg ->
            val notif = NotificationCompat.Builder(ctx, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.ic_dialog_info)
                .setContentTitle("台股訊號")
                .setContentText(msg)
                .setPriority(NotificationCompat.PRIORITY_HIGH)
                .setAutoCancel(true)
                .build()
            try { nm.notify(System.currentTimeMillis().toInt() + i, notif) }
            catch (_: SecurityException) { }
        }
    }
}
