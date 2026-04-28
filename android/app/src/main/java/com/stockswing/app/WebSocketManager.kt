package com.stockswing.app

import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import okhttp3.*
import java.util.concurrent.TimeUnit

enum class WsState { DISCONNECTED, CONNECTING, CONNECTED, ERROR }

class WebSocketManager {

    private val client = OkHttpClient.Builder()
        .pingInterval(20, TimeUnit.SECONDS)
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)   // no read timeout for WS
        .build()

    private var ws: WebSocket? = null
    private var currentUrl: String = ""

    private val _messages = MutableSharedFlow<String>(extraBufferCapacity = 128)
    val messages: SharedFlow<String> = _messages

    private val _state = MutableStateFlow(WsState.DISCONNECTED)
    val state: StateFlow<WsState> = _state

    fun connect(url: String) {
        if (_state.value == WsState.CONNECTING || _state.value == WsState.CONNECTED) {
            if (url == currentUrl) return
            disconnect()
        }
        currentUrl = url
        _state.value = WsState.CONNECTING

        val request = Request.Builder().url(url).build()
        ws = client.newWebSocket(request, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                _state.value = WsState.CONNECTED
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                _messages.tryEmit(text)
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                _state.value = WsState.ERROR
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                _state.value = WsState.DISCONNECTED
            }
        })
    }

    fun disconnect() {
        ws?.close(1000, "disconnect")
        ws = null
        _state.value = WsState.DISCONNECTED
    }
}
