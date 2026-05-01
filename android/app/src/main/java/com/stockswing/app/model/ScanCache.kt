package com.stockswing.app.model

import kotlinx.serialization.Serializable

@Serializable
data class ScanCache(
    val results: List<SignalResult>,
    val scanTime: String,    // 執行掃描的時間，如 "04/29 14:35"
    val signalDate: String = "",  // 策略訊號日（K 棒最後一根的日期），如 "04/28"
)
