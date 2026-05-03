package com.stockswing.app.model

import kotlinx.serialization.Serializable

@Serializable
/** 單支股票的選股結果（即時行情 + 策略訊號 + 命中 preset combo） */
data class SignalResult(
    val code: String,
    val name: String,
    val quote: RealtimeQuote,
    val signals: StrategySignals,
    // key = preset.key（如 "long3_lean"）, value = 命中的 combo 列表（如 ["B+K+R", "B+R"]）
    val hitPresets: Map<String, List<String>> = emptyMap(),
    val ma5:  Double = 0.0,
    val ma10: Double = 0.0,
    val ma20: Double = 0.0,
) {
    val totalComboHits: Int get() = hitPresets.values.sumOf { it.size }
}
