package com.stockswing.app.model

/** 單支股票的選股結果（即時行情 + 策略訊號） */
data class SignalResult(
    val code: String,
    val name: String,
    val quote: RealtimeQuote,
    val signals: StrategySignals,
)
