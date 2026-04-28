package com.stockswing.app.model

/** TWSE MIS API 的即時行情快照 */
data class RealtimeQuote(
    val code: String,
    val name: String,
    val price: Double,
    val open: Double,
    val high: Double,
    val low: Double,
    val prevClose: Double,
    val chgPct: Double,
    val totalVolLots: Long,
    val updateTime: String,
)
