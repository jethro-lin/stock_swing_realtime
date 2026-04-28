package com.stockswing.app.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class StockSignal(
    val code: String,
    val name: String,
    val price: Double,
    @SerialName("chg_pct")   val chgPct: Double,
    @SerialName("hit_long")  val hitLong: Int,
    @SerialName("hit_short") val hitShort: Int,
    val hit: Int,
    val direction: String,
    @SerialName("active_signals") val activeSignals: List<String>,
    @SerialName("update_time")    val updateTime: String,
)

@Serializable
data class Position(
    val code: String,
    val name: String,
    val direction: String,
    val entry: Double,
    val stop: Double,
    @SerialName("curr_price") val currPrice: Double = 0.0,
    @SerialName("pnl_pct")   val pnlPct: Double = 0.0,
    val status: String,
    @SerialName("entered_at") val enteredAt: String,
)

@Serializable
data class TradeSummary(
    val trades: Int,
    val win: Int,
    val lose: Int,
    @SerialName("total_pnl") val totalPnl: Double,
)

@Serializable
data class StockUpdate(
    val type: String,
    val timestamp: String,
    @SerialName("scan_n")  val scanN: Int,
    @SerialName("min_hit") val minHit: Int,
    val signals: List<StockSignal>,
    val positions: List<Position>,
    val alerts: List<String>,
    val summary: TradeSummary,
)
