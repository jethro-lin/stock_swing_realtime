package com.stockswing.app.model

import java.time.LocalDate

/** 單日 OHLCV，volume 單位：張（1000 股） */
data class HistoricalBar(
    val date: LocalDate,
    val open: Double,
    val high: Double,
    val low: Double,
    val close: Double,
    val volumeLots: Long,
)
