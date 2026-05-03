package com.stockswing.app.model

/** 對應 Python build_base() 的回傳值，以昨日收盤資料預算一次 */
data class StrategyBase(
    // 量
    val avg5: Double,
    val avg20Vol: Double,
    val volYesterday: Double,
    val volExpand: Boolean,
    val volShrink: Boolean,
    val tideShrink: Boolean,
    // 均線
    val ma5: Double,
    val ma10: Double,
    val ma20: Double,
    // 均線方向：1=上升, -1=下降, 0=持平
    val ma5Dir:  Int,
    val ma10Dir: Int,
    val ma20Dir: Int,
    // RSI
    val rsiNow: Double,
    val rsiPrv: Double,
    // 高低
    val high5: Double,
    val low5: Double,
    // 連漲/連跌
    val threeUp: Boolean,
    val threeDn: Boolean,
    // 昨日K棒
    val prevClose: Double,
    val prevOpen: Double,
    val prevBody: Double,
    val prevBull: Boolean,
    val prevBear: Boolean,
    // MACD (12/26/9)
    val macdT: Double, val macdP: Double,
    val msigT: Double, val msigP: Double,
    // Bollinger (20, 2)
    val bbUpperT: Double, val bbUpperP: Double,
    val bbLowerT: Double, val bbLowerP: Double,
    // KD (9/3/3)
    val kkT: Double, val kdT: Double,
    val kkP: Double, val kdP: Double,
    // Williams %R (14)
    val wrT: Double, val wrP: Double,
    // 晨星/黃昏之星
    val starD1Body: Double, val starD1Mid: Double,
    val starD1Bear: Boolean, val starD1Bull: Boolean,
    val starD2Body: Double,
    // 紅三兵/黑三兵
    val solD1O: Double, val solD1C: Double,
    val solD2O: Double, val solD2C: Double,
    // Inside Bar
    val ibPrev2H: Double, val ibPrev2L: Double,
    val ibIsInside: Boolean,
)
