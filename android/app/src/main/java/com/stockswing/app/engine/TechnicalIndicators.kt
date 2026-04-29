package com.stockswing.app.engine

/** 純技術指標計算，對應 Python daytrade_live.py / swing_trade_v2.py 的計算邏輯 */
object TechnicalIndicators {

    // ── 工具 ─────────────────────────────────────────────────────────

    /** 最後 n 期簡單移動平均 */
    fun sma(values: DoubleArray, period: Int): Double {
        if (values.isEmpty()) return 0.0
        return values.takeLast(minOf(period, values.size)).average()
    }

    /**
     * EMA，對應 pandas ewm(span=span, adjust=False)。
     * alpha = 2 / (span + 1)
     */
    fun ema(values: DoubleArray, span: Int): DoubleArray {
        if (values.isEmpty()) return DoubleArray(0)
        val alpha = 2.0 / (span + 1)
        val result = DoubleArray(values.size)
        result[0] = values[0]
        for (i in 1 until values.size) {
            result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1]
        }
        return result
    }

    // ── RSI (14) ─────────────────────────────────────────────────────

    /**
     * 對應 Python _calc_rsi()，使用最後 period 根 delta 的簡單平均（非 Wilder 平滑）。
     * 回傳 [rsiNow, rsiPrev]
     */
    fun rsi(closes: DoubleArray, period: Int = 14): DoubleArray {
        fun calcAt(arr: DoubleArray): Double {
            if (arr.size < period + 1) return 50.0
            val deltas = DoubleArray(arr.size - 1) { arr[it + 1] - arr[it] }
            val recent = deltas.takeLast(period)
            val avgGain = recent.map { maxOf(it, 0.0) }.average()
            val avgLoss = recent.map { maxOf(-it, 0.0) }.average()
            if (avgLoss == 0.0) return 100.0
            return 100.0 - 100.0 / (1.0 + avgGain / avgLoss)
        }
        val now  = calcAt(closes)
        val prev = if (closes.size > 1) calcAt(closes.copyOf(closes.size - 1)) else now
        return doubleArrayOf(now, prev)
    }

    // ── MACD (12/26/9) ────────────────────────────────────────────────

    /**
     * 回傳 [macdNow, macdPrev, signalNow, signalPrev]
     * 對應 Python 的 ema12 - ema26 (MACD line) 與 ewm(span=9) signal line
     */
    fun macd(closes: DoubleArray, fast: Int = 12, slow: Int = 26, signal: Int = 9): DoubleArray {
        val ema12 = ema(closes, fast)
        val ema26 = ema(closes, slow)
        val macdLine = DoubleArray(closes.size) { ema12[it] - ema26[it] }
        val sigLine  = ema(macdLine, signal)
        val n = closes.size
        return doubleArrayOf(
            macdLine[n - 1],
            if (n >= 2) macdLine[n - 2] else macdLine[n - 1],
            sigLine[n - 1],
            if (n >= 2) sigLine[n - 2] else sigLine[n - 1],
        )
    }

    // ── Bollinger Bands (20, 2) ────────────────────────────────────────

    /**
     * 回傳 [upperNow, lowerNow, upperPrev, lowerPrev]
     * 使用 rolling std（樣本標準差）
     */
    fun bollingerBands(closes: DoubleArray, period: Int = 20, k: Double = 2.0): DoubleArray {
        fun bbAt(endIdx: Int): Pair<Double, Double> {
            val n = minOf(period, endIdx + 1)
            val slice = closes.copyOfRange(endIdx - n + 1, endIdx + 1)
            val mean  = slice.average()
            // ddof=1 sample std, matching pandas rolling().std()
            val variance = if (slice.size > 1) slice.sumOf { (it - mean) * (it - mean) } / (slice.size - 1) else 0.0
            val std  = Math.sqrt(variance)
            return Pair(mean + k * std, mean - k * std)
        }
        val n = closes.size
        if (n < period) {
            val c = closes.last()
            return doubleArrayOf(c, c, c, c)
        }
        val (upT, loT) = bbAt(n - 1)
        val (upP, loP) = if (n >= 2) bbAt(n - 2) else Pair(upT, loT)
        return doubleArrayOf(upT, loT, upP, loP)
    }

    // ── KD 隨機指標 (9/3/3) ────────────────────────────────────────────

    /**
     * 對應 Python ewm(com=2) = alpha=1/3。
     * 回傳 [kNow, dNow, kPrev, dPrev]
     */
    fun kd(highs: DoubleArray, lows: DoubleArray, closes: DoubleArray, period: Int = 9): DoubleArray {
        val n = closes.size
        if (n < period) return doubleArrayOf(50.0, 50.0, 50.0, 50.0)

        val rsv = DoubleArray(n) { i ->
            val start = maxOf(0, i - period + 1)
            val hi9 = highs.copyOfRange(start, i + 1).max()
            val lo9 = lows.copyOfRange(start, i + 1).min()
            if (hi9 == lo9) 50.0 else (closes[i] - lo9) / (hi9 - lo9) * 100.0
        }

        val alpha = 1.0 / 3.0
        val kLine = DoubleArray(n).also { it[0] = rsv[0] }
        val dLine = DoubleArray(n).also { it[0] = rsv[0] }
        for (i in 1 until n) {
            kLine[i] = alpha * rsv[i]   + (1 - alpha) * kLine[i - 1]
            dLine[i] = alpha * kLine[i] + (1 - alpha) * dLine[i - 1]
        }

        return doubleArrayOf(
            kLine[n - 1], dLine[n - 1],
            if (n >= 2) kLine[n - 2] else kLine[n - 1],
            if (n >= 2) dLine[n - 2] else dLine[n - 1],
        )
    }

    // ── Williams %R (14) ─────────────────────────────────────────────

    /** 回傳 [wrNow, wrPrev] */
    fun williamsR(highs: DoubleArray, lows: DoubleArray, closes: DoubleArray, period: Int = 14): DoubleArray {
        val n = closes.size
        if (n < period) return doubleArrayOf(-50.0, -50.0)

        fun wrAt(endIdx: Int): Double {
            val start = maxOf(0, endIdx - period + 1)
            val hi14 = highs.copyOfRange(start, endIdx + 1).max()
            val lo14 = lows.copyOfRange(start, endIdx + 1).min()
            return if (hi14 == lo14) -50.0
            else (hi14 - closes[endIdx]) / (hi14 - lo14) * -100.0
        }

        return doubleArrayOf(wrAt(n - 1), if (n >= 2) wrAt(n - 2) else wrAt(n - 1))
    }

    // ── 工具擴充 ─────────────────────────────────────────────────────

    private fun DoubleArray.takeLast(n: Int): List<Double> =
        toList().takeLast(n)

    private fun DoubleArray.max(): Double = maxOrNull() ?: 0.0
    private fun DoubleArray.min(): Double = minOrNull() ?: 0.0
}
