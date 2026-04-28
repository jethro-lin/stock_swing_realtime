package com.stockswing.app.engine

import com.stockswing.app.model.HistoricalBar
import com.stockswing.app.model.StrategyBase
import com.stockswing.app.model.StrategySignals
import kotlin.math.abs

/**
 * 對應 Python daytrade_live.py 的 build_base() + check_signals()。
 * 37 個策略（A–R + B2），多空各自命中計算。
 */
object StrategyEngine {

    private const val MIN_AVG_VOL = 3000.0  // 5日均量最低門檻（張）
    private const val VOL_MULT    = 1.5

    // ── buildBase ─────────────────────────────────────────────────────

    /**
     * 用歷史日K預算所有技術指標基準。
     * 回傳 null 表示資料不足或均量太低（流動性差）。
     */
    fun buildBase(bars: List<HistoricalBar>): StrategyBase? {
        if (bars.size < 20) return null

        val closes = bars.map { it.close }.toDoubleArray()
        val opens  = bars.map { it.open  }.toDoubleArray()
        val highs  = bars.map { it.high  }.toDoubleArray()
        val lows   = bars.map { it.low   }.toDoubleArray()
        val volsK  = bars.map { it.volumeLots.toDouble() }.toDoubleArray()

        val n = bars.size

        // ── 量 ────────────────────────────────────────────────────────
        val avg5       = volsK.takeLast(5).average()
        if (avg5 < MIN_AVG_VOL) return null

        val avg20Vol   = if (n >= 22) volsK.copyOfRange(n - 21, n - 1).average() else avg5
        val volYesterday = volsK[n - 1]
        val volExpand  = if (avg20Vol > 0) avg5 > avg20Vol * 1.2 else false
        val volShrink  = if (avg20Vol > 0) avg5 < avg20Vol * 0.8 else false
        val tideShrink = if (n >= 5) (1..3).all { j -> volsK[n - j - 1] < volsK[n - j - 2] } else false

        // ── 均線 ─────────────────────────────────────────────────────
        val ma5  = closes.takeLast(5).average()
        val ma10 = if (closes.size >= 10) closes.takeLast(10).average() else ma5
        val ma20 = closes.takeLast(20).average()

        // ── RSI ──────────────────────────────────────────────────────
        val rsiArr = TechnicalIndicators.rsi(closes)
        val rsiNow = rsiArr[0]; val rsiPrv = rsiArr[1]

        // ── 5日高低 ──────────────────────────────────────────────────
        val high5 = highs.takeLast(5).max()
        val low5  = lows.takeLast(5).min()

        // ── 三連漲/跌 ────────────────────────────────────────────────
        val threeUp = if (n >= 4) (1..3).all { j -> closes[n-j-1] > opens[n-j-1] } else false
        val threeDn = if (n >= 4) (1..3).all { j -> closes[n-j-1] < opens[n-j-1] } else false

        // ── 昨日K棒 ─────────────────────────────────────────────────
        val prevClose = closes[n - 1]
        val prevOpen  = opens[n - 1]
        val prevBody  = abs(prevClose - prevOpen)
        val prevBull  = prevClose > prevOpen
        val prevBear  = prevClose < prevOpen

        // ── MACD ─────────────────────────────────────────────────────
        val m = TechnicalIndicators.macd(closes)
        val macdT = m[0]; val macdP = m[1]; val msigT = m[2]; val msigP = m[3]

        // ── Bollinger Bands ──────────────────────────────────────────
        val bb = TechnicalIndicators.bollingerBands(closes)
        val bbUpperT = bb[0]; val bbLowerT = bb[1]
        val bbUpperP = bb[2]; val bbLowerP = bb[3]

        // ── KD ───────────────────────────────────────────────────────
        val kd = TechnicalIndicators.kd(highs, lows, closes)
        val kkT = kd[0]; val kdT = kd[1]; val kkP = kd[2]; val kdP = kd[3]

        // ── Williams %R ──────────────────────────────────────────────
        val wr = TechnicalIndicators.williamsR(highs, lows, closes)
        val wrT = wr[0]; val wrP = wr[1]

        // ── 晨星/黃昏之星（需要前兩日）───────────────────────────────
        val starD1Body: Double; val starD1Mid: Double
        val starD1Bear: Boolean; val starD1Bull: Boolean
        if (n >= 3) {
            val d1O = opens[n - 2]; val d1C = closes[n - 2]
            starD1Body = abs(d1C - d1O); starD1Mid = (d1C + d1O) / 2
            starD1Bear = d1C < d1O;      starD1Bull = d1C > d1O
        } else {
            starD1Body = 0.0; starD1Mid = prevClose
            starD1Bear = false; starD1Bull = false
        }
        val starD2Body = prevBody

        // ── 紅三兵/黑三兵 ────────────────────────────────────────────
        val solD1O = if (n >= 3) opens[n - 2] else prevOpen
        val solD1C = if (n >= 3) closes[n - 2] else prevClose

        // ── Inside Bar ───────────────────────────────────────────────
        val ibPrev2H: Double; val ibPrev2L: Double; val ibIsInside: Boolean
        if (n >= 2) {
            ibPrev2H = highs[n - 2]; ibPrev2L = lows[n - 2]
            ibIsInside = highs[n - 1] < ibPrev2H && lows[n - 1] > ibPrev2L
        } else {
            ibPrev2H = 0.0; ibPrev2L = 0.0; ibIsInside = false
        }

        return StrategyBase(
            avg5 = avg5, avg20Vol = avg20Vol, volYesterday = volYesterday,
            volExpand = volExpand, volShrink = volShrink, tideShrink = tideShrink,
            ma5 = ma5, ma10 = ma10, ma20 = ma20,
            rsiNow = rsiNow, rsiPrv = rsiPrv,
            high5 = high5, low5 = low5,
            threeUp = threeUp, threeDn = threeDn,
            prevClose = prevClose, prevOpen = prevOpen, prevBody = prevBody,
            prevBull = prevBull, prevBear = prevBear,
            macdT = macdT, macdP = macdP, msigT = msigT, msigP = msigP,
            bbUpperT = bbUpperT, bbUpperP = bbUpperP,
            bbLowerT = bbLowerT, bbLowerP = bbLowerP,
            kkT = kkT, kdT = kdT, kkP = kkP, kdP = kdP,
            wrT = wrT, wrP = wrP,
            starD1Body = starD1Body, starD1Mid = starD1Mid,
            starD1Bear = starD1Bear, starD1Bull = starD1Bull,
            starD2Body = starD2Body,
            solD1O = solD1O, solD1C = solD1C,
            solD2O = prevOpen, solD2C = prevClose,
            ibPrev2H = ibPrev2H, ibPrev2L = ibPrev2L, ibIsInside = ibIsInside,
        )
    }

    // ── checkSignals ──────────────────────────────────────────────────

    /**
     * 盤中策略判斷，所有指標基準來自 buildBase()，盤中不重算。
     * price / openP / chgPct / highP / lowP 為即時資料。
     */
    fun checkSignals(
        base: StrategyBase,
        price: Double,
        openP: Double,
        chgPct: Double,
        highP: Double  = 0.0,
        lowP: Double   = 0.0,
        volMult: Double = VOL_MULT,
    ): StrategySignals {
        if (price == 0.0) return StrategySignals.empty()

        val avg5       = base.avg5
        val volRatio   = if (avg5 > 0) base.volYesterday / avg5 else 0.0
        val prevClose  = base.prevClose
        val gapPct     = if (prevClose > 0) (openP - prevClose) / prevClose * 100.0 else 0.0

        // ── K棒型態 ───────────────────────────────────────────────────
        var body = 0.0; var upper = 0.0; var lower = 0.0
        var isBull = false; var isBear = false
        var hammer = false; var shootStar = false
        var engulfBull = false; var engulfBear = false

        if (openP > 0) {
            body   = abs(price - openP)
            isBull = price > openP
            isBear = price < openP
        }
        if (highP > 0 && lowP > 0 && openP > 0) {
            upper = highP - maxOf(price, openP)
            lower = minOf(price, openP) - lowP
            hammer     = body > 0 && lower >= 2*body && upper <= body && chgPct < 0 && base.prevBear
            shootStar  = body > 0 && upper >= 2*body && lower <= body && chgPct > 0 && base.prevBull
            engulfBull = isBull && base.prevBear && openP <= prevClose
                        && price >= base.prevOpen && body > base.prevBody * 0.8
            engulfBear = isBear && base.prevBull && openP >= prevClose
                        && price <= base.prevOpen && body > base.prevBody * 0.8
        }

        // ── 晨星/黃昏之星 ─────────────────────────────────────────────
        val d1Body = base.starD1Body; val d1Mid = base.starD1Mid
        val morningStar = base.starD1Bear && d1Body > 0
                && base.starD2Body < d1Body * 0.4
                && isBull && price > d1Mid && body >= d1Body * 0.5
        val eveningStar = base.starD1Bull && d1Body > 0
                && base.starD2Body < d1Body * 0.4
                && isBear && price < d1Mid && body >= d1Body * 0.5

        // ── 紅三兵/黑三兵 ─────────────────────────────────────────────
        val threeSoldiers = base.solD1C > base.solD1O && base.solD2C > base.solD2O
                && isBull && base.solD2C > base.solD1C && price > base.solD2C
                && base.solD2O >= base.solD1O && openP >= base.solD2O
        val threeCrows    = base.solD1C < base.solD1O && base.solD2C < base.solD2O
                && isBear && base.solD2C < base.solD1C && price < base.solD2C
                && base.solD2O <= base.solD1O && openP <= base.solD2O

        // ── Inside Bar ────────────────────────────────────────────────
        val insideBreakout  = base.ibIsInside && price > base.ibPrev2H
        val insideBreakdown = base.ibIsInside && price < base.ibPrev2L && volRatio >= volMult

        // ── BIAS ──────────────────────────────────────────────────────
        val bias = if (base.ma20 > 0) (price - base.ma20) / base.ma20 * 100.0 else 0.0

        // ── MA 排列 ────────────────────────────────────────────────────
        val maBullAlign = base.ma5 > base.ma10 && base.ma10 > base.ma20
        val maBearAlign = base.ma5 < base.ma10 && base.ma10 < base.ma20

        return StrategySignals(
            // A/AS — 均線突破/死亡（多方移除爆量條件）
            A  = base.ma5 > base.ma20 && chgPct > 0,
            AS = base.ma5 < base.ma20 && volRatio >= volMult && chgPct < 0,
            // B/BS — 跳空缺口
            B  = gapPct >= 2.0 && chgPct > 0,
            BS = gapPct <= -2.0 && volRatio >= 1.3 && chgPct < 0,
            // C/CS — RSI 超賣/超買
            C  = base.rsiPrv < 35 && base.rsiNow > base.rsiPrv && price > base.ma5,
            CS = base.rsiPrv > 65 && base.rsiNow < base.rsiPrv && price < base.ma5,
            // D/DS — 突破/跌破前高低
            D  = price > base.high5,
            DS = price < base.low5 && volRatio >= volMult,
            // E/ES — 強勢連漲/弱勢連跌
            E  = base.threeUp && maBullAlign && chgPct > 0,
            ES = base.threeDn && maBearAlign && chgPct < 0,
            // F/FS — 均量擴張/萎縮
            F  = base.volExpand && chgPct > 0,
            FS = base.volShrink && volRatio >= volMult && chgPct < 0,
            // G/GS — 縮量後上漲/爆跌
            G  = base.tideShrink && chgPct > 0,
            GS = base.tideShrink && volRatio >= volMult && chgPct < 0,
            // H/HS — 鎚子K/射擊之星
            H  = hammer, HS = shootStar,
            // I/IS — 吞噬陽線/陰線
            I  = engulfBull, IS = engulfBear,
            // J/JS — MACD 黃金/死亡交叉
            J  = base.macdP < base.msigP && base.macdT > base.msigT && chgPct > 0,
            JS = base.macdP > base.msigP && base.macdT < base.msigT && chgPct < 0,
            // K/KS — 布林下軌反彈/上軌反壓
            K  = prevClose <= base.bbLowerP && price > base.bbLowerT,
            KS = prevClose >= base.bbUpperP && price < base.bbUpperT,
            // L/LS — KD 超賣/超買交叉
            L  = base.kkT < 30 && base.kkP < base.kdP && base.kkT > base.kdT,
            LS = base.kkT > 70 && base.kkP > base.kdP && base.kkT < base.kdT,
            // M/MS — Williams %R 超賣/超買
            M  = base.wrP < -80 && base.wrT > base.wrP && chgPct > 0,
            MS = base.wrP > -20 && base.wrT < base.wrP && chgPct < 0,
            // N/NS — MA 排列回測站回/跌破
            N  = maBullAlign && prevClose < base.ma5 && price >= base.ma5,
            NS = maBearAlign && prevClose > base.ma5 && price <= base.ma5,
            // O/OS — 晨星/黃昏之星
            O = morningStar, OS = eveningStar,
            // P/PS — 紅三兵/黑三兵
            P = threeSoldiers, PS = threeCrows,
            // Q/QS — Inside Bar 突破/跌破
            Q = insideBreakout, QS = insideBreakdown,
            // R/RS — BIAS 超跌/超漲
            R  = bias < -8.0 && chgPct > 0,
            RS = bias > 8.0  && chgPct < 0,
            // B2 — 大跳空(≥5%) + 量比≥0.8x
            B2 = gapPct >= 5.0 && volRatio >= 0.8 && chgPct > 0,
        )
    }

    // ── helpers ───────────────────────────────────────────────────────

    private fun DoubleArray.takeLast(n: Int): List<Double> = toList().takeLast(n)
    private fun DoubleArray.max(): Double = maxOrNull() ?: 0.0
    private fun DoubleArray.min(): Double = minOrNull() ?: 0.0
}
