import Foundation

enum StrategyEngine {

    private static let minAvgVol = 3000.0
    private static let volMult   = 1.5

    static func buildBase(_ bars: [HistoricalBar]) -> StrategyBase? {
        guard bars.count >= 20 else { return nil }

        let closes = bars.map(\.close)
        let opens  = bars.map(\.open)
        let highs  = bars.map(\.high)
        let lows   = bars.map(\.low)
        let volsK  = bars.map { Double($0.volumeLots) }

        let n = bars.count

        let avg5 = Array(volsK[(n - 6) ..< (n - 1)]).reduce(0, +) / 5.0
        guard avg5 >= minAvgVol else { return nil }

        let avg20Vol: Double = n >= 22
            ? Array(volsK[(n - 21) ..< (n - 1)]).reduce(0, +) / 20.0
            : avg5
        let volYesterday = volsK[n - 1]
        let volExpand = avg20Vol > 0 ? avg5 > avg20Vol * 1.2 : false
        let volShrink = avg20Vol > 0 ? avg5 < avg20Vol * 0.8 : false
        let tideShrink: Bool = n >= 6
            ? (1 ... 3).allSatisfy { j in volsK[n - j - 1] < volsK[n - j - 2] }
            : false

        let ma5  = Array(closes.suffix(5)).reduce(0, +) / 5.0
        let ma10 = closes.count >= 10 ? Array(closes.suffix(10)).reduce(0, +) / 10.0 : ma5
        let ma20 = Array(closes.suffix(20)).reduce(0, +) / 20.0

        let rsi    = TechnicalIndicators.rsi(closes)
        let rsiNow = rsi.now; let rsiPrv = rsi.prev

        let high5 = Array(highs[(n - 6) ..< (n - 1)]).max() ?? 0
        let low5  = Array(lows[(n - 6)  ..< (n - 1)]).min() ?? 0

        let threeUp = n >= 5 ? (1 ... 3).allSatisfy { j in closes[n-j-1] > opens[n-j-1] } : false
        let threeDn = n >= 5 ? (1 ... 3).allSatisfy { j in closes[n-j-1] < opens[n-j-1] } : false

        let prevClose = closes[n - 2]
        let prevOpen  = opens[n - 2]
        let prevBody  = abs(prevClose - prevOpen)
        let prevBull  = prevClose > prevOpen
        let prevBear  = prevClose < prevOpen

        let m = TechnicalIndicators.macd(closes)

        let bb = TechnicalIndicators.bollingerBands(closes)

        let kd = TechnicalIndicators.kd(highs, lows, closes)

        let wr = TechnicalIndicators.williamsR(highs, lows, closes)

        var starD1Body = 0.0; var starD1Mid = prevClose
        var starD1Bear = false; var starD1Bull = false
        if n >= 4 {
            let d1O = opens[n - 3]; let d1C = closes[n - 3]
            starD1Body = abs(d1C - d1O); starD1Mid = (d1C + d1O) / 2
            starD1Bear = d1C < d1O;      starD1Bull = d1C > d1O
        }
        let starD2Body = prevBody

        let solD1O = n >= 4 ? opens[n - 3]  : prevOpen
        let solD1C = n >= 4 ? closes[n - 3] : prevClose

        var ibPrev2H = 0.0; var ibPrev2L = 0.0; var ibIsInside = false
        if n >= 3 {
            ibPrev2H = highs[n - 3]; ibPrev2L = lows[n - 3]
            ibIsInside = highs[n - 2] < ibPrev2H && lows[n - 2] > ibPrev2L
        }

        return StrategyBase(
            avg5: avg5, avg20Vol: avg20Vol, volYesterday: volYesterday,
            volExpand: volExpand, volShrink: volShrink, tideShrink: tideShrink,
            ma5: ma5, ma10: ma10, ma20: ma20,
            rsiNow: rsiNow, rsiPrv: rsiPrv,
            high5: high5, low5: low5,
            threeUp: threeUp, threeDn: threeDn,
            prevClose: prevClose, prevOpen: prevOpen, prevBody: prevBody,
            prevBull: prevBull, prevBear: prevBear,
            macdT: m.macdT, macdP: m.macdP, msigT: m.msigT, msigP: m.msigP,
            bbUpperT: bb.upT, bbUpperP: bb.upP, bbLowerT: bb.loT, bbLowerP: bb.loP,
            kkT: kd.kkT, kdT: kd.kdT, kkP: kd.kkP, kdP: kd.kdP,
            wrT: wr.now, wrP: wr.prev,
            starD1Body: starD1Body, starD1Mid: starD1Mid,
            starD1Bear: starD1Bear, starD1Bull: starD1Bull,
            starD2Body: starD2Body,
            solD1O: solD1O, solD1C: solD1C,
            solD2O: prevOpen, solD2C: prevClose,
            ibPrev2H: ibPrev2H, ibPrev2L: ibPrev2L, ibIsInside: ibIsInside
        )
    }

    static func checkSignals(
        base: StrategyBase,
        price: Double,
        openP: Double,
        chgPct: Double,
        highP: Double = 0,
        lowP: Double  = 0,
        volMultiplier: Double = 1.5
    ) -> StrategySignals {
        guard price != 0 else { return .empty }

        let avg5     = base.avg5
        let volRatio = avg5 > 0 ? base.volYesterday / avg5 : 0.0
        let prevClose = base.prevClose
        let gapPct    = prevClose > 0 ? (openP - prevClose) / prevClose * 100 : 0.0

        var body = 0.0; var upper = 0.0; var lower = 0.0
        var isBull = false; var isBear = false
        var hammer = false; var shootStar = false
        var engulfBull = false; var engulfBear = false

        if openP > 0 {
            body   = abs(price - openP)
            isBull = price > openP
            isBear = price < openP
        }
        if highP > 0 && lowP > 0 && openP > 0 {
            upper = highP - max(price, openP)
            lower = min(price, openP) - lowP
            hammer     = body > 0 && lower >= 2*body && upper <= body && chgPct < 0 && base.prevBear
            shootStar  = body > 0 && upper >= 2*body && lower <= body && chgPct > 0 && base.prevBull
            engulfBull = isBull && base.prevBear && openP <= prevClose
                && price >= base.prevOpen && body > base.prevBody * 0.8
            engulfBear = isBear && base.prevBull && openP >= prevClose
                && price <= base.prevOpen && body > base.prevBody * 0.8
        }

        let d1Body = base.starD1Body; let d1Mid = base.starD1Mid
        let morningStar = base.starD1Bear && d1Body > 0
            && base.starD2Body < d1Body * 0.4
            && isBull && price > d1Mid && body >= d1Body * 0.5
        let eveningStar = base.starD1Bull && d1Body > 0
            && base.starD2Body < d1Body * 0.4
            && isBear && price < d1Mid && body >= d1Body * 0.5

        let threeSoldiers = base.solD1C > base.solD1O && base.solD2C > base.solD2O
            && isBull && base.solD2C > base.solD1C && price > base.solD2C
            && base.solD2O >= base.solD1O && openP >= base.solD2O
        let threeCrows = base.solD1C < base.solD1O && base.solD2C < base.solD2O
            && isBear && base.solD2C < base.solD1C && price < base.solD2C
            && base.solD2O <= base.solD1O && openP <= base.solD2O

        let insideBreakout  = base.ibIsInside && price > base.ibPrev2H
        let insideBreakdown = base.ibIsInside && price < base.ibPrev2L && volRatio >= volMultiplier

        let bias = base.ma20 > 0 ? (price - base.ma20) / base.ma20 * 100 : 0.0

        let maBullAlign = base.ma5 > base.ma10 && base.ma10 > base.ma20
        let maBearAlign = base.ma5 < base.ma10 && base.ma10 < base.ma20

        var s = StrategySignals()
        s.A  = base.ma5 > base.ma20 && chgPct > 0
        s.AS = base.ma5 < base.ma20 && volRatio >= volMultiplier && chgPct < 0
        s.B  = gapPct >= 2.0 && chgPct > 0
        s.BS = gapPct <= -2.0 && volRatio >= 1.3 && chgPct < 0
        s.C  = base.rsiPrv < 35 && base.rsiNow > base.rsiPrv && price > base.ma5
        s.CS = base.rsiPrv > 65 && base.rsiNow < base.rsiPrv && price < base.ma5
        s.D  = price > base.high5
        s.DS = price < base.low5 && volRatio >= volMultiplier
        s.E  = base.threeUp && maBullAlign && chgPct > 0
        s.ES = base.threeDn && maBearAlign && chgPct < 0
        s.F  = base.volExpand && chgPct > 0
        s.FS = base.volShrink && volRatio >= volMultiplier && chgPct < 0
        s.G  = base.tideShrink && chgPct > 0
        s.GS = base.tideShrink && volRatio >= volMultiplier && chgPct < 0
        s.H  = hammer;  s.HS = shootStar
        s.I  = engulfBull; s.IS = engulfBear
        s.J  = base.macdP < base.msigP && base.macdT > base.msigT && chgPct > 0
        s.JS = base.macdP > base.msigP && base.macdT < base.msigT && chgPct < 0
        s.K  = prevClose <= base.bbLowerP && price > base.bbLowerT
        s.KS = prevClose >= base.bbUpperP && price < base.bbUpperT
        s.L  = base.kkT < 30 && base.kkP < base.kdP && base.kkT > base.kdT
        s.LS = base.kkT > 70 && base.kkP > base.kdP && base.kkT < base.kdT
        s.M  = base.wrP < -80 && base.wrT > base.wrP && chgPct > 0
        s.MS = base.wrP > -20 && base.wrT < base.wrP && chgPct < 0
        s.N  = maBullAlign && prevClose < base.ma5 && price >= base.ma5
        s.NS = maBearAlign && prevClose > base.ma5 && price <= base.ma5
        s.O  = morningStar; s.OS = eveningStar
        s.P  = threeSoldiers; s.PS = threeCrows
        s.Q  = insideBreakout; s.QS = insideBreakdown
        s.R  = bias < -8.0 && chgPct > 0
        s.RS = bias > 8.0  && chgPct < 0
        s.B2 = gapPct >= 5.0 && volRatio >= 0.8 && chgPct > 0
        return s
    }
}
