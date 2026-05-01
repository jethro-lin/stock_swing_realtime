import Foundation

enum TechnicalIndicators {

    static func sma(_ values: [Double], period: Int) -> Double {
        guard !values.isEmpty else { return 0 }
        let slice = Array(values.suffix(min(period, values.count)))
        return slice.reduce(0, +) / Double(slice.count)
    }

    // EMA: alpha = 2 / (span + 1), matching pandas ewm(span=, adjust=False)
    static func ema(_ values: [Double], span: Int) -> [Double] {
        guard !values.isEmpty else { return [] }
        let alpha = 2.0 / Double(span + 1)
        var result = [Double](repeating: 0, count: values.count)
        result[0] = values[0]
        for i in 1 ..< values.count {
            result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1]
        }
        return result
    }

    // RSI(14): simple average of gains/losses over last `period` deltas
    // Returns [rsiNow, rsiPrev]
    static func rsi(_ closes: [Double], period: Int = 14) -> (now: Double, prev: Double) {
        func calcAt(_ arr: [Double]) -> Double {
            guard arr.count >= period + 1 else { return 50 }
            let deltas = zip(arr.dropFirst(), arr).map { $0 - $1 }
            let recent = Array(deltas.suffix(period))
            let avgGain = recent.map { max($0, 0) }.reduce(0, +) / Double(period)
            let avgLoss = recent.map { max(-$0, 0) }.reduce(0, +) / Double(period)
            if avgLoss == 0 { return 100 }
            return 100 - 100 / (1 + avgGain / avgLoss)
        }
        let now  = calcAt(closes)
        let prev = closes.count > 1 ? calcAt(Array(closes.dropLast())) : now
        return (now, prev)
    }

    // MACD(12/26/9): returns (macdNow, macdPrev, signalNow, signalPrev)
    static func macd(_ closes: [Double], fast: Int = 12, slow: Int = 26, signal: Int = 9)
        -> (macdT: Double, macdP: Double, msigT: Double, msigP: Double)
    {
        let ema12 = ema(closes, span: fast)
        let ema26 = ema(closes, span: slow)
        let macdLine = zip(ema12, ema26).map { $0 - $1 }
        let sigLine  = ema(macdLine, span: signal)
        let n = closes.count
        return (
            macdLine[n - 1],
            n >= 2 ? macdLine[n - 2] : macdLine[n - 1],
            sigLine[n - 1],
            n >= 2 ? sigLine[n - 2] : sigLine[n - 1]
        )
    }

    // Bollinger Bands(20, 2): returns (upperNow, lowerNow, upperPrev, lowerPrev)
    static func bollingerBands(_ closes: [Double], period: Int = 20, k: Double = 2.0)
        -> (upT: Double, loT: Double, upP: Double, loP: Double)
    {
        func bbAt(_ endIdx: Int) -> (Double, Double) {
            let n = min(period, endIdx + 1)
            let slice = Array(closes[(endIdx - n + 1) ... endIdx])
            let mean = slice.reduce(0, +) / Double(slice.count)
            let variance = slice.count > 1
                ? slice.map { ($0 - mean) * ($0 - mean) }.reduce(0, +) / Double(slice.count - 1)
                : 0.0
            let std = variance.squareRoot()
            return (mean + k * std, mean - k * std)
        }
        let n = closes.count
        guard n >= period else {
            let c = closes.last ?? 0
            return (c, c, c, c)
        }
        let (upT, loT) = bbAt(n - 1)
        let (upP, loP) = n >= 2 ? bbAt(n - 2) : (upT, loT)
        return (upT, loT, upP, loP)
    }

    // KD(9/3/3): returns (kNow, dNow, kPrev, dPrev)
    static func kd(_ highs: [Double], _ lows: [Double], _ closes: [Double], period: Int = 9)
        -> (kkT: Double, kdT: Double, kkP: Double, kdP: Double)
    {
        let n = closes.count
        guard n >= period else { return (50, 50, 50, 50) }
        var rsv = [Double](repeating: 0, count: n)
        for i in 0 ..< n {
            let start = max(0, i - period + 1)
            let hi9 = highs[start ... i].max() ?? 0
            let lo9 = lows[start ... i].min() ?? 0
            rsv[i] = hi9 == lo9 ? 50 : (closes[i] - lo9) / (hi9 - lo9) * 100
        }
        let alpha = 1.0 / 3.0
        var kLine = [Double](repeating: 0, count: n)
        var dLine = [Double](repeating: 0, count: n)
        kLine[0] = rsv[0]; dLine[0] = rsv[0]
        for i in 1 ..< n {
            kLine[i] = alpha * rsv[i]    + (1 - alpha) * kLine[i - 1]
            dLine[i] = alpha * kLine[i]  + (1 - alpha) * dLine[i - 1]
        }
        return (
            kLine[n - 1], dLine[n - 1],
            n >= 2 ? kLine[n - 2] : kLine[n - 1],
            n >= 2 ? dLine[n - 2] : dLine[n - 1]
        )
    }

    // Williams %R(14): returns (wrNow, wrPrev)
    static func williamsR(_ highs: [Double], _ lows: [Double], _ closes: [Double], period: Int = 14)
        -> (now: Double, prev: Double)
    {
        let n = closes.count
        guard n >= period else { return (-50, -50) }
        func wrAt(_ endIdx: Int) -> Double {
            let start = max(0, endIdx - period + 1)
            let hi14 = highs[start ... endIdx].max() ?? 0
            let lo14 = lows[start ... endIdx].min() ?? 0
            return hi14 == lo14 ? -50 : (hi14 - closes[endIdx]) / (hi14 - lo14) * -100
        }
        return (wrAt(n - 1), n >= 2 ? wrAt(n - 2) : wrAt(n - 1))
    }
}
