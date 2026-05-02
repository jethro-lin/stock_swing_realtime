import SwiftUI

private let chartGreen  = Color(red: 0,     green: 0.784, blue: 0.325)
private let chartRed    = Color(red: 0.835, green: 0,     blue: 0)
private let chartBlue   = Color(red: 0.129, green: 0.588, blue: 0.953)
private let chartOrange = Color(red: 1,     green: 0.596, blue: 0)

// MARK: - Hit direction helpers

private extension Dictionary where Key == String, Value == [String] {
    var isLong: Bool {
        if let ch = self["custom"] { return ch.contains { !$0.hasSuffix("S") || $0 == "B2" } }
        return keys.contains { $0 != Preset.short3Lean.rawValue }
    }
    var isShort: Bool {
        if let ch = self["custom"] { return ch.contains { $0.hasSuffix("S") && $0 != "B2" } }
        return self[Preset.short3Lean.rawValue] != nil
    }
}

// MARK: - Sheet wrapper

struct KBarChartSheet: View {
    @EnvironmentObject var vm: StockViewModel
    let result: SignalResult

    var body: some View {
        NavigationView {
            ScrollView {
                VStack(alignment: .leading, spacing: 0) {
                    // Header
                    HStack(alignment: .bottom) {
                        VStack(alignment: .leading, spacing: 2) {
                            Text("\(result.code)  \(result.name)")
                                .font(.system(size: 16, weight: .bold))
                            let pctColor = result.quote.chgPct >= 0 ? chartGreen : chartRed
                            Text(String(format: "%.2f  %+.2f%%", result.quote.price, result.quote.chgPct))
                                .font(.system(size: 14, weight: .semibold))
                                .foregroundColor(pctColor)
                        }
                        Spacer()
                        HStack(spacing: 10) {
                            MALegend(label: "MA5",  color: chartGreen)
                            MALegend(label: "MA10", color: chartBlue)
                            MALegend(label: "MA20", color: chartOrange)
                        }
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 12)

                    Divider()

                    if vm.chartBars.isEmpty {
                        HStack { Spacer(); ProgressView(); Spacer() }
                            .frame(height: 280)
                    } else {
                        CandlestickChart(bars: vm.chartBars, hitPresets: result.hitPresets)
                            .frame(height: 300)
                            .padding(.horizontal, 8)
                            .padding(.vertical, 12)

                        Divider()

                        // Signal chips
                        VStack(alignment: .leading, spacing: 6) {
                            Text("命中策略")
                                .font(.system(size: 11))
                                .foregroundColor(.gray)

                            if let custom = result.hitPresets["custom"] {
                                let longHits  = custom.filter { !$0.hasSuffix("S") || $0 == "B2" }
                                let shortHits = custom.filter { $0.hasSuffix("S") && $0 != "B2" }
                                if !longHits.isEmpty  { SignalChipRow(label: "多方訊號", chips: longHits,  color: chartGreen) }
                                if !shortHits.isEmpty { SignalChipRow(label: "空方訊號", chips: shortHits, color: chartRed) }
                            } else {
                                ForEach(result.hitPresets.sorted(by: { $0.key < $1.key }), id: \.key) { key, combos in
                                    let preset = Preset(rawValue: key)
                                    let color  = preset == .short3Lean ? chartRed : chartGreen
                                    let label  = preset?.label ?? key
                                    SignalChipRow(label: label, chips: combos, color: color)
                                }
                            }
                        }
                        .padding(.horizontal, 16)
                        .padding(.vertical, 10)
                    }
                }
            }
            .navigationTitle("日K圖")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("關閉") { vm.closeChart() }
                }
            }
        }
    }
}

// MARK: - Signal chip row

private struct SignalChipRow: View {
    let label: String
    let chips: [String]
    let color: Color

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 4) {
                Text(label)
                    .font(.system(size: 11, weight: .semibold))
                    .foregroundColor(color.opacity(0.7))
                    .frame(minWidth: 52, alignment: .leading)
                ForEach(chips, id: \.self) { chip in
                    Text(chip)
                        .font(.system(size: 11, weight: .medium))
                        .foregroundColor(color)
                        .padding(.horizontal, 6)
                        .padding(.vertical, 2)
                        .background(color.opacity(0.10))
                        .cornerRadius(4)
                }
            }
        }
    }
}

// MARK: - MA legend

private struct MALegend: View {
    let label: String
    let color: Color
    var body: some View {
        HStack(spacing: 3) {
            Rectangle().fill(color).frame(width: 14, height: 2)
            Text(label).font(.system(size: 10)).foregroundColor(color)
        }
    }
}

// MARK: - Candlestick chart

struct CandlestickChart: View {
    let bars: [HistoricalBar]
    var hitPresets: [String: [String]] = [:]

    private var display: [HistoricalBar] { Array(bars.suffix(60)) }

    var body: some View {
        GeometryReader { _ in
            Canvas { ctx, size in
                let n = display.count
                guard n > 0 else { return }

                let isLong  = hitPresets.isLong
                let isShort = hitPresets.isShort

                // Layout
                let priceH = size.height * 0.76
                let volH   = size.height * 0.18
                let gapH   = size.height * 0.06
                let volTop = priceH + gapH

                let candleW = size.width / CGFloat(n)
                let bodyW   = max(candleW * 0.65, 2)

                // Ranges
                let maxHigh   = display.map(\.high).max() ?? 0
                let minLow    = display.map(\.low).min() ?? 0
                let pad       = (maxHigh - minLow) * 0.08
                let priceMax  = maxHigh + pad
                let priceMin  = minLow  - pad
                let priceSpan = max(priceMax - priceMin, 0.01)

                func py(_ p: Double) -> CGFloat {
                    CGFloat(priceH * (1.0 - (p - priceMin) / priceSpan))
                }
                let maxVol = display.map(\.volumeLots).max().map(Double.init) ?? 1
                func vy(_ v: Int64) -> CGFloat {
                    CGFloat(volTop + volH * (1.0 - Double(v) / maxVol))
                }

                // Signal day highlight (last candle)
                let signalX = CGFloat(n - 1) * candleW
                ctx.fill(
                    Path(CGRect(x: signalX, y: 0, width: candleW, height: priceH)),
                    with: .color(Color(red: 1, green: 0.843, blue: 0).opacity(0.12))
                )

                // Grid lines
                for k in 0 ... 4 {
                    let y = priceH * Double(k) / 4
                    ctx.stroke(
                        Path { p in p.move(to: .init(x: 0, y: y)); p.addLine(to: .init(x: size.width, y: y)) },
                        with: .color(.gray.opacity(0.2)), lineWidth: 0.5
                    )
                }

                // Price labels
                for k in 0 ... 4 {
                    let p = priceMax - (priceMax - priceMin) * Double(k) / 4
                    let y = priceH * Double(k) / 4
                    ctx.draw(
                        Text(String(format: "%.1f", p))
                            .font(.system(size: 9))
                            .foregroundColor(.gray.opacity(0.7)),
                        at: CGPoint(x: size.width - 2, y: y - 1),
                        anchor: .bottomTrailing
                    )
                }

                // Candles + volume
                for (i, bar) in display.enumerated() {
                    let cx    = CGFloat(i) * candleW + candleW / 2
                    let isUp  = bar.close >= bar.open
                    let color: GraphicsContext.Shading       = isUp ? .color(chartGreen)            : .color(chartRed)
                    let colorFaint: GraphicsContext.Shading  = isUp ? .color(chartGreen.opacity(0.45)) : .color(chartRed.opacity(0.45))

                    ctx.stroke(
                        Path { p in p.move(to: .init(x: cx, y: py(bar.high))); p.addLine(to: .init(x: cx, y: py(bar.low))) },
                        with: color, lineWidth: 1
                    )
                    let top = py(max(bar.open, bar.close))
                    let bot = py(min(bar.open, bar.close))
                    ctx.fill(Path(CGRect(x: cx - bodyW/2, y: top, width: bodyW, height: max(bot - top, 1))), with: color)
                    ctx.fill(
                        Path(CGRect(x: cx - bodyW/2, y: vy(bar.volumeLots),
                                    width: bodyW, height: CGFloat(volTop + volH) - vy(bar.volumeLots))),
                        with: colorFaint
                    )
                }

                // MA lines
                let closes = display.map(\.close)
                drawMA(ctx, values: rollingAvg(closes, 5),  candleW: candleW, py: py, color: chartGreen)
                drawMA(ctx, values: rollingAvg(closes, 10), candleW: candleW, py: py, color: chartBlue)
                drawMA(ctx, values: rollingAvg(closes, 20), candleW: candleW, py: py, color: chartOrange)

                // Signal direction triangles on last candle
                let signalBar = display.last!
                let scx       = CGFloat(n - 1) * candleW + candleW / 2
                let triW      = min(max(candleW * 0.7, 5), 10)
                let triH      = triW * 0.8

                if isLong {
                    let tipY = py(signalBar.high) - 4
                    ctx.fill(
                        Path { p in
                            p.move(to: .init(x: scx, y: tipY - triH))
                            p.addLine(to: .init(x: scx - triW/2, y: tipY))
                            p.addLine(to: .init(x: scx + triW/2, y: tipY))
                            p.closeSubpath()
                        },
                        with: .color(chartGreen)
                    )
                }
                if isShort {
                    let tipY = py(signalBar.low) + 4
                    ctx.fill(
                        Path { p in
                            p.move(to: .init(x: scx, y: tipY + triH))
                            p.addLine(to: .init(x: scx - triW/2, y: tipY))
                            p.addLine(to: .init(x: scx + triW/2, y: tipY))
                            p.closeSubpath()
                        },
                        with: .color(chartRed)
                    )
                }
            }
        }
    }

    private func drawMA(_ ctx: GraphicsContext, values: [Double], candleW: CGFloat,
                        py: (Double) -> CGFloat, color: Color) {
        var path = Path()
        for (i, v) in values.enumerated() {
            let pt = CGPoint(x: CGFloat(i) * candleW + candleW / 2, y: py(v))
            if i == 0 { path.move(to: pt) } else { path.addLine(to: pt) }
        }
        ctx.stroke(path, with: .color(color), lineWidth: 1.5)
    }

    private func rollingAvg(_ values: [Double], _ period: Int) -> [Double] {
        values.indices.map { i in
            let slice = Array(values[max(0, i - period + 1) ... i])
            return slice.reduce(0, +) / Double(slice.count)
        }
    }
}
