import SwiftUI

private let chartGreen  = Color(red: 0,     green: 0.784, blue: 0.325)
private let chartRed    = Color(red: 0.835, green: 0,     blue: 0)
private let chartBlue   = Color(red: 0.129, green: 0.588, blue: 0.953)
private let chartOrange = Color(red: 1,     green: 0.596, blue: 0)

// MARK: - Sheet wrapper

struct KBarChartSheet: View {
    @EnvironmentObject var vm: StockViewModel
    let result: SignalResult

    var body: some View {
        NavigationView {
            VStack(spacing: 0) {
                // Header info
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
                    // MA legend
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
                    Spacer()
                    ProgressView()
                    Spacer()
                } else {
                    CandlestickChart(bars: vm.chartBars)
                        .padding(.horizontal, 8)
                        .padding(.vertical, 12)
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

// MARK: - MA legend chip

private struct MALegend: View {
    let label: String
    let color: Color
    var body: some View {
        HStack(spacing: 3) {
            Rectangle()
                .fill(color)
                .frame(width: 14, height: 2)
            Text(label)
                .font(.system(size: 10))
                .foregroundColor(color)
        }
    }
}

// MARK: - Candlestick chart

struct CandlestickChart: View {
    let bars: [HistoricalBar]

    private var display: [HistoricalBar] { Array(bars.suffix(60)) }

    var body: some View {
        GeometryReader { geo in
            Canvas { ctx, size in
                let n = display.count
                guard n > 0 else { return }

                // Layout
                let priceH = size.height * 0.76
                let volH   = size.height * 0.18
                let gapH   = size.height * 0.06
                let volTop = priceH + gapH

                let candleW = size.width / CGFloat(n)
                let bodyW   = max(candleW * 0.65, 2)

                // Ranges
                let maxHigh = display.map(\.high).max() ?? 0
                let minLow  = display.map(\.low).min() ?? 0
                let pad     = (maxHigh - minLow) * 0.08
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
                    let color: GraphicsContext.Shading = isUp
                        ? .color(chartGreen) : .color(chartRed)
                    let colorFaint: GraphicsContext.Shading = isUp
                        ? .color(chartGreen.opacity(0.45)) : .color(chartRed.opacity(0.45))

                    // Wick
                    ctx.stroke(
                        Path { p in
                            p.move(to: .init(x: cx, y: py(bar.high)))
                            p.addLine(to: .init(x: cx, y: py(bar.low)))
                        },
                        with: color, lineWidth: 1
                    )
                    // Body
                    let top = py(max(bar.open, bar.close))
                    let bot = py(min(bar.open, bar.close))
                    ctx.fill(
                        Path(CGRect(x: cx - bodyW/2, y: top, width: bodyW, height: max(bot - top, 1))),
                        with: color
                    )
                    // Volume
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
            }
        }
    }

    private func drawMA(
        _ ctx: GraphicsContext,
        values: [Double],
        candleW: CGFloat,
        py: (Double) -> CGFloat,
        color: Color
    ) {
        var path = Path()
        for (i, v) in values.enumerated() {
            let x = CGFloat(i) * candleW + candleW / 2
            let y = py(v)
            if i == 0 { path.move(to: .init(x: x, y: y)) }
            else       { path.addLine(to: .init(x: x, y: y)) }
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
