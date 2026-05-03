import SwiftUI

private let greenStrong = Color(red: 0,     green: 0.784, blue: 0.325)
private let redStrong   = Color(red: 0.835, green: 0,     blue: 0)
private let grayDot     = Color(white: 0.741)

// 台灣慣例：漲 = 紅，跌 = 綠
private let colorUp   = redStrong
private let colorDown = greenStrong

struct StockRowView: View {
    let result: SignalResult
    var onTap: (() -> Void)? = nil
    @State private var copied = false

    private var customHits: [String]? { result.hitPresets["custom"] }

    private var isLong: Bool {
        if let ch = customHits { return ch.contains { !$0.hasSuffix("S") || $0 == "B2" } }
        return result.hitPresets.keys.contains { $0 != Preset.short3Lean.rawValue }
    }
    private var isShort: Bool {
        if let ch = customHits { return ch.contains { $0.hasSuffix("S") && $0 != "B2" } }
        return result.hitPresets[Preset.short3Lean.rawValue] != nil
    }
    private var dotColor: Color {
        if isLong && isShort { return Color(red: 1, green: 0.627, blue: 0) }
        if isLong  { return colorUp }
        if isShort { return colorDown }
        return grayDot
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            HStack(alignment: .center, spacing: 6) {
                Circle().fill(dotColor).frame(width: 8, height: 8)

                VStack(alignment: .leading, spacing: 0) {
                    Text(result.code)
                        .font(.system(size: 13, weight: .semibold))
                        .lineLimit(1)
                    Text(result.name)
                        .font(.system(size: 15, weight: .medium))
                        .lineLimit(1)
                }
                .frame(maxWidth: .infinity, alignment: .leading)

                Text(String(format: "%.2f", result.quote.price))
                    .font(.system(size: 13, weight: .semibold))
                    .frame(minWidth: 60, alignment: .trailing)

                Text(String(format: "%+.2f%%", result.quote.chgPct))
                    .font(.system(size: 12))
                    .foregroundColor(result.quote.chgPct >= 0 ? colorUp : colorDown)
                    .frame(minWidth: 64, alignment: .trailing)

                Text(formatVol(result.quote.totalVolLots))
                    .font(.system(size: 11))
                    .foregroundColor(.gray)
                    .frame(minWidth: 60, alignment: .trailing)
            }

            // MA row
            if result.ma5 > 0 {
                MARowView(price: result.quote.price,
                          ma5: result.ma5,   ma5Dir: result.ma5Dir,
                          ma10: result.ma10, ma10Dir: result.ma10Dir,
                          ma20: result.ma20, ma20Dir: result.ma20Dir)
            }

            // Signal chips
            if let ch = customHits {
                let longHits  = ch.filter { !$0.hasSuffix("S") || $0 == "B2" }
                let shortHits = ch.filter { $0.hasSuffix("S") && $0 != "B2" }
                if !longHits.isEmpty  { SignalRowView(label: "多方", chips: longHits,  color: colorUp) }
                if !shortHits.isEmpty { SignalRowView(label: "空方", chips: shortHits, color: colorDown) }
            } else {
                ForEach(result.hitPresets.sorted(by: { $0.key < $1.key }), id: \.key) { key, combos in
                    let preset = Preset(rawValue: key)
                    let color  = preset == .short3Lean ? colorDown : colorUp
                    let label  = preset?.label ?? key
                    SignalRowView(label: label, chips: combos, color: color)
                }
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 5)
        .contentShape(Rectangle())
        .onTapGesture { onTap?() }
        .onLongPressGesture {
            UIPasteboard.general.string = result.code
            copied = true
            DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) { copied = false }
        }
        .overlay(alignment: .top) {
            if copied {
                Text("已複製 \(result.code)")
                    .font(.caption)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 4)
                    .background(Color(.systemGray5))
                    .clipShape(Capsule())
                    .transition(.move(edge: .top).combined(with: .opacity))
                    .padding(.top, 2)
            }
        }
        .animation(.easeInOut(duration: 0.2), value: copied)
    }
}

struct SignalRowView: View {
    let label: String
    let chips: [String]
    let color: Color

    var body: some View {
        HStack(spacing: 4) {
            Text(label)
                .font(.system(size: 10, weight: .semibold))
                .foregroundColor(color.opacity(0.7))
            ForEach(chips, id: \.self) { chip in
                ComboChipView(label: chip, color: color)
            }
        }
        .padding(.leading, 20)
    }
}

struct ComboChipView: View {
    let label: String
    let color: Color

    var body: some View {
        Text(label)
            .font(.system(size: 10, weight: .medium))
            .foregroundColor(color)
            .padding(.horizontal, 4)
            .padding(.vertical, 1)
            .background(color.opacity(0.10))
            .cornerRadius(4)
    }
}

struct MARowView: View {
    let price:   Double
    let ma5:     Double; let ma5Dir:  Int
    let ma10:    Double; let ma10Dir: Int
    let ma20:    Double; let ma20Dir: Int

    private func arrow(_ dir: Int) -> String {
        dir > 0 ? "↑" : dir < 0 ? "↓" : "→"
    }

    var body: some View {
        HStack(spacing: 6) {
            Spacer().frame(width: 14)
            ForEach([("MA5", ma5, ma5Dir), ("MA10", ma10, ma10Dir), ("MA20", ma20, ma20Dir)],
                    id: \.0) { label, ma, dir in
                let above = price >= ma
                let color  = above ? colorUp : colorDown
                HStack(spacing: 1) {
                    Text(label)
                        .font(.system(size: 9))
                        .foregroundColor(color.opacity(0.7))
                    Text(arrow(dir))
                        .font(.system(size: 9, weight: .bold))
                        .foregroundColor(dir > 0 ? colorUp : dir < 0 ? colorDown : .gray)
                    Text(String(format: "%.1f", ma))
                        .font(.system(size: 9, weight: .semibold))
                        .foregroundColor(color)
                }
            }
        }
    }
}

private func formatVol(_ lots: Int64) -> String {
    if lots >= 10_000 { return String(format: "%.1f萬", Double(lots) / 10_000) }
    if lots >= 1_000  { return String(format: "%.1f千", Double(lots) / 1_000) }
    return "\(lots)"
}
