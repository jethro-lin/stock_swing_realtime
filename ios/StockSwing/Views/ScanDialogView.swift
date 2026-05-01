import SwiftUI

private let greenStrong = Color(red: 0, green: 0.784, blue: 0.325)
private let redStrong   = Color(red: 0.835, green: 0, blue: 0)

private enum ScanMode { case preset, custom }

private let longSignalDefs: [(String, String)] = [
    ("A",  "均線多頭排列"),
    ("B",  "跳空向上(≥2%)"),
    ("C",  "RSI 超賣反彈"),
    ("D",  "突破前5日高點"),
    ("E",  "強勢連漲"),
    ("F",  "量能擴張上漲"),
    ("G",  "縮量後上漲"),
    ("H",  "鎚子K線"),
    ("I",  "吞噬陽線"),
    ("J",  "MACD 黃金交叉"),
    ("K",  "布林下軌反彈"),
    ("L",  "KD 超賣交叉"),
    ("M",  "W%R 超賣反彈"),
    ("N",  "均線回測站回"),
    ("O",  "晨星型態"),
    ("P",  "紅三兵"),
    ("Q",  "Inside Bar 突破"),
    ("R",  "BIAS 超跌反彈"),
    ("B2", "大跳空(≥5%)+量比"),
]

private let shortSignalDefs: [(String, String)] = [
    ("AS", "均線空頭排列"),
    ("BS", "跳空向下(≥2%)"),
    ("CS", "RSI 超買回落"),
    ("DS", "跌破前5日低點"),
    ("ES", "弱勢連跌"),
    ("FS", "量能萎縮下跌"),
    ("GS", "縮量後急跌"),
    ("HS", "射擊之星"),
    ("IS", "吞噬陰線"),
    ("JS", "MACD 死亡交叉"),
    ("KS", "布林上軌反壓"),
    ("LS", "KD 超買交叉"),
    ("MS", "W%R 超買回落"),
    ("NS", "均線回測跌破"),
    ("OS", "黃昏之星型態"),
    ("PS", "黑三兵"),
    ("QS", "Inside Bar 跌破"),
    ("RS", "BIAS 超漲回落"),
]

struct ScanDialogView: View {
    let initialMode: String
    let initialPresets: Set<Preset>
    let initialCustomSigs: Set<String>
    let onDismiss: () -> Void
    let onConfirm: (Set<Preset>, Set<String>, Date?) -> Void

    @State private var scanMode: ScanMode
    @State private var selectedPresets: Set<Preset>
    @State private var customSigs: Set<String>
    @State private var useCustomDate = false
    @State private var customDate: Date? = nil
    @State private var showDatePicker = false

    private let lastTradeDate: Date = lastCompleteTradeDate()
    private let tpeZone = TimeZone(identifier: "Asia/Taipei")!

    init(initialMode: String, initialPresets: Set<Preset>, initialCustomSigs: Set<String>,
         onDismiss: @escaping () -> Void, onConfirm: @escaping (Set<Preset>, Set<String>, Date?) -> Void)
    {
        self.initialMode       = initialMode
        self.initialPresets    = initialPresets
        self.initialCustomSigs = initialCustomSigs
        self.onDismiss         = onDismiss
        self.onConfirm         = onConfirm
        _scanMode        = State(initialValue: initialMode == "custom" ? .custom : .preset)
        _selectedPresets = State(initialValue: initialPresets)
        _customSigs      = State(initialValue: initialCustomSigs)
    }

    private var canConfirm: Bool {
        switch scanMode {
        case .preset: return !selectedPresets.isEmpty
        case .custom: return !customSigs.isEmpty
        }
    }

    var body: some View {
        NavigationView {
            Form {
                // Mode toggle
                Section("策略模式") {
                    HStack(spacing: 8) {
                        modeChip("預設組合", isSelected: scanMode == .preset) { scanMode = .preset }
                        modeChip("自選訊號", isSelected: scanMode == .custom) { scanMode = .custom }
                        Spacer()
                    }
                    .listRowBackground(Color.clear)
                    .listRowInsets(EdgeInsets(top: 4, leading: 0, bottom: 4, trailing: 0))
                }

                if scanMode == .preset {
                    Section("選擇組合") {
                        FlowChipGroup(
                            items: Preset.allCases,
                            selected: { selectedPresets.contains($0) },
                            label: { $0.label },
                            chipColor: { preset in
                                preset == .short3Lean ? redStrong.opacity(0.15) : greenStrong.opacity(0.15)
                            },
                            onTap: { preset in
                                if selectedPresets.contains(preset) {
                                    selectedPresets.remove(preset)
                                } else {
                                    selectedPresets.insert(preset)
                                }
                            }
                        )
                        .listRowBackground(Color.clear)
                        .listRowInsets(EdgeInsets(top: 4, leading: 0, bottom: 4, trailing: 0))
                    }
                } else {
                    Section {
                        HStack {
                            Text("多方訊號").font(.caption).foregroundColor(.gray)
                            Spacer()
                            if !customSigs.isEmpty {
                                Button("清除") { customSigs = [] }.font(.caption)
                            }
                        }
                        FlowChipGroup(
                            items: longSignalDefs.map { $0.0 },
                            selected: { customSigs.contains($0) },
                            label: { sig in
                                let desc = longSignalDefs.first { $0.0 == sig }?.1 ?? ""
                                return "\(sig)  \(desc)"
                            },
                            chipColor: { _ in greenStrong.opacity(0.15) },
                            onTap: { sig in
                                if customSigs.contains(sig) { customSigs.remove(sig) }
                                else { customSigs.insert(sig) }
                            }
                        )
                        .listRowBackground(Color.clear)
                        .listRowInsets(EdgeInsets(top: 4, leading: 0, bottom: 4, trailing: 0))
                    }
                    Section {
                        Text("空方訊號").font(.caption).foregroundColor(.gray)
                        FlowChipGroup(
                            items: shortSignalDefs.map { $0.0 },
                            selected: { customSigs.contains($0) },
                            label: { sig in
                                let desc = shortSignalDefs.first { $0.0 == sig }?.1 ?? ""
                                return "\(sig)  \(desc)"
                            },
                            chipColor: { _ in redStrong.opacity(0.15) },
                            onTap: { sig in
                                if customSigs.contains(sig) { customSigs.remove(sig) }
                                else { customSigs.insert(sig) }
                            }
                        )
                        .listRowBackground(Color.clear)
                        .listRowInsets(EdgeInsets(top: 4, leading: 0, bottom: 4, trailing: 0))
                    }
                }

                Section("訊號日") {
                    HStack(spacing: 8) {
                        filterChip(
                            label: formatDate(lastTradeDate),
                            isSelected: !useCustomDate
                        ) {
                            useCustomDate = false
                            customDate    = nil
                        }
                        filterChip(
                            label: customDate != nil ? formatDate(customDate!) : "指定日期",
                            isSelected: useCustomDate
                        ) {
                            useCustomDate = true
                            if customDate == nil { customDate = lastTradeDate }
                            showDatePicker = true
                        }
                        Spacer()
                    }
                    .listRowBackground(Color.clear)
                    .listRowInsets(EdgeInsets(top: 4, leading: 0, bottom: 4, trailing: 0))
                }
            }
            .navigationTitle("掃描設定")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("取消", action: onDismiss)
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("開始掃描") {
                        let finalPresets = scanMode == .preset ? selectedPresets : []
                        let finalSigs    = scanMode == .custom  ? customSigs     : []
                        onConfirm(finalPresets, finalSigs, useCustomDate ? customDate : nil)
                    }
                    .disabled(!canConfirm)
                }
            }
            .sheet(isPresented: $showDatePicker) {
                DatePickerSheet(
                    selection: Binding(
                        get: { customDate ?? lastTradeDate },
                        set: { customDate = $0 }
                    ),
                    maxDate: lastTradeDate
                ) { showDatePicker = false }
            }
        }
    }

    private func modeChip(_ label: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(label)
                .font(.system(size: 13))
                .padding(.horizontal, 12)
                .padding(.vertical, 6)
                .background(isSelected ? Color.accentColor.opacity(0.15) : Color(.systemGray5))
                .foregroundColor(isSelected ? .accentColor : .primary)
                .cornerRadius(16)
                .overlay(
                    RoundedRectangle(cornerRadius: 16)
                        .stroke(isSelected ? Color.accentColor : Color.clear, lineWidth: 1)
                )
        }
        .buttonStyle(.plain)
    }

    private func filterChip(label: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Text(label)
                .font(.system(size: 13))
                .padding(.horizontal, 12)
                .padding(.vertical, 6)
                .background(isSelected ? Color.accentColor.opacity(0.15) : Color(.systemGray5))
                .foregroundColor(isSelected ? .accentColor : .primary)
                .cornerRadius(16)
                .overlay(
                    RoundedRectangle(cornerRadius: 16)
                        .stroke(isSelected ? Color.accentColor : Color.clear, lineWidth: 1)
                )
        }
        .buttonStyle(.plain)
    }

    private func formatDate(_ date: Date) -> String {
        let fmt = DateFormatter()
        fmt.timeZone = tpeZone
        fmt.dateFormat = "MM/dd"
        return fmt.string(from: date)
    }
}

// MARK: - DatePickerSheet

struct DatePickerSheet: View {
    @Binding var selection: Date
    let maxDate: Date
    let onDone: () -> Void

    var body: some View {
        NavigationView {
            DatePicker(
                "",
                selection: $selection,
                in: ...maxDate,
                displayedComponents: .date
            )
            .datePickerStyle(.graphical)
            .padding()
            .navigationTitle("選擇日期")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("確定", action: onDone)
                }
            }
        }
    }
}

// MARK: - FlowChipGroup

struct FlowChipGroup<T>: View {
    let items: [T]
    let selected: (T) -> Bool
    let label: (T) -> String
    let chipColor: (T) -> Color
    let onTap: (T) -> Void

    var body: some View {
        FlowLayout(spacing: 6) {
            ForEach(Array(items.enumerated()), id: \.offset) { _, item in
                let isSelected = selected(item)
                Button { onTap(item) } label: {
                    Text(label(item))
                        .font(.system(size: 12))
                        .padding(.horizontal, 10)
                        .padding(.vertical, 5)
                        .background(isSelected ? chipColor(item) : Color(.systemGray5))
                        .cornerRadius(14)
                        .overlay(
                            RoundedRectangle(cornerRadius: 14)
                                .stroke(isSelected ? Color.accentColor.opacity(0.5) : Color.clear, lineWidth: 1)
                        )
                }
                .buttonStyle(.plain)
            }
        }
    }
}

// MARK: - FlowLayout

struct FlowLayout: Layout {
    var spacing: CGFloat = 4

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        let rows = computeRows(proposal: proposal, subviews: subviews)
        let height = rows.map { $0.map { $0.sizeThatFits(.unspecified).height }.max() ?? 0 }
            .reduce(0) { $0 + $1 + spacing } - spacing
        return CGSize(width: proposal.width ?? 0, height: max(height, 0))
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let rows = computeRows(proposal: proposal, subviews: subviews)
        var y = bounds.minY
        for row in rows {
            var x = bounds.minX
            let rowHeight = row.map { $0.sizeThatFits(.unspecified).height }.max() ?? 0
            for subview in row {
                let size = subview.sizeThatFits(.unspecified)
                subview.place(at: CGPoint(x: x, y: y), proposal: ProposedViewSize(size))
                x += size.width + spacing
            }
            y += rowHeight + spacing
        }
    }

    private func computeRows(proposal: ProposedViewSize, subviews: Subviews) -> [[LayoutSubview]] {
        let maxWidth = proposal.width ?? .infinity
        var rows: [[LayoutSubview]] = [[]]
        var rowWidth: CGFloat = 0
        for subview in subviews {
            let size = subview.sizeThatFits(.unspecified)
            if rowWidth + size.width > maxWidth && !rows.last!.isEmpty {
                rows.append([])
                rowWidth = 0
            }
            rows[rows.count - 1].append(subview)
            rowWidth += size.width + spacing
        }
        return rows
    }
}

// MARK: - lastCompleteTradeDate

func lastCompleteTradeDate() -> Date {
    let tpeZone = TimeZone(identifier: "Asia/Taipei")!
    var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
    let now   = Date()
    let comps = cal.dateComponents([.hour, .minute, .weekday], from: now)
    let isClosed = (comps.hour! > 14) || (comps.hour! == 14 && comps.minute! >= 30)

    var candidate: Date
    if isClosed {
        candidate = cal.startOfDay(for: now)
    } else {
        candidate = cal.date(byAdding: .day, value: -1, to: cal.startOfDay(for: now))!
    }
    // skip weekends
    while true {
        let wd = cal.component(.weekday, from: candidate)
        if wd != 1 && wd != 7 { break }  // 1=Sun, 7=Sat
        candidate = cal.date(byAdding: .day, value: -1, to: candidate)!
    }
    return candidate
}
