import Foundation

enum Preset: String, CaseIterable, Codable {
    case long3Lean     = "long3_lean"
    case long3Pattern  = "long3_pattern"
    case longMomentum  = "long_momentum"
    case longTrend     = "long_trend"
    case short3Lean    = "short3_lean"

    var label: String {
        switch self {
        case .long3Lean:    return "多方精簡"
        case .long3Pattern: return "多方形態"
        case .longMomentum: return "多方動能"
        case .longTrend:    return "多方趨勢"
        case .short3Lean:   return "空方精簡"
        }
    }
}

private let combos: [Preset: [[String]]] = [
    .long3Lean: [
        ["M","R","B2"], ["R","B2"],     ["B","K","R"],
        ["B","K","L"],  ["B2","K"],
        ["B","F","L"],  ["B","F","K"],  ["B","L","R"],
        ["B","F","R"],  ["K","L","R"],  ["B","M","R"],
        ["B","R"],      ["B","K"],      ["K","R"],     ["B2","M"],
    ],
    // long3_pattern — 形態×超賣組合（2026-05-05 新增，B+P/B2+J 因 P 參數調整後轉負 EV 移除）
    .long3Pattern: [
        ["R","O"], ["M","G"],
        ["B","O"], ["B","J"], ["B","Q"],
    ],
    // long_momentum — 趨勢動能，持倉 15 日（止損 7%）
    .longMomentum: [
        ["D","N"],     ["N","F"],     ["D","F"],     ["D","J"],
        ["A","D"],     ["A","J"],     ["A","N"],     ["D","Q"],
        ["E","N"],     ["A","F"],     ["D","N","F"], ["N","J"],
    ],
    // long_trend 已於 swing_trade_v2.py 廢止（回測負 EV），保留 enum 但清空 combo
    .longTrend: [],
    .short3Lean: [
        ["BS","GS"],         ["BS","MS","RS"], ["BS","RS"],
        ["BS","FS","NS"],
        ["BS","FS","JS"],    ["AS","DS","GS"], ["FS","RS"],
        ["BS","KS"],         ["BS","OS"],
        ["BS","DS","NS"],    ["CS","FS","JS"], ["AS","BS","DS"],
        ["BS","CS","JS"],    ["BS","MS"],
    ],
]

extension Preset {
    /// 該 preset 是否有非空的 combo 定義（空的不在選擇器中顯示）
    var hasCombo: Bool { !(combos[self] ?? []).isEmpty }
}

extension StrategySignals {
    func matchedComboLabels(preset: Preset) -> [String] {
        guard let presetCombos = combos[preset] else { return [] }
        return presetCombos
            .filter { combo in combo.allSatisfy { flag(for: $0) } }
            .map    { combo in combo.joined(separator: "+") }
    }
}
