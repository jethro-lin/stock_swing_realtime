import Foundation

enum Preset: String, CaseIterable, Codable {
    case long3Lean  = "long3_lean"
    case longTrend  = "long_trend"
    case short3Lean = "short3_lean"

    var label: String {
        switch self {
        case .long3Lean:  return "多方精簡"
        case .longTrend:  return "多方趨勢"
        case .short3Lean: return "空方精簡"
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
    .longTrend: [
        ["B2","N","F"], ["B2","F"],     ["B2","A","D"],
        ["B2","A"],     ["B2","D"],     ["B2","N"],
        ["B","N","F"],  ["B","E","F"],  ["B","F"],
        ["B","E","D"],  ["B","E"],      ["B","N"],
        ["B","A","D"],  ["B","N","D"],  ["B","A"],     ["B","D"],
    ],
    .short3Lean: [
        ["BS","GS"],         ["BS","MS","RS"], ["BS","RS"],
        ["BS","FS","NS"],
        ["BS","FS","JS"],    ["AS","DS","GS"], ["FS","RS"],
        ["BS","KS"],         ["BS","OS"],
        ["BS","DS","NS"],    ["CS","FS","JS"], ["AS","BS","DS"],
        ["BS","CS","JS"],    ["BS","MS"],
    ],
]

extension StrategySignals {
    func matchedComboLabels(preset: Preset) -> [String] {
        guard let presetCombos = combos[preset] else { return [] }
        return presetCombos
            .filter { combo in combo.allSatisfy { flag(for: $0) } }
            .map    { combo in combo.joined(separator: "+") }
    }
}
