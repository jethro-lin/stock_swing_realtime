import Foundation

struct SignalResult: Codable, Identifiable {
    let code: String
    let name: String
    let quote: RealtimeQuote
    let signals: StrategySignals
    let hitPresets: [String: [String]]
    var ma5:     Double = 0
    var ma10:    Double = 0
    var ma20:    Double = 0
    var ma5Dir:  Int    = 0   // 1=↑, -1=↓, 0=→
    var ma10Dir: Int    = 0
    var ma20Dir: Int    = 0

    var id: String { code }
    var totalComboHits: Int { hitPresets.values.reduce(0) { $0 + $1.count } }
}
