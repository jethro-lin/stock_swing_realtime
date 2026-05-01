import Foundation

struct RealtimeQuote: Codable {
    let code: String
    let name: String
    let price: Double
    let open: Double
    let high: Double
    let low: Double
    let prevClose: Double
    let chgPct: Double
    let totalVolLots: Int64
    let updateTime: String
}
