import Foundation

struct ScanCache: Codable {
    let results: [SignalResult]
    let scanTime: String
    let signalDate: String
}
