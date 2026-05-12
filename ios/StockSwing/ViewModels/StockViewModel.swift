import Foundation
import Combine

private struct EodEntry {
    let base: StrategyBase
    let quote: RealtimeQuote
}

@MainActor
class StockViewModel: ObservableObject {

    // MARK: - Persistent settings

    @Published var selectedPresets: Set<Preset> = Set(Preset.allCases)
    @Published var lastScanMode: String = "preset"
    @Published var selectedCustomSignals: Set<String> = []

    // MARK: - Scan state

    @Published var isLoading = false
    @Published var loadingMsg = ""
    @Published var scanProgress: (done: Int, total: Int) = (0, 0)
    @Published var error: String? = nil
    @Published var scanResults: [SignalResult] = []
    @Published var lastScanTime = ""
    @Published var signalDate = ""

    // MARK: - Chart state

    @Published var chartTarget: SignalResult? = nil
    @Published var chartBars: [HistoricalBar] = []

    func openChart(_ result: SignalResult) {
        chartTarget = result
        chartBars   = []
        Task.detached(priority: .userInitiated) { [weak self] in
            guard let self else { return }
            let bars = await self.api.loadCachedBars(code: result.code) ?? []
            await MainActor.run { self.chartBars = bars }
        }
    }

    func closeChart() {
        chartTarget = nil
        chartBars   = []
    }

    private let api = TwseApiService()
    private var scanTask: Task<Void, Never>?

    private let scanCacheURL: URL = {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
        return docs.appendingPathComponent("last_scan.json")
    }()

    private let tpeZone = TimeZone(identifier: "Asia/Taipei")!
    private static let topStocks = 300
    private static let maxConcurrency = 3

    // MARK: - Init

    init() {
        loadSettings()
        loadLastScan()
    }

    // MARK: - Settings persistence

    private func loadSettings() {
        let ud = UserDefaults.standard
        if let raw = ud.string(forKey: "selected_presets_v3"), !raw.isEmpty {
            let loaded = raw.split(separator: ",").compactMap { Preset(rawValue: String($0)) }
            selectedPresets = loaded.isEmpty ? Set(Preset.allCases) : Set(loaded)
        }
        lastScanMode = ud.string(forKey: "scan_mode_v1") ?? "preset"
        if let raw = ud.string(forKey: "custom_signals_v1"), !raw.isEmpty {
            selectedCustomSignals = Set(raw.split(separator: ",").map(String.init))
        }
    }

    private func saveSettings(presets: Set<Preset>, mode: String, customSigs: Set<String>) {
        let ud = UserDefaults.standard
        ud.set(presets.map(\.rawValue).joined(separator: ","), forKey: "selected_presets_v3")
        ud.set(mode,                                            forKey: "scan_mode_v1")
        ud.set(customSigs.sorted().joined(separator: ","),      forKey: "custom_signals_v1")
    }

    // MARK: - Last scan persistence

    private func loadLastScan() {
        Task.detached(priority: .background) { [weak self] in
            guard let self,
                  let data  = try? Data(contentsOf: self.scanCacheURL),
                  let cache = try? JSONDecoder().decode(ScanCache.self, from: data)
            else { return }
            await MainActor.run {
                self.scanResults  = cache.results
                self.lastScanTime = cache.scanTime
                self.signalDate   = cache.signalDate
            }
        }
    }

    private func saveLastScan(results: [SignalResult], scanTime: String, signalDate: String) {
        let url = scanCacheURL
        Task.detached(priority: .background) {
            let cache = ScanCache(results: results, scanTime: scanTime, signalDate: signalDate)
            if let data = try? JSONEncoder().encode(cache) {
                try? data.write(to: url)
            }
        }
    }

    // MARK: - Public actions

    func stopScan() { scanTask?.cancel() }

    func scan(
        presets: Set<Preset>? = nil,
        customSignals: Set<String> = [],
        overrideDate: Date? = nil
    ) {
        let effectivePresets = presets ?? selectedPresets
        let isCustomMode     = !customSignals.isEmpty
        if !isCustomMode && effectivePresets.isEmpty { return }

        scanTask?.cancel()
        scanTask = Task {
            defer { isLoading = false; loadingMsg = "" }
            isLoading    = true
            error        = nil
            scanProgress = (0, 0)

            saveSettings(presets: effectivePresets,
                         mode: isCustomMode ? "custom" : "preset",
                         customSigs: customSignals)
            selectedPresets       = effectivePresets
            lastScanMode          = isCustomMode ? "custom" : "preset"
            selectedCustomSignals = customSignals

            // 1. Fetch stock list
            loadingMsg = "取得上市/上櫃股票清單…"
            let codeNames = await api.fetchAllCodesWithNames(limit: Self.topStocks)
            guard !Task.isCancelled else { return }
            guard !codeNames.isEmpty else {
                error = "無法取得股票清單，請確認網路狀況"
                return
            }
            let allCodes = Array(codeNames.keys)

            // 2. Determine signal cutoff
            var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
            let nowTpe   = Date()
            let nowHour  = cal.component(.hour, from: nowTpe)
            let nowMin   = cal.component(.minute, from: nowTpe)
            let lookback = 3

            let signalCutoff: Date
            let monthsToDownload: Int
            if let od = overrideDate {
                signalCutoff     = cal.date(byAdding: .day, value: 1, to: startOfDay(od))!
                let lookbackStart = cal.date(byAdding: .month, value: -lookback, to: od)!
                let sComps = cal.dateComponents([.year, .month], from: lookbackStart)
                let tComps = cal.dateComponents([.year, .month], from: nowTpe)
                let mf = (tComps.year! - sComps.year!) * 12 + (tComps.month! - sComps.month!) + 1
                monthsToDownload = min(max(mf, lookback), 12)
            } else {
                let isIntraday = (nowHour >= 9 && nowHour <= 13) || (nowHour == 14 && nowMin < 30)
                signalCutoff     = isIntraday
                    ? startOfDay(nowTpe)
                    : cal.date(byAdding: .day, value: 1, to: startOfDay(nowTpe))!
                monthsToDownload = lookback
            }

            // 3. Parallel K-bar download (limited concurrency via semaphore actor)
            scanProgress = (0, allCodes.count)
            let limiter  = ConcurrencyLimiter(limit: Self.maxConcurrency)
            var eodEntries: [(String, EodEntry)] = []
            var doneCount = 0

            await withTaskGroup(of: (String, EodEntry)?.self) { group in
                for code in allCodes {
                    group.addTask { [self] in
                        guard !Task.isCancelled else { return nil }
                        await limiter.acquire()
                        defer { Task { await limiter.release() } }
                        guard !Task.isCancelled else { return nil }
                        return await self.processCode(code: code, codeNames: codeNames,
                                                       months: monthsToDownload, signalCutoff: signalCutoff)
                    }
                }
                for await result in group {
                    doneCount += 1
                    scanProgress = (doneCount, allCodes.count)
                    loadingMsg = "載入中… (\(doneCount)/\(allCodes.count))"
                    if let (code, entry) = result {
                        eodEntries.append((code, entry))
                    }
                }
            }

            guard !Task.isCancelled else { return }
            guard !eodEntries.isEmpty else {
                error = "無法計算技術指標，請確認網路狀況後重新掃描"
                return
            }

            // 4. Compute signals
            loadingMsg = "計算策略訊號…"
            var results: [SignalResult] = []
            for (code, entry) in eodEntries {
                let sigs = StrategyEngine.checkSignals(
                    base: entry.base, price: entry.quote.price,
                    openP: entry.quote.open, chgPct: entry.quote.chgPct,
                    highP: entry.quote.high, lowP: entry.quote.low
                )
                let hitPresets: [String: [String]]
                if isCustomMode {
                    // 個別訊號命中
                    let individualHits = sigs.activeSignals.filter { customSignals.contains($0) }
                    // 組合命中（"A+B" 格式：所有子訊號都必須成立）
                    let comboHits = customSignals
                        .filter  { $0.contains("+") }
                        .filter  { combo in combo.split(separator: "+").allSatisfy { sigs.flag(for: String($0)) } }
                        .sorted()
                    let hit = individualHits + comboHits
                    if hit.isEmpty { continue }
                    hitPresets = ["custom": hit]
                } else {
                    var hitMap: [String: [String]] = [:]
                    for preset in effectivePresets {
                        let labels = sigs.matchedComboLabels(preset: preset)
                        if !labels.isEmpty { hitMap[preset.rawValue] = labels }
                    }
                    if hitMap.isEmpty { continue }
                    hitPresets = hitMap
                }
                results.append(SignalResult(code: code, name: entry.quote.name,
                                            quote: entry.quote, signals: sigs,
                                            hitPresets: hitPresets,
                                            ma5: entry.base.ma5, ma10: entry.base.ma10, ma20: entry.base.ma20,
                                            ma5Dir: entry.base.ma5Dir, ma10Dir: entry.base.ma10Dir, ma20Dir: entry.base.ma20Dir,
                                            ma20Trend: entry.base.ma20Trend))
            }

            results.sort {
                if $0.totalComboHits != $1.totalComboHits { return $0.totalComboHits > $1.totalComboHits }
                return $0.quote.totalVolLots > $1.quote.totalVolLots
            }

            let dateFmt = DateFormatter(); dateFmt.timeZone = tpeZone; dateFmt.dateFormat = "MM/dd"
            let timeFmt = DateFormatter(); timeFmt.timeZone = tpeZone; timeFmt.dateFormat = "MM/dd HH:mm"

            let sigDate: String = overrideDate != nil
                ? dateFmt.string(from: overrideDate!)
                : (results.first?.quote.updateTime ?? dateFmt.string(from: startOfDay(nowTpe)))
            let scanTime = timeFmt.string(from: Date())

            signalDate   = sigDate
            scanResults  = results
            lastScanTime = scanTime
            saveLastScan(results: results, scanTime: scanTime, signalDate: sigDate)
        }
    }

    // MARK: - Private helpers

    private func processCode(
        code: String,
        codeNames: [String: String],
        months: Int,
        signalCutoff: Date
    ) async -> (String, EodEntry)? {
        let raw  = await api.fetchHistorical(code: code, months: months)
        var cal  = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
        let bars = raw.filter { $0.date < signalCutoff }
        guard bars.count >= 22, let base = StrategyEngine.buildBase(bars) else { return nil }

        let today   = bars.last!
        let prevDay = bars[bars.count - 2]
        let chgPct  = prevDay.close > 0 ? (today.close - prevDay.close) / prevDay.close * 100 : 0.0

        let dateFmt = DateFormatter(); dateFmt.timeZone = tpeZone; dateFmt.dateFormat = "MM/dd"
        let quote = RealtimeQuote(
            code: code, name: codeNames[code] ?? code,
            price: today.close, open: today.open, high: today.high, low: today.low,
            prevClose: prevDay.close, chgPct: chgPct,
            totalVolLots: today.volumeLots, updateTime: dateFmt.string(from: today.date)
        )
        return (code, EodEntry(base: base, quote: quote))
    }

    private func startOfDay(_ date: Date) -> Date {
        var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
        return cal.startOfDay(for: date)
    }
}

// MARK: - ConcurrencyLimiter

private actor ConcurrencyLimiter {
    private let limit: Int
    private var active = 0
    private var waiters: [CheckedContinuation<Void, Never>] = []

    init(limit: Int) { self.limit = limit }

    func acquire() async {
        if active < limit {
            active += 1
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func release() {
        if waiters.isEmpty {
            active -= 1
        } else {
            let next = waiters.removeFirst()
            next.resume()
        }
    }
}
