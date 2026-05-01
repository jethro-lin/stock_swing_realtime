import Foundation

private let kbarDir: URL = {
    let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    let dir  = docs.appendingPathComponent("kbar", isDirectory: true)
    try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    return dir
}()

private let tpeZone = TimeZone(identifier: "Asia/Taipei")!

actor TwseApiService {

    private let session: URLSession = {
        let cfg = URLSessionConfiguration.default
        cfg.timeoutIntervalForRequest  = 15
        cfg.timeoutIntervalForResource = 15
        return URLSession(configuration: cfg)
    }()

    // MARK: - K-bar cache

    private func cacheFile(code: String) -> URL {
        kbarDir.appendingPathComponent("\(code).csv")
    }

    func loadCachedBars(code: String) -> [HistoricalBar]? { readCacheRaw(code: code) }

    private func readCacheRaw(code: String) -> [HistoricalBar]? {
        let file = cacheFile(code: code)
        guard let text = try? String(contentsOf: file, encoding: .utf8) else { return nil }
        let bars: [HistoricalBar] = text.components(separatedBy: "\n").compactMap { line in
            let p = line.components(separatedBy: "|")
            guard p.count == 6,
                  let date = Self.parseDate(p[0]),
                  let open = Double(p[1]), let high = Double(p[2]),
                  let low  = Double(p[3]), let close = Double(p[4]),
                  let vol  = Int64(p[5])
            else { return nil }
            return HistoricalBar(date: date, open: open, high: high, low: low, close: close, volumeLots: vol)
        }
        return bars.isEmpty ? nil : bars
    }

    private func loadCache(code: String) -> [HistoricalBar]? {
        let file = cacheFile(code: code)
        guard let bars = readCacheRaw(code: code) else { return nil }

        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = tpeZone
        let nowTpe  = Date()
        let comps   = cal.dateComponents([.hour, .minute, .day, .month, .year], from: nowTpe)
        let isAfterClose = (comps.hour! > 14) || (comps.hour! == 14 && comps.minute! >= 30)

        if isAfterClose {
            let todayComps = cal.dateComponents([.year, .month, .day], from: nowTpe)
            let lastBar    = bars.last!
            let lastBarComps = cal.dateComponents([.year, .month, .day], from: lastBar.date)
            if lastBarComps.year != todayComps.year
                || lastBarComps.month != todayComps.month
                || lastBarComps.day  != todayComps.day
            {
                // Check file mtime
                if let attrs = try? FileManager.default.attributesOfItem(atPath: file.path),
                   let mtime = attrs[.modificationDate] as? Date
                {
                    let mtimeComps = cal.dateComponents([.year, .month, .day], from: mtime)
                    if mtimeComps.year != todayComps.year
                        || mtimeComps.month != todayComps.month
                        || mtimeComps.day   != todayComps.day
                    {
                        return nil  // stale
                    }
                    // written today — check if before close
                    var closeThreshComps = todayComps
                    closeThreshComps.hour = 14; closeThreshComps.minute = 30
                    if let closeThresh = cal.date(from: closeThreshComps),
                       mtime < closeThresh
                    {
                        return nil  // written before close, needs post-close refetch
                    }
                } else {
                    return nil
                }
            }
        }
        return bars
    }

    private func saveCache(code: String, bars: [HistoricalBar]) {
        let file = cacheFile(code: code)
        let text = bars.map { b -> String in
            let ds = Self.formatDate(b.date)
            return "\(ds)|\(b.open)|\(b.high)|\(b.low)|\(b.close)|\(b.volumeLots)"
        }.joined(separator: "\n")
        try? text.write(to: file, atomically: true, encoding: .utf8)
    }

    // MARK: - Historical K-bars

    func fetchHistorical(code: String, months: Int = 6) async -> [HistoricalBar] {
        if let cached = loadCache(code: code) { return cached }

        let stale = readCacheRaw(code: code)
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = tpeZone

        let today = Date()
        let fetchFrom: Date
        if let stale, let lastDate = stale.last?.date {
            fetchFrom = cal.date(from: cal.dateComponents([.year, .month], from: lastDate))!
        } else {
            fetchFrom = cal.date(byAdding: .month, value: -months, to: today)!
        }

        let fromComps = cal.dateComponents([.year, .month], from: fetchFrom)
        let todayComps = cal.dateComponents([.year, .month], from: today)
        let monthsToFetch = (todayComps.year! - fromComps.year!) * 12
            + (todayComps.month! - fromComps.month!)

        var newBars: [HistoricalBar] = []
        for m in stride(from: monthsToFetch, through: 0, by: -1) {
            guard let date = cal.date(byAdding: .month, value: -m, to: today) else { continue }
            let comps  = cal.dateComponents([.year, .month], from: date)
            let year   = comps.year!; let month = comps.month!
            let ym     = String(format: "%04d%02d", year, month)
            let rocY   = year - 1911
            let rocMM  = String(format: "%02d", month)

            var twse = await fetchTwseMonth(code: code, ym: ym)
            if let t = twse, t.isEmpty { twse = nil }

            if let bars = twse {
                newBars += bars
            } else if let bars = await fetchTpexMonth(code: code, rocYear: rocY, rocMM: rocMM) {
                newBars += bars
            }
            try? await Task.sleep(nanoseconds: 400_000_000)
        }

        var result = ((stale ?? []) + newBars)
            .sorted { $0.date < $1.date }
        result = uniqueByDate(result)

        if !result.isEmpty && (!newBars.isEmpty || stale == nil) {
            saveCache(code: code, bars: result)
        }
        return result
    }

    private func fetchTwseMonth(code: String, ym: String) async -> [HistoricalBar]? {
        let urlStr = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=\(ym)01&stockNo=\(code)"
        guard let body = await get(urlStr) else { return nil }
        guard let root = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any],
              let stat = root["stat"] as? String, stat == "OK",
              let data = root["data"] as? [[Any]]
        else { return nil }

        return data.compactMap { row -> HistoricalBar? in
            guard row.count >= 7,
                  let dateStr = row[0] as? String,
                  let volStr  = row[1] as? String,
                  let openStr = row[3] as? String,
                  let highStr = row[4] as? String,
                  let lowStr  = row[5] as? String,
                  let closeStr = row[6] as? String
            else { return nil }
            let parts = dateStr.components(separatedBy: "/")
            guard parts.count == 3,
                  let y = Int(parts[0]), let mo = Int(parts[1]), let d = Int(parts[2])
            else { return nil }
            var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
            var dc = DateComponents(); dc.year = y + 1911; dc.month = mo; dc.day = d
            guard let date = cal.date(from: dc) else { return nil }
            return HistoricalBar(
                date: date,
                open: cleanNum(openStr),
                high: cleanNum(highStr),
                low:  cleanNum(lowStr),
                close: cleanNum(closeStr),
                volumeLots: Int64(cleanNum(volStr) / 1000)
            )
        }
    }

    private func fetchTpexMonth(code: String, rocYear: Int, rocMM: String) async -> [HistoricalBar]? {
        let d      = "\(rocYear)/\(rocMM)"
        let urlStr = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=\(d)&stkno=\(code)&_=1"
        guard let body = await get(urlStr) else { return nil }
        guard let root = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any],
              let data = root["aaData"] as? [[Any]], !data.isEmpty
        else { return nil }

        return data.compactMap { row -> HistoricalBar? in
            guard row.count >= 8,
                  let dateStr  = row[0] as? String,
                  let volStr   = row[1] as? String,
                  let openStr  = row[4] as? String,
                  let highStr  = row[5] as? String,
                  let lowStr   = row[6] as? String,
                  let closeStr = row[7] as? String
            else { return nil }
            let parts = dateStr.components(separatedBy: "/")
            guard parts.count == 3,
                  let y = Int(parts[0]), let mo = Int(parts[1]), let d = Int(parts[2])
            else { return nil }
            var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
            var dc = DateComponents(); dc.year = y + 1911; dc.month = mo; dc.day = d
            guard let date = cal.date(from: dc) else { return nil }
            return HistoricalBar(
                date: date,
                open: cleanNum(openStr),
                high: cleanNum(highStr),
                low:  cleanNum(lowStr),
                close: cleanNum(closeStr),
                volumeLots: Int64(cleanNum(volStr) / 1000)
            )
        }
    }

    // MARK: - Realtime price

    func fetchRealtimePrice(codes: [String]) async -> [String: RealtimeQuote] {
        guard !codes.isEmpty else { return [:] }
        var result: [String: RealtimeQuote] = [:]
        let chunks = stride(from: 0, to: codes.count, by: 80).map {
            Array(codes[$0 ..< min($0 + 80, codes.count)])
        }
        for chunk in chunks {
            let exCh = chunk.flatMap { ["tse_\($0).tw", "otc_\($0).tw"] }.joined(separator: "|")
            let urlStr = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=\(exCh)&json=1&delay=0"
            guard let body = await get(urlStr),
                  let root = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any],
                  let msgs = root["msgArray"] as? [[String: Any]]
            else { continue }

            for obj in msgs {
                guard let code  = obj["c"] as? String else { continue }
                let name  = obj["n"] as? String ?? code
                let yRaw  = obj["y"] as? String ?? "0"
                let zRaw  = obj["z"] as? String ?? "-"
                guard let prevC = Double(yRaw) else { continue }
                let price = (zRaw == "-" || zRaw.isEmpty) ? prevC : (Double(zRaw) ?? prevC)
                let open  = Double(obj["o"] as? String ?? "") ?? price
                let high  = Double(obj["h"] as? String ?? "") ?? price
                let low   = Double(obj["l"] as? String ?? "") ?? price
                let vol   = Int64(obj["v"] as? String ?? "") ?? 0
                let chg   = prevC > 0 ? (price - prevC) / prevC * 100 : 0.0

                let fmt = DateFormatter()
                fmt.timeZone = tpeZone
                fmt.dateFormat = "HH:mm:ss"
                result[code] = RealtimeQuote(
                    code: code, name: name, price: price, open: open,
                    high: high, low: low, prevClose: prevC, chgPct: chg,
                    totalVolLots: vol, updateTime: fmt.string(from: Date())
                )
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }
        return result
    }

    // MARK: - All codes by volume

    func fetchAllCodesWithNames(limit: Int = Int.max) async -> [String: String] {
        struct Entry { let code: String; let name: String; let volLots: Int64 }
        var entries: [Entry] = []

        // TWSE
        if let body = await get("https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"),
           let root = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any],
           let stat = root["stat"] as? String, stat == "OK",
           let data = root["data"] as? [[Any]]
        {
            for row in data {
                guard let code = row[0] as? String, isValidStockCode(code),
                      let name = row[1] as? String,
                      let volStr = row[2] as? String
                else { continue }
                let vol = Int64(cleanNum(volStr) / 1000)
                entries.append(Entry(code: code.trimmingCharacters(in: .whitespaces),
                                     name: name.trimmingCharacters(in: .whitespaces),
                                     volLots: vol))
            }
        }

        // TPEX
        if let body = await get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"),
           let arr = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [[String: Any]]
        {
            for obj in arr {
                guard let code = obj["SecuritiesCompanyCode"] as? String,
                      isValidStockCode(code)
                else { continue }
                let name = obj["CompanyName"] as? String ?? code
                let volStr = obj["TradeVolume"] as? String ?? "0"
                let vol = Int64((Double(volStr.replacingOccurrences(of: ",", with: "")) ?? 0) / 1000)
                entries.append(Entry(code: code.trimmingCharacters(in: .whitespaces),
                                     name: name.trimmingCharacters(in: .whitespaces),
                                     volLots: vol))
            }
        }

        if !entries.isEmpty {
            let limited = entries.sorted { $0.volLots > $1.volLots }.prefix(limit)
            var dict: [String: String] = [:]
            for e in limited { dict[e.code] = e.name }
            return dict
        }

        // Fallback
        var fallback: [String: String] = [:]
        if let body = await get("https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&selectType=ALL"),
           let root = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any],
           let stat = root["stat"] as? String, stat == "OK",
           let data = root["data"] as? [[Any]]
        {
            for row in data {
                guard let code = row[safe: 0] as? String, isValidStockCode(code),
                      let name = row[safe: 1] as? String
                else { continue }
                fallback[code] = name
            }
        }
        if let body = await get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"),
           let arr = try? JSONSerialization.jsonObject(with: Data(body.utf8)) as? [[String: Any]]
        {
            for obj in arr {
                guard let code = obj["SecuritiesCompanyCode"] as? String, isValidStockCode(code),
                      let name = obj["CompanyName"] as? String
                else { continue }
                fallback[code] = name
            }
        }
        return fallback
    }

    // MARK: - Helpers

    private func get(_ urlString: String) async -> String? {
        guard let url = URL(string: urlString) else { return nil }
        var req = URLRequest(url: url)
        req.setValue("Mozilla/5.0", forHTTPHeaderField: "User-Agent")
        do {
            let (data, resp) = try await session.data(for: req)
            guard (resp as? HTTPURLResponse)?.statusCode == 200 else { return nil }
            return String(data: data, encoding: .utf8)
        } catch {
            return nil
        }
    }

    private func cleanNum(_ s: String) -> Double {
        Double(s.replacingOccurrences(of: ",", with: "").replacingOccurrences(of: "X", with: "")) ?? 0
    }

    private func isValidStockCode(_ code: String) -> Bool {
        let trimmed = code.trimmingCharacters(in: .whitespaces)
        return trimmed.count == 4 && trimmed.allSatisfy(\.isNumber)
    }

    private func uniqueByDate(_ bars: [HistoricalBar]) -> [HistoricalBar] {
        var seen = Set<Date>()
        return bars.filter { seen.insert($0.date).inserted }
    }

    static func parseDate(_ s: String) -> Date? {
        let parts = s.components(separatedBy: "-")
        guard parts.count == 3, let y = Int(parts[0]), let m = Int(parts[1]), let d = Int(parts[2])
        else { return nil }
        var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
        var dc = DateComponents(); dc.year = y; dc.month = m; dc.day = d
        return cal.date(from: dc)
    }

    static func formatDate(_ date: Date) -> String {
        var cal = Calendar(identifier: .gregorian); cal.timeZone = tpeZone
        let dc = cal.dateComponents([.year, .month, .day], from: date)
        return String(format: "%04d-%02d-%02d", dc.year!, dc.month!, dc.day!)
    }
}

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
