package com.stockswing.app.data

import com.stockswing.app.model.HistoricalBar
import com.stockswing.app.model.RealtimeQuote
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.time.LocalDate
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

class TwseApiService(private val cacheDir: File? = null) {

    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .build()

    private val json = Json { ignoreUnknownKeys = true }

    // ── K 棒磁碟快取 ─────────────────────────────────────────────────

    /** 快取檔案：{cacheDir}/kbar/{code}.csv（跨日保留，補檔更新）*/
    private fun kbarCacheFile(code: String): File? {
        val base = cacheDir ?: return null
        return File(base, "kbar/$code.csv").also { it.parentFile?.mkdirs() }
    }

    /** 讀取快取檔原始資料，不做新鮮度檢查（供補檔合併用）*/
    private fun readCacheRaw(code: String): List<HistoricalBar>? {
        val file = kbarCacheFile(code) ?: return null
        if (!file.exists()) return null
        return try {
            file.readLines().mapNotNull { line ->
                val p = line.split("|")
                if (p.size != 6) null
                else HistoricalBar(
                    date       = LocalDate.parse(p[0]),
                    open       = p[1].toDouble(),
                    high       = p[2].toDouble(),
                    low        = p[3].toDouble(),
                    close      = p[4].toDouble(),
                    volumeLots = p[5].toLong(),
                )
            }.ifEmpty { null }
        } catch (_: Exception) { null }
    }

    /**
     * 讀取快取並檢查新鮮度。
     * 盤後（台灣時間 ≥14:30）若不含今日 K 棒且檔案今日尚未更新，回傳 null 觸發補檔。
     * 若檔案今日已更新（假日/休市），則接受快取避免無限重試。
     */
    private fun loadCache(code: String): List<HistoricalBar>? {
        val file = kbarCacheFile(code) ?: return null
        if (!file.exists()) return null
        val bars = readCacheRaw(code) ?: return null

        val nowTpe = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Taipei"))
        val isAfterClose = nowTpe.hour > 14 || (nowTpe.hour == 14 && nowTpe.minute >= 30)
        if (isAfterClose) {
            val today = nowTpe.toLocalDate()
            if (bars.none { it.date == today }) {
                val fileDate = java.time.Instant.ofEpochMilli(file.lastModified())
                    .atZone(java.time.ZoneId.of("Asia/Taipei")).toLocalDate()
                if (fileDate < today) return null  // 昨日快取，需補今日
            }
        }
        return bars
    }

    private fun saveCache(code: String, bars: List<HistoricalBar>) {
        val file = kbarCacheFile(code) ?: return
        try {
            file.writeText(bars.joinToString("\n") { b ->
                "${b.date}|${b.open}|${b.high}|${b.low}|${b.close}|${b.volumeLots}"
            })
        } catch (_: Exception) {}
    }

    // ── 歷史日K ──────────────────────────────────────────────────────

    /**
     * 取得日K，優先使用磁碟快取，只補抓缺少的月份。
     * - 快取新鮮 → 直接回傳
     * - 快取存在但過期 → 從最後一筆的當月開始補抓，合併後存檔
     * - 無快取 → 全量下載 [months] 個月
     * 先試 TWSE（上市），若無資料改試 TPEX（上櫃）。
     */
    suspend fun fetchHistorical(code: String, months: Int = 6): List<HistoricalBar> =
        withContext(Dispatchers.IO) {
            // 快取新鮮 → 直接回傳，不需要任何 HTTP 請求
            loadCache(code)?.let { return@withContext it }

            val today = LocalDate.now()
            // 讀取舊快取作為補檔基底（即使不新鮮也保留歷史資料）
            val stale = readCacheRaw(code)

            // 決定補抓起始月份：有舊快取從最後一筆當月補；否則全量下載
            val fetchFrom = if (stale != null) {
                stale.last().date.withDayOfMonth(1)
            } else {
                today.minusMonths(months.toLong())
            }
            val monthsToFetch = (today.year - fetchFrom.year) * 12 +
                                 (today.monthValue - fetchFrom.monthValue)

            val newBars = mutableListOf<HistoricalBar>()
            for (m in monthsToFetch downTo 0) {
                val date  = today.minusMonths(m.toLong())
                val ym    = date.format(DateTimeFormatter.ofPattern("yyyyMM"))
                val rocY  = date.year - 1911
                val rocMM = date.monthValue.toString().padStart(2, '0')

                val twse = fetchTwseMonth(code, ym)
                if (twse != null) {
                    newBars += twse
                } else {
                    val tpex = fetchTpexMonth(code, rocY, rocMM)
                    if (tpex != null) newBars += tpex
                }
                delay(150)  // 避免觸發 TWSE rate limit
            }

            // 合併舊快取 + 新抓資料，去重排序後存檔（含假日無新資料的情況，仍更新 mtime）
            val result = ((stale ?: emptyList()) + newBars)
                .sortedBy { it.date }.distinctBy { it.date }
            if (result.isNotEmpty()) saveCache(code, result)
            result
        }

    private suspend fun fetchTwseMonth(code: String, ym: String): List<HistoricalBar>? =
        withContext(Dispatchers.IO) {
            try {
                val url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY" +
                          "?response=json&date=${ym}01&stockNo=$code"
                val body = get(url) ?: return@withContext null
                val root = json.parseToJsonElement(body).jsonObject
                if (root["stat"]?.jsonPrimitive?.content != "OK") return@withContext null
                val data = root["data"]?.jsonArray ?: return@withContext null

                data.mapNotNull { row ->
                    try {
                        val r = row.jsonArray
                        val parts = r[0].jsonPrimitive.content.split("/")
                        val year  = parts[0].toInt() + 1911
                        val date  = LocalDate.of(year, parts[1].toInt(), parts[2].toInt())
                        HistoricalBar(
                            date        = date,
                            open        = cleanNum(r[3].jsonPrimitive.content),
                            high        = cleanNum(r[4].jsonPrimitive.content),
                            low         = cleanNum(r[5].jsonPrimitive.content),
                            close       = cleanNum(r[6].jsonPrimitive.content),
                            volumeLots  = (cleanNum(r[1].jsonPrimitive.content) / 1000).toLong(),
                        )
                    } catch (_: Exception) { null }
                }
            } catch (_: Exception) { null }
        }

    private suspend fun fetchTpexMonth(code: String, rocYear: Int, rocMM: String): List<HistoricalBar>? =
        withContext(Dispatchers.IO) {
            try {
                val d   = "$rocYear/$rocMM"
                val url = "https://www.tpex.org.tw/web/stock/aftertrading/" +
                          "daily_trading_info/st43_result.php" +
                          "?l=zh-tw&d=$d&stkno=$code&_=1"
                val body = get(url) ?: return@withContext null
                val root = json.parseToJsonElement(body).jsonObject
                val data = root["aaData"]?.jsonArray ?: return@withContext null
                if (data.isEmpty()) return@withContext null

                data.mapNotNull { row ->
                    try {
                        val r = row.jsonArray
                        val parts = r[0].jsonPrimitive.content.split("/")
                        val year  = parts[0].toInt() + 1911
                        val date  = LocalDate.of(year, parts[1].toInt(), parts[2].toInt())
                        HistoricalBar(
                            date        = date,
                            open        = cleanNum(r[4].jsonPrimitive.content),
                            high        = cleanNum(r[5].jsonPrimitive.content),
                            low         = cleanNum(r[6].jsonPrimitive.content),
                            close       = cleanNum(r[7].jsonPrimitive.content),
                            volumeLots  = (cleanNum(r[1].jsonPrimitive.content) / 1000).toLong(),
                        )
                    } catch (_: Exception) { null }
                }
            } catch (_: Exception) { null }
        }

    // ── 即時行情（TWSE MIS API）───────────────────────────────────────

    /**
     * 用 TWSE MIS getStockInfo 一次取多支股票的即時快照。
     * 上市用 tse_XXXX.tw，上櫃用 otc_XXXX.tw；兩個都送，回傳有資料的那個。
     * 非交易時段 z 欄位為 "-"，此時改用昨收 y 作為現價。
     */
    suspend fun fetchRealtimePrice(codes: List<String>): Map<String, RealtimeQuote> =
        withContext(Dispatchers.IO) {
            if (codes.isEmpty()) return@withContext emptyMap()

            val result = mutableMapOf<String, RealtimeQuote>()
            // 每批最多 80 支（避免 URL 過長）
            codes.chunked(80).forEach { chunk ->
                val exCh = chunk.flatMap { listOf("tse_$it.tw", "otc_$it.tw") }
                    .joinToString("|")
                val url  = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp" +
                           "?ex_ch=$exCh&json=1&delay=0"
                try {
                    val body = get(url) ?: return@forEach
                    val root = json.parseToJsonElement(body).jsonObject
                    val msgs = root["msgArray"]?.jsonArray ?: return@forEach

                    for (item in msgs) {
                        try {
                            val obj  = item.jsonObject
                            val code = obj["c"]?.jsonPrimitive?.content ?: continue
                            val name = obj["n"]?.jsonPrimitive?.content ?: code

                            // z = 最新成交價，非交易時段可能是 "-"
                            val zRaw  = obj["z"]?.jsonPrimitive?.content ?: "-"
                            val yRaw  = obj["y"]?.jsonPrimitive?.content ?: "0"
                            val prevC = yRaw.toDoubleOrNull() ?: continue
                            val price = if (zRaw == "-" || zRaw.isBlank()) prevC
                                        else zRaw.toDoubleOrNull() ?: prevC

                            val open  = obj["o"]?.jsonPrimitive?.content?.toDoubleOrNull() ?: price
                            val high  = obj["h"]?.jsonPrimitive?.content?.toDoubleOrNull() ?: price
                            val low   = obj["l"]?.jsonPrimitive?.content?.toDoubleOrNull() ?: price
                            val vol   = obj["v"]?.jsonPrimitive?.content?.toLongOrNull() ?: 0L
                            val chg   = if (prevC > 0) (price - prevC) / prevC * 100.0 else 0.0

                            result[code] = RealtimeQuote(
                                code        = code,
                                name        = name,
                                price       = price,
                                open        = open,
                                high        = high,
                                low         = low,
                                prevClose   = prevC,
                                chgPct      = chg,
                                totalVolLots = vol,
                                updateTime  = LocalTime.now()
                                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                            )
                        } catch (_: Exception) { }
                    }
                } catch (_: Exception) { }
                delay(200)
            }
            result
        }

    // ── 全市場股票代號 ────────────────────────────────────────────────

    /**
     * 取得全市場上市/上櫃股票代號 → 公司名稱對應表。
     * 上市來源：TWSE BWIBBU_d（欄 0=代號, 1=名稱）。
     * 上櫃來源：TPEX openapi peratio_analysis（SecuritiesCompanyCode / CompanyName）。
     * 只保留 4 位純數字代號（排除 ETF、權證）。
     */
    suspend fun fetchAllCodesWithNames(): Map<String, String> = withContext(Dispatchers.IO) {
        val result = mutableMapOf<String, String>()

        // 上市（TWSE）
        try {
            val url  = "https://www.twse.com.tw/exchangeReport/BWIBBU_d" +
                       "?response=json&selectType=ALL"
            val body = get(url) ?: return@withContext emptyMap()
            val root = json.parseToJsonElement(body).jsonObject
            if (root["stat"]?.jsonPrimitive?.content == "OK") {
                root["data"]?.jsonArray?.forEach { row ->
                    val r    = row.jsonArray
                    val code = r.getOrNull(0)?.jsonPrimitive?.content?.trim() ?: return@forEach
                    val name = r.getOrNull(1)?.jsonPrimitive?.content?.trim() ?: code
                    if (code.matches(Regex("\\d{4}"))) result[code] = name
                }
            }
        } catch (_: Exception) {}

        // 上櫃（TPEX openapi）
        try {
            val url  = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
            val body = get(url) ?: return@withContext result
            val arr  = json.parseToJsonElement(body).jsonArray
            arr.forEach { item ->
                val obj  = item.jsonObject
                val code = obj["SecuritiesCompanyCode"]?.jsonPrimitive?.content?.trim()
                    ?: return@forEach
                val name = obj["CompanyName"]?.jsonPrimitive?.content?.trim() ?: code
                if (code.matches(Regex("\\d{4}"))) result[code] = name
            }
        } catch (_: Exception) {}

        result
    }

    // ── 工具 ─────────────────────────────────────────────────────────

    private fun get(url: String): String? = try {
        val req = Request.Builder().url(url)
            .header("User-Agent", "Mozilla/5.0")
            .build()
        client.newCall(req).execute().use { it.body?.string() }
    } catch (_: Exception) { null }

    /** 清除千分位逗號與停牌標記 X，回傳 Double */
    private fun cleanNum(s: String): Double =
        s.replace(",", "").replace("X", "").toDoubleOrNull() ?: 0.0
}
