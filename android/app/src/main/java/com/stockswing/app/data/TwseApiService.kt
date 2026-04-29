package com.stockswing.app.data

import android.util.Log
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

private const val TAG = "StockApp"

class TwseApiService(private val cacheDir: File? = null) {

    init {
        migrateLegacyCache()
    }

    private val client = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .build()

    private val json = Json { ignoreUnknownKeys = true }

    // ── K 棒磁碟快取 ─────────────────────────────────────────────────

    /**
     * 一次性搬遷：將舊格式 kbar/{date}/{code}.csv 搬到 kbar/{code}.csv。
     * 只取最新日期目錄，不覆蓋已存在的新格式快取。
     */
    private fun migrateLegacyCache() {
        val base = cacheDir ?: return
        val kbarDir = File(base, "kbar")
        if (!kbarDir.exists()) return

        val latestDateDir = kbarDir.listFiles()
            ?.filter { it.isDirectory && it.name.matches(Regex("\\d{4}-\\d{2}-\\d{2}")) }
            ?.maxByOrNull { it.name }
            ?: return

        var migrated = 0
        latestDateDir.listFiles()?.forEach { src ->
            if (!src.name.endsWith(".csv")) return@forEach
            val dest = File(kbarDir, src.name)
            if (!dest.exists()) {
                try {
                    src.copyTo(dest)
                    dest.setLastModified(src.lastModified())  // 保留原始 mtime 讓新鮮度判斷正確
                    migrated++
                } catch (e: Exception) {
                    Log.w(TAG, "migration failed ${src.name}: $e")
                }
            }
        }
        if (migrated > 0)
            Log.i(TAG, "migrateLegacyCache: $migrated files from ${latestDateDir.name}")
        else
            Log.d(TAG, "migrateLegacyCache: nothing to migrate from ${latestDateDir.name}")
    }

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
        } catch (e: Exception) {
            Log.w(TAG, "readCacheRaw[$code] parse error: $e")
            null
        }
    }

    /**
     * 讀取快取並檢查新鮮度。
     * 盤後（台灣時間 >=14:30）若不含今日 K 棒且檔案今日尚未更新，回傳 null 觸發補檔。
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
                if (fileDate < today) {
                    Log.d(TAG, "cache[$code] stale: lastBar=${bars.last().date}, fileDate=$fileDate -> refetch")
                    return null
                }
                Log.d(TAG, "cache[$code] afterClose no today bar but file updated today (holiday) -> accept ${bars.size} bars")
            }
        }
        Log.d(TAG, "cache[$code] hit: ${bars.size} bars, last=${bars.last().date}")
        return bars
    }

    private fun saveCache(code: String, bars: List<HistoricalBar>) {
        val file = kbarCacheFile(code) ?: return
        try {
            file.writeText(bars.joinToString("\n") { b ->
                "${b.date}|${b.open}|${b.high}|${b.low}|${b.close}|${b.volumeLots}"
            })
            Log.d(TAG, "cache[$code] saved: ${bars.size} bars, last=${bars.last().date}")
        } catch (e: Exception) {
            Log.w(TAG, "cache[$code] save failed: $e")
        }
    }

    // ── 歷史日K ──────────────────────────────────────────────────────

    /**
     * 取得日K，優先使用磁碟快取，只補抓缺少的月份。
     * - 快取新鮮 -> 直接回傳
     * - 快取存在但過期 -> 從最後一筆的當月開始補抓，合併後存檔
     * - 無快取 -> 全量下載 [months] 個月
     * 先試 TWSE（上市），若無資料改試 TPEX（上櫃）。
     */
    suspend fun fetchHistorical(code: String, months: Int = 6): List<HistoricalBar> =
        withContext(Dispatchers.IO) {
            // 快取新鮮 -> 直接回傳，不需要任何 HTTP 請求
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

            Log.d(TAG, "fetchHistorical[$code] stale=${stale?.size ?: "none"}, fetchFrom=$fetchFrom, monthsToFetch=$monthsToFetch")

            val newBars = mutableListOf<HistoricalBar>()
            for (m in monthsToFetch downTo 0) {
                val date  = today.minusMonths(m.toLong())
                val ym    = date.format(DateTimeFormatter.ofPattern("yyyyMM"))
                val rocY  = date.year - 1911
                val rocMM = date.monthValue.toString().padStart(2, '0')

                val twse = fetchTwseMonth(code, ym)
                if (twse != null) {
                    Log.d(TAG, "  twse[$code][$ym] -> ${twse.size} bars")
                    newBars += twse
                } else {
                    val tpex = fetchTpexMonth(code, rocY, rocMM)
                    if (tpex != null) {
                        Log.d(TAG, "  tpex[$code][$rocY/$rocMM] -> ${tpex.size} bars")
                        newBars += tpex
                    } else {
                        Log.d(TAG, "  both miss[$code][$ym]")
                    }
                }
                delay(150)  // 避免觸發 TWSE rate limit
            }

            // 合併舊快取 + 新抓資料，去重排序後存檔（含假日無新資料的情況，仍更新 mtime）
            val result = ((stale ?: emptyList()) + newBars)
                .sortedBy { it.date }.distinctBy { it.date }
            Log.d(TAG, "fetchHistorical[$code] done: ${result.size} bars total (new=${newBars.size})")
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
                val stat = root["stat"]?.jsonPrimitive?.content
                if (stat != "OK") {
                    Log.d(TAG, "twse[$code][$ym] stat=$stat")
                    return@withContext null
                }
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
            } catch (e: Exception) {
                Log.w(TAG, "twse[$code][$ym] exception: $e")
                null
            }
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
            } catch (e: Exception) {
                Log.w(TAG, "tpex[$code][$rocYear/$rocMM] exception: $e")
                null
            }
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
                } catch (e: Exception) {
                    Log.w(TAG, "fetchRealtimePrice batch exception: $e")
                }
                delay(200)
            }
            result
        }

    // ── 全市場股票代號 ────────────────────────────────────────────────

    /**
     * 取得全市場上市/上櫃股票代號 -> 公司名稱對應表。
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
            Log.d(TAG, "fetchAllCodes: TWSE request start")
            val body = get(url)
            if (body == null) {
                Log.w(TAG, "fetchAllCodes: TWSE body null (network error?)")
            } else {
                val root = json.parseToJsonElement(body).jsonObject
                val stat = root["stat"]?.jsonPrimitive?.content
                if (stat == "OK") {
                    root["data"]?.jsonArray?.forEach { row ->
                        val r    = row.jsonArray
                        val code = r.getOrNull(0)?.jsonPrimitive?.content?.trim() ?: return@forEach
                        val name = r.getOrNull(1)?.jsonPrimitive?.content?.trim() ?: code
                        if (code.matches(Regex("\\d{4}"))) result[code] = name
                    }
                    Log.d(TAG, "fetchAllCodes: TWSE OK, listed=${result.size}")
                } else {
                    Log.w(TAG, "fetchAllCodes: TWSE stat=$stat body(100)=${body.take(100)}")
                }
            }
        } catch (e: Exception) {
            Log.e(TAG, "fetchAllCodes: TWSE exception: $e")
        }

        val twseCount = result.size

        // 上櫃（TPEX openapi）
        try {
            val url  = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
            Log.d(TAG, "fetchAllCodes: TPEX request start")
            val body = get(url)
            if (body == null) {
                Log.w(TAG, "fetchAllCodes: TPEX body null (network error?)")
            } else {
                val arr = json.parseToJsonElement(body).jsonArray
                arr.forEach { item ->
                    val obj  = item.jsonObject
                    val code = obj["SecuritiesCompanyCode"]?.jsonPrimitive?.content?.trim()
                        ?: return@forEach
                    val name = obj["CompanyName"]?.jsonPrimitive?.content?.trim() ?: code
                    if (code.matches(Regex("\\d{4}"))) result[code] = name
                }
                Log.d(TAG, "fetchAllCodes: TPEX OK, otc=${result.size - twseCount}, total=${result.size}")
            }
        } catch (e: Exception) {
            Log.e(TAG, "fetchAllCodes: TPEX exception: $e")
        }

        if (result.isEmpty()) Log.e(TAG, "fetchAllCodes: BOTH sources failed, returning empty map")
        result
    }

    // ── 工具 ─────────────────────────────────────────────────────────

    private fun get(url: String): String? = try {
        val req = Request.Builder().url(url)
            .header("User-Agent", "Mozilla/5.0")
            .build()
        val resp = client.newCall(req).execute()
        val code = resp.code
        val body = resp.use { it.body?.string() }
        if (code != 200) Log.w(TAG, "HTTP $code for $url")
        body
    } catch (e: Exception) {
        Log.w(TAG, "get() failed url=$url err=$e")
        null
    }

    /** 清除千分位逗號與停牌標記 X，回傳 Double */
    private fun cleanNum(s: String): Double =
        s.replace(",", "").replace("X", "").toDoubleOrNull() ?: 0.0
}
