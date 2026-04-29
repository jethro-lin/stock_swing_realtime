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

    /** 快取目錄：{cacheDir}/kbar/{today}/ */
    private fun kbarCacheDir(): File? {
        val base = cacheDir ?: return null
        return File(base, "kbar/${LocalDate.now()}").also { it.mkdirs() }
    }

    private fun loadCache(code: String): List<HistoricalBar>? {
        val dir = kbarCacheDir() ?: return null
        val file = File(dir, "$code.csv")
        if (!file.exists()) return null
        val bars = try {
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
            }.ifEmpty { return null }
        } catch (_: Exception) { return null }

        // 盤後（台灣時間 14:30 後）若快取不含今日 K 棒，強制重新下載
        val nowTpe = java.time.ZonedDateTime.now(java.time.ZoneId.of("Asia/Taipei"))
        val isAfterClose = nowTpe.hour > 14 || (nowTpe.hour == 14 && nowTpe.minute >= 30)
        if (isAfterClose) {
            val today = nowTpe.toLocalDate()
            if (bars.none { it.date == today }) return null
        }
        return bars
    }

    private fun saveCache(code: String, bars: List<HistoricalBar>) {
        val dir = kbarCacheDir() ?: return
        try {
            File(dir, "$code.csv").writeText(
                bars.joinToString("\n") { b ->
                    "${b.date}|${b.open}|${b.high}|${b.low}|${b.close}|${b.volumeLots}"
                }
            )
        } catch (_: Exception) {}
    }

    // ── 歷史日K ──────────────────────────────────────────────────────

    /**
     * 抓最近 [months] 個月的日K。快取命中則直接回傳，否則從 TWSE/TPEX 下載並寫入快取。
     * 先試 TWSE（上市），若無資料改試 TPEX（上櫃）。
     * 回傳以日期升冪排序、去重的 bar list。
     */
    suspend fun fetchHistorical(code: String, months: Int = 6): List<HistoricalBar> =
        withContext(Dispatchers.IO) {
            loadCache(code)?.let { return@withContext it }

            val today = LocalDate.now()
            val bars  = mutableListOf<HistoricalBar>()

            for (m in months downTo 0) {
                val date  = today.minusMonths(m.toLong())
                val ym    = date.format(DateTimeFormatter.ofPattern("yyyyMM"))
                val rocY  = date.year - 1911
                val rocMM = date.monthValue.toString().padStart(2, '0')

                val twse = fetchTwseMonth(code, ym)
                if (twse != null) {
                    bars += twse
                } else {
                    val tpex = fetchTpexMonth(code, rocY, rocMM)
                    if (tpex != null) bars += tpex
                }
                delay(150)  // 避免觸發 TWSE rate limit
            }

            val result = bars.sortedBy { it.date }.distinctBy { it.date }
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
