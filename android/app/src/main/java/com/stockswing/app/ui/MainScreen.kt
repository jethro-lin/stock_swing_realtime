@file:OptIn(androidx.compose.foundation.layout.ExperimentalLayoutApi::class)

package com.stockswing.app.ui

import android.Manifest
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.content.ContextCompat
import com.google.android.gms.ads.AdListener
import com.google.android.gms.ads.AdRequest
import com.google.android.gms.ads.AdSize
import com.google.android.gms.ads.AdView
import com.google.android.gms.ads.LoadAdError
import com.stockswing.app.BuildConfig
import com.stockswing.app.StockViewModel
import com.stockswing.app.model.Preset
import com.stockswing.app.model.SignalResult
import java.time.DayOfWeek
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

private val GreenStrong = Color(0xFF00C853)
private val RedStrong   = Color(0xFFD50000)
private val GrayDot     = Color(0xFFBDBDBD)

// 台灣慣例：漲 = 紅，跌 = 綠
private val ColorUp   = RedStrong
private val ColorDown = GreenStrong

// ── 掃描模式 ──────────────────────────────────────────────────────
private enum class ScanMode { PRESET, CUSTOM }

// 個別訊號定義：code to 中文說明
private val LONG_SIGNAL_DEFS = listOf(
    "A"  to "均線多頭排列",
    "B"  to "跳空向上(≥2%)",
    "C"  to "RSI 超賣反彈",
    "D"  to "突破前5日高點",
    "E"  to "強勢連漲",
    "F"  to "量能擴張上漲",
    "G"  to "縮量後上漲",
    "H"  to "鎚子K線",
    "I"  to "吞噬陽線",
    "J"  to "MACD 黃金交叉",
    "K"  to "布林下軌反彈",
    "L"  to "KD 超賣交叉",
    "M"  to "W%R 超賣反彈",
    "N"  to "均線回測站回",
    "O"  to "晨星型態",
    "P"  to "紅三兵",
    "Q"  to "Inside Bar 突破",
    "R"  to "BIAS 超跌反彈",
    "B2" to "大跳空(≥5%)+量比",
)
private val SHORT_SIGNAL_DEFS = listOf(
    "AS" to "均線空頭排列",
    "BS" to "跳空向下(≥2%)",
    "CS" to "RSI 超買回落",
    "DS" to "跌破前5日低點",
    "ES" to "弱勢連跌",
    "FS" to "量能萎縮下跌",
    "GS" to "縮量後急跌",
    "HS" to "射擊之星",
    "IS" to "吞噬陰線",
    "JS" to "MACD 死亡交叉",
    "KS" to "布林上軌反壓",
    "LS" to "KD 超買交叉",
    "MS" to "W%R 超買回落",
    "NS" to "均線回測跌破",
    "OS" to "黃昏之星型態",
    "PS" to "黑三兵",
    "QS" to "Inside Bar 跌破",
    "RS" to "BIAS 超漲回落",
)

// ── 價位分群 ──────────────────────────────────────────────────────
private enum class PriceGroup(val label: String) {
    ALL("全部"), P50("< 50"), P50_100("50~100"), P100_300("100~300"),
    P300_500("300~500"), P500_1000("500~1000"), P1000("> 1000")
}

private fun SignalResult.priceGroup(): PriceGroup = when {
    quote.price < 50   -> PriceGroup.P50
    quote.price < 100  -> PriceGroup.P50_100
    quote.price < 300  -> PriceGroup.P100_300
    quote.price < 500  -> PriceGroup.P300_500
    quote.price < 1000 -> PriceGroup.P500_1000
    else               -> PriceGroup.P1000
}

// ── 主畫面 ────────────────────────────────────────────────────────
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainScreen(vm: StockViewModel) {
    val selectedPresets     by vm.selectedPresets.collectAsState()
    val lastScanMode        by vm.lastScanMode.collectAsState()
    val selectedCustomSigs  by vm.selectedCustomSignals.collectAsState()
    val isLoading           by vm.isLoading.collectAsState()
    val loadingMsg          by vm.loadingMsg.collectAsState()
    val scanProgress        by vm.scanProgress.collectAsState()
    val error               by vm.error.collectAsState()
    val results             by vm.scanResults.collectAsState()
    val lastScanTime        by vm.lastScanTime.collectAsState()
    val signalDate          by vm.signalDate.collectAsState()
    val chartTarget         by vm.chartTarget.collectAsState()
    val chartBars           by vm.chartBars.collectAsState()

    var selectedTab         by remember { mutableIntStateOf(0) }
    val groups               = PriceGroup.entries

    var showScanDialog      by remember { mutableStateOf(false) }

    // 掃描出結果後才詢問通知權限（比冷啟動就彈更符合 Play Store 政策）
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
        val context = LocalContext.current
        val notifLauncher = rememberLauncherForActivityResult(
            ActivityResultContracts.RequestPermission()
        ) {}
        LaunchedEffect(results.isNotEmpty()) {
            if (results.isNotEmpty() &&
                ContextCompat.checkSelfPermission(
                    context, Manifest.permission.POST_NOTIFICATIONS
                ) != PackageManager.PERMISSION_GRANTED
            ) {
                notifLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
    }

    Scaffold(
        bottomBar = { BannerAd() },
        topBar = {
            TopAppBar(
                title = { Text("台股選股", fontWeight = FontWeight.Bold) },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer
                ),
                actions = {
                    if (isLoading) {
                        OutlinedButton(
                            onClick        = { vm.stopScan() },
                            contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp),
                            modifier       = Modifier.padding(end = 8.dp),
                        ) { Text("停止") }
                    } else {
                        Button(
                            onClick        = { showScanDialog = true },
                            contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp),
                            modifier       = Modifier.padding(end = 8.dp),
                        ) { Text("掃描") }
                    }
                }
            )
        }
    ) { padding ->
        Column(Modifier.padding(padding)) {

            // ── 進度 / 狀態列 ─────────────────────────────────────────
            if (isLoading) {
                val (done, total) = scanProgress
                Column(
                    Modifier
                        .fillMaxWidth()
                        .background(MaterialTheme.colorScheme.secondaryContainer)
                        .padding(horizontal = 12.dp, vertical = 6.dp),
                ) {
                    Row(
                        Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment     = Alignment.CenterVertically,
                    ) {
                        Text(loadingMsg, fontSize = 12.sp,
                            maxLines = 1, overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.weight(1f))
                        if (total > 0) {
                            Text("$done/$total", fontSize = 11.sp, color = Color.Gray)
                        }
                    }
                    if (total > 0) {
                        Spacer(Modifier.height(4.dp))
                        LinearProgressIndicator(
                            progress = { done.toFloat() / total },
                            modifier = Modifier.fillMaxWidth(),
                        )
                    } else {
                        LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                    }
                }
            }

            // ── 錯誤訊息 ──────────────────────────────────────────────
            error?.let { msg ->
                Text(
                    msg,
                    color    = RedStrong,
                    fontSize = 12.sp,
                    modifier = Modifier
                        .fillMaxWidth()
                        .background(Color(0xFFFFEBEE))
                        .padding(horizontal = 12.dp, vertical = 6.dp),
                )
            }

            // ── 掃描資訊列 ────────────────────────────────────────────
            if (!isLoading && lastScanTime.isNotEmpty()) {
                Row(
                    Modifier
                        .fillMaxWidth()
                        .background(MaterialTheme.colorScheme.surfaceVariant)
                        .padding(horizontal = 12.dp, vertical = 4.dp),
                    horizontalArrangement = Arrangement.spacedBy(16.dp),
                    verticalAlignment     = Alignment.CenterVertically,
                ) {
                    if (signalDate.isNotEmpty()) {
                        Text("訊號日", fontSize = 11.sp, color = Color.Gray)
                        Text(signalDate, fontSize = 12.sp, fontWeight = FontWeight.SemiBold)
                    }
                    Spacer(Modifier.weight(1f))
                    Text("掃描", fontSize = 11.sp, color = Color.Gray)
                    Text(lastScanTime, fontSize = 12.sp, color = Color.Gray)
                }
            }

            // ── 空提示 ────────────────────────────────────────────────
            if (!isLoading && results.isEmpty() && error == null) {
                Box(Modifier.fillMaxSize(), Alignment.Center) {
                    Text(
                        if (lastScanTime.isEmpty()) "請點「掃描」開始選股"
                        else "本次掃描無符合標的",
                        color    = Color.Gray,
                        fontSize = 14.sp,
                    )
                }
                return@Scaffold
            }

            // ── 價位分頁 ──────────────────────────────────────────────
            ScrollableTabRow(
                selectedTabIndex = selectedTab,
                edgePadding      = 0.dp,
            ) {
                groups.forEachIndexed { i, group ->
                    val count = if (group == PriceGroup.ALL) results.size
                                else results.count { it.priceGroup() == group }
                    Tab(
                        selected = selectedTab == i,
                        onClick  = { selectedTab = i },
                        text     = {
                            Text(
                                if (count > 0) "${group.label} ($count)" else group.label,
                                fontSize = 12.sp,
                            )
                        }
                    )
                }
            }

            val tabItems = if (groups[selectedTab] == PriceGroup.ALL) results
                           else results.filter { it.priceGroup() == groups[selectedTab] }

            if (tabItems.isEmpty()) {
                Box(Modifier.fillMaxSize(), Alignment.Center) {
                    Text("此價位區間無標的", color = Color.Gray, fontSize = 14.sp)
                }
            } else {
                StockListHeader()
                HorizontalDivider()
                LazyColumn {
                    items(tabItems, key = { it.code }) { result ->
                        StockRow(result, onTap = { vm.openChart(result) })
                        HorizontalDivider(color = Color(0xFFEEEEEE))
                    }
                }
            }
        }
    }

    // ── 日K圖表 Sheet ────────────────────────────────────────────
    if (chartTarget != null) {
        KBarChartSheet(
            result    = chartTarget!!,
            bars      = chartBars,
            onDismiss = { vm.closeChart() },
        )
    }

    // ── 掃描設定 Dialog ───────────────────────────────────────────
    if (showScanDialog) {
        ScanDialog(
            initialMode        = if (lastScanMode == "custom") ScanMode.CUSTOM else ScanMode.PRESET,
            initialPresets     = selectedPresets,
            initialCustomSigs  = selectedCustomSigs,
            onDismiss          = { showScanDialog = false },
            onConfirm          = { presets, customSigs, date ->
                showScanDialog = false
                vm.scan(presets, customSigs, date)
            },
        )
    }
}

// ── 掃描設定 Dialog ───────────────────────────────────────────────
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun ScanDialog(
    initialMode:       ScanMode,
    initialPresets:    Set<Preset>,
    initialCustomSigs: Set<String>,
    onDismiss:         () -> Unit,
    onConfirm:         (Set<Preset>, Set<String>, LocalDate?) -> Unit,
) {
    var scanMode        by remember { mutableStateOf(initialMode) }
    var dialogPresets   by remember { mutableStateOf(initialPresets) }
    var customSigs      by remember { mutableStateOf(initialCustomSigs) }
    var useCustomDate   by remember { mutableStateOf(false) }
    var customDate      by remember { mutableStateOf<LocalDate?>(null) }
    var showDatePicker  by remember { mutableStateOf(false) }

    val tpe            = ZoneId.of("Asia/Taipei")
    val lastTradeDate  = remember { lastCompleteTradeDate(ZonedDateTime.now(tpe)) }
    val dateFmt        = remember { DateTimeFormatter.ofPattern("MM/dd") }

    val canConfirm = when (scanMode) {
        ScanMode.PRESET -> dialogPresets.isNotEmpty()
        ScanMode.CUSTOM -> customSigs.isNotEmpty()
    }

    AlertDialog(
        onDismissRequest = onDismiss,
        title            = { Text("掃描設定", fontWeight = FontWeight.Bold) },
        text = {
            Column(
                modifier = Modifier.verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(12.dp),
            ) {

                // ── 模式切換 ──────────────────────────────────────
                Text("策略模式", fontSize = 13.sp, color = Color.Gray)
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    FilterChip(
                        selected = scanMode == ScanMode.PRESET,
                        onClick  = { scanMode = ScanMode.PRESET },
                        label    = { Text("預設組合", fontSize = 13.sp) },
                    )
                    FilterChip(
                        selected = scanMode == ScanMode.CUSTOM,
                        onClick  = { scanMode = ScanMode.CUSTOM },
                        label    = { Text("自選訊號", fontSize = 13.sp) },
                    )
                }

                HorizontalDivider()

                if (scanMode == ScanMode.PRESET) {
                    // ── 預設組合（多選） ──────────────────────────
                    Text("選擇組合", fontSize = 13.sp, color = Color.Gray)
                    FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Preset.entries.forEach { preset ->
                            val selected = preset in dialogPresets
                            FilterChip(
                                selected = selected,
                                onClick  = {
                                    dialogPresets = if (selected)
                                        dialogPresets - preset
                                    else
                                        dialogPresets + preset
                                },
                                label  = { Text(preset.label, fontSize = 13.sp) },
                                colors = FilterChipDefaults.filterChipColors(
                                    selectedContainerColor = when (preset) {
                                        Preset.LONG3_LEAN, Preset.LONG_TREND ->
                                            GreenStrong.copy(alpha = 0.15f)
                                        Preset.SHORT3_LEAN ->
                                            RedStrong.copy(alpha = 0.15f)
                                    }
                                )
                            )
                        }
                    }
                } else {
                    // ── 自選訊號（多選） ──────────────────────────
                    Row(
                        Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.SpaceBetween,
                        verticalAlignment     = Alignment.CenterVertically,
                    ) {
                        Text("多方訊號", fontSize = 13.sp, color = Color.Gray)
                        if (customSigs.isNotEmpty()) {
                            TextButton(
                                onClick      = { customSigs = emptySet() },
                                contentPadding = PaddingValues(horizontal = 8.dp, vertical = 0.dp),
                            ) { Text("清除", fontSize = 12.sp) }
                        }
                    }
                    FlowRow(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                        LONG_SIGNAL_DEFS.forEach { (sig, desc) ->
                            val selected = sig in customSigs
                            FilterChip(
                                selected = selected,
                                onClick  = {
                                    customSigs = if (selected) customSigs - sig else customSigs + sig
                                },
                                label  = { Text("$sig  $desc", fontSize = 12.sp) },
                                colors = FilterChipDefaults.filterChipColors(
                                    selectedContainerColor = GreenStrong.copy(alpha = 0.15f),
                                    selectedLabelColor     = GreenStrong,
                                ),
                            )
                        }
                    }

                    Text("空方訊號", fontSize = 13.sp, color = Color.Gray)
                    FlowRow(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                        SHORT_SIGNAL_DEFS.forEach { (sig, desc) ->
                            val selected = sig in customSigs
                            FilterChip(
                                selected = selected,
                                onClick  = {
                                    customSigs = if (selected) customSigs - sig else customSigs + sig
                                },
                                label  = { Text("$sig  $desc", fontSize = 12.sp) },
                                colors = FilterChipDefaults.filterChipColors(
                                    selectedContainerColor = RedStrong.copy(alpha = 0.15f),
                                    selectedLabelColor     = RedStrong,
                                ),
                            )
                        }
                    }
                }

                HorizontalDivider()

                // ── 訊號日選擇 ────────────────────────────────────
                Text("訊號日", fontSize = 13.sp, color = Color.Gray)
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    FilterChip(
                        selected = !useCustomDate,
                        onClick  = { useCustomDate = false; customDate = null },
                        label    = { Text(lastTradeDate.format(dateFmt), fontSize = 13.sp) },
                    )
                    FilterChip(
                        selected = useCustomDate,
                        onClick  = {
                            useCustomDate = true
                            if (customDate == null) customDate = lastTradeDate
                            showDatePicker = true
                        },
                        label    = {
                            Text(
                                customDate?.format(dateFmt) ?: "指定日期",
                                fontSize = 13.sp,
                            )
                        },
                    )
                }
            }
        },
        confirmButton = {
            Button(
                enabled = canConfirm,
                onClick = {
                    val finalPresets = if (scanMode == ScanMode.PRESET) dialogPresets else emptySet()
                    val finalSigs    = if (scanMode == ScanMode.CUSTOM) customSigs else emptySet()
                    onConfirm(finalPresets, finalSigs, if (useCustomDate) customDate else null)
                },
            ) { Text("開始掃描") }
        },
        dismissButton = {
            TextButton(onClick = onDismiss) { Text("取消") }
        },
    )

    // ── 日期選擇器 ────────────────────────────────────────────────
    if (showDatePicker) {
        val maxMillis = lastTradeDate.atStartOfDay(tpe).toInstant().toEpochMilli()
        val dpState = rememberDatePickerState(
            initialSelectedDateMillis = (customDate ?: lastTradeDate)
                .atStartOfDay(tpe).toInstant().toEpochMilli(),
            selectableDates = object : SelectableDates {
                override fun isSelectableDate(utcTimeMillis: Long): Boolean {
                    if (utcTimeMillis > maxMillis) return false
                    val dow = Instant.ofEpochMilli(utcTimeMillis).atZone(tpe).dayOfWeek
                    return dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY
                }
                override fun isSelectableYear(year: Int) = year <= lastTradeDate.year
            }
        )
        DatePickerDialog(
            onDismissRequest = { showDatePicker = false },
            confirmButton    = {
                TextButton(onClick = {
                    dpState.selectedDateMillis?.let { millis ->
                        customDate = Instant.ofEpochMilli(millis)
                            .atZone(tpe)
                            .toLocalDate()
                    }
                    showDatePicker = false
                }) { Text("確定") }
            },
            dismissButton = {
                TextButton(onClick = { showDatePicker = false }) { Text("取消") }
            },
        ) {
            DatePicker(state = dpState)
        }
    }
}

// ── 欄位標頭 ──────────────────────────────────────────────────────
@Composable
private fun StockListHeader() {
    Row(
        Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(horizontal = 8.dp, vertical = 4.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Spacer(Modifier.width(14.dp))
        Text("代號/名稱", fontSize = 11.sp, color = Color.Gray,
            modifier = Modifier.weight(1.8f))
        Text("現價", fontSize = 11.sp, color = Color.Gray,
            modifier = Modifier.weight(1f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
        Text("漲跌幅", fontSize = 11.sp, color = Color.Gray,
            modifier = Modifier.weight(1.1f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
        Text("成交量", fontSize = 11.sp, color = Color.Gray,
            modifier = Modifier.weight(1.2f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
    }
}

// ── 個股行 ────────────────────────────────────────────────────────
@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun StockRow(result: SignalResult, onTap: () -> Unit = {}) {
    val quote      = result.quote
    val customHits = result.hitPresets["custom"]

    val isLong = if (customHits != null)
        customHits.any { !it.endsWith("S") || it == "B2" }
    else
        result.hitPresets.keys.any { it != Preset.SHORT3_LEAN.key }

    val isShort = if (customHits != null)
        customHits.any { it.endsWith("S") && it != "B2" }
    else
        result.hitPresets.containsKey(Preset.SHORT3_LEAN.key)

    val dotColor = when {
        isLong && isShort -> Color(0xFFFFA000)
        isLong            -> ColorUp
        isShort           -> ColorDown
        else              -> GrayDot
    }
    val pctColor = if (quote.chgPct >= 0) ColorUp else ColorDown
    val context  = LocalContext.current

    Column(
        Modifier
            .fillMaxWidth()
            .combinedClickable(
                onClick      = onTap,
                onLongClick  = {
                    val cm = context.getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
                    cm.setPrimaryClip(ClipData.newPlainText("stock_code", result.code))
                    Toast.makeText(context, "已複製 ${result.code}", Toast.LENGTH_SHORT).show()
                },
            )
            .padding(horizontal = 8.dp, vertical = 5.dp)
    ) {
        // ── 主資料行 ──────────────────────────────────────────────
        Row(verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(8.dp).background(dotColor, CircleShape))
            Spacer(Modifier.width(6.dp))

            Column(Modifier.weight(1.8f)) {
                Text(result.code, fontWeight = FontWeight.SemiBold, fontSize = 13.sp,
                    maxLines = 1)
                Text(result.name, fontSize = 15.sp, fontWeight = FontWeight.Medium,
                    maxLines = 1, overflow = TextOverflow.Ellipsis)
            }
            Text(
                "%.2f".format(quote.price),
                fontSize   = 13.sp,
                fontWeight = FontWeight.SemiBold,
                modifier   = Modifier.weight(1f),
                textAlign  = androidx.compose.ui.text.style.TextAlign.End,
            )
            Text(
                "%+.2f%%".format(quote.chgPct),
                fontSize  = 12.sp,
                color     = pctColor,
                modifier  = Modifier.weight(1.1f),
                textAlign = androidx.compose.ui.text.style.TextAlign.End,
            )
            Text(
                formatVol(quote.totalVolLots),
                fontSize  = 11.sp,
                color     = Color.Gray,
                modifier  = Modifier.weight(1.2f),
                textAlign = androidx.compose.ui.text.style.TextAlign.End,
            )
        }

        // ── MA 標注 ───────────────────────────────────────────────
        if (result.ma5 > 0) {
            MaRow(price = quote.price, ma5 = result.ma5, ma10 = result.ma10, ma20 = result.ma20)
        }

        // ── 命中標籤 ──────────────────────────────────────────────
        if (customHits != null) {
            // 自選模式：分多空兩群顯示命中訊號
            val longHits  = customHits.filter { !it.endsWith("S") || it == "B2" }
            val shortHits = customHits.filter { it.endsWith("S") && it != "B2" }
            if (longHits.isNotEmpty()) {
                SignalRow("多方", longHits, ColorUp)
            }
            if (shortHits.isNotEmpty()) {
                SignalRow("空方", shortHits, ColorDown)
            }
        } else {
            // 預設模式：每個 preset 一行
            result.hitPresets.forEach { (presetKey, combos) ->
                val preset = Preset.entries.find { it.key == presetKey } ?: return@forEach
                val color  = if (preset == Preset.SHORT3_LEAN) ColorDown else ColorUp
                SignalRow(preset.label, combos, color)
            }
        }
    }
}

@Composable
private fun MaRow(price: Double, ma5: Double, ma10: Double, ma20: Double) {
    Row(
        Modifier.padding(start = 20.dp, top = 2.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment     = Alignment.CenterVertically,
    ) {
        listOf("MA5" to ma5, "MA10" to ma10, "MA20" to ma20).forEach { (label, ma) ->
            val color = if (price >= ma) ColorUp else ColorDown
            Row(horizontalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(label, fontSize = 9.sp, color = color.copy(alpha = 0.7f))
                Text("%.1f".format(ma), fontSize = 9.sp, fontWeight = FontWeight.SemiBold, color = color)
            }
        }
    }
}

@Composable
private fun SignalRow(label: String, chips: List<String>, color: Color) {
    Row(
        Modifier.padding(start = 20.dp, top = 2.dp),
        horizontalArrangement = Arrangement.spacedBy(4.dp),
        verticalAlignment     = Alignment.CenterVertically,
    ) {
        Text(
            label,
            fontSize   = 10.sp,
            color      = color.copy(alpha = 0.7f),
            fontWeight = FontWeight.SemiBold,
        )
        chips.forEach { chip -> ComboChip(chip, color) }
    }
}

@Composable
private fun ComboChip(label: String, color: Color) {
    Surface(
        shape = RoundedCornerShape(4.dp),
        color = color.copy(alpha = 0.10f),
    ) {
        Text(
            label,
            fontSize   = 10.sp,
            color      = color,
            fontWeight = FontWeight.Medium,
            modifier   = Modifier.padding(horizontal = 4.dp, vertical = 1.dp),
        )
    }
}

// ── Banner 廣告 ────────────────────────────────────────────────
@Composable
private fun BannerAd() {
    val adUnitId = if (BuildConfig.DEBUG)
        "ca-app-pub-3940256099942544/6300978111"  // Google 官方測試 banner ID
    else
        "ca-app-pub-6931612619540388/3751631649"  // 正式 ID

    Box(
        modifier          = Modifier
            .fillMaxWidth()
            .height(50.dp)
            .background(Color(0xFFF5F5F5)),
        contentAlignment  = Alignment.Center,
    ) {
        AndroidView(
            modifier = Modifier.fillMaxSize(),
            factory  = { ctx ->
                AdView(ctx).apply {
                    setAdSize(AdSize.BANNER)
                    this.adUnitId = adUnitId
                    adListener = object : AdListener() {
                        override fun onAdLoaded() {
                            Log.d("AdMob", "banner loaded")
                        }
                        override fun onAdFailedToLoad(err: LoadAdError) {
                            Log.w("AdMob", "banner failed: ${err.code} ${err.message}")
                        }
                    }
                    loadAd(AdRequest.Builder().build())
                }
            },
        )
    }
}

// ── 工具 ─────────────────────────────────────────────────────────
private fun formatVol(lots: Long): String = when {
    lots >= 10_000L -> "${"%.1f".format(lots / 10_000.0)}萬"
    lots >= 1_000L  -> "${"%.1f".format(lots / 1_000.0)}千"
    else            -> "$lots"
}

/**
 * 最後一個完整交易日（略去假日，只排週末）：
 * - 台灣時間 14:30 後 → 今天（若為平日）
 * - 14:30 前 → 往回找最近的平日
 */
private fun lastCompleteTradeDate(now: ZonedDateTime): LocalDate {
    val isClosed = now.hour > 14 || (now.hour == 14 && now.minute >= 30)
    var candidate = if (isClosed) now.toLocalDate() else now.toLocalDate().minusDays(1)
    while (candidate.dayOfWeek == DayOfWeek.SATURDAY ||
           candidate.dayOfWeek == DayOfWeek.SUNDAY) {
        candidate = candidate.minusDays(1)
    }
    return candidate
}
