@file:OptIn(androidx.compose.foundation.layout.ExperimentalLayoutApi::class)

package com.stockswing.app.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.stockswing.app.StockViewModel
import com.stockswing.app.model.Preset
import com.stockswing.app.model.SignalResult

private val GreenStrong = Color(0xFF00C853)
private val RedStrong   = Color(0xFFD50000)
private val GrayDot     = Color(0xFFBDBDBD)

// ── 價位分群 ──────────────────────────────────────────────────────
private enum class PriceGroup(val label: String) {
    ALL("全部"), LOW("< 30"), MID_LOW("30~100"), MID_HIGH("100~500"), HIGH("> 500")
}

private fun SignalResult.priceGroup(): PriceGroup = when {
    quote.price < 30   -> PriceGroup.LOW
    quote.price < 100  -> PriceGroup.MID_LOW
    quote.price < 500  -> PriceGroup.MID_HIGH
    else               -> PriceGroup.HIGH
}

// ── 主畫面 ────────────────────────────────────────────────────────
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainScreen(vm: StockViewModel) {
    val selectedPresets by vm.selectedPresets.collectAsState()
    val isLoading       by vm.isLoading.collectAsState()
    val loadingMsg      by vm.loadingMsg.collectAsState()
    val scanProgress    by vm.scanProgress.collectAsState()
    val error           by vm.error.collectAsState()
    val results         by vm.scanResults.collectAsState()
    val lastScanTime    by vm.lastScanTime.collectAsState()

    var selectedTab by remember { mutableIntStateOf(0) }
    val groups       = PriceGroup.entries

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("台股選股", fontWeight = FontWeight.Bold) },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer
                ),
                actions = {
                    if (lastScanTime.isNotEmpty()) {
                        Text(lastScanTime, fontSize = 11.sp, color = Color.Gray,
                            modifier = Modifier.padding(end = 4.dp))
                    }
                    if (isLoading) {
                        OutlinedButton(
                            onClick        = { vm.stopScan() },
                            contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp),
                            modifier       = Modifier.padding(end = 8.dp),
                        ) { Text("停止") }
                    } else {
                        Button(
                            onClick        = { vm.scan() },
                            contentPadding = PaddingValues(horizontal = 12.dp, vertical = 6.dp),
                            modifier       = Modifier.padding(end = 8.dp),
                        ) { Text("掃描") }
                    }
                }
            )
        }
    ) { padding ->
        Column(Modifier.padding(padding)) {

            // ── Preset 選擇 ───────────────────────────────────────────
            Row(
                Modifier
                    .fillMaxWidth()
                    .background(MaterialTheme.colorScheme.surfaceVariant)
                    .padding(horizontal = 8.dp, vertical = 6.dp),
                horizontalArrangement = Arrangement.spacedBy(6.dp),
                verticalAlignment     = Alignment.CenterVertically,
            ) {
                Text("策略", fontSize = 12.sp, color = Color.Gray)
                Preset.entries.forEach { preset ->
                    val selected = preset in selectedPresets
                    FilterChip(
                        selected = selected,
                        onClick  = { vm.togglePreset(preset) },
                        label    = { Text(preset.label, fontSize = 12.sp) },
                        colors   = FilterChipDefaults.filterChipColors(
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
                        StockRow(result)
                        HorizontalDivider(color = Color(0xFFEEEEEE))
                    }
                }
            }
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
@Composable
private fun StockRow(result: SignalResult) {
    val quote    = result.quote
    val isLong   = result.hitPresets.keys.any { it != Preset.SHORT3_LEAN.key }
    val isShort  = result.hitPresets.containsKey(Preset.SHORT3_LEAN.key)
    val dotColor = when {
        isLong && isShort -> Color(0xFFFFA000) // 多空並存 → 橙色
        isLong            -> GreenStrong
        isShort           -> RedStrong
        else              -> GrayDot
    }
    val pctColor = if (quote.chgPct >= 0) GreenStrong else RedStrong

    Column(
        Modifier
            .fillMaxWidth()
            .padding(horizontal = 8.dp, vertical = 5.dp)
    ) {
        // ── 主資料行 ──────────────────────────────────────────────
        Row(verticalAlignment = Alignment.CenterVertically) {
            Box(Modifier.size(8.dp).background(dotColor, CircleShape))
            Spacer(Modifier.width(6.dp))

            Column(Modifier.weight(1.8f)) {
                Text(result.code, fontWeight = FontWeight.SemiBold, fontSize = 13.sp,
                    maxLines = 1)
                Text(result.name, fontSize = 11.sp, color = Color.Gray,
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

        // ── 命中 combo 標籤（每個 preset 一行） ───────────────────
        result.hitPresets.forEach { (presetKey, combos) ->
            val preset = Preset.entries.find { it.key == presetKey } ?: return@forEach
            val color  = if (preset == Preset.SHORT3_LEAN) RedStrong else GreenStrong
            Row(
                Modifier.padding(start = 20.dp, top = 2.dp),
                horizontalArrangement = Arrangement.spacedBy(4.dp),
                verticalAlignment     = Alignment.CenterVertically,
            ) {
                Text(
                    preset.label,
                    fontSize  = 10.sp,
                    color     = color.copy(alpha = 0.7f),
                    fontWeight = FontWeight.SemiBold,
                )
                combos.forEach { combo ->
                    ComboChip(combo, color)
                }
            }
        }
    }
}

@Composable
private fun ComboChip(label: String, color: Color) {
    Surface(
        shape  = RoundedCornerShape(4.dp),
        color  = color.copy(alpha = 0.10f),
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

// ── 工具 ─────────────────────────────────────────────────────────
private fun formatVol(lots: Long): String = when {
    lots >= 10_000L -> "${"%.1f".format(lots / 10_000.0)}萬"
    lots >= 1_000L  -> "${"%.1f".format(lots / 1_000.0)}千"
    else            -> "$lots"
}
