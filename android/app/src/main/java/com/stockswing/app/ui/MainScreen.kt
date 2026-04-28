@file:OptIn(androidx.compose.foundation.layout.ExperimentalLayoutApi::class)

package com.stockswing.app.ui

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.stockswing.app.StockViewModel
import com.stockswing.app.model.RealtimeQuote
import com.stockswing.app.model.SignalResult
import com.stockswing.app.model.StrategySignals

// ── 顏色 ────────────────────────────────────────────────────────
private val GreenStrong = Color(0xFF00C853)
private val RedStrong   = Color(0xFFD50000)
private val GreenLight  = Color(0xFFE8F5E9)
private val RedLight    = Color(0xFFFFEBEE)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainScreen(vm: StockViewModel) {
    val codes      by vm.stockCodes.collectAsState()
    val minHit     by vm.minHit.collectAsState()
    val isLoading  by vm.isLoading.collectAsState()
    val loadingMsg by vm.loadingMsg.collectAsState()
    val error      by vm.error.collectAsState()
    val results    by vm.signalResults.collectAsState()
    val lastUpdate by vm.lastUpdate.collectAsState()

    var codeInput by remember { mutableStateOf("") }
    val focusMgr  = LocalFocusManager.current

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("台股選股", fontWeight = FontWeight.Bold) },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer
                ),
                actions = {
                    if (lastUpdate.isNotEmpty()) {
                        Text(lastUpdate, fontSize = 12.sp, color = Color.Gray,
                            modifier = Modifier.padding(end = 4.dp))
                    }
                    IconButton(onClick = { vm.refresh() }, enabled = !isLoading) {
                        Text("↻", fontSize = 20.sp)
                    }
                }
            )
        }
    ) { padding ->
        LazyColumn(
            modifier        = Modifier.padding(padding),
            contentPadding  = PaddingValues(8.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            // ── 新增股票輸入 ──────────────────────────────────────────
            item {
                Row(
                    Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    OutlinedTextField(
                        value         = codeInput,
                        onValueChange = { codeInput = it.uppercase() },
                        label         = { Text("輸入股票代號") },
                        placeholder   = { Text("如 2330 或 2330,2317") },
                        singleLine    = true,
                        modifier      = Modifier.weight(1f),
                        keyboardOptions = KeyboardOptions(
                            keyboardType = KeyboardType.Number,
                            imeAction    = ImeAction.Done,
                        ),
                        keyboardActions = KeyboardActions(onDone = {
                            focusMgr.clearFocus()
                            addCodes(vm, codeInput)
                            codeInput = ""
                        }),
                    )
                    Spacer(Modifier.width(8.dp))
                    Button(
                        onClick = {
                            focusMgr.clearFocus()
                            addCodes(vm, codeInput)
                            codeInput = ""
                        },
                        enabled = codeInput.isNotBlank(),
                    ) { Text("新增") }
                }
            }

            // ── 已加入的股票代號 Chip 列 ─────────────────────────────
            if (codes.isNotEmpty()) {
                item {
                    LazyRow(
                        horizontalArrangement = Arrangement.spacedBy(6.dp),
                        contentPadding        = PaddingValues(horizontal = 2.dp),
                    ) {
                        items(codes) { code ->
                            InputChip(
                                selected  = false,
                                onClick   = {},
                                label     = { Text(code) },
                                trailingIcon = {
                                    IconButton(
                                        onClick  = { vm.removeCode(code) },
                                        modifier = Modifier.size(16.dp),
                                    ) {
                                        Text("×", fontSize = 12.sp)
                                    }
                                }
                            )
                        }
                    }
                }
            }

            // ── 命中門檻選擇 ──────────────────────────────────────────
            item {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                ) {
                    Text("命中門檻：", fontSize = 13.sp)
                    (1..5).forEach { n ->
                        FilterChip(
                            selected = minHit == n,
                            onClick  = { vm.setMinHit(n) },
                            label    = { Text("≥$n") },
                        )
                    }
                }
            }

            // ── 載入中 ────────────────────────────────────────────────
            if (isLoading) {
                item {
                    Column(
                        Modifier.fillMaxWidth().padding(24.dp),
                        horizontalAlignment = Alignment.CenterHorizontally,
                    ) {
                        CircularProgressIndicator()
                        if (loadingMsg.isNotEmpty()) {
                            Spacer(Modifier.height(8.dp))
                            Text(loadingMsg, fontSize = 13.sp, color = Color.Gray)
                        }
                    }
                }
            }

            // ── 錯誤訊息 ──────────────────────────────────────────────
            error?.let { msg ->
                item {
                    Card(
                        colors    = CardDefaults.cardColors(containerColor = RedLight),
                        modifier  = Modifier.fillMaxWidth(),
                    ) {
                        Text(msg, color = RedStrong,
                            modifier = Modifier.padding(12.dp), fontSize = 13.sp)
                    }
                }
            }

            // ── 空提示 ────────────────────────────────────────────────
            if (!isLoading && error == null && codes.isEmpty()) {
                item {
                    Box(Modifier.fillMaxWidth().padding(32.dp), Alignment.Center) {
                        Text("請輸入股票代號開始選股", color = Color.Gray, fontSize = 15.sp)
                    }
                }
            }

            // ── 訊號清單 ──────────────────────────────────────────────
            val filtered = results.filter { it.signals.hit >= minHit }

            if (!isLoading && codes.isNotEmpty() && filtered.isEmpty() && error == null) {
                item {
                    Box(Modifier.fillMaxWidth().padding(32.dp), Alignment.Center) {
                        Text("目前無訊號達到門檻 ≥$minHit", color = Color.Gray, fontSize = 15.sp)
                    }
                }
            }

            items(filtered, key = { it.code }) { result ->
                SignalCard(result)
            }
        }
    }
}

// ── 批次新增（支援逗號分隔）────────────────────────────────────────
private fun addCodes(vm: StockViewModel, input: String) {
    input.split(",", "，", " ").forEach { vm.addCode(it) }
}

// ── 訊號卡片 ─────────────────────────────────────────────────────
@Composable
private fun SignalCard(result: SignalResult) {
    val sigs    = result.signals
    val quote   = result.quote
    val isLong  = sigs.hitLong >= sigs.hitShort
    val dirColor = if (isLong) GreenStrong else RedStrong
    val bgColor  = if (isLong) GreenLight  else RedLight

    Card(
        modifier  = Modifier.fillMaxWidth(),
        colors    = CardDefaults.cardColors(containerColor = bgColor),
        elevation = CardDefaults.cardElevation(2.dp),
    ) {
        Column(Modifier.padding(12.dp)) {

            // 第一行：代號 / 名稱 ┆ 方向 badge + 命中數
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment     = Alignment.CenterVertically,
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(result.code, fontWeight = FontWeight.Bold, fontSize = 16.sp)
                    Spacer(Modifier.width(6.dp))
                    Text(result.name, fontSize = 13.sp, color = Color.Gray)
                }
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    DirectionBadge(sigs.direction, dirColor)
                    HitBadge(sigs.hit, dirColor)
                }
            }

            Spacer(Modifier.height(6.dp))

            // 第二行：現價 + 漲跌幅 + 更新時間
            Row(
                Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.Bottom,
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Row(verticalAlignment = Alignment.Bottom) {
                    Text("%.2f".format(quote.price), fontSize = 20.sp, fontWeight = FontWeight.Bold)
                    Spacer(Modifier.width(8.dp))
                    val pctColor = if (quote.chgPct >= 0) GreenStrong else RedStrong
                    Text("%+.2f%%".format(quote.chgPct), color = pctColor, fontSize = 14.sp)
                }
                Text(quote.updateTime, fontSize = 11.sp, color = Color.Gray)
            }

            // 第三行：觸發訊號 chips
            if (sigs.activeSignals.isNotEmpty()) {
                Spacer(Modifier.height(6.dp))
                SignalChips(sigs.activeSignals, dirColor)
            }

            // 第四行：多/空命中數細節
            Spacer(Modifier.height(4.dp))
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                if (sigs.hitLong  > 0) SmallLabel("多 ${sigs.hitLong}", GreenStrong)
                if (sigs.hitShort > 0) SmallLabel("空 ${sigs.hitShort}", RedStrong)
                Text("量比 ${"%.1f".format(quote.totalVolLots.toDouble() / maxOf(1, quote.totalVolLots))}x",
                    fontSize = 11.sp, color = Color.Gray)
            }
        }
    }
}

@Composable
private fun SignalChips(signals: List<String>, color: Color) {
    FlowRow(
        horizontalArrangement = Arrangement.spacedBy(4.dp),
        verticalArrangement   = Arrangement.spacedBy(4.dp),
    ) {
        signals.forEach { s ->
            Surface(
                shape  = RoundedCornerShape(8.dp),
                color  = color.copy(alpha = 0.15f),
                border = BorderStroke(1.dp, color.copy(alpha = 0.5f)),
            ) {
                Text(
                    s,
                    fontSize   = 11.sp,
                    color      = color,
                    fontWeight = FontWeight.SemiBold,
                    modifier   = Modifier.padding(horizontal = 6.dp, vertical = 2.dp),
                )
            }
        }
    }
}

@Composable
private fun DirectionBadge(direction: String, color: Color) {
    Surface(
        shape  = RoundedCornerShape(6.dp),
        color  = color.copy(alpha = 0.15f),
        border = BorderStroke(1.dp, color),
    ) {
        Text(
            direction,
            color      = color,
            fontWeight = FontWeight.Bold,
            fontSize   = 13.sp,
            modifier   = Modifier.padding(horizontal = 8.dp, vertical = 2.dp),
        )
    }
}

@Composable
private fun HitBadge(hit: Int, color: Color) {
    Surface(shape = RoundedCornerShape(12.dp), color = color) {
        Text(
            "$hit",
            color      = Color.White,
            fontWeight = FontWeight.Bold,
            fontSize   = 13.sp,
            textAlign  = TextAlign.Center,
            modifier   = Modifier.size(26.dp).wrapContentHeight(),
        )
    }
}

@Composable
private fun SmallLabel(text: String, color: Color) {
    Text(text, fontSize = 11.sp, color = color, fontWeight = FontWeight.SemiBold)
}
