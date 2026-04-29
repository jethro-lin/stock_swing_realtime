@file:OptIn(androidx.compose.foundation.layout.ExperimentalLayoutApi::class)

package com.stockswing.app.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.LazyRow
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
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
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.stockswing.app.StockViewModel
import com.stockswing.app.model.SignalResult

private val GreenStrong = Color(0xFF00C853)
private val RedStrong   = Color(0xFFD50000)
private val GrayDot     = Color(0xFFBDBDBD)

// ── 價位分群 ──────────────────────────────────────────────────────
private enum class PriceGroup(val label: String) {
    ALL("全部"),
    LOW("< 30"),
    MID_LOW("30~100"),
    MID_HIGH("100~500"),
    HIGH("> 500"),
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
    val codes      by vm.stockCodes.collectAsState()
    val minHit     by vm.minHit.collectAsState()
    val isLoading  by vm.isLoading.collectAsState()
    val loadingMsg by vm.loadingMsg.collectAsState()
    val error      by vm.error.collectAsState()
    val results    by vm.signalResults.collectAsState()
    val lastUpdate by vm.lastUpdate.collectAsState()

    var codeInput    by remember { mutableStateOf("") }
    var selectedTab  by remember { mutableIntStateOf(0) }
    val focusMgr     = LocalFocusManager.current
    val groups       = PriceGroup.entries

    Scaffold(
        topBar = {
            TopAppBar(
                title = { Text("台股選股", fontWeight = FontWeight.Bold) },
                colors = TopAppBarDefaults.topAppBarColors(
                    containerColor = MaterialTheme.colorScheme.primaryContainer
                ),
                actions = {
                    if (lastUpdate.isNotEmpty()) {
                        Text(lastUpdate, fontSize = 11.sp, color = Color.Gray,
                            modifier = Modifier.padding(end = 4.dp))
                    }
                    IconButton(onClick = { vm.refresh() }, enabled = !isLoading) {
                        Text("↻", fontSize = 20.sp)
                    }
                }
            )
        }
    ) { padding ->
        Column(Modifier.padding(padding)) {

            // ── 新增股票輸入 ──────────────────────────────────────────
            Row(
                Modifier
                    .fillMaxWidth()
                    .padding(horizontal = 8.dp, vertical = 4.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                OutlinedTextField(
                    value         = codeInput,
                    onValueChange = { codeInput = it.uppercase() },
                    label         = { Text("輸入股票代號") },
                    placeholder   = { Text("如 2330 或 2330,2317") },
                    singleLine    = true,
                    modifier      = Modifier.weight(1f),
                    textStyle     = LocalTextStyle.current.copy(fontSize = 13.sp),
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
                Spacer(Modifier.width(6.dp))
                Button(
                    onClick = {
                        focusMgr.clearFocus()
                        addCodes(vm, codeInput)
                        codeInput = ""
                    },
                    enabled = codeInput.isNotBlank(),
                    contentPadding = PaddingValues(horizontal = 12.dp, vertical = 8.dp),
                ) { Text("新增") }
            }

            // ── 已加入股票 Chip 列 ────────────────────────────────────
            if (codes.isNotEmpty()) {
                LazyRow(
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                    contentPadding        = PaddingValues(horizontal = 8.dp),
                    modifier              = Modifier.padding(bottom = 2.dp),
                ) {
                    items(codes) { code ->
                        InputChip(
                            selected  = false,
                            onClick   = {},
                            label     = { Text(code, fontSize = 12.sp) },
                            trailingIcon = {
                                IconButton(
                                    onClick  = { vm.removeCode(code) },
                                    modifier = Modifier.size(16.dp),
                                ) { Text("×", fontSize = 12.sp) }
                            }
                        )
                    }
                }
            }

            // ── 命中門檻 ──────────────────────────────────────────────
            Row(
                Modifier.padding(horizontal = 8.dp, vertical = 2.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(4.dp),
            ) {
                Text("門檻", fontSize = 12.sp, color = Color.Gray)
                (1..5).forEach { n ->
                    FilterChip(
                        selected = minHit == n,
                        onClick  = { vm.setMinHit(n) },
                        label    = { Text("≥$n", fontSize = 11.sp) },
                    )
                }
            }

            // ── 載入中 ────────────────────────────────────────────────
            if (isLoading) {
                Row(
                    Modifier
                        .fillMaxWidth()
                        .padding(8.dp),
                    verticalAlignment     = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.Center,
                ) {
                    CircularProgressIndicator(Modifier.size(18.dp), strokeWidth = 2.dp)
                    if (loadingMsg.isNotEmpty()) {
                        Spacer(Modifier.width(8.dp))
                        Text(loadingMsg, fontSize = 12.sp, color = Color.Gray)
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
            if (!isLoading && error == null && codes.isEmpty()) {
                Box(Modifier.fillMaxWidth().padding(32.dp), Alignment.Center) {
                    Text("請輸入股票代號開始選股", color = Color.Gray, fontSize = 14.sp)
                }
                return@Scaffold
            }

            // ── 價位分頁 ──────────────────────────────────────────────
            val filtered = results.filter { it.signals.hit >= minHit }

            ScrollableTabRow(
                selectedTabIndex = selectedTab,
                edgePadding      = 0.dp,
            ) {
                groups.forEachIndexed { i, group ->
                    val count = if (group == PriceGroup.ALL) filtered.size
                                else filtered.count { it.priceGroup() == group }
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

            val tabItems = if (groups[selectedTab] == PriceGroup.ALL) filtered
                           else filtered.filter { it.priceGroup() == groups[selectedTab] }

            if (tabItems.isEmpty() && !isLoading) {
                Box(Modifier.fillMaxSize(), Alignment.Center) {
                    Text("此價位區間無訊號", color = Color.Gray, fontSize = 14.sp)
                }
            } else {
                // ── 欄位標頭 ─────────────────────────────────────────
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
        Spacer(Modifier.width(10.dp))
        Text("代號/名稱", fontSize = 11.sp, color = Color.Gray, modifier = Modifier.weight(1.8f))
        Text("現價", fontSize = 11.sp, color = Color.Gray, modifier = Modifier.weight(1f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
        Text("漲跌幅", fontSize = 11.sp, color = Color.Gray, modifier = Modifier.weight(1.1f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
        Text("成交量(張)", fontSize = 11.sp, color = Color.Gray, modifier = Modifier.weight(1.3f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End)
    }
}

// ── 個股行 ────────────────────────────────────────────────────────
@Composable
private fun StockRow(result: SignalResult) {
    val sigs  = result.signals
    val quote = result.quote
    val dotColor = when {
        sigs.hit == 0    -> GrayDot
        sigs.hitLong >= sigs.hitShort -> GreenStrong
        else             -> RedStrong
    }
    val pctColor = if (quote.chgPct >= 0) GreenStrong else RedStrong

    Row(
        Modifier
            .fillMaxWidth()
            .padding(horizontal = 8.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        // 訊號圓點
        Box(
            Modifier
                .size(8.dp)
                .background(dotColor, CircleShape)
        )
        Spacer(Modifier.width(6.dp))

        // 代號 + 名稱
        Column(Modifier.weight(1.8f)) {
            Text(result.code, fontWeight = FontWeight.SemiBold, fontSize = 13.sp,
                maxLines = 1)
            Text(result.name, fontSize = 11.sp, color = Color.Gray,
                maxLines = 1, overflow = TextOverflow.Ellipsis)
        }

        // 現價
        Text(
            "%.2f".format(quote.price),
            fontSize   = 13.sp,
            fontWeight = FontWeight.SemiBold,
            modifier   = Modifier.weight(1f),
            textAlign  = androidx.compose.ui.text.style.TextAlign.End,
        )

        // 漲跌幅
        Text(
            "%+.2f%%".format(quote.chgPct),
            fontSize  = 12.sp,
            color     = pctColor,
            modifier  = Modifier.weight(1.1f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End,
        )

        // 成交量
        Text(
            formatVol(quote.totalVolLots),
            fontSize  = 11.sp,
            color     = Color.Gray,
            modifier  = Modifier.weight(1.3f),
            textAlign = androidx.compose.ui.text.style.TextAlign.End,
        )
    }
}

// ── 工具 ─────────────────────────────────────────────────────────
private fun addCodes(vm: StockViewModel, input: String) {
    input.split(",", "，", " ").filter(String::isNotBlank).forEach { vm.addCode(it) }
}

private fun formatVol(lots: Long): String = when {
    lots >= 10_000L -> "${"%.1f".format(lots / 10_000.0)}萬"
    lots >= 1_000L  -> "${"%.1f".format(lots / 1_000.0)}千"
    else            -> "$lots"
}
