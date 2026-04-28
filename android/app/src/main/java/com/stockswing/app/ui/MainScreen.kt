package com.stockswing.app.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.itemsIndexed
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
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.stockswing.app.StockViewModel
import com.stockswing.app.WsState
import com.stockswing.app.model.Position
import com.stockswing.app.model.StockSignal
import com.stockswing.app.model.StockUpdate

// ── 顏色常數 ────────────────────────────────────────────────────
private val GreenStrong  = Color(0xFF00C853)
private val RedStrong    = Color(0xFFD50000)
private val GreenLight   = Color(0xFFE8F5E9)
private val RedLight     = Color(0xFFFFEBEE)
private val NeutralLight = Color(0xFFF5F5F5)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun MainScreen(vm: StockViewModel) {
    val update   by vm.update.collectAsState()
    val wsState  by vm.wsState.collectAsState()
    val savedUrl by vm.savedUrl.collectAsState()

    var urlInput  by remember { mutableStateOf("") }
    var selectedTab by remember { mutableIntStateOf(0) }

    // 初始化 urlInput
    LaunchedEffect(savedUrl) {
        if (urlInput.isEmpty()) urlInput = savedUrl
    }

    Scaffold(
        topBar = {
            Column {
                TopAppBar(
                    title = { Text("台股即時監控", fontWeight = FontWeight.Bold) },
                    colors = TopAppBarDefaults.topAppBarColors(
                        containerColor = MaterialTheme.colorScheme.primaryContainer
                    ),
                    actions = {
                        ConnectionBadge(wsState)
                        Spacer(Modifier.width(8.dp))
                    }
                )
                ConnectionBar(
                    url        = urlInput,
                    wsState    = wsState,
                    onUrlChange = { urlInput = it },
                    onConnect   = {
                        vm.saveUrl(urlInput)
                        vm.connect(urlInput)
                    },
                    onDisconnect = { vm.disconnect() }
                )
                update?.let { StatusBar(it) }
            }
        }
    ) { padding ->
        Column(Modifier.padding(padding)) {
            TabRow(selectedTabIndex = selectedTab) {
                val signalCount   = update?.signals?.count { it.hit > 0 } ?: 0
                val positionCount = update?.positions?.size ?: 0
                val alertCount    = update?.alerts?.size ?: 0
                Tab(selected = selectedTab == 0, onClick = { selectedTab = 0 },
                    text = { Text("訊號 ($signalCount)") })
                Tab(selected = selectedTab == 1, onClick = { selectedTab = 1 },
                    text = { Text("部位 ($positionCount)") })
                Tab(selected = selectedTab == 2, onClick = { selectedTab = 2 },
                    text = { Text("警報 ($alertCount)") })
            }
            when (selectedTab) {
                0 -> SignalList(update?.signals ?: emptyList(), update?.minHit ?: 1)
                1 -> PositionList(update?.positions ?: emptyList(), update?.summary)
                2 -> AlertList(update?.alerts ?: emptyList())
            }
        }
    }
}

// ── 連線狀態 Badge ───────────────────────────────────────────────
@Composable
private fun ConnectionBadge(state: WsState) {
    val (color, label) = when (state) {
        WsState.CONNECTED    -> GreenStrong  to "已連線"
        WsState.CONNECTING   -> Color(0xFFFFA000) to "連線中"
        WsState.ERROR        -> RedStrong    to "錯誤"
        WsState.DISCONNECTED -> Color.Gray   to "未連線"
    }
    Surface(
        shape  = RoundedCornerShape(12.dp),
        color  = color.copy(alpha = 0.15f),
        border = androidx.compose.foundation.BorderStroke(1.dp, color)
    ) {
        Text(
            label,
            color    = color,
            fontSize = 12.sp,
            modifier = Modifier.padding(horizontal = 8.dp, vertical = 4.dp)
        )
    }
}

// ── 連線輸入列 ────────────────────────────────────────────────────
@Composable
private fun ConnectionBar(
    url: String, wsState: WsState,
    onUrlChange: (String) -> Unit,
    onConnect: () -> Unit, onDisconnect: () -> Unit,
) {
    val focusManager = LocalFocusManager.current
    Row(
        Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(horizontal = 8.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        OutlinedTextField(
            value         = url,
            onValueChange = onUrlChange,
            label         = { Text("WebSocket URL") },
            singleLine    = true,
            modifier      = Modifier.weight(1f),
            textStyle     = LocalTextStyle.current.copy(fontSize = 13.sp),
            keyboardOptions = KeyboardOptions(imeAction = ImeAction.Go),
            keyboardActions = KeyboardActions(onGo = {
                focusManager.clearFocus()
                onConnect()
            })
        )
        Spacer(Modifier.width(6.dp))
        if (wsState == WsState.CONNECTED || wsState == WsState.CONNECTING) {
            OutlinedButton(onClick = onDisconnect) { Text("斷開") }
        } else {
            Button(onClick = {
                focusManager.clearFocus()
                onConnect()
            }) { Text("連線") }
        }
    }
}

// ── 狀態列（最後更新時間 + 掃描次數）────────────────────────────────
@Composable
private fun StatusBar(u: StockUpdate) {
    Row(
        Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.secondaryContainer)
            .padding(horizontal = 12.dp, vertical = 4.dp),
        horizontalArrangement = Arrangement.SpaceBetween
    ) {
        Text("更新：${u.timestamp}", fontSize = 12.sp)
        Text("第 ${u.scanN} 次掃描  命中≥${u.minHit}", fontSize = 12.sp)
    }
}

// ────────────────────────────────────────────────────────────────
// 訊號頁
// ────────────────────────────────────────────────────────────────
@Composable
private fun SignalList(signals: List<StockSignal>, minHit: Int) {
    val filtered = signals.filter { it.hit >= minHit }
    if (filtered.isEmpty()) {
        EmptyHint("目前無訊號達到門檻 ≥$minHit")
        return
    }
    LazyColumn(
        contentPadding    = PaddingValues(8.dp),
        verticalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        items(filtered, key = { it.code }) { sig ->
            SignalCard(sig)
        }
    }
}

@Composable
private fun SignalCard(sig: StockSignal) {
    val isLong  = sig.hitLong >= sig.hitShort
    val bgColor = if (isLong) GreenLight else RedLight
    val dirColor= if (isLong) GreenStrong else RedStrong

    Card(
        modifier = Modifier.fillMaxWidth(),
        colors   = CardDefaults.cardColors(containerColor = bgColor),
        elevation = CardDefaults.cardElevation(2.dp)
    ) {
        Column(Modifier.padding(12.dp)) {
            // 第一行：代號 / 名稱 / 方向 / 命中數
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment     = Alignment.CenterVertically
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(sig.code, fontWeight = FontWeight.Bold, fontSize = 16.sp)
                    Spacer(Modifier.width(6.dp))
                    Text(sig.name, fontSize = 13.sp, color = Color.Gray)
                }
                Row(verticalAlignment = Alignment.CenterVertically) {
                    DirectionBadge(sig.direction, dirColor)
                    Spacer(Modifier.width(6.dp))
                    HitBadge(sig.hit, dirColor)
                }
            }
            Spacer(Modifier.height(6.dp))
            // 第二行：現價 + 漲跌幅
            Row(verticalAlignment = Alignment.Bottom) {
                Text(
                    "%.2f".format(sig.price),
                    fontSize   = 20.sp,
                    fontWeight = FontWeight.Bold
                )
                Spacer(Modifier.width(8.dp))
                val pctColor = if (sig.chgPct >= 0) GreenStrong else RedStrong
                Text(
                    "%+.2f%%".format(sig.chgPct),
                    color    = pctColor,
                    fontSize = 14.sp
                )
                Spacer(Modifier.weight(1f))
                if (sig.updateTime.isNotEmpty()) {
                    Text(sig.updateTime, fontSize = 11.sp, color = Color.Gray)
                }
            }
            // 第三行：觸發訊號 chips
            if (sig.activeSignals.isNotEmpty()) {
                Spacer(Modifier.height(6.dp))
                FlowRow(sig.activeSignals, dirColor)
            }
        }
    }
}

@Composable
private fun FlowRow(signals: List<String>, color: Color) {
    Row(horizontalArrangement = Arrangement.spacedBy(4.dp), modifier = Modifier.fillMaxWidth()) {
        signals.forEach { s ->
            Surface(
                shape = RoundedCornerShape(8.dp),
                color = color.copy(alpha = 0.15f),
                border = androidx.compose.foundation.BorderStroke(1.dp, color.copy(alpha = 0.5f))
            ) {
                Text(
                    s,
                    fontSize = 11.sp,
                    color    = color,
                    fontWeight = FontWeight.SemiBold,
                    modifier = Modifier.padding(horizontal = 6.dp, vertical = 2.dp)
                )
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────
// 部位頁
// ────────────────────────────────────────────────────────────────
@Composable
private fun PositionList(
    positions: List<Position>,
    summary: com.stockswing.app.model.TradeSummary?,
) {
    LazyColumn(contentPadding = PaddingValues(8.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
        if (positions.isEmpty()) {
            item { EmptyHint("目前無持倉") }
        } else {
            items(positions, key = { it.code }) { pos ->
                PositionCard(pos)
            }
        }
        summary?.let { s ->
            if (s.trades > 0) {
                item { SummaryCard(s) }
            }
        }
    }
}

@Composable
private fun PositionCard(pos: Position) {
    val isLong   = pos.direction == "多"
    val pnlColor = when {
        pos.pnlPct > 0  -> GreenStrong
        pos.pnlPct < 0  -> RedStrong
        else            -> Color.Gray
    }
    val bgColor  = if (pos.status == "停損") RedLight else NeutralLight

    Card(
        modifier  = Modifier.fillMaxWidth(),
        colors    = CardDefaults.cardColors(containerColor = bgColor),
        elevation = CardDefaults.cardElevation(2.dp)
    ) {
        Column(Modifier.padding(12.dp)) {
            Row(
                Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment     = Alignment.CenterVertically
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(pos.code, fontWeight = FontWeight.Bold, fontSize = 16.sp)
                    Spacer(Modifier.width(6.dp))
                    Text(pos.name, fontSize = 13.sp, color = Color.Gray)
                }
                val dirColor = if (isLong) GreenStrong else RedStrong
                DirectionBadge(pos.direction, dirColor)
            }
            Spacer(Modifier.height(6.dp))
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                LabelValue("進場", "%.2f".format(pos.entry))
                LabelValue("現價", "%.2f".format(pos.currPrice))
                LabelValue("停損", "%.2f".format(pos.stop))
                Column(horizontalAlignment = Alignment.End) {
                    Text("損益", fontSize = 11.sp, color = Color.Gray)
                    Text(
                        "%+.2f%%".format(pos.pnlPct),
                        color      = pnlColor,
                        fontWeight = FontWeight.Bold,
                        fontSize   = 15.sp
                    )
                }
            }
            Spacer(Modifier.height(4.dp))
            Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
                Text("進場：${pos.enteredAt}", fontSize = 11.sp, color = Color.Gray)
                if (pos.status == "停損") {
                    Text("⚠ 停損", fontSize = 11.sp, color = RedStrong, fontWeight = FontWeight.SemiBold)
                }
            }
        }
    }
}

@Composable
private fun SummaryCard(s: com.stockswing.app.model.TradeSummary) {
    val pnlColor = when {
        s.totalPnl > 0 -> GreenStrong
        s.totalPnl < 0 -> RedStrong
        else           -> Color.Gray
    }
    val wr = if (s.trades > 0) s.win.toFloat() / s.trades * 100 else 0f

    Card(
        modifier  = Modifier.fillMaxWidth(),
        colors    = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.tertiaryContainer),
        elevation = CardDefaults.cardElevation(2.dp)
    ) {
        Row(
            Modifier
                .fillMaxWidth()
                .padding(12.dp),
            horizontalArrangement = Arrangement.SpaceAround
        ) {
            LabelValue("總交易", "${s.trades} 筆")
            LabelValue("勝率", "%.0f%%".format(wr))
            Column(horizontalAlignment = Alignment.CenterHorizontally) {
                Text("總損益", fontSize = 11.sp, color = Color.Gray)
                Text(
                    "%+.2f%%".format(s.totalPnl),
                    color      = pnlColor,
                    fontWeight = FontWeight.Bold
                )
            }
        }
    }
}

// ────────────────────────────────────────────────────────────────
// 警報頁
// ────────────────────────────────────────────────────────────────
@Composable
private fun AlertList(alerts: List<String>) {
    if (alerts.isEmpty()) {
        EmptyHint("尚無警報記錄")
        return
    }
    LazyColumn(
        contentPadding = PaddingValues(8.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp)
    ) {
        // 最新的顯示在最上面
        itemsIndexed(alerts.reversed()) { _, msg ->
            Surface(
                modifier  = Modifier.fillMaxWidth(),
                shape     = RoundedCornerShape(8.dp),
                color     = MaterialTheme.colorScheme.surfaceVariant,
                tonalElevation = 1.dp
            ) {
                Text(
                    msg,
                    fontSize = 13.sp,
                    modifier = Modifier.padding(10.dp)
                )
            }
        }
    }
}

// ── 共用小元件 ────────────────────────────────────────────────────
@Composable
private fun DirectionBadge(direction: String, color: Color) {
    Surface(
        shape  = RoundedCornerShape(6.dp),
        color  = color.copy(alpha = 0.15f),
        border = androidx.compose.foundation.BorderStroke(1.dp, color)
    ) {
        Text(
            direction,
            color      = color,
            fontWeight = FontWeight.Bold,
            fontSize   = 13.sp,
            modifier   = Modifier.padding(horizontal = 8.dp, vertical = 2.dp)
        )
    }
}

@Composable
private fun HitBadge(hit: Int, color: Color) {
    Surface(
        shape = RoundedCornerShape(12.dp),
        color = color
    ) {
        Text(
            "$hit",
            color      = Color.White,
            fontWeight = FontWeight.Bold,
            fontSize   = 13.sp,
            textAlign  = TextAlign.Center,
            modifier   = Modifier
                .size(26.dp)
                .wrapContentHeight()
        )
    }
}

@Composable
private fun LabelValue(label: String, value: String) {
    Column(horizontalAlignment = Alignment.CenterHorizontally) {
        Text(label, fontSize = 11.sp, color = Color.Gray)
        Text(value, fontWeight = FontWeight.SemiBold, fontSize = 14.sp)
    }
}

@Composable
private fun EmptyHint(text: String) {
    Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
        Text(text, color = Color.Gray, fontSize = 15.sp)
    }
}
