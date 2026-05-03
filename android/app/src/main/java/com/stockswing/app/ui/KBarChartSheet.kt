package com.stockswing.app.ui

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.gestures.detectHorizontalDragGestures
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.nativeCanvas
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import kotlin.math.abs
import com.stockswing.app.model.HistoricalBar
import com.stockswing.app.model.Preset
import com.stockswing.app.model.SignalResult

private val ChartGreen  = Color(0xFF00C853)
private val ChartRed    = Color(0xFFD50000)
private val ChartBlue   = Color(0xFF2196F3)
private val ChartOrange = Color(0xFFFF9800)

// 台灣慣例：漲 = 紅，跌 = 綠
private val ColorUp   = ChartRed
private val ColorDown = ChartGreen

// ── 從 hitPresets 判斷多空方向 ──────────────────────────────────────
private fun Map<String, List<String>>.isLong(): Boolean {
    val custom = this["custom"]
    return if (custom != null)
        custom.any { !it.endsWith("S") || it == "B2" }
    else
        keys.any { it != Preset.SHORT3_LEAN.key }
}

private fun Map<String, List<String>>.isShort(): Boolean {
    val custom = this["custom"]
    return if (custom != null)
        custom.any { it.endsWith("S") && it != "B2" }
    else
        containsKey(Preset.SHORT3_LEAN.key)
}

// ── Bottom Sheet ────────────────────────────────────────────────────
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun KBarChartSheet(
    result:    SignalResult,
    bars:      List<HistoricalBar>,
    onDismiss: () -> Unit,
) {
    ModalBottomSheet(
        onDismissRequest = onDismiss,
        sheetState = rememberModalBottomSheetState(skipPartiallyExpanded = true),
        dragHandle  = { BottomSheetDefaults.DragHandle() },
    ) {
        Column(
            Modifier
                .fillMaxWidth()
                .padding(horizontal = 16.dp)
                .padding(bottom = 24.dp)
        ) {
            // ── 標頭 ──────────────────────────────────────────────────
            Row(
                Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.Bottom,
                horizontalArrangement = Arrangement.SpaceBetween,
            ) {
                Column {
                    Text(
                        "${result.code}  ${result.name}",
                        fontWeight = FontWeight.Bold,
                        fontSize   = 16.sp,
                    )
                    val pctColor = if (result.quote.chgPct >= 0) ColorUp else ColorDown
                    Text(
                        "%.2f  %+.2f%%".format(result.quote.price, result.quote.chgPct),
                        color      = pctColor,
                        fontSize   = 14.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                }
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    MaLegend("MA5",  ChartGreen)
                    MaLegend("MA10", ChartBlue)
                    MaLegend("MA20", ChartOrange)
                }
            }

            Spacer(Modifier.height(12.dp))

            if (bars.isEmpty()) {
                Box(Modifier.fillMaxWidth().height(280.dp), Alignment.Center) {
                    CircularProgressIndicator(modifier = Modifier.size(32.dp))
                }
            } else {
                // ── 蠟燭圖 ───────────────────────────────────────────
                CandlestickChart(
                    bars       = bars,
                    hitPresets = result.hitPresets,
                    modifier   = Modifier.fillMaxWidth().height(300.dp),
                )

                Spacer(Modifier.height(12.dp))
                HorizontalDivider()
                Spacer(Modifier.height(8.dp))

                // ── 命中策略 ──────────────────────────────────────────
                Text("命中策略", fontSize = 11.sp, color = Color.Gray)
                Spacer(Modifier.height(4.dp))

                val custom = result.hitPresets["custom"]
                if (custom != null) {
                    val longHits  = custom.filter { !it.endsWith("S") || it == "B2" }
                    val shortHits = custom.filter { it.endsWith("S") && it != "B2" }
                    if (longHits.isNotEmpty())  SignalChipRow("多方訊號", longHits,  ColorUp)
                    if (shortHits.isNotEmpty()) SignalChipRow("空方訊號", shortHits, ColorDown)
                } else {
                    result.hitPresets.forEach { (key, combos) ->
                        val preset = Preset.entries.find { it.key == key } ?: return@forEach
                        val color  = if (preset == Preset.SHORT3_LEAN) ColorDown else ColorUp
                        SignalChipRow(preset.label, combos, color)
                    }
                }
            }
        }
    }
}

@Composable
private fun SignalChipRow(label: String, chips: List<String>, color: Color) {
    Row(
        Modifier
            .fillMaxWidth()
            .horizontalScroll(rememberScrollState())
            .padding(vertical = 2.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        Text(
            label,
            fontSize   = 11.sp,
            color      = color.copy(alpha = 0.7f),
            fontWeight = FontWeight.SemiBold,
            modifier   = Modifier.widthIn(min = 48.dp),
        )
        chips.forEach { chip ->
            Surface(shape = RoundedCornerShape(4.dp), color = color.copy(alpha = 0.10f)) {
                Text(
                    chip,
                    fontSize   = 11.sp,
                    color      = color,
                    fontWeight = FontWeight.Medium,
                    modifier   = Modifier.padding(horizontal = 6.dp, vertical = 2.dp),
                )
            }
        }
    }
}

@Composable
private fun MaLegend(label: String, color: Color) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(3.dp),
    ) {
        Box(Modifier.size(width = 14.dp, height = 2.dp).background(color))
        Text(label, fontSize = 10.sp, color = color)
    }
}

// ── Pattern annotation helpers ─────────────────────────────────────

private data class PatternAnnotation(val offsets: List<Int>, val isLong: Boolean, val key: String)

private val MULTI_CANDLE_PATTERNS = listOf(
    PatternAnnotation(listOf(2, 1, 0), true,  "P"),   // 紅三兵
    PatternAnnotation(listOf(2, 1, 0), false, "PS"),  // 黑三兵
    PatternAnnotation(listOf(3, 2, 1), true,  "E"),   // 強勢連漲
    PatternAnnotation(listOf(3, 2, 1), false, "ES"),  // 弱勢連跌
    PatternAnnotation(listOf(2, 1, 0), true,  "O"),   // 晨星
    PatternAnnotation(listOf(2, 1, 0), false, "OS"),  // 黃昏之星
    PatternAnnotation(listOf(1, 0),    true,  "I"),   // 吞噬陽線
    PatternAnnotation(listOf(1, 0),    false, "IS"),  // 吞噬陰線
    PatternAnnotation(listOf(2, 1, 0), true,  "Q"),   // Inside Bar 突破
    PatternAnnotation(listOf(2, 1, 0), false, "QS"),  // Inside Bar 跌破
)

private fun Map<String, List<String>>.activeSignals(): Set<String> {
    val custom = this["custom"]
    if (custom != null) return custom.toSet()
    return values.flatten()
        .flatMap { combo -> combo.split("+") }
        .toSet()
}

// ── 蠟燭圖 Canvas ───────────────────────────────────────────────────
@Composable
fun CandlestickChart(
    bars:       List<HistoricalBar>,
    hitPresets: Map<String, List<String>> = emptyMap(),
    modifier:   Modifier = Modifier,
) {
    if (bars.isEmpty()) return

    val windowSize = 60
    var barOffset      by remember { mutableIntStateOf(0) }
    var dragBaseOffset by remember { mutableIntStateOf(0) }
    var lastDragX      by remember { mutableFloatStateOf(0f) }

    val display = remember(bars, barOffset) {
        val endIdx   = bars.size - barOffset
        val startIdx = maxOf(0, endIdx - windowSize)
        if (endIdx > 0) bars.subList(startIdx, endIdx) else emptyList()
    }
    if (display.isEmpty()) return

    val showSignals = barOffset == 0

    val closes = remember(display) { display.map { it.close } }
    val ma5  = remember(closes) { rollingAvg(closes, 5)  }
    val ma10 = remember(closes) { rollingAvg(closes, 10) }
    val ma20 = remember(closes) { rollingAvg(closes, 20) }

    Canvas(
        modifier = modifier.pointerInput(bars.size) {
            if (bars.size <= windowSize) return@pointerInput
            detectHorizontalDragGestures(
                onDragStart = { lastDragX = it.x },
                onDragEnd   = { dragBaseOffset = barOffset; lastDragX = 0f },
                onHorizontalDrag = { change, _ ->
                    val candleW = size.width / windowSize.toFloat()
                    val delta   = change.position.x - lastDragX
                    val barDelta = (-delta / candleW).toInt()
                    if (abs(barDelta) >= 1) {
                        barOffset  = (dragBaseOffset + barDelta).coerceIn(0, bars.size - windowSize)
                        dragBaseOffset = barOffset
                        lastDragX  = change.position.x
                    }
                    change.consume()
                }
            )
        }
    ) {
        val n = display.size
        val w = size.width
        val h = size.height

        val priceH = h * 0.76f
        val volH   = h * 0.18f
        val gapH   = h * 0.06f
        val volTop = priceH + gapH

        val candleW = w / n
        val bodyW   = (candleW * 0.65f).coerceAtLeast(2f)

        val maxHigh   = display.maxOf { it.high }
        val minLow    = display.minOf { it.low }
        val pad       = (maxHigh - minLow) * 0.08
        val priceMax  = maxHigh + pad
        val priceMin  = minLow - pad
        val priceSpan = (priceMax - priceMin).coerceAtLeast(0.01)

        fun py(p: Double) = (priceH * (1.0 - (p - priceMin) / priceSpan)).toFloat()

        val maxVol = display.maxOf { it.volumeLots }.toDouble()
        fun vy(v: Long) = (volTop + volH * (1.0 - v.toDouble() / maxVol)).toFloat()

        // ── 格線 ──────────────────────────────────────────────────────
        repeat(5) { k ->
            val y = priceH * k / 4f
            drawLine(Color(0x22000000), Offset(0f, y), Offset(w, y), strokeWidth = 0.5f)
        }

        // ── 價格標籤 ─────────────────────────────────────────────────
        val textPaint = android.graphics.Paint().apply {
            color       = android.graphics.Color.argb(140, 100, 100, 100)
            textSize    = 22f
            textAlign   = android.graphics.Paint.Align.RIGHT
            isAntiAlias = true
        }
        repeat(5) { k ->
            val p = priceMax - (priceMax - priceMin) * k / 4
            drawContext.canvas.nativeCanvas.drawText(
                "%.1f".format(p), w - 2f, priceH * k / 4f - 4f, textPaint
            )
        }

        // ── 蠟燭 + 量柱 ───────────────────────────────────────────────
        display.forEachIndexed { i, bar ->
            val cx    = (i + 0.5f) * candleW
            val isUp  = bar.close >= bar.open
            val color = if (isUp) ColorUp else ColorDown

            drawLine(color, Offset(cx, py(bar.high)), Offset(cx, py(bar.low)), strokeWidth = 1f)

            val top = py(maxOf(bar.open, bar.close))
            val bot = py(minOf(bar.open, bar.close))
            drawRect(
                color   = color,
                topLeft = Offset(cx - bodyW / 2, top),
                size    = Size(bodyW, (bot - top).coerceAtLeast(1f)),
            )
            drawRect(
                color   = color.copy(alpha = 0.45f),
                topLeft = Offset(cx - bodyW / 2, vy(bar.volumeLots)),
                size    = Size(bodyW, volTop + volH - vy(bar.volumeLots)),
            )
        }

        // ── MA 線 ────────────────────────────────────────────────────
        fun drawMA(values: List<Double>, color: Color) {
            val path = Path()
            values.forEachIndexed { i, v ->
                val x = (i + 0.5f) * candleW
                val y = py(v)
                if (i == 0) path.moveTo(x, y) else path.lineTo(x, y)
            }
            drawPath(path, color, style = Stroke(width = 1.5f))
        }

        drawMA(ma5,  ChartGreen)
        drawMA(ma10, ChartBlue)
        drawMA(ma20, ChartOrange)

        // ── 訊號標記（僅顯示訊號日在最後一根時）─────────────────────
        if (showSignals) {
            val signalBar = display.last()
            val scx  = (n - 0.5f) * candleW
            val triW = (candleW * 0.7f).coerceIn(5f, 10f)
            val triH = triW * 0.8f

            if (hitPresets.isLong()) {
                val tipY = py(signalBar.high) - 4f
                drawPath(Path().apply {
                    moveTo(scx, tipY - triH)
                    lineTo(scx - triW / 2, tipY)
                    lineTo(scx + triW / 2, tipY)
                    close()
                }, ColorUp)
            }
            if (hitPresets.isShort()) {
                val tipY = py(signalBar.low) + 4f
                drawPath(Path().apply {
                    moveTo(scx, tipY + triH)
                    lineTo(scx - triW / 2, tipY)
                    lineTo(scx + triW / 2, tipY)
                    close()
                }, ColorDown)
            }

            // ── 多K形態括弧標記 ───────────────────────────────────────
            val signals = hitPresets.activeSignals()
            val labelPaint = android.graphics.Paint().apply {
                textSize       = 20f
                isFakeBoldText = true
                isAntiAlias    = true
                textAlign      = android.graphics.Paint.Align.RIGHT
            }
            var bracketRow = 0
            for (pat in MULTI_CANDLE_PATTERNS) {
                if (pat.key !in signals) continue
                val idxs = pat.offsets.mapNotNull { off ->
                    val i = n - 1 - off; if (i >= 0) i else null
                }
                if (idxs.size < 2) continue

                val patColor = if (pat.isLong) ColorUp else ColorDown
                val lowestY  = idxs.maxOf { py(display[it].low) }
                val bracketY = (lowestY + 10f + bracketRow * 12f).coerceAtMost(priceH - 4f)
                bracketRow++

                val firstX = (idxs.first() + 0.5f) * candleW
                val lastX  = (idxs.last()  + 0.5f) * candleW

                drawLine(patColor, Offset(firstX, bracketY), Offset(lastX, bracketY), strokeWidth = 1f)
                for (x in listOf(firstX, lastX)) {
                    drawLine(patColor, Offset(x, bracketY - 3f), Offset(x, bracketY + 3f), strokeWidth = 1f)
                }
                for (idx in idxs) {
                    drawCircle(patColor, radius = 2.5f, center = Offset((idx + 0.5f) * candleW, bracketY))
                }
                labelPaint.color = android.graphics.Color.argb(
                    255,
                    (patColor.red   * 255).toInt(),
                    (patColor.green * 255).toInt(),
                    (patColor.blue  * 255).toInt(),
                )
                drawContext.canvas.nativeCanvas.drawText(
                    pat.key, firstX - 4f, bracketY + 7f, labelPaint
                )
            }
        }
    }
}

private fun rollingAvg(values: List<Double>, period: Int): List<Double> =
    values.indices.map { i ->
        values.subList(maxOf(0, i - period + 1), i + 1).average()
    }
