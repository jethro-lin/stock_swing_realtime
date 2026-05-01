package com.stockswing.app.ui

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Size
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.graphics.nativeCanvas
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.stockswing.app.model.HistoricalBar
import com.stockswing.app.model.SignalResult

private val ChartGreen  = Color(0xFF00C853)
private val ChartRed    = Color(0xFFD50000)
private val ChartBlue   = Color(0xFF2196F3)
private val ChartOrange = Color(0xFFFF9800)

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
                    val pctColor = if (result.quote.chgPct >= 0) ChartGreen else ChartRed
                    Text(
                        "%.2f  %+.2f%%".format(result.quote.price, result.quote.chgPct),
                        color    = pctColor,
                        fontSize = 14.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                }
                // MA legend
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    MaLegend("MA5",  ChartGreen)
                    MaLegend("MA10", ChartBlue)
                    MaLegend("MA20", ChartOrange)
                }
            }

            Spacer(Modifier.height(12.dp))

            if (bars.isEmpty()) {
                Box(
                    Modifier
                        .fillMaxWidth()
                        .height(280.dp),
                    Alignment.Center
                ) {
                    CircularProgressIndicator(modifier = Modifier.size(32.dp))
                }
            } else {
                CandlestickChart(
                    bars     = bars,
                    modifier = Modifier
                        .fillMaxWidth()
                        .height(320.dp),
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

@Composable
fun CandlestickChart(bars: List<HistoricalBar>, modifier: Modifier = Modifier) {
    val display = bars.takeLast(60)
    if (display.isEmpty()) return

    val closes = display.map { it.close }
    val ma5  = rollingAvg(closes, 5)
    val ma10 = rollingAvg(closes, 10)
    val ma20 = rollingAvg(closes, 20)

    Canvas(modifier = modifier) {
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
        val gridColor = Color(0x22000000)
        repeat(5) { k ->
            val y = priceH * k / 4f
            drawLine(gridColor, Offset(0f, y), Offset(w, y), strokeWidth = 0.5f)
        }

        // ── 價格標籤 ─────────────────────────────────────────────────
        val textPaint = android.graphics.Paint().apply {
            color     = android.graphics.Color.argb(140, 100, 100, 100)
            textSize  = 22f
            textAlign = android.graphics.Paint.Align.RIGHT
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
            val color = if (isUp) ChartGreen else ChartRed

            // wick
            drawLine(color, Offset(cx, py(bar.high)), Offset(cx, py(bar.low)), strokeWidth = 1f)

            // body
            val top = py(maxOf(bar.open, bar.close))
            val bot = py(minOf(bar.open, bar.close))
            drawRect(
                color   = color,
                topLeft = Offset(cx - bodyW / 2, top),
                size    = Size(bodyW, (bot - top).coerceAtLeast(1f)),
            )

            // volume
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
    }
}

private fun rollingAvg(values: List<Double>, period: Int): List<Double> =
    values.indices.map { i ->
        values.subList(maxOf(0, i - period + 1), i + 1).average()
    }
