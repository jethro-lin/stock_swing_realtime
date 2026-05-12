package com.stockswing.app.model

enum class Preset(val key: String, val label: String) {
    LONG3_LEAN    ("long3_lean",    "多方精簡"),
    LONG3_PATTERN ("long3_pattern", "多方形態"),
    LONG_MOMENTUM ("long_momentum", "多方動能"),
    LONG_TREND    ("long_trend",    "多方趨勢"),
    SHORT3_LEAN   ("short3_lean",   "空方精簡"),
}

// 每個 combo = 必須同時成立的訊號集合，直接對應 Python COMBO_PRESETS
private val COMBOS: Map<Preset, List<List<String>>> = mapOf(
    Preset.LONG3_LEAN to listOf(
        listOf("M","R","B2"), listOf("R","B2"),    listOf("B","K","R"),
        listOf("B","K","L"),  listOf("B2","K"),
        listOf("B","F","L"),  listOf("B","F","K"), listOf("B","L","R"),
        listOf("B","F","R"),  listOf("K","L","R"), listOf("B","M","R"),
        listOf("B","R"),      listOf("B","K"),     listOf("K","R"), listOf("B2","M"),
    ),
    // long3_pattern — 形態×超賣組合（2026-05-05 新增，B+P/B2+J 因 P 參數調整後轉負 EV 移除）
    Preset.LONG3_PATTERN to listOf(
        listOf("R","O"), listOf("M","G"),
        listOf("B","O"), listOf("B","J"), listOf("B","Q"),
    ),
    // long_momentum — 趨勢動能，持倉 15 日（止損 7%）
    Preset.LONG_MOMENTUM to listOf(
        listOf("D","N"),     listOf("N","F"),     listOf("D","F"),     listOf("D","J"),
        listOf("A","D"),     listOf("A","J"),     listOf("A","N"),     listOf("D","Q"),
        listOf("E","N"),     listOf("A","F"),     listOf("D","N","F"), listOf("N","J"),
    ),
    // long_trend 已於 swing_trade_v2.py 廢止（回測負 EV），保留 enum 但清空 combo
    Preset.LONG_TREND to emptyList(),
    Preset.SHORT3_LEAN to listOf(
        listOf("BS","GS"),        listOf("BS","MS","RS"), listOf("BS","RS"),
        listOf("BS","FS","NS"),
        listOf("BS","FS","JS"),   listOf("AS","DS","GS"), listOf("FS","RS"),
        listOf("BS","KS"),        listOf("BS","OS"),
        listOf("BS","DS","NS"),   listOf("CS","FS","JS"), listOf("AS","BS","DS"),
        listOf("BS","CS","JS"),   listOf("BS","MS"),
    ),
)

/** 該 preset 是否有非空的 combo 定義（空的不在選擇器中顯示） */
val Preset.hasCombo: Boolean get() = COMBOS[this]?.isNotEmpty() == true

/** 自選模式：回傳所有命中的個別訊號 + 自訂組合標籤（"A+B" 格式） */
fun StrategySignals.matchedCustomLabels(customSigs: Set<String>): List<String> {
    val individualHits = activeSignals.filter { it in customSigs }
    val comboHits = customSigs
        .filter { it.contains("+") }
        .filter { combo -> combo.split("+").all { getSignalFlag(it) } }
        .sorted()
    return individualHits + comboHits
}

/** 是否命中指定 preset 的任意 combo */
fun StrategySignals.matchesPreset(preset: Preset): Boolean =
    COMBOS[preset]?.any { combo -> combo.all { getSignalFlag(it) } } ?: false

/** 回傳所有命中的 combo 標籤（如 ["B+K+R", "B+K+L"]） */
fun StrategySignals.matchedComboLabels(preset: Preset): List<String> =
    COMBOS[preset]
        ?.filter { combo -> combo.all { getSignalFlag(it) } }
        ?.map    { combo -> combo.joinToString("+") }
        ?: emptyList()

private fun StrategySignals.getSignalFlag(name: String): Boolean = when (name) {
    "A"  -> A;  "AS" -> AS
    "B"  -> B;  "BS" -> BS
    "C"  -> C;  "CS" -> CS
    "D"  -> D;  "DS" -> DS
    "E"  -> E;  "ES" -> ES
    "F"  -> F;  "FS" -> FS
    "G"  -> G;  "GS" -> GS
    "H"  -> H;  "HS" -> HS
    "I"  -> I;  "IS" -> IS
    "J"  -> J;  "JS" -> JS
    "K"  -> K;  "KS" -> KS
    "L"  -> L;  "LS" -> LS
    "M"  -> M;  "MS" -> MS
    "N"  -> N;  "NS" -> NS
    "O"  -> O;  "OS" -> OS
    "P"  -> P;  "PS" -> PS
    "Q"  -> Q;  "QS" -> QS
    "R"  -> R;  "RS" -> RS
    "B2" -> B2
    else -> false
}
