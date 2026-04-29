package com.stockswing.app.model

enum class Preset(val key: String, val label: String) {
    LONG3_LEAN("long3_lean",  "多方精簡"),
    LONG_TREND("long_trend",  "多方趨勢"),
    SHORT3_LEAN("short3_lean", "空方精簡"),
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
    Preset.LONG_TREND to listOf(
        listOf("B2","N","F"), listOf("B2","F"),    listOf("B2","A","D"),
        listOf("B2","A"),     listOf("B2","D"),    listOf("B2","N"),
        listOf("B","N","F"),  listOf("B","E","F"), listOf("B","F"),
        listOf("B","E","D"),  listOf("B","E"),     listOf("B","N"),
        listOf("B","A","D"),  listOf("B","N","D"), listOf("B","A"), listOf("B","D"),
    ),
    Preset.SHORT3_LEAN to listOf(
        listOf("BS","GS"),        listOf("BS","MS","RS"), listOf("BS","RS"),
        listOf("BS","FS","NS"),
        listOf("BS","FS","JS"),   listOf("AS","DS","GS"), listOf("FS","RS"),
        listOf("BS","KS"),        listOf("BS","OS"),
        listOf("BS","DS","NS"),   listOf("CS","FS","JS"), listOf("AS","BS","DS"),
        listOf("BS","CS","JS"),   listOf("BS","MS"),
    ),
)

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
