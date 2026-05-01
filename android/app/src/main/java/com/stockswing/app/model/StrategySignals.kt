package com.stockswing.app.model

import kotlinx.serialization.Serializable

@Serializable
/** 對應 Python check_signals() 的回傳值，37 個策略旗標 */
data class StrategySignals(
    val A: Boolean = false,  val AS: Boolean = false,
    val B: Boolean = false,  val BS: Boolean = false,
    val C: Boolean = false,  val CS: Boolean = false,
    val D: Boolean = false,  val DS: Boolean = false,
    val E: Boolean = false,  val ES: Boolean = false,
    val F: Boolean = false,  val FS: Boolean = false,
    val G: Boolean = false,  val GS: Boolean = false,
    val H: Boolean = false,  val HS: Boolean = false,
    val I: Boolean = false,  val IS: Boolean = false,
    val J: Boolean = false,  val JS: Boolean = false,
    val K: Boolean = false,  val KS: Boolean = false,
    val L: Boolean = false,  val LS: Boolean = false,
    val M: Boolean = false,  val MS: Boolean = false,
    val N: Boolean = false,  val NS: Boolean = false,
    val O: Boolean = false,  val OS: Boolean = false,
    val P: Boolean = false,  val PS: Boolean = false,
    val Q: Boolean = false,  val QS: Boolean = false,
    val R: Boolean = false,  val RS: Boolean = false,
    val B2: Boolean = false,
) {
    // 多方命中：不以 S 結尾（含 B2）
    val hitLong: Int get() = listOf(A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,B2).count { it }
    // 空方命中：以 S 結尾
    val hitShort: Int get() = listOf(AS,BS,CS,DS,ES,FS,GS,HS,IS,JS,KS,LS,MS,NS,OS,PS,QS,RS).count { it }
    val hit: Int get() = maxOf(hitLong, hitShort)
    val direction: String get() = if (hitLong >= hitShort) "多" else "空"

    val activeSignals: List<String> get() = buildList {
        if (A)  add("A");  if (AS) add("AS")
        if (B)  add("B");  if (BS) add("BS")
        if (C)  add("C");  if (CS) add("CS")
        if (D)  add("D");  if (DS) add("DS")
        if (E)  add("E");  if (ES) add("ES")
        if (F)  add("F");  if (FS) add("FS")
        if (G)  add("G");  if (GS) add("GS")
        if (H)  add("H");  if (HS) add("HS")
        if (I)  add("I");  if (IS) add("IS")
        if (J)  add("J");  if (JS) add("JS")
        if (K)  add("K");  if (KS) add("KS")
        if (L)  add("L");  if (LS) add("LS")
        if (M)  add("M");  if (MS) add("MS")
        if (N)  add("N");  if (NS) add("NS")
        if (O)  add("O");  if (OS) add("OS")
        if (P)  add("P");  if (PS) add("PS")
        if (Q)  add("Q");  if (QS) add("QS")
        if (R)  add("R");  if (RS) add("RS")
        if (B2) add("B2")
    }

    companion object {
        fun empty() = StrategySignals()
    }
}
