import Foundation

struct StrategyBase {
    // 量
    let avg5: Double
    let avg20Vol: Double
    let volYesterday: Double
    let volExpand: Bool
    let volShrink: Bool
    let tideShrink: Bool
    // 均線
    let ma5: Double
    let ma10: Double
    let ma20: Double
    // 均線方向：1=上升, -1=下降, 0=持平
    let ma5Dir:  Int
    let ma10Dir: Int
    let ma20Dir: Int
    // MA20 趨勢（10日斜率）：1=上升趨勢, -1=下降趨勢, 0=橫盤
    let ma20Trend: Int
    // RSI
    let rsiNow: Double
    let rsiPrv: Double
    // 高低
    let high5: Double
    let low5: Double
    // 連漲/連跌
    let threeUp: Bool
    let threeDn: Bool
    // 昨日K棒
    let prevClose: Double
    let prevOpen: Double
    let prevBody: Double
    let prevBull: Bool
    let prevBear: Bool
    // MACD
    let macdT: Double; let macdP: Double
    let msigT: Double; let msigP: Double
    // Bollinger
    let bbUpperT: Double; let bbUpperP: Double
    let bbLowerT: Double; let bbLowerP: Double
    // KD
    let kkT: Double; let kdT: Double
    let kkP: Double; let kdP: Double
    // Williams %R
    let wrT: Double; let wrP: Double
    // 晨星/黃昏之星
    let starD1Body: Double; let starD1Mid: Double
    let starD1Bear: Bool;   let starD1Bull: Bool
    let starD2Body: Double
    // 紅三兵/黑三兵
    let solD1O: Double; let solD1C: Double
    let solD2O: Double; let solD2C: Double
    // Inside Bar
    let ibPrev2H: Double; let ibPrev2L: Double
    let ibIsInside: Bool
}
