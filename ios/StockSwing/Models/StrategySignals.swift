import Foundation

struct StrategySignals: Codable {
    var A: Bool = false;  var AS: Bool = false
    var B: Bool = false;  var BS: Bool = false
    var C: Bool = false;  var CS: Bool = false
    var D: Bool = false;  var DS: Bool = false
    var E: Bool = false;  var ES: Bool = false
    var F: Bool = false;  var FS: Bool = false
    var G: Bool = false;  var GS: Bool = false
    var H: Bool = false;  var HS: Bool = false
    var I: Bool = false;  var IS: Bool = false
    var J: Bool = false;  var JS: Bool = false
    var K: Bool = false;  var KS: Bool = false
    var L: Bool = false;  var LS: Bool = false
    var M: Bool = false;  var MS: Bool = false
    var N: Bool = false;  var NS: Bool = false
    var O: Bool = false;  var OS: Bool = false
    var P: Bool = false;  var PS: Bool = false
    var Q: Bool = false;  var QS: Bool = false
    var R: Bool = false;  var RS: Bool = false
    var B2: Bool = false

    var hitLong: Int {
        [A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, B2].filter { $0 }.count
    }
    var hitShort: Int {
        [AS, BS, CS, DS, ES, FS, GS, HS, IS, JS, KS, LS, MS, NS, OS, PS, QS, RS].filter { $0 }.count
    }
    var hit: Int { max(hitLong, hitShort) }
    var direction: String { hitLong >= hitShort ? "多" : "空" }

    var activeSignals: [String] {
        var result: [String] = []
        if A  { result.append("A")  }; if AS { result.append("AS") }
        if B  { result.append("B")  }; if BS { result.append("BS") }
        if C  { result.append("C")  }; if CS { result.append("CS") }
        if D  { result.append("D")  }; if DS { result.append("DS") }
        if E  { result.append("E")  }; if ES { result.append("ES") }
        if F  { result.append("F")  }; if FS { result.append("FS") }
        if G  { result.append("G")  }; if GS { result.append("GS") }
        if H  { result.append("H")  }; if HS { result.append("HS") }
        if I  { result.append("I")  }; if IS { result.append("IS") }
        if J  { result.append("J")  }; if JS { result.append("JS") }
        if K  { result.append("K")  }; if KS { result.append("KS") }
        if L  { result.append("L")  }; if LS { result.append("LS") }
        if M  { result.append("M")  }; if MS { result.append("MS") }
        if N  { result.append("N")  }; if NS { result.append("NS") }
        if O  { result.append("O")  }; if OS { result.append("OS") }
        if P  { result.append("P")  }; if PS { result.append("PS") }
        if Q  { result.append("Q")  }; if QS { result.append("QS") }
        if R  { result.append("R")  }; if RS { result.append("RS") }
        if B2 { result.append("B2") }
        return result
    }

    func flag(for name: String) -> Bool {
        switch name {
        case "A":  return A;  case "AS": return AS
        case "B":  return B;  case "BS": return BS
        case "C":  return C;  case "CS": return CS
        case "D":  return D;  case "DS": return DS
        case "E":  return E;  case "ES": return ES
        case "F":  return F;  case "FS": return FS
        case "G":  return G;  case "GS": return GS
        case "H":  return H;  case "HS": return HS
        case "I":  return I;  case "IS": return IS
        case "J":  return J;  case "JS": return JS
        case "K":  return K;  case "KS": return KS
        case "L":  return L;  case "LS": return LS
        case "M":  return M;  case "MS": return MS
        case "N":  return N;  case "NS": return NS
        case "O":  return O;  case "OS": return OS
        case "P":  return P;  case "PS": return PS
        case "Q":  return Q;  case "QS": return QS
        case "R":  return R;  case "RS": return RS
        case "B2": return B2
        default: return false
        }
    }

    static var empty: StrategySignals { StrategySignals() }
}
