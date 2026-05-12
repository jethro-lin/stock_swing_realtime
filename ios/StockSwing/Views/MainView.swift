import SwiftUI

private enum PriceGroup: String, CaseIterable {
    case all       = "全部"
    case p50       = "< 50"
    case p50_100   = "50~100"
    case p100_300  = "100~300"
    case p300_500  = "300~500"
    case p500_1000 = "500~1000"
    case p1000     = "> 1000"
}

private extension SignalResult {
    var priceGroup: PriceGroup {
        switch quote.price {
        case ..<50:   return .p50
        case ..<100:  return .p50_100
        case ..<300:  return .p100_300
        case ..<500:  return .p300_500
        case ..<1000: return .p500_1000
        default:      return .p1000
        }
    }
}

struct MainView: View {
    @EnvironmentObject var vm: StockViewModel
    @State private var selectedTab   = 0
    @State private var showScanSheet = false

    private var groups: [PriceGroup] { PriceGroup.allCases }

    private func items(for group: PriceGroup) -> [SignalResult] {
        group == .all ? vm.scanResults : vm.scanResults.filter { $0.priceGroup == group }
    }

    var body: some View {
        VStack(spacing: 0) {
            // TopBar
            topBar

            // Progress / Error / Info
            if vm.isLoading { progressBar }
            if let err = vm.error { errorBanner(err) }
            if !vm.isLoading && !vm.lastScanTime.isEmpty { scanInfoBar }

            // Empty state
            if !vm.isLoading && vm.scanResults.isEmpty && vm.error == nil {
                Spacer()
                Text(vm.lastScanTime.isEmpty ? "請點「掃描」開始選股" : "本次掃描無符合標的")
                    .foregroundColor(.gray)
                    .font(.system(size: 14))
                Spacer()
            } else {
                tabBar
                Divider()
                listHeader
                Divider()
                // TabView with page style enables horizontal swipe between tabs
                TabView(selection: $selectedTab) {
                    ForEach(Array(groups.enumerated()), id: \.offset) { idx, group in
                        let pageItems = items(for: group)
                        Group {
                            if pageItems.isEmpty {
                                VStack {
                                    Spacer()
                                    Text("此價位區間無標的")
                                        .foregroundColor(.gray)
                                        .font(.system(size: 14))
                                    Spacer()
                                }
                            } else {
                                List(pageItems) { result in
                                    StockRowView(result: result, onTap: { vm.openChart(result) })
                                        .listRowInsets(EdgeInsets())
                                        .listRowSeparatorTint(Color(white: 0.93))
                                }
                                .listStyle(.plain)
                            }
                        }
                        .tag(idx)
                    }
                }
                .tabViewStyle(.page(indexDisplayMode: .never))
                .animation(.easeInOut(duration: 0.2), value: selectedTab)
            }
        }
        .sheet(item: $vm.chartTarget) { result in
            KBarChartSheet(result: result)
                .environmentObject(vm)
        }
        .sheet(isPresented: $showScanSheet) {
            ScanDialogView(
                initialMode:       vm.lastScanMode,
                initialPresets:    vm.selectedPresets,
                initialCustomSigs: vm.selectedCustomSignals,
                onDismiss:         { showScanSheet = false },
                onConfirm:         { presets, customSigs, date in
                    showScanSheet = false
                    vm.scan(presets: presets, customSignals: customSigs, overrideDate: date)
                }
            )
        }
    }

    // MARK: - Sub-views

    private var topBar: some View {
        HStack {
            Text("台股選股")
                .font(.headline)
                .fontWeight(.bold)
            Spacer()
            if vm.isLoading {
                Button("停止") { vm.stopScan() }
                    .buttonStyle(.bordered)
            } else {
                Button("掃描") { showScanSheet = true }
                    .buttonStyle(.borderedProminent)
            }
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
        .background(Color(.systemGroupedBackground))
    }

    private var progressBar: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack {
                Text(vm.loadingMsg)
                    .font(.system(size: 12))
                    .lineLimit(1)
                    .truncationMode(.tail)
                Spacer()
                if vm.scanProgress.total > 0 {
                    Text("\(vm.scanProgress.done)/\(vm.scanProgress.total)")
                        .font(.system(size: 11))
                        .foregroundColor(.gray)
                }
            }
            if vm.scanProgress.total > 0 {
                ProgressView(value: Double(vm.scanProgress.done),
                             total: Double(vm.scanProgress.total))
            } else {
                ProgressView()
                    .progressViewStyle(.linear)
                    .frame(maxWidth: .infinity)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .background(Color(.secondarySystemBackground))
    }

    private func errorBanner(_ msg: String) -> some View {
        Text(msg)
            .font(.system(size: 12))
            .foregroundColor(Color(red: 0.835, green: 0, blue: 0))
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 12)
            .padding(.vertical, 6)
            .background(Color(red: 1, green: 0.922, blue: 0.922))
    }

    private var scanInfoBar: some View {
        HStack(spacing: 16) {
            if !vm.signalDate.isEmpty {
                Text("訊號日").font(.system(size: 11)).foregroundColor(.gray)
                Text(vm.signalDate).font(.system(size: 12, weight: .semibold))
            }
            Spacer()
            Text("掃描").font(.system(size: 11)).foregroundColor(.gray)
            Text(vm.lastScanTime).font(.system(size: 12)).foregroundColor(.gray)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 4)
        .background(Color(.systemGroupedBackground))
    }

    private var tabBar: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 0) {
                ForEach(Array(groups.enumerated()), id: \.offset) { idx, group in
                    let count = items(for: group).count
                    let label = count > 0 ? "\(group.rawValue) (\(count))" : group.rawValue
                    Button {
                        selectedTab = idx
                    } label: {
                        VStack(spacing: 2) {
                            Text(label)
                                .font(.system(size: 12))
                                .foregroundColor(selectedTab == idx ? .accentColor : .gray)
                                .padding(.horizontal, 12)
                                .padding(.vertical, 8)
                            if selectedTab == idx {
                                Rectangle()
                                    .fill(Color.accentColor)
                                    .frame(height: 2)
                            } else {
                                Rectangle()
                                    .fill(Color.clear)
                                    .frame(height: 2)
                            }
                        }
                    }
                    .buttonStyle(.plain)
                }
            }
        }
        .background(Color(.systemBackground))
    }

    private var listHeader: some View {
        HStack {
            Spacer().frame(width: 14)
            Text("代號/名稱")
                .font(.system(size: 11))
                .foregroundColor(.gray)
                .frame(maxWidth: .infinity, alignment: .leading)
            Text("現價")
                .font(.system(size: 11))
                .foregroundColor(.gray)
                .frame(minWidth: 60, alignment: .trailing)
            Text("漲跌幅")
                .font(.system(size: 11))
                .foregroundColor(.gray)
                .frame(minWidth: 64, alignment: .trailing)
            Text("成交量")
                .font(.system(size: 11))
                .foregroundColor(.gray)
                .frame(minWidth: 60, alignment: .trailing)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(Color(.systemGroupedBackground))
    }
}
