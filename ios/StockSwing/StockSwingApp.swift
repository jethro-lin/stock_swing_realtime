import SwiftUI

@main
struct StockSwingApp: App {
    @StateObject private var viewModel = StockViewModel()

    var body: some Scene {
        WindowGroup {
            MainView()
                .environmentObject(viewModel)
        }
    }
}
