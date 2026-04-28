package com.stockswing.app

import android.Manifest
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.material3.MaterialTheme
import androidx.lifecycle.lifecycleScope
import com.stockswing.app.ui.MainScreen
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {

    private val vm: StockViewModel by viewModels()

    private val notifPermission =
        registerForActivityResult(ActivityResultContracts.RequestPermission()) { }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // 請求通知權限（Android 13+）
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            notifPermission.launch(Manifest.permission.POST_NOTIFICATIONS)
        }

        // 啟動時自動連線到上次儲存的 URL
        lifecycleScope.launch {
            vm.savedUrl.collect { url ->
                if (url.isNotBlank() && vm.wsState.value == WsState.DISCONNECTED) {
                    vm.connect(url)
                }
            }
        }

        setContent {
            MaterialTheme {
                MainScreen(vm)
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        vm.disconnect()
    }
}
