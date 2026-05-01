package com.stockswing.app

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.material3.MaterialTheme
import com.google.android.gms.ads.MobileAds

class MainActivity : ComponentActivity() {

    private val vm: StockViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        MobileAds.initialize(this) {}

        setContent {
            MaterialTheme {
                com.stockswing.app.ui.MainScreen(vm)
            }
        }
    }
}
