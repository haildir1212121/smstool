package com.dispatchcommand.sms;

import android.os.Bundle;
import android.webkit.WebSettings;
import android.webkit.WebView;
import com.getcapacitor.BridgeActivity;

public class MainActivity extends BridgeActivity {

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    public void onStart() {
        super.onStart();

        // Configure WebView for optimal app performance
        WebView webView = getBridge().getWebView();
        WebSettings settings = webView.getSettings();

        // Enable DOM storage (used by Firebase and app state)
        settings.setDomStorageEnabled(true);

        // Enable JavaScript (required)
        settings.setJavaScriptEnabled(true);

        // Enable database storage
        settings.setDatabaseEnabled(true);

        // Allow file access for Excel/CSV uploads
        settings.setAllowFileAccess(true);
        settings.setAllowContentAccess(true);

        // Enable mixed content (HTTPS to HTTPS only in our case)
        settings.setMediaPlaybackRequiresUserGesture(false);

        // Improve rendering performance
        settings.setCacheMode(WebSettings.LOAD_DEFAULT);
        settings.setLoadWithOverviewMode(true);
        settings.setUseWideViewPort(true);
    }
}
