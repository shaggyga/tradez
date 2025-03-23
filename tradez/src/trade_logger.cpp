#define _CRT_SECURE_NO_WARNINGS
#include <stdio.h>
#include <time.h>
#include "trade_logger.h"

void log_trade(const char* action, double price, double quantity, double usd_balance, double btc_balance) {
    FILE* file = fopen("data/trades.csv", "a");  // Append mode
    if (!file) {
        printf("⚠️ Could not open trades.csv for logging.\n");
        return;
    }

    // Get current timestamp
    time_t t = time(NULL);
    struct tm* tm_info = localtime(&t);
    char timestamp[26];
    strftime(timestamp, 26, "%Y-%m-%d %H:%M:%S", tm_info);

    // Write to file
    fprintf(file, "%s,%s,%.4f,%.2f,%.2f,%.4f\n",
        timestamp, action, quantity, price, usd_balance, btc_balance);

    fclose(file);
}
