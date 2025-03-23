#define _CRT_SECURE_NO_WARNINGS
#define PRICE_HISTORY_SIZE 60

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "dummy_account.h"
#include "market_data.h"
#include "trade_logger.h"
#include <time.h>


void track_price(double price, double* price_history, int* price_index) {
    price_history[*price_index % PRICE_HISTORY_SIZE] = price;
    (*price_index)++;
}
void show_trade_history(const char* filename, int num_lines) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        printf("⚠️ Could not open trade history file.\n");
        return;
    }

    // First, count total lines
    int total_lines = 0;
    char buffer[256];
    while (fgets(buffer, sizeof(buffer), file)) {
        total_lines++;
    }

    // Rewind and print the last `num_lines`
    rewind(file);
    int current = 0;
    while (fgets(buffer, sizeof(buffer), file)) {
        if (current >= total_lines - num_lines) {
            printf("%s", buffer);
        }
        current++;
    }

    fclose(file);
}

int main() {
    char input[100] = { 0 };
    char command[20] = { 0 };
    double quantity = 1.0;
    int running = 1;
    double price_history[PRICE_HISTORY_SIZE] = { 0 };
    int price_index = 0;
    Account my_account;

    init_account(&my_account, 100000.0);
    printf("1:59\n");

    while (running) {
        printf("\n> ");
        fflush(stdout);

        if (fgets(input, sizeof(input), stdin) == NULL) {
            printf("Input error.\n");
            break;
        }

        input[strcspn(input, "\r\n")] = '\0';
        strcpy(command, "");
        quantity = 1.0;
        sscanf(input, "%s %lf", command, &quantity);

        if (strcmp(command, "price") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) {
                printf("api fucked up");
                price = 42000.0;
            }
            track_price(price, price_history, &price_index);
            printf("BTC Price: $%.2f\n", price);
            printf("AYYYYYY");
            time_t now = time(NULL); // Get current time
            printf("Fetched at: %s", ctime(&now));
        }

        else if (strcmp(command, "buy") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) price = 42000.0;
            track_price(price, price_history, &price_index);

            if (quantity <= 0.0) {
                printf("Invalid quantity.\n");
                continue;
            }

            if (buy_btc(&my_account, price, quantity)) {
                log_trade("BUY", price, quantity, my_account.usd_balance, my_account.btc_balance);
            }
        }

        else if (strcmp(command, "sell") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) price = 42000.0;
            track_price(price, price_history, &price_index);

            if (quantity <= 0.0) {
                printf("Invalid quantity.\n");
                continue;
            }

            if (sell_btc(&my_account, price, quantity)) {
                log_trade("SELL", price, quantity, my_account.usd_balance, my_account.btc_balance);
            }
        }
        else if (strcmp(command, "history") == 0) {
            printf("\n📜 Last 10 Trades:\n");
            show_trade_history("data/trades.csv", 10);
        }

        else if (strcmp(command, "status") == 0) {
            show_account(&my_account);
        }

        else if (strcmp(command, "pnl") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) { 
                price = 42000.0; }

            track_price(price, price_history, &price_index);

            double pnl = calculate_unrealized_pnl(&my_account, price);
            printf("📊 Unrealized PnL: %s$%.2f\n", pnl >= 0 ? "" : "-", fabs(pnl));
        }

        else if (strcmp(command, "summary") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) price = 42000.0;
            track_price(price, price_history, &price_index);

            double pnl = calculate_unrealized_pnl(&my_account, price);
            printf("\n📋 SESSION SUMMARY 📋\n");
            printf("Total trades:          %d\n", my_account.total_trades);
            printf("BTC bought:            %.6f BTC\n", my_account.total_btc_bought);
            printf("BTC sold:              %.6f BTC\n", my_account.total_btc_sold);
            printf("USD spent:             $%.2f\n", my_account.usd_spent);
            printf("USD earned:            $%.2f\n", my_account.usd_earned);
            printf("----------------------------------\n");
            printf("USD Balance:           $%.2f\n", my_account.usd_balance);
            printf("BTC Balance:           %.6f BTC\n", my_account.btc_balance);
            printf("📊 Unrealized PnL:     %s$%.2f\n", pnl >= 0 ? "" : "-", fabs(pnl));
        }

        else if (strcmp(command, "signal") == 0) {
            double price = fetch_btc_price();
            if (price <= 0) price = 42000.0;
            track_price(price, price_history, &price_index);

            if (price_index < 2) {
                printf("📉 Not enough data for signal detection yet.\n");
            }
            else {
                int latest = (price_index - 1) % PRICE_HISTORY_SIZE;
                int prev = (price_index - 2) % PRICE_HISTORY_SIZE;

                double price_now = price_history[latest];
                double price_prev = price_history[prev];

                double change = price_now - price_prev;
                double percent = (change / price_prev) * 100.0;

                printf("📊 Price Now: $%.2f\n", price_now);
                printf("📉 Price Change: %.2f%%\n", percent);
				


                if (percent >= 1.0) {
                    printf("📈 Signal: Spike detected!\n");
                }
                else if (percent <= -1.0) {
                    printf("📉 Signal: Drop detected!\n");
                }
                else {
                    printf("😐 No strong signal.\n");
                }
            }
        }
        else if (strcmp(command, "exit") == 0) {
            running = 0;
        }

        else {
            printf("Unknown command: '%s'\n", command);
        }

        memset(input, 0, sizeof(input));
    }

    printf("Goodbye!\n");
    return 0;
}
