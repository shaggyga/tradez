#include <stdio.h>
#include "dummy_account.h"

void init_account(Account* acc, double starting_usd) {
    acc->usd_balance = starting_usd;
    acc->btc_balance = 0.0;

    // 🧼 Initialize all session tracking fields
    acc->total_trades = 0;
    acc->total_btc_bought = 0.0;
    acc->total_btc_sold = 0.0;
    acc->usd_spent = 0.0;
    acc->usd_earned = 0.0;
}


int buy_btc(Account* acc, double price, double quantity) {
    double cost = price * quantity;

    if (acc->usd_balance < cost) {
        printf("❌ Not enough USD to buy %.4f BTC ($%.2f).\n", quantity, cost);
        return 0;
    }

    acc->btc_balance += quantity;
    acc->usd_balance -= cost;

    // 📈 Update stats
    acc->total_trades++;
    acc->total_btc_bought += quantity;
    acc->usd_spent += cost;

    printf("✅ Bought %.4f BTC for $%.2f\n", quantity, cost);
    return 1;
}

int sell_btc(Account* acc, double price, double quantity) {
    if (acc->btc_balance < quantity) {
        printf("❌ Not enough BTC to sell %.4f.\n", quantity);
        return 0;
    }

    double gain = price * quantity;

    acc->btc_balance -= quantity;
    acc->usd_balance += gain;

    // 📈 Update stats
    acc->total_trades++;
    acc->total_btc_sold += quantity;
    acc->usd_earned += gain;

    printf("✅ Sold %.4f BTC for $%.2f\n", quantity, gain);
    return 1;
}


void show_account(const Account* acc) {
    printf("Account Status:\n");
    printf("  USD Balance: $%.2f\n", acc->usd_balance);
    printf("  BTC Balance: %.4f BTC\n", acc->btc_balance);
}

double calculate_unrealized_pnl(const Account* acc, double current_price) {
    double btc_value = acc->btc_balance * current_price;
    double total_value = acc->usd_balance + btc_value;
    double initial_balance = 1000.0;  // 🔧 Change this if you allow custom starting value

    return total_value - initial_balance;
}

