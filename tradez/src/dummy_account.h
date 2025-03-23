#ifndef DUMMY_ACCOUNT_H
#define DUMMY_ACCOUNT_H

typedef struct {
    double usd_balance;
    double btc_balance;

    // 🔢 Session stats
    int total_trades;
    double total_btc_bought;
    double total_btc_sold;
    double usd_spent;
    double usd_earned;
} Account;

void init_account(Account* acc, double starting_usd);
int buy_btc(Account* acc, double price, double quantity);
int sell_btc(Account* acc, double price, double quantity);
void show_account(const Account* acc);

// ✅ ADD THIS LINE
double calculate_unrealized_pnl(const Account* acc, double current_price);

#endif
