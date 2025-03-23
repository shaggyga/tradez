#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "market_data.h"

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    size_t totalSize = size * nmemb;
    strncat_s((char*)userp, 1024, (char*)contents, totalSize);
    return totalSize;
}

double fetch_btc_price() {
    CURL* curl;
    CURLcode res;
    char buffer[1024] = { 0 };

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();
    if (!curl) return -1;

    curl_easy_setopt(curl, CURLOPT_URL, "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, buffer);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) return -1;

    // Parse price from response
    char* price_ptr = strstr(buffer, "usd\":");
    if (!price_ptr) return -1;

    double price = atof(price_ptr + 5);
    return price;
}
