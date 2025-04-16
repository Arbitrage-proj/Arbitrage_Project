import asyncio
import requests
import os
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP as BybitClient
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
import ccxt

# ===== API Keys =====
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
BYBIT_API_KEY = os.environ.get("BYBIT_API_KEY")
BYBIT_SECRET = os.environ.get("BYBIT_API_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

# ===== Инициализация клиентов =====
binance_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
bybit_client = BybitClient(api_key=BYBIT_API_KEY, api_secret=BYBIT_SECRET)

# ===== Порог прибыли =====
THRESHOLD = 0

# ===== Получение общих торговых пар =====
def fetch_common_tokens():
    binance = ccxt.binance({"enableRateLimit": True})
    bybit = ccxt.bybit({
        'apiKey': BYBIT_API_KEY,
        'secret': BYBIT_SECRET,
        'enableRateLimit': True
    })

    try:
        binance.load_markets()
        bybit.load_markets()

        binance_tokens = set(binance.symbols)
        bybit_tokens = set(bybit.symbols)

        common = binance_tokens.intersection(bybit_tokens)
        return list(common)[:20]  # Limit to 20 tokens to reduce API load
    except Exception as e:
        print(f"[ERROR] Failed to fetch common tokens: {e}")
        return ["BTC/USDT"]

# ===== Получение цены по паре =====
def get_binance_price(symbol):
    try:
        ticker = binance_client.get_symbol_ticker(symbol=symbol.replace("/", ""))
        price = float(ticker["price"])
        print(f"[INFO] Binance {symbol} Price: {price}")
        return price
    except Exception as e:
        print(f"[ERROR] Error fetching Binance price for {symbol}: {e}")
        return 0.0

def get_bybit_price(symbol):
    try:
        response = bybit_client.get_tickers(category="linear", symbol=symbol.replace("/", ""))
        if "result" in response and response["result"]["list"]:
            price = float(response["result"]["list"][0]["lastPrice"])
            print(f"[INFO] Bybit {symbol} Price: {price}")
            return price
        else:
            print("[ERROR] Invalid Bybit response", response)
            return 0.0
    except Exception as e:
        print(f"[ERROR] Error fetching Bybit price for {symbol}: {e}")
        return 0.0

# ===== Telegram Bot Command =====
async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tokens = fetch_common_tokens()
    messages = []
    for symbol in tokens:
        binance_price = get_binance_price(symbol)
        bybit_price = get_bybit_price(symbol)

        if not binance_price or not bybit_price:
            continue

        diff = abs(binance_price - bybit_price)
        if diff >= THRESHOLD:
            msg = (
                f"⚡️ Arbitrage Alert!\n"
                f"{symbol}\n"
                f"Binance: {binance_price}$\n"
                f"Bybit: {bybit_price}$\n"
                f"📈 Diff: {round(diff, 2)}$"
            )
            messages.append(msg)

    if messages:
        for msg in messages:
            await update.message.reply_text(msg)
    else:
        await update.message.reply_text("🔍 No arbitrage opportunities above threshold.")

# ===== Main App =====
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("scan", scan_command))
    print("[INFO] Telegram bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
