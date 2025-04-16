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
KRAKEN_API_KEY = os.environ.get("KRAKEN_API_KEY")
KRAKEN_SECRET = os.environ.get("KRAKEN_API_SECRET")
KUCOIN_API_KEY = os.environ.get("KUCOIN_API_KEY")
KUCOIN_SECRET = os.environ.get("KUCOIN_API_SECRET")
KUCOIN_PASSPHRASE = os.environ.get("KUCOIN_PASSPHRASE")
OKX_API_KEY = os.environ.get("OKX_API_KEY")
OKX_SECRET = os.environ.get("OKX_API_SECRET")
OKX_PASSPHRASE = os.environ.get("OKX_PASSPHRASE")
BINGX_API_KEY = os.environ.get("BINGX_API_KEY")
BINGX_SECRET = os.environ.get("BINGX_API_SECRET")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

# ===== Инициализация клиентов =====
binance_client = BinanceClient(BINANCE_API_KEY, BINANCE_API_SECRET)
bybit_client = BybitClient(api_key=BYBIT_API_KEY, api_secret=BYBIT_SECRET)

# ===== Порог прибыли =====
THRESHOLD = 0

# ===== Получение общих торговых пар =====
def fetch_common_tokens():
    exchanges = {
        'bybit': ccxt.bybit({
            'apiKey': BYBIT_API_KEY,
            'secret': BYBIT_SECRET,
            'enableRateLimit': True
        }),
        'kraken': ccxt.kraken({
            'apiKey': KRAKEN_API_KEY,
            'secret': KRAKEN_SECRET,
            'enableRateLimit': True
        }),
        'kucoin': ccxt.kucoin({
            'apiKey': KUCOIN_API_KEY,
            'secret': KUCOIN_SECRET,
            'password': KUCOIN_PASSPHRASE,
            'enableRateLimit': True
        }),
        'okx': ccxt.okx({
            'apiKey': OKX_API_KEY,
            'secret': OKX_SECRET,
            'password': OKX_PASSPHRASE,
            'enableRateLimit': True
        }),
        'bingx': ccxt.bingx({
            'apiKey': BINGX_API_KEY,
            'secret': BINGX_SECRET,
            'enableRateLimit': True
        })
    }

    try:
        # Try to add Binance if available
        try:
            exchanges['binance'] = ccxt.binance({"enableRateLimit": True})
        except Exception as e:
            print(f"[WARNING] Binance is not available: {e}")

        # Load markets for all exchanges
        for exchange in exchanges.values():
            try:
                exchange.load_markets()
            except Exception as e:
                print(f"[WARNING] Failed to load markets for {exchange.id}: {e}")

        # Get all symbols from each exchange
        all_symbols = {}
        for exchange_name, exchange in exchanges.items():
            try:
                all_symbols[exchange_name] = set(exchange.symbols)
            except Exception as e:
                print(f"[WARNING] Failed to get symbols for {exchange_name}: {e}")
                all_symbols[exchange_name] = set()

        # Find common symbols across all exchanges
        common = set.intersection(*[s for s in all_symbols.values() if s])  # Only use non-empty sets
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
        print(f"[WARNING] Binance is not available for {symbol}: {e}")
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

def get_kraken_price(symbol):
    try:
        kraken = ccxt.kraken({
            'apiKey': KRAKEN_API_KEY,
            'secret': KRAKEN_SECRET,
            'enableRateLimit': True
        })
        ticker = kraken.fetch_ticker(symbol)
        price = float(ticker['last'])
        print(f"[INFO] Kraken {symbol} Price: {price}")
        return price
    except Exception as e:
        print(f"[ERROR] Error fetching Kraken price for {symbol}: {e}")
        return 0.0

def get_kucoin_price(symbol):
    try:
        kucoin = ccxt.kucoin({
            'apiKey': KUCOIN_API_KEY,
            'secret': KUCOIN_SECRET,
            'password': KUCOIN_PASSPHRASE,
            'enableRateLimit': True
        })
        ticker = kucoin.fetch_ticker(symbol)
        price = float(ticker['last'])
        print(f"[INFO] KuCoin {symbol} Price: {price}")
        return price
    except Exception as e:
        print(f"[ERROR] Error fetching KuCoin price for {symbol}: {e}")
        return 0.0

def get_okx_price(symbol):
    try:
        okx = ccxt.okx({
            'apiKey': OKX_API_KEY,
            'secret': OKX_SECRET,
            'password': OKX_PASSPHRASE,
            'enableRateLimit': True
        })
        ticker = okx.fetch_ticker(symbol)
        price = float(ticker['last'])
        print(f"[INFO] OKX {symbol} Price: {price}")
        return price
    except Exception as e:
        print(f"[ERROR] Error fetching OKX price for {symbol}: {e}")
        return 0.0

def get_bingx_price(symbol):
    try:
        bingx = ccxt.bingx({
            'apiKey': BINGX_API_KEY,
            'secret': BINGX_SECRET,
            'enableRateLimit': True
        })
        ticker = bingx.fetch_ticker(symbol)
        price = float(ticker['last'])
        print(f"[INFO] BingX {symbol} Price: {price}")
        return price
    except Exception as e:
        print(f"[ERROR] Error fetching BingX price for {symbol}: {e}")
        return 0.0

# ===== Telegram Bot Command =====
async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tokens = fetch_common_tokens()
    messages = []
    
    # Add a warning message if Binance is not available
    try:
        binance_client.get_symbol_ticker(symbol="BTCUSDT")
    except Exception as e:
        await update.message.reply_text(
            "⚠️ Note: Binance is not available in your region. "
            "Prices will be compared across other exchanges only."
        )
    
    for symbol in tokens:
        prices = {
            'Binance': get_binance_price(symbol),
            'Bybit': get_bybit_price(symbol),
            'Kraken': get_kraken_price(symbol),
            'KuCoin': get_kucoin_price(symbol),
            'OKX': get_okx_price(symbol),
            'BingX': get_bingx_price(symbol)
        }
        
        # Filter out zero prices
        valid_prices = {k: v for k, v in prices.items() if v > 0}
        
        if len(valid_prices) < 2:  # Need at least 2 valid prices to compare
            continue
            
        # Find min and max prices
        min_price = min(valid_prices.values())
        max_price = max(valid_prices.values())
        diff = max_price - min_price
        
        if diff >= THRESHOLD:
            msg = (
                f"⚡️ Arbitrage Alert!\n"
                f"{symbol}\n"
                f"📊 Prices:\n"
            )
            
            # Add prices for each exchange
            for exchange, price in valid_prices.items():
                msg += f"{exchange}: {price}$\n"
            
            msg += f"📈 Max Difference: {round(diff, 2)}$\n"
            msg += f"🔽 Min Price: {min_price}$\n"
            msg += f"🔼 Max Price: {max_price}$"
            
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
