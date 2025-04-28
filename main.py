from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import logging
import time
import requests
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP as BybitClient
import ccxt

# === Logging ===
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === API Clients ===
binance = BinanceClient("your_binance_api_key", "your_binance_api_secret")
bybit = BybitClient(api_key="MddybceGH9RuPrmk2s", api_secret="i3yrlXj5TX4TKXZpERUMMs8R4yLjoaTV78MV")

EXCHANGES = {
    'okx': ccxt.okx({
        'apiKey': os.environ.get("OKX_API_KEY"),
        'secret': os.environ.get("OKX_API_SECRET"),
        'enableRateLimit': True
    }),
    'bingx': ccxt.bingx({'enableRateLimit': True}),
    'kucoin': ccxt.kucoin({'enableRateLimit': True}),
}

DEFAULT_FEES = {
    'binance': 0.1,
    'kraken': 0.26,
    'bybit': 0.075,
    'okx': 0.1,
    'bingx': 0.1,
    'kucoin': 0.1,
}


def get_trading_fee(exchange_id: str, exchange: ccxt.Exchange = None) -> float:
    return DEFAULT_FEES.get(exchange_id, 0.1)


def fetch_binance_tokens():
    try:
        info = binance.get_exchange_info()
        symbols = info["symbols"]
        return [
            f"{s['baseAsset']}/{s['quoteAsset']}"
            for s in symbols
            if s['status'] == "TRADING" and s['quoteAsset'] == "USDT"
        ]
    except Exception as e:
        logger.error(f"Failed to fetch Binance tokens: {e}")
        return []


def fetch_bybit_tokens():
    try:
        response = bybit.get_tickers(category="spot")
        pairs = []
        for item in response["result"]["list"]:
            symbol = item["symbol"]
            if symbol.endswith("USDT"):
                base = symbol[:-4]
                quote = symbol[-4:]
                pairs.append(f"{base}/{quote}")
        return pairs
    except Exception as e:
        logger.error(f"Failed to fetch Bybit tokens: {e}")
        return []


def fetch_kraken_tokens():
    try:
        url = "https://api.kraken.com/0/public/AssetPairs"
        response = requests.get(url)
        data = response.json()
        pairs = data.get('result', {})
        tokens = []
        for v in pairs.values():
            wsname = v.get('wsname')
            if wsname and wsname.endswith('/USDT'):
                base, quote = wsname.split('/')
                if base == 'XBT':
                    base = 'BTC'
                tokens.append(f"{base}/{quote}")
        return tokens
    except Exception as e:
        logger.error(f"Failed to fetch Kraken tokens via API: {e}")
        return []


def fetch_okx_tokens():
    try:
        okx = EXCHANGES['okx']
        markets = okx.load_markets()
        return [symbol for symbol in markets if symbol.endswith('/USDT')]
    except Exception as e:
        logger.error(f"Failed to fetch OKX tokens: {e}")
        return []


def fetch_exchange_tokens() -> dict:
    tokens = {
        'binance': fetch_binance_tokens(),
        'bybit': fetch_bybit_tokens(),
        'kraken': fetch_kraken_tokens(),
        'okx': fetch_okx_tokens(),
    }

    for exchange_id, exchange in EXCHANGES.items():
        if exchange_id in tokens:
            continue  # Already fetched above
        try:
            markets = exchange.load_markets()
            tokens[exchange_id] = [symbol for symbol in markets if symbol.endswith('/USDT')]
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")
    return tokens


def get_market_prices(token: str) -> dict:
    prices = {}

    base, quote = token.split('/')
    symbol_noslash = f"{base}{quote}"

    with ThreadPoolExecutor() as executor:
        futures = {}

        if quote == "USDT":
            futures[executor.submit(
                lambda: float(
                    bybit.get_tickers(category="spot", symbol=symbol_noslash)["result"]["list"][0]["lastPrice"]
                )
            )] = 'bybit'

        futures[executor.submit(
            lambda: float(binance.get_symbol_ticker(symbol=symbol_noslash)["price"])
        )] = 'binance'

        for ex_id, ex in EXCHANGES.items():
            futures[executor.submit(ex.fetch_ticker, token)] = ex_id

        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                result = future.result()
                prices[ex_id] = result if isinstance(result, float) else result['last']
            except Exception as e:
                logger.warning(f"Price fetch failed for {token} on {ex_id}: {e}")

    # Kraken via direct API
    try:
        response = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair": symbol_noslash})
        data = response.json()
        result = list(data["result"].values())[0]
        prices['kraken'] = float(result["c"][0])
    except Exception as e:
        logger.warning(f"Price fetch failed for {token} on Kraken: {e}")

    return prices


def analyze_arbitrage(prices: dict, token: str, fees: dict) -> dict:
    if len(prices) < 2:
        return None

    sorted_exchanges = sorted(prices.items(), key=lambda x: x[1])
    buy_ex, buy_price = sorted_exchanges[0]
    sell_ex, sell_price = sorted_exchanges[-1]

    buy_fee = fees.get(buy_ex, 0.1)
    sell_fee = fees.get(sell_ex, 0.1)

    buy_total = buy_price * (1 + buy_fee / 100)
    sell_total = sell_price * (1 - sell_fee / 100)
    profit_pct = ((sell_total - buy_total) / buy_total) * 100

    if profit_pct > 0.1:
        return {
            'token': token,
            'buy_exchange': buy_ex,
            'sell_exchange': sell_ex,
            'buy_price': buy_price,
            'sell_price': sell_price,
            'profit': round(profit_pct, 2)
        }
    return None


def find_opportunities() -> list:
    logger.info("Starting arbitrage scan...")
    tokens = fetch_exchange_tokens()
    logger.info(f"Fetched tokens for exchanges: { {ex: len(tokens[ex]) for ex in tokens} }")
    exchanges = list(tokens.keys())
    logger.info(f"Exchanges considered for intersection: {exchanges}")
    try:
        common_tokens = set.intersection(*[set(tokens[ex]) for ex in exchanges])
    except Exception as e:
        logger.error(f"Error finding common tokens: {e}")
        return []
    logger.info(f"Number of common tokens: {len(common_tokens)}")
    fees = {ex_id: get_trading_fee(ex_id, EXCHANGES.get(ex_id)) for ex_id in exchanges}

    opportunities = []
    for token in common_tokens:
        if len(opportunities) >= 10:
            break
        logger.info(f"Checking arbitrage for token: {token}")
        try:
            prices = get_market_prices(token)
            logger.info(f"Prices for {token}: {prices}")
            result = analyze_arbitrage(prices, token, fees)
            if result:
                logger.info(f"Arbitrage opportunity found: {result}")
                opportunities.append(result)
        except Exception as e:
            logger.error(f"Error processing token {token}: {e}")
        time.sleep(0.5)

    logger.info(f"Total opportunities found: {len(opportunities)}")
    if opportunities:
        max_opp = max(opportunities, key=lambda x: x['profit'])
        logger.info(f"Max profit: {max_opp['profit']}% for {max_opp['token']} (buy on {max_opp['buy_exchange']}, sell on {max_opp['sell_exchange']})")
    return sorted(opportunities, key=lambda x: x['profit'], reverse=True)


def get_exchange_url(exchange, token):
    base, quote = token.split('/')
    if exchange == 'binance':
        return f"https://www.binance.com/en/trade/{base}_{quote}?type=spot"
    elif exchange == 'kraken':
        return f"https://trade.kraken.com/markets/{base}-{quote}"
    elif exchange == 'bybit':
        return f"https://www.bybit.com/trade/spot/{base}{quote}"
    elif exchange == 'okx':
        return f"https://www.okx.com/trade-spot/{base}-{quote}"
    elif exchange == 'bingx':
        return f"https://bingx.com/en/spot/{base}{quote}/"
    elif exchange == 'kucoin':
        return f"https://www.kucoin.com/trade/{base}-{quote}"
    else:
        return ""


def format_opportunities(opportunities: list) -> str:
    if not opportunities:
        return "🔍 No profitable arbitrage opportunities found currently."

    message = ["🚀 *Top Arbitrage Opportunities:* 🚀\n"]
    for idx, opp in enumerate(opportunities, 1):
        buy_link = get_exchange_url(opp['buy_exchange'], opp['token'])
        sell_link = get_exchange_url(opp['sell_exchange'], opp['token'])

        message.append(
            f"{idx}. *{opp['token']}*\n"
            f"   ▼ Buy on: [{opp['buy_exchange'].title()}]({buy_link}) (${opp['buy_price']})\n"
            f"   ▲ Sell on: [{opp['sell_exchange'].title()}]({sell_link}) (${opp['sell_price']})\n"
            f"   💰 Profit: {opp['profit']}%\n"
            f"   ――――――――――――――――――――"
        )

    return "\n".join(message)


def get_default_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([['/scan', '/help']], resize_keyboard=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = get_default_keyboard()
    welcome_msg = (
        "🤖 *Crypto Arbitrage Bot*\n\n"
        "Available commands:\n"
        "/start - Show this message\n"
        "/scan - Find arbitrage opportunities\n"
        "/help - Get help information\n\n"
        "Currently monitoring:\n"
        "- Binance\n"
        "- Kraken\n"
        "- ByBit\n"
        "- BingX\n"
        "- KuCoin\n"
    )
    await update.message.reply_text(welcome_msg, parse_mode='Markdown', reply_markup=reply_markup)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Scanning for arbitrage opportunities...")
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Failed to scan for opportunities: {e}")
        await update.message.reply_text("⚠️ Error scanning for opportunities. Please try again later.")


def fetch_okx_tickers():
    """
    Fetch all OKX spot tickers and their last prices.
    Returns a list of dicts: [{'instId': 'BTC-USDT', 'last': 'price'}, ...]
    """
    url = 'https://www.okx.com/api/v5/market/tickers?instType=SPOT'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get('data', [])
        # Each item in data is a dict with keys like 'instId', 'last', etc.
        tickers = [{'instId': item['instId'], 'last': item['last']} for item in data]
        return tickers
    except Exception as e:
        logger.error(f"Failed to fetch OKX tickers: {e}")
        return []


def main():
    TOKEN = '8033159498:AAHX9srehZoT2M8e2AabLuO_w2FVKOCv_b0'
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("start", start_command))
    logger.info("Bot is running...")
    application.run_polling()


if __name__ == '__main__':
    main()
