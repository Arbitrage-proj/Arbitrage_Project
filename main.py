from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import logging
import time
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
    'kraken': ccxt.kraken({'enableRateLimit': True}),
    #'okx': ccxt.okx({'enableRateLimit': True}),
    'bingx': ccxt.bingx({
        'enableRateLimit': True,
        'api_key': "o1Vh3Mxd00FslQnRRqENgxEf9rAShOsUDynNQDlCce2jWpGsStLocO2QxWXe4ICRKyOgRZxp12mKsWSCUZ5lQ",
        'api_secret': "jxCELMjmLWGpDtax22XFdxaPGXrDyJ6LfKd7lZCClfIU0fJlix8Y7ngoSvmvuhAfzCY5VUzmZGIFxrYKJVg",
    }),
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
    try:
        return DEFAULT_FEES.get(exchange_id, 0.1)
    except Exception as e:
        logger.error(f"Fee error for {exchange_id}: {e}")
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
        pairs = [
            f"{item['symbol'].replace('USDT', '')}/USDT"
            for item in response["result"]["list"]
            if item["symbol"].endswith("USDT")
        ]
        return pairs
    except Exception as e:
        logger.error(f"Failed to fetch Bybit tokens: {e}")
        return []


def fetch_exchange_tokens() -> dict:
    tokens = {
        'binance': fetch_binance_tokens(),
        'bybit': fetch_bybit_tokens(),
    }

    for exchange_id, exchange in EXCHANGES.items():
        try:
            markets = exchange.load_markets()
            tokens[exchange_id] = list(markets.keys())
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")
    return tokens


def get_market_prices(token: str) -> dict:
    prices = {}

    base, quote = token.split('/')
    symbol_noslash = f"{base}{quote}"

    with ThreadPoolExecutor() as executor:
        futures = {}

        # Bybit
        if quote == "USDT":
            futures[executor.submit(
                lambda: float(
                    bybit.get_tickers(category="spot", symbol=symbol_noslash)["result"]["list"][0]["lastPrice"])
            )] = 'bybit'

        # Binance
        futures[executor.submit(
            lambda: float(binance.get_symbol_ticker(symbol=symbol_noslash)["price"])
        )] = 'binance'

        # CCXT exchanges
        futures.update({
            executor.submit(ex.fetch_ticker, token): ex_id
            for ex_id, ex in EXCHANGES.items()
        })

        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                result = future.result()
                prices[ex_id] = result if isinstance(result, float) else result['last']
            except Exception as e:
                logger.warning(f"Price fetch failed for {token} on {ex_id}: {e}")

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

    if profit_pct > 1:
        return {
            'token': token,
            'buy_exchange': buy_ex,
            'sell_exchange': sell_ex,
            'buy_price': round(buy_price, 4),
            'sell_price': round(sell_price, 4),
            'profit': round(profit_pct, 2)
        }
    return None


def find_opportunities() -> list:
    logger.info("Starting arbitrage scan...")
    tokens = fetch_exchange_tokens()
    exchanges = list(tokens.keys())
    common_tokens = set.intersection(*[set(tokens[ex]) for ex in exchanges])
    fees = {ex_id: get_trading_fee(ex_id, EXCHANGES.get(ex_id)) for ex_id in exchanges}

    opportunities = []
    for token in common_tokens:
        if len(opportunities) >= 10:
            break
        prices = get_market_prices(token)
        result = analyze_arbitrage(prices, token, fees)
        if result:
            opportunities.append(result)
        time.sleep(0.5)

    return sorted(opportunities, key=lambda x: x['profit'], reverse=True)


EXCHANGE_URLS = {
    'binance': "https://www.binance.com/en/trade/{}USDT",
    'kraken': "https://trade.kraken.com/markets/{}-USDT",
    'bybit': "https://www.bybit.com/trade/spot/{}USDT",
    'okx': "https://www.okx.com/trade-spot/{}-USDT",
    'bingx': "https://www.bingx.com/en-us/trade/{}/USDT",
    'kucoin': "https://www.kucoin.com/trade/{}-USDT"
}


def format_opportunities(opportunities: list) -> str:
    """Format results into a readable message with trading links"""
    if not opportunities:
        return "🔍 No profitable arbitrage opportunities found currently."

    message = ["🚀 *Top Arbitrage Opportunities:* 🚀\n"]
    for idx, opp in enumerate(opportunities, 1):
        buy_link = EXCHANGE_URLS.get(opp['buy_exchange'], "").format(opp['token'])
        sell_link = EXCHANGE_URLS.get(opp['sell_exchange'], "").format(opp['token'])

        message.append(
            f"{idx}. *{opp['token']}*\n"
            f"   ▼ Buy on: [{opp['buy_exchange'].title()}]({buy_link}) (${opp['buy_price']})\n"
            f"   ▲ Sell on: [{opp['sell_exchange'].title()}]({sell_link}) (${opp['sell_price']})\n"
            f"   💰 Profit: {opp['profit']}%\n"
            f"   ――――――――――――――――――――"
        )

    return "\n".join(message)


def get_default_keyboard() -> ReplyKeyboardMarkup:
    """Return a default reply keyboard markup with common commands."""
    keyboard = [['/scan', '/help']]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


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
        "- OKX\n"
        "- BingX\n"
        "- KuCoin\n"
    )
    await update.message.reply_text(welcome_msg, parse_mode='Markdown', reply_markup=reply_markup)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Scanning exchanges... (This may take 20–30 seconds)")
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        await update.message.reply_text("⚠️ Error during scan. Please try again later.")


def main():
    TOKEN = '8033159498:AAHX9srehZoT2M8e2AabLuO_w2FVKOCv_b0'
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("start", start_command))
    logger.info("Bot is running...")
    application.run_polling()


if __name__ == '__main__':
    main()
