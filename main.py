from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import ccxt
import logging
import os
import time
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
MIN_PROFIT_THRESHOLD = 1.0
MAX_TOKENS_TO_SCAN = 20
REQUEST_TIMEOUT = 10

EXCHANGES = {
    'binance': ccxt.binance({'enableRateLimit': True, 'timeout': REQUEST_TIMEOUT * 1000}),
    'kraken': ccxt.kraken({'enableRateLimit': True, 'timeout': REQUEST_TIMEOUT * 1000}),
    'bybit': ccxt.bybit({
        'apiKey': os.environ.get("BYBIT_API_KEY"),
        'secret': os.environ.get("BYBIT_API_SECRET"),
        'enableRateLimit': True,
        'timeout': REQUEST_TIMEOUT * 1000
    }),
    'okx': ccxt.okx({
        'apiKey': os.environ.get("OKX_API_KEY"),
        'secret': os.environ.get("OKX_API_SECRET"),
        'enableRateLimit': True,
        'timeout': REQUEST_TIMEOUT * 1000
    }),
    'bingx': ccxt.bingx({'enableRateLimit': True, 'timeout': REQUEST_TIMEOUT * 1000}),
    'kucoin': ccxt.kucoin({'enableRateLimit': True, 'timeout': REQUEST_TIMEOUT * 1000}),
}

DEFAULT_FEES = {
    'binance': 0.1,
    'kraken': 0.26,
    'bybit': 0.075,
    'okx': 0.1,
    'bingx': 0.1,
    'kucoin': 0.1,
}

def check_exchange_status(exchange_id: str, exchange: ccxt.Exchange) -> bool:
    try:
        exchange.load_markets()
        return True
    except Exception as e:
        logger.error(f"Exchange {exchange_id} is not available: {e}")
        return False

def get_trading_fee(exchange_id: str, exchange: ccxt.Exchange) -> float:
    try:
        if 'fetchTradingFees' in exchange.has and exchange.has['fetchTradingFees']:
            fees = exchange.fetch_trading_fees()
            return fees.get('taker', DEFAULT_FEES.get(exchange_id, 0.1))
        return DEFAULT_FEES.get(exchange_id, 0.1)
    except Exception as e:
        logger.error(f"Fee error for {exchange_id}: {e}")
        return DEFAULT_FEES.get(exchange_id, 0.1)

def fetch_exchange_tokens(operational_exchanges: dict) -> dict:
    tokens = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(ex.load_markets): ex_id for ex_id, ex in operational_exchanges.items()}
        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                markets = future.result()
                tokens[ex_id] = list(markets.keys())
                logger.info(f"Successfully loaded markets for {ex_id}")
            except Exception as e:
                logger.error(f"Market load failed for {ex_id}: {e}")
    return tokens

def get_market_prices(token: str, exchanges: dict) -> dict:
    prices = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(ex.fetch_ticker, token): ex_id for ex_id, ex in exchanges.items()}
        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                ticker = future.result()
                if ticker and 'last' in ticker and ticker['last']:
                    prices[ex_id] = float(ticker['last'])
                    logger.debug(f"Price for {token} on {ex_id}: {prices[ex_id]}")
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

    if profit_pct >= MIN_PROFIT_THRESHOLD:
        return {
            'token': token,
            'buy_exchange': buy_ex,
            'sell_exchange': sell_ex,
            'buy_price': round(buy_price, 4),
            'sell_price': round(sell_price, 4),
            'profit': round(profit_pct, 2),
            'volume': round((sell_price - buy_price) * 1000, 2)
        }
    return None

def find_opportunities() -> list:
    logger.info("Starting arbitrage scan...")

    operational_exchanges = {ex_id: ex for ex_id, ex in EXCHANGES.items()
                             if check_exchange_status(ex_id, ex)}

    if not operational_exchanges:
        logger.error("No operational exchanges found")
        return []

    tokens = fetch_exchange_tokens(operational_exchanges)
    valid_tokens = [set(tokens[ex]) for ex in operational_exchanges if ex in tokens]
    if not valid_tokens:
        return []

    common_tokens = set.intersection(*valid_tokens)
    fees = {ex_id: get_trading_fee(ex_id, ex) for ex_id, ex in operational_exchanges.items()}

    opportunities = []
    for token in list(common_tokens)[:MAX_TOKENS_TO_SCAN]:
        prices = get_market_prices(token, operational_exchanges)
        result = analyze_arbitrage(prices, token, fees)
        if result:
            opportunities.append(result)
        time.sleep(0.5)

    return sorted(opportunities, key=lambda x: x['profit'], reverse=True)

def format_opportunities(opportunities: list) -> str:
    if not opportunities:
        return "🔍 No profitable arbitrage opportunities found currently."

    message = ["🚀 *Top Arbitrage Opportunities:* 🚀\n"]
    for idx, opp in enumerate(opportunities, 1):
        message.append(
            f"{idx}. *{opp['token']}*\n"
            f"   ▼ Buy on: {opp['buy_exchange']} (${opp['buy_price']})\n"
            f"   ▲ Sell on: {opp['sell_exchange']} (${opp['sell_price']})\n"
            f"   💰 Profit: {opp['profit']}%\n"
            f"   📊 Volume: ${opp['volume']} (per $1000)\n"
            f"   ――――――――――――――――――――"
        )
    return "\n".join(message)

async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Scanning exchanges... (This may take 20–30 seconds)")
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        await update.message.reply_text("⚠️ Error during scan. Please try again later.")

def main():
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not found in environment variables")
        return

    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("scan", scan_command))
    logger.info("Bot is running...")
    application.run_polling()

if __name__ == '__main__':
    main()
