from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import ccxt
import logging
import os
import time
from bybit import Bybit
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

DEFAULT_FEES = {
    'binance': 0.1,
    'bybit': 0.075,
    'kraken': 0.26,
    'okx': 0.1,
    'bingx': 0.1,
    'kucoin': 0.1,
}

EXCHANGES = {
    'binance': ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    }),
    'kraken': ccxt.kraken({'enableRateLimit': True}),
    'okx': ccxt.okx({
        'apiKey': os.environ.get("OKX_API_KEY"),
        'secret': os.environ.get("OKX_API_SECRET"),
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    }),
    'bingx': ccxt.bingx({'enableRateLimit': True}),
    'kucoin': ccxt.kucoin({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    }),
}

BYBIT_CLIENT = Bybit(
    api_key=os.environ.get("BYBIT_API_KEY"),
    api_secret=os.environ.get("BYBIT_API_SECRET")
)


def validate_env_keys():
    """Validate required environment variables."""
    required_keys = [
        "OKX_API_KEY", "OKX_API_SECRET", "BYBIT_API_KEY", "BYBIT_API_SECRET", "TELEGRAM_BOT_TOKEN"
    ]
    missing_keys = [key for key in required_keys if not os.environ.get(key)]
    if missing_keys:
        logger.warning(f"Missing environment variables: {', '.join(missing_keys)}")
    else:
        logger.info("All required environment variables are set.")


def get_bybit_symbols():
    """Fetch active spot trading pairs from Bybit using their official SDK."""
    try:
        response = BYBIT_CLIENT.Market.market_symbolInfo().result()
        symbols = []
        for item in response[0]['result']:
            if item['status'] == 'Trading' and item['quote_currency'] == 'USDT':
                symbol = f"{item['base_currency']}/{item['quote_currency']}"
                symbols.append(symbol)
        return symbols
    except Exception as e:
        logger.error(f"Bybit symbol fetch error: {e}")
        return []


def fetch_exchange_tokens() -> dict:
    """Fetch trading pairs from all exchanges."""
    tokens = {}

    # Fetch Bybit symbols using official SDK
    bybit_symbols = get_bybit_symbols()
    tokens['bybit'] = bybit_symbols
    logger.info(f"Loaded {len(bybit_symbols)} Bybit symbols")

    # Fetch other exchanges using CCXT
    for exchange_id, exchange in EXCHANGES.items():
        try:
            markets = exchange.load_markets()
            valid_symbols = [
                s for s in markets
                if markets[s]['active'] and '/' in s
            ]
            tokens[exchange_id] = valid_symbols
            logger.info(f"Loaded {len(valid_symbols)} tokens from {exchange_id}")
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")

    return tokens


def get_market_prices(token: str) -> dict:
    """Get prices for a token across all exchanges."""
    prices = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}

        # Add Bybit price fetch using CCXT
        futures[executor.submit(
            ccxt.bybit({'enableRateLimit': True}).fetch_ticker,
            token
        )] = 'bybit'

        # Add other exchanges
        for ex_id, ex in EXCHANGES.items():
            futures[executor.submit(ex.fetch_ticker, token)] = ex_id

        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                ticker = future.result()
                prices[ex_id] = ticker['last']
            except Exception as e:
                logger.warning(f"Price fetch failed on {ex_id}: {e}")

    return prices


def analyze_arbitrage(prices: dict, token: str, fees: dict) -> dict:
    if len(prices) < 2:
        return None

    sorted_exchanges = sorted(prices.items(), key=lambda x: x[1])
    buy_ex, buy_price = sorted_exchanges[0]
    sell_ex, sell_price = sorted_exchanges[-1]

    # Calculate fees
    buy_fee = fees.get(buy_ex, DEFAULT_FEES.get(buy_ex, 0.1))
    sell_fee = fees.get(sell_ex, DEFAULT_FEES.get(sell_ex, 0.1))

    # Calculate profit
    buy_total = buy_price * (1 + buy_fee / 100)
    sell_total = sell_price * (1 - sell_fee / 100)
    profit_pct = ((sell_total - buy_total) / buy_total) * 100

    if profit_pct > 0.5:  # Reduced threshold to 0.5%
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

    # Find common tokens with at least 2 exchanges
    common_tokens = set.intersection(*[
        set(tokens[ex]) for ex in tokens if len(tokens[ex]) > 0
    ])
    logger.info(f"Found {len(common_tokens)} common tokens across exchanges")

    opportunities = []
    max_tokens_to_check = 100  # Limit for testing
    checked_tokens = 0

    with ThreadPoolExecutor() as executor:
        futures = []
        for token in common_tokens:
            if checked_tokens >= max_tokens_to_check:
                break
            futures.append(executor.submit(process_token, token))
            checked_tokens += 1

        for future in as_completed(futures):
            result = future.result()
            if result:
                opportunities.append(result)

    return sorted(opportunities, key=lambda x: x['profit'], reverse=True)[:10]


def process_token(token: str) -> dict:
    try:
        prices = get_market_prices(token)
        return analyze_arbitrage(prices, token, DEFAULT_FEES)
    except Exception as e:
        logger.warning(f"Error processing {token}: {str(e)}")
    return None


def format_opportunities(opportunities: list) -> str:
    if not opportunities:
        return "🔍 No profitable arbitrage opportunities found currently."

    message = ["🚀 *Top Arbitrage Opportunities:* 🚀\n"]
    for idx, opp in enumerate(opportunities, 1):
        message.append(
            f"{idx}. *{opp['token']}*\n"
            f"   ▼ Buy on: {opp['buy_exchange'].title()} (${opp['buy_price']})\n"
            f"   ▲ Sell on: {opp['sell_exchange'].title()} (${opp['sell_price']})\n"
            f"   💰 Profit: {opp['profit']}%\n"
            f"   ――――――――――――――――――――"
        )
    return "\n".join(message)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Scanning exchanges... (This may take 1-2 minutes)")
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Scan failed: {str(e)}")
        await update.message.reply_text("⚠️ Error during scan. Please try again later.")


def main():
    validate_env_keys()
    TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")

    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("scan", scan_command))

    logger.info("Bot is starting...")
    application.run_polling()


if __name__ == '__main__':
    main()