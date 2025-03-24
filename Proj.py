from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import ccxt
import logging
import time
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

EXCHANGES = {
    'binance': ccxt.binance({'enableRateLimit': True}),
    'kraken': ccxt.kraken({'enableRateLimit': True}),
    'bybit': ccxt.bybit({'enableRateLimit': True}),
    'okx': ccxt.okx({'enableRateLimit': True}),
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


def get_trading_fee(exchange_id: str, exchange: ccxt.Exchange) -> float:
    try:
        if hasattr(exchange, 'fetch_trading_fees'):
            fees = exchange.fetch_trading_fees()
            return fees.get('taker', DEFAULT_FEES.get(exchange_id, 0.1))
        return DEFAULT_FEES.get(exchange_id, 0.1)
    except Exception as e:
        logger.error(f"Fee error for {exchange_id}: {e}")
        return DEFAULT_FEES.get(exchange_id, 0.1)


def fetch_exchange_tokens() -> dict:
    tokens = {}
    for exchange_id, exchange in EXCHANGES.items():
        try:
            markets = exchange.load_markets()
            tokens[exchange_id] = list(markets.keys())
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")
    return tokens


def get_market_prices(token: str) -> dict:
    prices = {}
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(ex.fetch_ticker, token): ex_id for ex_id, ex in EXCHANGES.items()}
        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                ticker = future.result()
                prices[ex_id] = ticker['last']
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

    if profit_pct > 0.1:
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
    common_tokens = set.intersection(*[set(tokens[ex]) for ex in EXCHANGES if ex in tokens])
    fees = {ex_id: get_trading_fee(ex_id, ex) for ex_id, ex in EXCHANGES.items()}

    opportunities = []
    for token in common_tokens:
        if len(opportunities) >= 10:
            break
        prices = get_market_prices(token)
        result = analyze_arbitrage(prices, token, fees)
        if result:
            opportunities.append(result)
        time.sleep(1)

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
            f"   ――――――――――――――――――――"
        )

    return "\n".join(message)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Scanning exchanges... (This may take 20-30 seconds)")
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        await update.message.reply_text("⚠️ Error during scan. Please try again later.")


def main():
    TOKEN = "7985058577:AAElBD7nNAKHdTWMOBYEGP0TM-P3FNxfD1w"
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("scan", scan_command))
    logger.info("Bot is running...")
    application.run_polling()


if __name__ == '__main__':
    main()