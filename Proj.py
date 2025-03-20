from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
import ccxt
import pandas as pd
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
    'bitfinex': ccxt.bitfinex({'enableRateLimit': True}),
    'huobi': ccxt.huobi({'enableRateLimit': True}),
    'gateio': ccxt.gateio({'enableRateLimit': True}),
    'coinbasepro': ccxt.coinbasepro({'enableRateLimit': True}),
    'bitstamp': ccxt.bitstamp({'enableRateLimit': True}),
    'mexc': ccxt.mexc({'enableRateLimit': True}),
    'ascendex': ccxt.ascendex({'enableRateLimit': True}),
    'cryptocom': ccxt.cryptocom({'enableRateLimit': True}),
}


DEFAULT_FEES = {
    'binance': 0.1,
    'kraken': 0.26,
    'bybit': 0.075,
    'okx': 0.1,
    'bingx': 0.1,
    'kucoin': 0.1,
    'bitfinex': 0.2,
}


def get_trading_fee(exchange_id: str, exchange: ccxt.Exchange) -> float:
    """Retrieve trading fee for a specific exchange"""
    try:
        if hasattr(exchange, 'fetch_trading_fees'):
            fees = exchange.fetch_trading_fees()
            return fees.get('taker', DEFAULT_FEES.get(exchange_id, 0.1))
        return DEFAULT_FEES.get(exchange_id, 0.1)
    except Exception as e:
        logger.error(f"Fee error for {exchange_id}: {e}")
        return DEFAULT_FEES.get(exchange_id, 0.1)


def fetch_exchange_tokens() -> dict:
    """Get all available trading pairs from each exchange"""
    tokens = {}
    for exchange_id, exchange in EXCHANGES.items():
        try:
            markets = exchange.load_markets()
            tokens[exchange_id] = list(markets.keys())
            logger.info(f"Loaded {len(markets)} pairs from {exchange_id}")
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")
    return tokens


def get_market_prices(token: str) -> dict:
    """Fetch current prices for a token across all exchanges"""
    prices = {}
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(ex.fetch_ticker, token): ex_id
            for ex_id, ex in EXCHANGES.items()
        }
        for future in as_completed(futures):
            ex_id = futures[future]
            try:
                ticker = future.result()
                prices[ex_id] = ticker['last']
            except Exception as e:
                logger.warning(f"Price fetch failed for {token} on {ex_id}: {e}")
    return prices


def analyze_arbitrage(prices: dict, token: str, fees: dict) -> dict:
    """Calculate arbitrage opportunities for a given token"""
    if len(prices) < 2:
        return None

    df = pd.DataFrame(prices.items(), columns=['Exchange', 'Price'])
    df['Fee'] = df['Exchange'].map(fees)
    df = df.sort_values('Price')

    buy_ex, buy_price, buy_fee = df.iloc[0][['Exchange', 'Price', 'Fee']]
    sell_ex, sell_price, sell_fee = df.iloc[-1][['Exchange', 'Price', 'Fee']]

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
    """Main function to find arbitrage opportunities"""
    logger.info("Starting arbitrage scan...")
    tokens = fetch_exchange_tokens()
    common_tokens = set.intersection(*[set(tokens[ex]) for ex in EXCHANGES if ex in tokens])

    fees = {ex_id: get_trading_fee(ex_id, ex) for ex_id, ex in EXCHANGES.items()}

    opportunities = []
    for token in common_tokens:
        if len(opportunities) >= 10:
            break

        logger.info(f"Analyzing {token}")
        prices = get_market_prices(token)
        result = analyze_arbitrage(prices, token, fees)
        if result:
            opportunities.append(result)
        time.sleep(1)  # Respect API rate limits

    return sorted(opportunities, key=lambda x: x['profit'], reverse=True)


EXCHANGE_URLS = {
    'binance': "https://www.binance.com/en/trade/{}USDT",
    'kraken': "https://trade.kraken.com/markets/{}-USDT",
    'bybit': "https://www.bybit.com/trade/spot/{}USDT",
    'okx': "https://www.okx.com/trade-spot/{}-USDT",
    'bingx': "https://www.bingx.com/en-us/trade/{}/USDT",
    'kucoin': "https://www.kucoin.com/trade/{}-USDT",
    'bitfinex': "https://trading.bitfinex.com/t/{}:USDT",
    'huobi': "https://www.huobi.com/en-us/exchange/{}usdt",
    'gateio': "https://www.gate.io/trade/{}USDT",
    'coinbasepro': "https://www.coinbase.com/price/{}",
    'bitstamp': "https://www.bitstamp.net/markets/{}/usdt/",
    'mexc': "https://www.mexc.com/exchange/{}_USDT",
    'ascendex': "https://ascendex.com/en/cashtrade-spottrading/usdt/{}",
    'cryptocom': "https://crypto.com/exchange/trade/{}USDT"
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
    """Handle /start command"""
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
        "- BitFinex"
    )
    await update.message.reply_text(welcome_msg, parse_mode='Markdown', reply_markup=reply_markup)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /scan command"""
    reply_markup = get_default_keyboard()
    await update.message.reply_text("🔄 Scanning exchanges... (This may take 20-30 seconds)", reply_markup=reply_markup)
    try:
        loop = asyncio.get_running_loop()
        opportunities = await loop.run_in_executor(None, find_opportunities)
        response = format_opportunities(opportunities)
        await update.message.reply_text(response, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        await update.message.reply_text("⚠️ Error during scan. Please try again later.", reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    reply_markup = get_default_keyboard()
    help_text = (
        "🆘 *Help Guide*\n\n"
        "This bot detects price differences across major crypto exchanges.\n\n"
        "*How it works:*\n"
        "1. Scans 7 exchanges simultaneously\n"
        "2. Compares prices for common trading pairs\n"
        "3. Calculates profits after fees\n"
        "4. Shows top 5 most profitable opportunities\n\n"
        "*Commands:*\n"
        "/scan - Start a new arbitrage scan\n"
        "/alert - (Coming soon) Price alerts\n"
        "/exchanges - (Coming soon) Exchange list"
    )
    await update.message.reply_text(help_text, parse_mode='Markdown', reply_markup=reply_markup)


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(msg="Exception occurred:", exc_info=context.error)
    if update.message:
        reply_markup = get_default_keyboard()
        await update.message.reply_text("❌ An error occurred. Please try again later.", reply_markup=reply_markup)


def main():
    """Start the bot"""
    TOKEN = "7985058577:AAElBD7nNAKHdTWMOBYEGP0TM-P3FNxfD1w"
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_error_handler(error_handler)

    logger.info("Bot is running...")
    application.run_polling()


if __name__ == '__main__':
    main()
