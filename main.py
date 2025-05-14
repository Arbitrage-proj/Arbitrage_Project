import os
import sys
import asyncio
import ccxt.async_support as ccxt
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.error import Conflict
from dotenv import load_dotenv
import logging
import time
import requests
from binance.client import Client as BinanceClient
from pybit.unified_trading import HTTP as BybitClient

# Fix for aiodns on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Load environment variables
load_dotenv()
TELEGRAM_TOKEN = "8033159498:AAHX9srehZoT2M8e2AabLuO_w2FVKOCv_b0"

# Logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Store user data (in production, use encrypted database)
user_data = {}

# Supported exchanges
EXCHANGES = ["binance", "bybit", "kucoin", "kraken", "bingx", "okx"]

# Default trading fees
DEFAULT_FEES = {
    'binance': 0.1,
    'kraken': 0.26,
    'bybit': 0.075,
    'okx': 0.1,
    'bingx': 0.1,
    'kucoin': 0.1,
}

# API Clients (placeholders; updated with user keys)
binance = None
bybit = None
ccxt_exchanges = {
    'bingx': ccxt.bingx({'enableRateLimit': True}),
    'kucoin': ccxt.kucoin({'enableRateLimit': True}),
    'kraken': ccxt.kraken({'enableRateLimit': True}),
    'okx': ccxt.okx({'enableRateLimit': True}),
}


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors, including Conflict errors."""
    if isinstance(context.error, Conflict):
        logger.error("Conflict error: Multiple bot instances detected.")
        await update.message.reply_text(
            "Error: Another instance of this bot is running. Please stop all other instances and try again."
        )
        await context.application.stop()
    else:
        logger.error(f"Update {update} caused error {context.error}")
        if update and update.message:
            await update.message.reply_text(f"Error: {str(context.error)}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_markup = get_default_keyboard()
    welcome_msg = (
        "🤖 *Crypto Arbitrage Bot*\n\n"
        "Available commands:\n"
        "/start - Show this message\n"
        "/setkeys <exchange> - Set API keys\n"
        "/scan - Find arbitrage opportunities\n"
        "/getip - Check your IP for whitelisting\n"
        "/help - Get help information\n\n"
        "Currently monitoring:\n"
        "- Binance\n- Bybit\n- KuCoin\n- Kraken\n- BingX\n- OKX\n\n"
        "Set API keys with /setkeys to start trading."
    )
    await update.message.reply_text(welcome_msg, parse_mode='Markdown', reply_markup=reply_markup)


async def set_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or args[0].lower() not in EXCHANGES:
        await update.message.reply_text(
            f"Please specify a valid exchange: {', '.join(EXCHANGES)}.\nExample: /setkeys binance"
        )
        return
    exchange = args[0].lower()
    prompt = (
        f"Please send your {exchange} API key, secret, and passphrase (for OKX) in the format: "
        "`key:secret:passphrase` for OKX, or `key:secret` for others."
    )
    context.user_data["awaiting_keys"] = exchange
    context.user_data.pop("awaiting_amount", None)
    logger.info(f"User {update.message.from_user.id} requested to set keys for {exchange}")
    await update.message.reply_text(prompt)


async def handle_keys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_keys"):
        logger.debug(
            f"No awaiting_keys state for user {update.message.from_user.id}, ignoring message: {update.message.text}")
        return
    user_id = update.message.from_user.id
    exchange = context.user_data["awaiting_keys"]
    input_text = update.message.text
    logger.info(f"User {user_id} sent key input for {exchange}: {input_text}")

    try:
        parts = input_text.split(":")
        if exchange == "okx" and len(parts) != 3:
            raise ValueError("OKX requires key:secret:passphrase format.")
        if exchange != "okx" and len(parts) != 2:
            raise ValueError("Use key:secret format for this exchange.")

        api_key, api_secret = parts[0], parts[1]
        passphrase = parts[2] if exchange == "okx" else None

        if user_id not in user_data:
            user_data[user_id] = {}
        user_data[user_id][exchange] = {
            "api_key": api_key,
            "api_secret": api_secret,
            "passphrase": passphrase
        }
        logger.info(f"Keys saved for user {user_id}, exchange {exchange}: {user_data[user_id][exchange]}")
        await update.message.reply_text(f"{exchange.capitalize()} API keys saved!")
    except Exception as e:
        logger.error(f"Error processing keys for user {user_id}, exchange {exchange}: {str(e)}")
        await update.message.reply_text(f"Error: {str(e)}")
    finally:
        context.user_data.pop("awaiting_keys", None)


async def get_ip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        response = requests.get("https://api.ipify.org", timeout=5)
        ip = response.text
        await update.message.reply_text(
            f"Your public IP address is: {ip}\n"
            "Add this IP to your exchange's API whitelist (e.g., BingX: https://bingx.com/en/account/api/)."
        )
    except Exception as e:
        await update.message.reply_text(f"Error fetching IP: {str(e)}")


def get_default_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([['/scan', '/setkeys', '/getip', '/help']], resize_keyboard=True)


def fetch_binance_tokens():
    global binance
    if not binance:
        logger.warning("Binance client not initialized, skipping token fetch.")
        return []
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
    global bybit
    if not bybit:
        logger.warning("Bybit client not initialized, skipping token fetch.")
        return []
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
        response = requests.get(url, timeout=10)
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
        url = 'https://www.okx.com/api/v5/market/tickers?instType=SPOT'
        response = requests.get(url, timeout=10)
        data = response.json()
        return [item['instId'].replace('-', '/') for item in data['data'] if item['instId'].endswith('USDT')]
    except Exception as e:
        logger.error(f"Failed to fetch OKX tokens: {e}")
        return []


async def fetch_exchange_tokens(user_id: int) -> dict:
    tokens = {
        'binance': fetch_binance_tokens(),
        'bybit': fetch_bybit_tokens(),
        'kraken': fetch_kraken_tokens(),
        'okx': fetch_okx_tokens(),
    }
    for exchange_id, exchange in ccxt_exchanges.items():
        if exchange_id in tokens:
            continue
        try:
            markets = await exchange.load_markets()
            tokens[exchange_id] = [symbol for symbol in markets if symbol.endswith('/USDT')]
        except Exception as e:
            logger.error(f"Market load failed for {exchange_id}: {e}")
            tokens[exchange_id] = []

    available_exchanges = [ex for ex, t in tokens.items() if t]
    logger.info(f"Exchanges with tokens for user {user_id}: {available_exchanges}")
    return tokens


async def get_market_prices(token: str) -> dict:
    prices = {}
    base, quote = token.split('/')
    symbol_noslash = f"{base}{quote}"
    symbol_dash = f"{base}-{quote}"

    async def fetch_bybit_price():
        if quote == "USDT" and bybit:
            try:
                result = await asyncio.to_thread(
                    bybit.get_tickers, category="spot", symbol=symbol_noslash
                )
                return 'bybit', float(result["result"]["list"][0]["lastPrice"])
            except Exception as e:
                logger.warning(f"Price fetch failed for {token} on bybit: {e}")
                return 'bybit', None
        return 'bybit', None

    async def fetch_binance_price():
        if binance:
            try:
                result = await asyncio.to_thread(
                    binance.get_symbol_ticker, symbol=symbol_noslash
                )
                return 'binance', float(result["price"])
            except Exception as e:
                logger.warning(f"Price fetch failed for {token} on binance: {e}")
                return 'binance', None
        return 'binance', None

    async def fetch_ccxt_price(ex_id, ex):
        try:
            result = await ex.fetch_ticker(token)
            return ex_id, float(result['last'])
        except Exception as e:
            logger.warning(f"Price fetch failed for {token} on {ex_id}: {e}")
            return ex_id, None

    async def fetch_kraken_price():
        try:
            response = requests.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": symbol_noslash},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            result = list(data["result"].values())[0]
            return 'kraken', float(result["c"][0])
        except Exception as e:
            logger.warning(f"Price fetch failed for {token} on kraken: {e}")
            return 'kraken', None

    async def fetch_okx_price():
        try:
            url = f'https://www.okx.com/api/v5/market/ticker?instId={symbol_dash}'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if 'data' in data and data['data']:
                return 'okx', float(data['data'][0]['last'])
            return 'okx', None
        except Exception as e:
            logger.warning(f"Price fetch failed for {token} on okx: {e}")
            return 'okx', None

    # Run all price fetches concurrently
    tasks = [
                fetch_bybit_price(),
                fetch_binance_price(),
                fetch_kraken_price(),
                fetch_okx_price(),
            ] + [fetch_ccxt_price(ex_id, ex) for ex_id, ex in ccxt_exchanges.items()]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for ex_id, price in results:
        if price is not None:
            prices[ex_id] = price
            logger.debug(f"Fetched price for {token} on {ex_id}: {price}")

    return prices


def analyze_arbitrage(prices: dict, token: str, fees: dict) -> dict:
    if len(prices) < 2:
        logger.debug(f"Skipping arbitrage for {token}: fewer than 2 prices available")
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
    logger.debug(f"No profitable arbitrage for {token}: profit {profit_pct:.2f}%")
    return None


async def find_opportunities(user_id: int) -> list:
    logger.info(f"Starting arbitrage scan for user {user_id}...")
    tokens = await fetch_exchange_tokens(user_id)
    logger.info(f"Fetched tokens for exchanges: { {ex: len(tokens[ex]) for ex in tokens} }")
    exchanges = [ex for ex in tokens if tokens[ex]]
    if not exchanges:
        logger.error("No exchanges have tokens available.")
        return []
    try:
        token_sets = [set(tokens[ex]) for ex in exchanges]
        if not token_sets:
            logger.error("No token sets available for intersection.")
            return []
        common_tokens = set.intersection(*token_sets)
    except Exception as e:
        logger.error(f"Error finding common tokens: {e}")
        return []
    logger.info(f"Number of common tokens: {len(common_tokens)}")
    fees = {ex_id: DEFAULT_FEES.get(ex_id, 0.1) for ex_id in exchanges}
    opportunities = []

    # Fetch prices for all tokens concurrently
    async def process_token(token):
        try:
            logger.info(f"Checking arbitrage for token: {token}")
            prices = await get_market_prices(token)
            result = analyze_arbitrage(prices, token, fees)
            return result
        except Exception as e:
            logger.error(f"Error processing token {token}: {e}")
            return None

    tasks = [process_token(token) for token in common_tokens]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if result and isinstance(result, dict):
            opportunities.append(result)
        if len(opportunities) >= 10:
            break

    sorted_opportunities = sorted(opportunities, key=lambda x: x['profit'], reverse=True)
    logger.info(f"Found {len(sorted_opportunities)} arbitrage opportunities")
    return sorted_opportunities


def get_exchange_url(exchange, token):
    base, quote = token.split('/')
    if exchange == 'binance':
        return f"https://www.binance.com/en/trade/{base}_{quote}?type=spot"
    elif exchange == 'kraken':
        return f"https://trade.kraken.com/markets/{base}-{quote}"
    elif exchange == 'bybit':
        return f"https://www.bybit.com/trade/spot/{base}/{quote}"
    elif exchange == 'okx':
        return f"https://www.okx.com/trade-spot/{base}-{quote}"
    elif exchange == 'bingx':
        return f"https://bingx.com/en/spot/{base}{quote}/"
    elif exchange == 'kucoin':
        return f"https://www.kucoin.com/trade/{base}-{quote}"
    return ""


def format_opportunities_with_buttons(opportunities: list):
    messages = []
    if not opportunities:
        return [("🔍 No profitable arbitrage opportunities found currently.", None)]
    for idx, opp in enumerate(opportunities, 1):
        buy_link = get_exchange_url(opp['buy_exchange'], opp['token'])
        sell_link = get_exchange_url(opp['sell_exchange'], opp['token'])
        msg = (
            f"{idx}. *{opp['token']}*\n"
            f"   ▼ Buy on: [{opp['buy_exchange'].title()}]({buy_link}) (${opp['buy_price']})\n"
            f"   ▲ Sell on: [{opp['sell_exchange'].title()}]({sell_link}) (${opp['sell_price']})\n"
            f"   💰 Profit: {opp['profit']}%\n"
            f"   ――――――――――――――――――――"
        )
        callback_data = f"arbitrage|{opp['token']}|{opp['buy_exchange']}|{opp['sell_exchange']}|TRC20"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🚀 Arbitrage", callback_data=callback_data)]
        ])
        messages.append((msg, keyboard))
    return messages


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id not in user_data or not any(ex in user_data[user_id] for ex in EXCHANGES):
        await update.message.reply_text("Please set API keys for at least one exchange using /setkeys.")
        return
    await update.message.reply_text("🔄 Scanning for arbitrage opportunities...")
    global binance, bybit
    try:
        # Initialize clients with user keys
        for ex_id in user_data[user_id]:
            if ex_id == "binance":
                binance = BinanceClient(user_data[user_id][ex_id]["api_key"], user_data[user_id][ex_id]["api_secret"])
            elif ex_id == "bybit":
                bybit = BybitClient(api_key=user_data[user_id][ex_id]["api_key"],
                                    api_secret=user_data[user_id][ex_id]["api_secret"])
            elif ex_id in ccxt_exchanges:
                config = {
                    "apiKey": user_data[user_id][ex_id]["api_key"],
                    "secret": user_data[user_id][ex_id]["api_secret"],
                    "enableRateLimit": True,
                }
                if ex_id == "okx" and user_data[user_id][ex_id]["passphrase"]:
                    config["password"] = user_data[user_id][ex_id]["passphrase"]
                ccxt_exchanges[ex_id] = getattr(ccxt, ex_id)(config)
        opportunities = await find_opportunities(user_id)
        messages = format_opportunities_with_buttons(opportunities)
        for msg, keyboard in messages:
            await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Failed to scan for opportunities: {e}")
        await update.message.reply_text("⚠️ Error scanning for opportunities. Please try again later.")


async def arbitrage_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        data = query.data.split('|')
        if data[0] != 'arbitrage':
            return
        token, buy_ex, sell_ex, network = data[1], data[2], data[3], data[4]
        user_id = query.from_user.id
        if user_id not in user_data or buy_ex not in user_data[user_id] or sell_ex not in user_data[user_id]:
            await query.message.reply_text(
                f"Please set API keys for {buy_ex} and {sell_ex} using /setkeys."
            )
            return
        context.user_data["arbitrage_data"] = {
            "token": token,
            "buy_exchange": buy_ex,
            "sell_exchange": sell_ex,
            "network": network
        }
        context.user_data["awaiting_amount"] = True
        context.user_data.pop("awaiting_keys", None)
        await query.edit_message_text(
            f"Selected arbitrage for {token} (Buy on {buy_ex}, Sell on {sell_ex}).\n"
            "Please send the amount to trade (e.g., 10 for 10 USDT):"
        )
    except Exception as e:
        logger.error(f"Arbitrage button failed: {e}")
        await query.message.reply_text(f"Error: {e}")


async def handle_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_amount"):
        logger.debug(
            f"No awaiting_amount state for user {update.message.from_user.id}, ignoring message: {update.message.text}")
        return
    user_id = update.message.from_user.id
    logger.info(f"User {user_id} sent amount: {update.message.text}")
    try:
        amount = float(update.message.text)
        if amount <= 0:
            raise ValueError("Amount must be positive.")
        arbitrage_data = context.user_data.get("arbitrage_data", {})
        token = arbitrage_data["token"]
        buy_ex = arbitrage_data["buy_exchange"]
        sell_ex = arbitrage_data["sell_exchange"]
        network = arbitrage_data["network"]

        # Initialize exchange operations
        exchange_map = {
            'binance': BinanceOps(binance),
            'bybit': BybitOps(bybit),
            'kucoin': KucoinOps(ccxt_exchanges['kucoin']),
            'kraken': KrakenOps(ccxt_exchanges['kraken']),
            'bingx': BingxOps(ccxt_exchanges['bingx']),
            'okx': OkxOps(ccxt_exchanges['okx']),
        }
        buy_ops = exchange_map[buy_ex]
        sell_ops = exchange_map[sell_ex]

        # 1. Buy on buy_exchange
        await update.message.reply_text(f"Placing buy order for {token} on {buy_ex}...")
        buy_order = buy_ops.buy(token, amount)
        await update.message.reply_text(f"Bought {token} on {buy_ex}: {buy_order}")

        # 2. Fetch deposit address from sell_exchange
        asset = token.split('/')[0]
        await update.message.reply_text(f"Fetching deposit address for {asset} on {sell_ex}...")
        deposit_address = sell_ops.get_deposit_address(asset, network)
        await update.message.reply_text(f"Deposit address: {deposit_address}")

        # 3. Withdraw from buy_exchange to sell_exchange
        await update.message.reply_text(f"Withdrawing {asset} to {sell_ex}...")
        withdraw = buy_ops.withdraw(asset, amount, deposit_address, network)
        await update.message.reply_text(f"Withdrew {asset} to {sell_ex}: {withdraw}")

        # 4. Wait for deposit
        await update.message.reply_text(f"Waiting for deposit of {asset} on {sell_ex}...")
        deposited = sell_ops.wait_for_deposit(asset, amount)
        if not deposited:
            await update.message.reply_text(f"Deposit not detected on {sell_ex} after waiting. Aborting.")
            return
        await update.message.reply_text(f"Deposit confirmed on {sell_ex}.")

        # 5. Sell on sell_exchange
        await update.message.reply_text(f"Placing sell order for {token} on {sell_ex}...")
        sell_order = sell_ops.sell(token, amount)
        await update.message.reply_text(f"Sold {token} on {sell_ex}: {sell_order}")

        await update.message.reply_text("Arbitrage completed!")
    except ValueError as e:
        logger.error(f"Error processing amount for user {user_id}: {str(e)}")
        await update.message.reply_text(f"Error: {str(e)}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Arbitrage error for user {user_id}: {error_msg}")
        if "100419" in error_msg or "IP" in error_msg:
            try:
                ip_response = requests.get("https://api.ipify.org", timeout=5)
                current_ip = ip_response.text
                await update.message.reply_text(
                    f"IP Whitelist Error: Your current IP ({current_ip}) is not whitelisted for {buy_ex or sell_ex}. "
                    f"Add it at https://bingx.com/en/account/api/. Use /getip to check your IP."
                )
            except:
                await update.message.reply_text(
                    f"IP Whitelist Error: Your IP is not whitelisted for {buy_ex or sell_ex}. "
                    f"Visit https://bingx.com/en/account/api/ to add your IP. Use /getip to check your IP."
                )
        else:
            await update.message.reply_text(f"Error: {error_msg}")
    finally:
        context.user_data["awaiting_amount"] = False
        context.user_data.pop("arbitrage_data", None)


# Exchange Operation Classes
class ExchangeOps:
    def buy(self, token, amount):
        raise NotImplementedError

    def withdraw(self, asset, amount, address, network):
        raise NotImplementedError

    def get_deposit_address(self, asset, network):
        raise NotImplementedError

    def wait_for_deposit(self, asset, amount, timeout=600):
        raise NotImplementedError

    def sell(self, token, amount):
        raise NotImplementedError


class BinanceOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        symbol = token.replace('/', '')
        return self.client.create_order(symbol=symbol, side='BUY', type='MARKET', quoteOrderQty=amount)

    def withdraw(self, asset, amount, address, network):
        return self.client.withdraw(coin=asset, address=address, network=network, amount=amount)

    def get_deposit_address(self, asset, network):
        info = self.client.get_deposit_address(coin=asset, network=network)
        return info['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            history = self.client.get_deposit_history(coin=asset)
            for dep in history:
                if float(dep['amount']) >= float(amount) and dep['status'] == 1:
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        symbol = token.replace('/', '')
        return self.client.create_order(symbol=symbol, side='SELL', type='MARKET', quantity=amount)


class BybitOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        symbol = token.replace('/', '')
        return self.client.place_order(category="spot", symbol=symbol, side="Buy", orderType="Market", qty=amount)

    def withdraw(self, asset, amount, address, network):
        return self.client.withdraw(coin=asset, chain=network, address=address, amount=str(amount))

    def get_deposit_address(self, asset, network):
        info = self.client.get_deposit_address(coin=asset, chainType=network)
        return info['result']['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            history = self.client.get_deposit_records(coin=asset)
            for dep in history['result']['rows']:
                if float(dep['amount']) >= float(amount) and dep['status'] == 'success':
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        symbol = token.replace('/', '')
        return self.client.place_order(category="spot", symbol=symbol, side="Sell", orderType="Market", qty=amount)


class KucoinOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        return self.client.create_market_buy_order(token, amount)

    def withdraw(self, asset, amount, address, network):
        params = {"network": network}
        return self.client.withdraw(code=asset, amount=amount, address=address, params=params)

    def get_deposit_address(self, asset, network):
        info = self.client.fetch_deposit_address(asset, params={"network": network})
        return info['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            deposits = self.client.fetch_deposits(asset)
            for dep in deposits:
                if float(dep['amount']) >= float(amount) and dep['status'] == 'ok':
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        return self.client.create_market_sell_order(token, amount)


class KrakenOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        return self.client.create_market_buy_order(token, amount)

    def withdraw(self, asset, amount, address, network):
        raise NotImplementedError("Kraken withdrawal not implemented.")

    def get_deposit_address(self, asset, network):
        info = self.client.fetch_deposit_address(asset, params={"network": network})
        return info['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            deposits = self.client.fetch_deposits(asset)
            for dep in deposits:
                if float(dep['amount']) >= float(amount) and dep['status'] == 'ok':
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        return self.client.create_market_sell_order(token, amount)


class BingxOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        return self.client.create_market_buy_order(token, amount)

    def withdraw(self, asset, amount, address, network):
        params = {"network": network}
        return self.client.withdraw(code=asset, amount=amount, address=address, params=params)

    def get_deposit_address(self, asset, network):
        info = self.client.fetch_deposit_address(asset, params={"network": network})
        return info['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            deposits = self.client.fetch_deposits(asset)
            for dep in deposits:
                if float(dep['amount']) >= float(amount) and dep['status'] == 'ok':
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        return self.client.create_market_sell_order(token, amount)


class OkxOps(ExchangeOps):
    def __init__(self, client):
        self.client = client

    def buy(self, token, amount):
        return self.client.create_market_buy_order(token, amount)

    def withdraw(self, asset, amount, address, network):
        params = {"chain": f"{asset}-{network}"}
        return self.client.withdraw(code=asset, amount=amount, address=address, params=params)

    def get_deposit_address(self, asset, network):
        params = {"chain": f"{asset}-{network}"}
        info = self.client.fetch_deposit_address(asset, params=params)
        return info['address']

    def wait_for_deposit(self, asset, amount, timeout=600):
        for _ in range(timeout // 10):
            deposits = self.client.fetch_deposits(asset)
            for dep in deposits:
                if float(dep['amount']) >= float(amount) and dep['status'] == 'ok':
                    return True
            time.sleep(10)
        return False

    def sell(self, token, amount):
        return self.client.create_market_sell_order(token, amount)


def main():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("setkeys", set_keys))
    application.add_handler(CommandHandler("getip", get_ip))
    application.add_handler(CallbackQueryHandler(arbitrage_button_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.UpdateType.MESSAGE, handle_keys),
                            group=1)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.UpdateType.MESSAGE, handle_amount),
                            group=2)
    application.add_error_handler(error_handler)
    logger.info("Bot is running...")
    application.run_polling()


if __name__ == '__main__':
    main()