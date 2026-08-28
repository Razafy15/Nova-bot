import os
import json
import threading
import time
import requests
import websocket
from flask import Flask, jsonify, request, render_template_string
from threading import RLock, Lock

app = Flask(__name__)

# ============================================================
# ENVIRONMENT VARIABLES
# ============================================================
DEFAULT_APP_ID = os.environ.get("DERIV_APP_ID", "")
DEFAULT_PAT_TOKEN = os.environ.get("DERIV_PAT_TOKEN", "")
DEFAULT_ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID", "")

CURRENT_APP_ID = DEFAULT_APP_ID
CURRENT_PAT_TOKEN = DEFAULT_PAT_TOKEN
CURRENT_ACCOUNT_ID = DEFAULT_ACCOUNT_ID

if not CURRENT_APP_ID:
    CURRENT_APP_ID = "1089"

# ============================================================
# STATE
# ============================================================
state = {
    "connected": False,
    "connection_authenticated": False,
    "authorized": False,
    "balance": 0.0,
    "symbol": "",
    "symbol_display": "",
    "last_symbol": "",
    "last_price": None,
    "ticks_buffer": [],
    "running": False,
    "stake": 1.00,
    "multiplier": 300,              # <-- FANITSINA: 300 (farafahakeliny)
    "take_profit": 2.00,
    "stop_loss": 0.10,
    "duration_unit": "s",
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss_streak": 0,
    "max_loss_streak": 3,
    "daily_loss": 0.0,
    "max_daily_loss": 20.0,
    "max_trades_per_day": 100,
    "trades_today": 0,
    "current_trade": None,
    "boom_symbols": [],
    "available_contracts": {},
    "history": [],
    "logs": [],
    "last_error": "",
    "last_proposal": None,
    "last_trade_time": time.time(),
    "trade_interval": 5,
    "trade_state": "IDLE",
    "contract_info": None,
    "last_proposal_id": None,
    "session_start": time.time(),
    "day_start": time.time(),
    "daily_loss_reset_time": time.time(),
    "recovering": False,
    "recovery_pending": False,
    "pending_buy": None,
    "pending_buy_req_id": None,
    "pending_proposal_req_id": None,
    "req_id_counter": 0,
    "pending_buy_start_time": 0,
    "last_signal_sequence": None,
    "tick_subscription_id": None,
}

state_lock = RLock()

# ============================================================
# LOGGING
# ============================================================
def add_log(message):
    with state_lock:
        timestamp = time.strftime("%H:%M:%S")
        state["logs"].insert(0, {"time": timestamp, "message": str(message)})
        state["logs"] = state["logs"][:100]
    print(f"[{timestamp}] {message}", flush=True)

# ============================================================
# PERSISTENCE
# ============================================================
STATE_FILE = "bot_state.json"

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ Error loading state: {e}")
    return None

def save_state():
    try:
        current_trade = state.get("current_trade")
        if current_trade and current_trade.get("status") == "OPEN":
            current_trade_to_save = None
        else:
            current_trade_to_save = current_trade
            
        data_to_save = {
            "total_trades": state["total_trades"],
            "wins": state["wins"],
            "losses": state["losses"],
            "profit": state["profit"],
            "loss_streak": state["loss_streak"],
            "daily_loss": state["daily_loss"],
            "trades_today": state["trades_today"],
            "current_trade": current_trade_to_save,
            "trade_state": state["trade_state"],
            "last_trade_time": state["last_trade_time"],
            "symbol": state["symbol"],
            "symbol_display": state["symbol_display"],
            "stake": state["stake"],
            "multiplier": state["multiplier"],
            "take_profit": state["take_profit"],
            "stop_loss": state["stop_loss"],
            "duration_unit": state["duration_unit"],
            "trade_interval": state["trade_interval"],
            "max_loss_streak": state["max_loss_streak"],
            "max_daily_loss": state["max_daily_loss"],
            "max_trades_per_day": state["max_trades_per_day"],
        }
        with open(STATE_FILE, 'w') as f:
            json.dump(data_to_save, f)
    except Exception as e:
        print(f"⚠️ Error saving state: {e}")

saved_state = load_state()
if saved_state:
    for key, value in saved_state.items():
        if key in state:
            state[key] = value
    add_log("🔄 State restored from file")

# ============================================================
# RECONNECT CONTROLLER
# ============================================================
reconnect_lock = Lock()
reconnecting = False
reconnect_timer = None
reconnect_count = 0
MAX_RECONNECT = 10
RECONNECT_DELAY = 3

ws = None
ws_thread = None
ws_should_run = True

# ============================================================
# REQUEST ID
# ============================================================
def get_req_id():
    with state_lock:
        state["req_id_counter"] += 1
        return state["req_id_counter"]

# ============================================================
# WEBSOCKET SEND
# ============================================================
def send_ws(payload):
    global ws
    if ws is None:
        return False
    try:
        ws.send(json.dumps(payload))
        return True
    except Exception as exc:
        add_log(f"❌ WS SEND ERROR: {exc}")
        return False

# ============================================================
# API HELPERS
# ============================================================
def get_accounts(app_id, token):
    url = "https://api.derivws.com/trading/v1/options/accounts"
    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=20)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
    data = response.json()
    accounts = data.get("data", [])
    if isinstance(accounts, dict):
        accounts = [accounts]
    return accounts

def get_otp_url(app_id, token, account_id):
    url = f"https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp"
    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    response = requests.post(url, headers=headers, timeout=20)
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:200]}")
    data = response.json()
    ws_url = data.get("data", {}).get("url")
    if not ws_url:
        raise RuntimeError("OTP response did not contain data.url")
    return ws_url

# ============================================================
# UNSUBSCRIBE OLD SYMBOL
# ============================================================
def unsubscribe_old_symbol():
    old_symbol = state.get("last_symbol")
    if old_symbol and state["connected"]:
        sub_id = state.get("tick_subscription_id")
        add_log(f"🔄 Unsubscribing from {old_symbol}")
        if sub_id:
            send_ws({
                "forget": sub_id,
                "req_id": get_req_id()
            })
        else:
            send_ws({
                "forget_all": "ticks",
                "req_id": get_req_id()
            })
        with state_lock:
            state["tick_subscription_id"] = None
        time.sleep(0.5)
        return True
    return False

# ============================================================
# STRATEGIE: 3 TICKS MIDINA → MULTDOWN
# ============================================================
def check_strategy():
    buffer = state.get("ticks_buffer", [])
    if len(buffer) < 3:
        return False
    if not (buffer[-1] < buffer[-2] < buffer[-3]):
        return False
    current_sequence = tuple(buffer[-3:])
    last_sequence = state.get("last_signal_sequence")
    if last_sequence is not None and last_sequence == current_sequence:
        return False
    return True

# ============================================================
# CHECK IF MULTDOWN AVAILABLE
# ============================================================
def is_multdown_available(symbol):
    contracts = state.get("available_contracts", {})
    if isinstance(contracts, dict):
        return "MULTDOWN" in contracts
    elif isinstance(contracts, list):
        for contract in contracts:
            ctype = contract.get("contract_type")
            if ctype and "MULTDOWN" in ctype.upper():
                return True
    return False

# ============================================================
# RISK LIMITS
# ============================================================
def check_risk_limits():
    with state_lock:
        current_time = time.time()
        if current_time - state["daily_loss_reset_time"] > 86400:
            add_log("🔄 New day detected, resetting daily stats")
            state["daily_loss"] = 0.0
            state["trades_today"] = 0
            state["daily_loss_reset_time"] = current_time
        loss_streak = state["loss_streak"]
        max_loss_streak = state["max_loss_streak"]
        daily_loss = state["daily_loss"]
        max_daily_loss = state["max_daily_loss"]
        trades_today = state["trades_today"]
        max_trades_per_day = state["max_trades_per_day"]
    if loss_streak >= max_loss_streak:
        add_log(f"⚠️ Max loss streak reached: {loss_streak}")
        return False
    if daily_loss >= max_daily_loss:
        add_log(f"⚠️ Max daily loss reached: ${daily_loss:.2f}")
        return False
    if trades_today >= max_trades_per_day:
        add_log(f"⚠️ Max trades per day reached: {trades_today}")
        return False
    return True

# ============================================================
# RECOVERY
# ============================================================
def recover_active_contract():
    if state.get("recovering") or state.get("recovery_pending"):
        return
    with state_lock:
        state["recovering"] = True
        state["recovery_pending"] = True
        state["pending_buy"] = None
        state["pending_buy_req_id"] = None
        state["pending_proposal_req_id"] = None
    add_log("🔄 Recovery started...")
    symbol = state.get("symbol")
    if symbol and state["connected"]:
        add_log(f"🔄 Resubscribing to ticks for {symbol}")
        send_ws({"ticks": symbol, "subscribe": 1, "req_id": get_req_id()})
        send_ws({"contracts_for": symbol, "req_id": get_req_id()})
    send_ws({"portfolio": 1, "req_id": get_req_id()})

# ============================================================
# RECONNECT
# ============================================================
def schedule_reconnect():
    global reconnect_timer, reconnecting, reconnect_count, ws_should_run
    with reconnect_lock:
        if reconnecting:
            return
        if reconnect_timer is not None:
            return
        ws_should_run = True
        reconnecting = True
        def do_reconnect():
            global reconnect_timer, reconnecting, ws, ws_thread, reconnect_count, ws_should_run
            try:
                with reconnect_lock:
                    reconnect_timer = None
                reconnect_count += 1
                if reconnect_count > MAX_RECONNECT:
                    add_log(f"❌ Max reconnection attempts ({MAX_RECONNECT}) reached")
                    with reconnect_lock:
                        reconnecting = False
                    return
                add_log(f"🔄 Attempting to reconnect ({reconnect_count}/{MAX_RECONNECT})...")
                old_ws = ws
                if old_ws:
                    try:
                        old_ws.close()
                    except:
                        pass
                    time.sleep(1)
                if ws_thread and ws_thread.is_alive():
                    time.sleep(0.5)
                ws_url = get_otp_url(CURRENT_APP_ID, CURRENT_PAT_TOKEN, CURRENT_ACCOUNT_ID)
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws_thread = threading.Thread(
                    target=lambda: ws.run_forever(ping_interval=20, ping_timeout=10),
                    daemon=True
                )
                ws_thread.start()
                add_log("✅ Reconnection initiated")
            except Exception as e:
                add_log(f"❌ Reconnection failed: {e}")
                time.sleep(RECONNECT_DELAY)
                with reconnect_lock:
                    reconnecting = False
                    reconnect_timer = None
                if ws_should_run:
                    schedule_reconnect()
            finally:
                with reconnect_lock:
                    reconnecting = False
        reconnect_timer = threading.Thread(target=do_reconnect, daemon=True)
        reconnect_timer.start()

# ============================================================
# TRADING LOOP - FANITSINA FARANY
# ============================================================
def trading_loop():
    add_log("=== TRADING LOOP STARTED ===")
    loop_count = 0
    while True:
        with state_lock:
            if not state["running"]:
                break
        loop_count += 1
        try:
            if loop_count % 10 == 0:
                add_log(f"🔁 Trading loop alive (cycle {loop_count})")
            with state_lock:
                authenticated = state["connection_authenticated"]
                authorized = state["authorized"]
                symbol = state["symbol"]
                current_trade = state["current_trade"]
                trade_state = state["trade_state"]
                last_trade_time = state["last_trade_time"]
                trade_interval = state["trade_interval"]
                stake = state["stake"]
                multiplier = state["multiplier"]
                take_profit = state["take_profit"]
                stop_loss = state["stop_loss"]
                duration_unit = state["duration_unit"]
                recovering = state["recovering"]
                recovery_pending = state["recovery_pending"]
                pending_buy = state["pending_buy"]
            if pending_buy:
                if time.time() - state.get("pending_buy_start_time", 0) > 30:
                    add_log("⚠️ Pending buy timeout, resetting")
                    with state_lock:
                        state["pending_buy"] = None
                        state["pending_buy_req_id"] = None
                        state["trade_state"] = "IDLE"
                time.sleep(1)
                continue
            if recovering or recovery_pending:
                time.sleep(1)
                continue
            if not authenticated:
                time.sleep(1)
                continue
            if not authorized:
                time.sleep(1)
                continue
            if not symbol:
                time.sleep(1)
                continue
            if current_trade is not None:
                time.sleep(1)
                continue
            if trade_state != "IDLE":
                time.sleep(0.5)
                continue
            if not check_risk_limits():
                with state_lock:
                    state["running"] = False
                add_log("⛔ Risk limits reached, bot stopped")
                break
            if time.time() - last_trade_time < trade_interval:
                time.sleep(0.5)
                continue
            if not check_strategy():
                time.sleep(0.5)
                continue
            if not is_multdown_available(symbol):
                add_log(f"⛔ SKIP: MULTDOWN not available for {symbol}")
                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["last_trade_time"] = time.time()
                time.sleep(2)
                continue
            buffer = state.get("ticks_buffer", [])
            if len(buffer) >= 3:
                with state_lock:
                    state["last_signal_sequence"] = tuple(buffer[-3:])
            # FANITSINA: Log tsy misy duration
            add_log(f"📤 STRATEGY TRIGGERED: MULTDOWN | {symbol} | ${stake} | {multiplier}x | TP=${take_profit} | SL=${stop_loss}")
            # ==================================================
            # PROPOSAL - FANITSINA: TSY MISY DURATION
            # ==================================================
            proposal_req_id = get_req_id()
            with state_lock:
                state["last_proposal"] = None
                state["last_proposal_id"] = None
                state["trade_state"] = "PROPOSAL_PENDING"
                state["last_error"] = ""
                state["pending_proposal_req_id"] = proposal_req_id
            proposal_payload = {
                "proposal": 1,
                "amount": stake,
                "basis": "stake",
                "contract_type": "MULTDOWN",
                "currency": "USD",
                "duration_unit": duration_unit,  # "s"
                "multiplier": multiplier,        # 300
                "underlying_symbol": symbol,
                "limit_order": {
                    "take_profit": take_profit,
                    "stop_loss": stop_loss
                },
                "subscribe": 1,
                "req_id": proposal_req_id,
            }
            if not send_ws(proposal_payload):
                add_log("❌ Failed to send proposal")
                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None
                    state["last_trade_time"] = time.time()
                time.sleep(2)
                continue
            add_log(f"📤 Proposal sent: {json.dumps(proposal_payload)}")
            wait_start = time.time()
            proposal_received = False
            while time.time() - wait_start < 10:
                with state_lock:
                    if state.get("last_proposal") and state["last_proposal"].get("id"):
                        if state.get("pending_proposal_req_id") == proposal_req_id:
                            proposal_received = True
                            state["trade_state"] = "PROPOSAL_OK"
                            state["pending_proposal_req_id"] = None
                            add_log(f"✅ Proposal OK: {state['last_proposal']['id']}")
                            break
                time.sleep(0.5)
            if not proposal_received:
                with state_lock:
                    add_log(f"❌ PROPOSAL FAILED: {state.get('last_error', 'Timeout')}")
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None
                    state["last_trade_time"] = time.time()
                time.sleep(2)
                continue
            with state_lock:
                proposal_id = state["last_proposal"]["id"]
                ask_price = state["last_proposal"]["ask_price"]
            if ask_price is None:
                add_log("❌ No price in proposal")
                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None
                continue
            try:
                ask_price = float(ask_price)
            except:
                add_log(f"❌ Invalid price: {ask_price}")
                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None
                continue
            # ==================================================
            # BUY
            # ==================================================
            buy_req_id = get_req_id()
            with state_lock:
                state["current_trade"] = None
                state["trade_state"] = "BUY_PENDING"
                state["last_proposal_id"] = proposal_id
                state["pending_buy"] = {
                    "proposal_id": proposal_id,
                    "price": ask_price,
                    "start_time": time.time(),
                    "req_id": buy_req_id,
                }
                state["pending_buy_req_id"] = buy_req_id
                state["pending_buy_start_time"] = time.time()
            buy_payload = {
                "buy": proposal_id,
                "price": ask_price,
                "req_id": buy_req_id,
            }
            if not send_ws(buy_payload):
                add_log("❌ Failed to send BUY")
                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    state["last_trade_time"] = time.time()
                continue
            add_log(f"📤 BUY sent: proposal={proposal_id} price={ask_price} (req_id={buy_req_id})")
            wait_start = time.time()
            buy_success = False
            while time.time() - wait_start < 10:
                with state_lock:
                    if state.get("current_trade") and state["current_trade"].get("contract_id"):
                        if state.get("pending_buy_req_id") == buy_req_id:
                            buy_success = True
                            state["trade_state"] = "OPEN"
                            state["pending_buy"] = None
                            state["pending_buy_req_id"] = None
                            add_log(f"✅ CONTRACT OPENED: {state['current_trade']['contract_id']}")
                            break
                time.sleep(0.5)
            with state_lock:
                if not buy_success:
                    add_log(f"❌ BUY FAILED: {state.get('last_error', 'No contract opened')}")
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    state["last_trade_time"] = time.time()
                    continue
                state["last_trade_time"] = time.time()
            save_state()
            time.sleep(2)
        except Exception as e:
            add_log(f"❌ TRADING LOOP ERROR: {e}")
            import traceback
            add_log(traceback.format_exc())
            time.sleep(5)
    add_log("=== TRADING LOOP STOPPED ===")

# ============================================================
# WEBSOCKET EVENTS
# ============================================================
def on_open(socket):
    global reconnect_count
    with state_lock:
        state["connected"] = True
        state["connection_authenticated"] = True
        reconnect_count = 0
        state["pending_buy"] = None
        state["pending_buy_req_id"] = None
        state["pending_proposal_req_id"] = None
    add_log("✅ WebSocket connected (authenticated via OTP)")
    send_ws({"balance": 1, "subscribe": 1, "req_id": get_req_id()})
    send_ws({"active_symbols": "full", "req_id": get_req_id()})
    time.sleep(1)
    recover_active_contract()

# ============================================================
# WEBSOCKET MESSAGE
# ============================================================
def on_message(socket, message):
    try:
        data = json.loads(message)
    except:
        return
    msg_type = data.get("msg_type")
    req_id = data.get("req_id")
    # ERROR
    if msg_type == "error":
        error = data.get("error", {})
        code = error.get("code", "UNKNOWN")
        message_text = error.get("message", "Unknown error")
        with state_lock:
            state["last_error"] = f"{code}: {message_text}"
            if state.get("pending_proposal_req_id") == req_id:
                state["trade_state"] = "IDLE"
                state["pending_proposal_req_id"] = None
                add_log(f"❌ PROPOSAL ERROR: {code} - {message_text}")
            elif state.get("pending_buy_req_id") == req_id:
                state["trade_state"] = "IDLE"
                state["pending_buy"] = None
                state["pending_buy_req_id"] = None
                add_log(f"❌ BUY ERROR: {code} - {message_text}")
            else:
                add_log(f"❌ ERROR: {code} - {message_text}")
        if "multiplier" in message_text.lower():
            add_log("⚠️ MULTIPLIER: Check multiplier range (300-2000)")
        if "duration" in message_text.lower():
            add_log("⚠️ DURATION: Remove duration from proposal")
        return
    # BALANCE
    if msg_type == "balance":
        balance = data.get("balance", {})
        try:
            with state_lock:
                state["balance"] = float(balance.get("balance", 0))
                state["authorized"] = True
        except:
            pass
        add_log(f"✅ Authorized! Balance: ${state['balance']:.2f}")
        return
    # ACTIVE SYMBOLS
    if msg_type == "active_symbols":
        symbols = data.get("active_symbols", [])
        boom = []
        for item in symbols:
            symbol = item.get("underlying_symbol", "").strip()
            if not symbol:
                continue
            if "BOOM" in symbol.upper():
                boom.append({
                    "symbol": symbol,
                    "display_name": item.get("underlying_symbol_name", symbol)
                })
        with state_lock:
            state["boom_symbols"] = boom
        add_log(f"✅ BOOM symbols found: {len(boom)}")
        for b in boom[:10]:
            add_log(f"  - {b['symbol']} = {b['display_name']}")
        return
    # TICK
    if msg_type == "tick":
        tick = data.get("tick", {})
        try:
            price = float(tick.get("quote"))
            with state_lock:
                state["last_price"] = price
                state["ticks_buffer"].append(price)
                if len(state["ticks_buffer"]) > 20:
                    state["ticks_buffer"].pop(0)
                if state.get("tick_subscription_id") is None:
                    subscription = data.get("subscription")
                    if subscription:
                        state["tick_subscription_id"] = subscription.get("id")
        except:
            pass
        return
    # CONTRACTS FOR
    if msg_type == "contracts_for":
        result = data.get("contracts_for", {})
        available = result.get("available", [])
        contracts_dict = {}
        for contract in available:
            ctype = contract.get("contract_type")
            if ctype:
                contracts_dict[ctype] = contract
        with state_lock:
            state["available_contracts"] = contracts_dict
            state["contract_info"] = data
        add_log(f"✅ Contract info received for {state['symbol']}")
        for ctype, info in contracts_dict.items():
            expiry = info.get("expiry_type", "UNKNOWN")
            add_log(f"  - {ctype} | {expiry}")
        if "MULTDOWN" in contracts_dict:
            add_log("✅ MULTDOWN is available for this symbol")
        else:
            add_log("⚠️ MULTDOWN is NOT available for this symbol")
        return
    # PORTFOLIO
    if msg_type == "portfolio":
        portfolio = data.get("portfolio", {})
        contracts = portfolio.get("contracts", [])
        with state_lock:
            state["recovery_pending"] = False
        if contracts:
            add_log(f"📊 Found {len(contracts)} active contracts")
            open_contract = None
            for contract in contracts:
                if not contract.get("is_sold", True):
                    contract_id = contract.get("contract_id")
                    if contract_id:
                        open_contract = contract_id
                        break
            if open_contract:
                add_log(f"🔄 Found active contract: {open_contract}")
                with state_lock:
                    state["current_trade"] = {
                        "contract_id": open_contract,
                        "symbol": state["symbol"],
                        "stake": state["stake"],
                        "multiplier": state["multiplier"],
                        "take_profit": state["take_profit"],
                        "stop_loss": state["stop_loss"],
                        "tp_amount": state["take_profit"],
                        "sl_amount": state["stop_loss"],
                        "entry_price": None,
                        "tp_price": None,
                        "sl_price": None,
                        "status": "OPEN",
                        "start_time": time.time(),
                        "is_recovered": True,
                        "limit_order_raw": None,
                    }
                    state["trade_state"] = "OPEN"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    state["recovering"] = False
                send_ws({
                    "proposal_open_contract": 1,
                    "contract_id": open_contract,
                    "subscribe": 1,
                    "req_id": get_req_id(),
                })
                add_log("✅ Contract recovery initiated")
            else:
                with state_lock:
                    state["current_trade"] = None
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    state["recovering"] = False
                add_log("✅ No active contracts found")
        else:
            with state_lock:
                state["current_trade"] = None
                state["trade_state"] = "IDLE"
                state["pending_buy"] = None
                state["pending_buy_req_id"] = None
                state["recovering"] = False
            add_log("✅ No contracts in portfolio")
        return
    # PROPOSAL
    if msg_type == "proposal":
        proposal = data.get("proposal", {})
        if proposal.get("id"):
            with state_lock:
                pending_req_id = state.get("pending_proposal_req_id")
                if pending_req_id == req_id:
                    state["last_proposal"] = {
                        "id": proposal["id"],
                        "ask_price": proposal.get("ask_price")
                    }
                    if state["trade_state"] == "PROPOSAL_PENDING":
                        state["trade_state"] = "PROPOSAL_OK"
                    add_log(f"✅ Proposal OK: {proposal['id']} (req_id={req_id})")
                else:
                    add_log(f"⚠️ Ignoring proposal with mismatched req_id: {req_id} (expected {pending_req_id})")
        else:
            add_log(f"❌ Proposal FAILED: {json.dumps(data)}")
            with state_lock:
                state["last_proposal"] = None
                state["trade_state"] = "IDLE"
                state["pending_proposal_req_id"] = None
                if "error" in data:
                    state["last_error"] = data["error"].get("message", "Unknown")
        return
    # BUY
    if msg_type == "buy":
        buy = data.get("buy", {})
        contract_id = buy.get("contract_id")
        if "error" in data:
            error = data.get("error", {})
            with state_lock:
                state["last_error"] = f"BUY ERROR: {error.get('message', 'Unknown')}"
                if state.get("pending_buy_req_id") == req_id:
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
            add_log(f"❌ BUY ERROR: {error.get('message', 'Unknown')}")
            return
        if contract_id:
            with state_lock:
                pending_req_id = state.get("pending_buy_req_id")
                if pending_req_id == req_id:
                    buy_price = buy.get("buy_price")
                    if buy_price is not None:
                        try:
                            buy_price = float(buy_price)
                        except:
                            buy_price = None
                    else:
                        buy_price = None
                    state["current_trade"] = {
                        "contract_id": contract_id,
                        "symbol": state["symbol"],
                        "stake": state["stake"],
                        "multiplier": state["multiplier"],
                        "take_profit": state["take_profit"],
                        "stop_loss": state["stop_loss"],
                        "tp_amount": state["take_profit"],
                        "sl_amount": state["stop_loss"],
                        "entry_price": None,
                        "tp_price": None,
                        "sl_price": None,
                        "buy_price": buy_price,
                        "current_spot": None,
                        "current_pl": None,
                        "limit_order_raw": None,
                        "status": "OPEN",
                        "start_time": time.time(),
                        "is_recovered": False,
                    }
                    state["total_trades"] += 1
                    state["trades_today"] += 1
                    state["trade_state"] = "OPEN"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    add_log(f"✅ CONTRACT OPENED: {contract_id} (req_id={req_id})")
                    save_state()
                    send_ws({
                        "proposal_open_contract": 1,
                        "contract_id": contract_id,
                        "subscribe": 1,
                        "req_id": get_req_id(),
                    })
                else:
                    add_log(f"⚠️ Ignoring buy with mismatched req_id: {req_id} (expected {pending_req_id})")
        else:
            add_log(f"❌ BUY FAILED: {json.dumps(data)}")
            with state_lock:
                if state.get("pending_buy_req_id") == req_id:
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                if "error" in data:
                    state["last_error"] = data["error"].get("message", "Unknown")
        return
    # PROPOSAL OPEN CONTRACT
    if msg_type == "proposal_open_contract":
        contract = data.get("proposal_open_contract", {})
        entry_price = contract.get("entry_price")
        if entry_price is not None:
            try:
                entry_price = float(entry_price)
            except:
                entry_price = None
        current_spot = contract.get("current_spot")
        if current_spot is not None:
            try:
                current_spot = float(current_spot)
            except:
                current_spot = None
        exit_spot = contract.get("exit_spot")
        if exit_spot is not None:
            try:
                exit_spot = float(exit_spot)
            except:
                exit_spot = None
        profit = contract.get("profit")
        if profit is not None:
            try:
                profit = float(profit)
            except:
                profit = 0.0
        is_sold = contract.get("is_sold", False)
        is_expired = contract.get("is_expired", False)
        limit_order = contract.get("limit_order", {})
        tp_price = None
        sl_price = None
        if limit_order and state.get("current_trade") is not None:
            take_profit_obj = limit_order.get("take_profit")
            if isinstance(take_profit_obj, dict):
                tp_price = take_profit_obj.get("value")
                if tp_price is None:
                    tp_price = take_profit_obj.get("price")
                if tp_price is not None:
                    try:
                        tp_price = float(tp_price)
                        add_log(f"🔎 TP price from Deriv: {tp_price}")
                    except (TypeError, ValueError):
                        tp_price = None
            stop_loss_obj = limit_order.get("stop_loss")
            if isinstance(stop_loss_obj, dict):
                sl_price = stop_loss_obj.get("value")
                if sl_price is None:
                    sl_price = stop_loss_obj.get("price")
                if sl_price is not None:
                    try:
                        sl_price = float(sl_price)
                        add_log(f"🔎 SL price from Deriv: {sl_price}")
                    except (TypeError, ValueError):
                        sl_price = None
        if not (is_sold or is_expired):
            with state_lock:
                if state["current_trade"] is not None:
                    if entry_price is not None:
                        state["current_trade"]["entry_price"] = entry_price
                        add_log(f"📊 ENTRY PRICE: {entry_price:.4f}")
                    if current_spot is not None:
                        state["current_trade"]["current_spot"] = current_spot
                        add_log(f"📊 CURRENT SPOT: {current_spot:.4f}")
                    if profit is not None:
                        state["current_trade"]["current_pl"] = profit
                        add_log(f"📊 CURRENT P/L: {profit:+.2f}")
                    if tp_price is not None:
                        state["current_trade"]["tp_price"] = tp_price
                        add_log(f"   🟢 TP PRICE (from Deriv): {tp_price:.4f}")
                    if sl_price is not None:
                        state["current_trade"]["sl_price"] = sl_price
                        add_log(f"   🔴 SL PRICE (from Deriv): {sl_price:.4f}")
                    state["current_trade"]["tp_amount"] = state["take_profit"]
                    state["current_trade"]["sl_amount"] = state["stop_loss"]
                    if limit_order:
                        state["current_trade"]["limit_order_raw"] = limit_order
                    save_state()
            return
        with state_lock:
            final_profit = profit if profit is not None else 0.0
            state["profit"] += final_profit
            state["daily_loss"] += final_profit if final_profit < 0 else 0
            if final_profit > 0:
                state["wins"] += 1
                state["loss_streak"] = 0
                result = "WIN"
            else:
                state["losses"] += 1
                state["loss_streak"] += 1
                result = "LOSS"
            trade_record = {
                "time": time.strftime("%H:%M:%S"),
                "symbol": state["symbol_display"] or state["symbol"],
                "stake": state["stake"],
                "multiplier": state["multiplier"],
                "duration_unit": state["duration_unit"],
                "tp_amount": state["take_profit"],
                "sl_amount": state["stop_loss"],
                "result": result,
                "profit": final_profit,
            }
            if state["current_trade"]:
                trade_record["entry_price"] = state["current_trade"].get("entry_price")
                trade_record["tp_price"] = state["current_trade"].get("tp_price")
                trade_record["sl_price"] = state["current_trade"].get("sl_price")
                trade_record["buy_price"] = state["current_trade"].get("buy_price")
            if exit_spot is not None:
                trade_record["exit_price"] = exit_spot
            state["history"].insert(0, trade_record)
            state["history"] = state["history"][:100]
            state["current_trade"] = None
            state["trade_state"] = "IDLE"
            state["last_trade_time"] = time.time()
            state["recovering"] = False
            state["pending_buy"] = None
            state["pending_buy_req_id"] = None
        add_log(f"{result}: {final_profit:+.2f} | Loss streak: {state['loss_streak']}")
        if exit_spot is not None:
            add_log(f"   Exit Price: {exit_spot:.4f}")
        save_state()
        return

# ============================================================
# WEBSOCKET ERROR & CLOSE
# ============================================================
def on_error(socket, error):
    with state_lock:
        state["connected"] = False
        state["connection_authenticated"] = False
    add_log(f"❌ WebSocket error: {error}")
    with state_lock:
        running = state["running"]
    if running:
        time.sleep(1)
        schedule_reconnect()

def on_close(socket, code, reason):
    with state_lock:
        state["connected"] = False
        state["connection_authenticated"] = False
    add_log(f"🔒 WebSocket closed: {code} {reason}")
    with state_lock:
        running = state["running"]
    if running:
        time.sleep(1)
        schedule_reconnect()

# ============================================================
# WEBSOCKET THREAD
# ============================================================
def websocket_thread(ws_url):
    global ws, ws_should_run
    try:
        add_log("Opening WebSocket...")
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except Exception as exc:
        with state_lock:
            state["connected"] = False
            state["connection_authenticated"] = False
        add_log(f"❌ WS THREAD ERROR: {exc}")
        with state_lock:
            running = state["running"]
        if running and ws_should_run:
            time.sleep(1)
            schedule_reconnect()

# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/")
def index():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>NOVA BOOM BOT - MULTDOWN</title>
        <style>
            * { box-sizing: border-box; }
            body { background: #0a0e1a; color: #fff; font-family: Arial, sans-serif; padding: 20px; margin: 0; }
            .container { max-width: 1200px; margin: auto; }
            .panel { background: #141a2e; border-radius: 12px; padding: 20px; margin-bottom: 15px; border: 1px solid #2a3355; }
            .panel-title { font-size: 18px; font-weight: bold; margin-bottom: 15px; color: #a28dff; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px; }
            .field { margin-bottom: 10px; }
            .field label { display: block; color: #8991ad; font-size: 11px; margin-bottom: 4px; text-transform: uppercase; }
            input, select, button { width: 100%; padding: 10px; border-radius: 8px; border: 1px solid #2a3355; background: #0a0e1a; color: #fff; font-size: 14px; }
            input:focus, select:focus { border-color: #7654ff; outline: none; }
            .btn { border: none; padding: 12px 20px; cursor: pointer; font-weight: bold; border-radius: 8px; font-size: 14px; }
            .btn-purple { background: #7654ff; color: white; }
            .btn-green { background: #19c477; color: white; }
            .btn-red { background: #ed4665; color: white; }
            .btn-yellow { background: #e0a51b; color: white; }
            .btn-blue { background: #32a8ff; color: white; }
            .status { padding: 8px 16px; border-radius: 20px; font-size: 12px; font-weight: bold; display: inline-block; }
            .status.connected { background: rgba(25,196,119,0.2); color: #19c477; border: 1px solid rgba(25,196,119,0.3); }
            .status.disconnected { background: rgba(237,70,101,0.2); color: #ed4665; border: 1px solid rgba(237,70,101,0.3); }
            .stat { background: #0a0e1a; padding: 15px; border-radius: 10px; text-align: center; border: 1px solid #1a2340; }
            .stat-label { color: #8991ad; font-size: 10px; text-transform: uppercase; }
            .stat-value { font-size: 20px; font-weight: bold; margin-top: 5px; }
            .stat-value.green { color: #19c477; }
            .stat-value.red { color: #ed4665; }
            .stat-value.purple { color: #a28dff; }
            .log-box { background: #05080f; padding: 12px; border-radius: 8px; height: 200px; overflow-y: auto; font-family: monospace; font-size: 11px; }
            .log-line { padding: 4px 0; border-bottom: 1px solid #1a2340; }
            .log-time { color: #7654ff; margin-right: 8px; }
            .table-wrap { overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; font-size: 12px; }
            th, td { padding: 10px 8px; text-align: left; border-bottom: 1px solid #1a2340; }
            th { color: #8991ad; font-size: 10px; text-transform: uppercase; }
            .win { color: #19c477; font-weight: bold; }
            .loss { color: #ed4665; font-weight: bold; }
            .empty { color: #8991ad; text-align: center; padding: 20px; }
            .header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
            .brand { display: flex; align-items: center; gap: 10px; }
            .brand-icon { font-size: 28px; }
            .brand h1 { margin: 0; font-size: 22px; }
            .brand p { margin: 0; color: #8991ad; font-size: 12px; }
            .btn-group { display: flex; gap: 10px; flex-wrap: wrap; }
            .btn-group .btn { flex: 1; min-width: 80px; }
            @media (max-width: 600px) {
                .grid { grid-template-columns: 1fr; }
                .header { flex-direction: column; align-items: flex-start; }
            }
            .strategy-info { background: #1a2340; padding: 10px; border-radius: 8px; border-left: 3px solid #7654ff; margin-top: 10px; font-size: 12px; color: #a28dff; }
            .entry-display { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-top: 10px; }
            .entry-item { background: #0a0e1a; padding: 8px 12px; border-radius: 6px; border: 1px solid #1a2340; text-align: center; }
            .entry-item .label { font-size: 9px; color: #8991ad; text-transform: uppercase; }
            .entry-item .value { font-size: 13px; font-weight: bold; margin-top: 2px; }
            .entry-item .value.entry { color: #ffffff; }
            .entry-item .value.tp { color: #19c477; }
            .entry-item .value.sl { color: #ed4665; }
            .entry-item .value.pl { color: #fdcb6e; }
        </style>
    </head>
    <body>
    <div class="container">
        <div class="panel">
            <div class="header">
                <div class="brand">
                    <span class="brand-icon">🤖</span>
                    <div>
                        <h1>NOVA BOOM BOT</h1>
                        <p>MULTDOWN Strategy - 3 Ticks Down</p>
                    </div>
                </div>
                <div id="connectionStatus" class="status disconnected">● DISCONNECTED</div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">🔐 DERIV CONNECTION</div>
            <div class="grid">
                <div class="field">
                    <label>App ID</label>
                    <input id="appId" type="text" placeholder="App ID">
                </div>
                <div class="field">
                    <label>PAT Token</label>
                    <input id="apiToken" type="password" placeholder="PAT Token">
                </div>
                <div class="field">
                    <label>Account ID</label>
                    <input id="accountId" type="text" placeholder="Account ID">
                </div>
                <div class="field" style="display:flex;align-items:end;">
                    <button class="btn btn-purple" onclick="connectDeriv()">🔌 CONNECT</button>
                </div>
            </div>
            <div id="connectionMessage" style="margin-top:10px;color:#8991ad;font-size:12px;">Enter credentials and connect</div>
            <div class="warning-box">
                ⚠️ Ampiasao ny environment variables DERIV_APP_ID, DERIV_PAT_TOKEN, DERIV_ACCOUNT_ID ho an'ny production.
            </div>
        </div>

        <div class="panel">
            <div class="grid" style="grid-template-columns: repeat(6, 1fr);">
                <div class="stat"><div class="stat-label">Balance</div><div id="balance" class="stat-value">$0.00</div></div>
                <div class="stat"><div class="stat-label">Profit</div><div id="profit" class="stat-value green">$0.00</div></div>
                <div class="stat"><div class="stat-label">Win Rate</div><div id="winRate" class="stat-value purple">0%</div></div>
                <div class="stat"><div class="stat-label">Trades</div><div id="totalTrades" class="stat-value">0</div></div>
                <div class="stat"><div class="stat-label">W/L</div><div id="winsLosses" class="stat-value"><span class="green">0</span>/<span class="red">0</span></div></div>
                <div class="stat"><div class="stat-label">Loss Streak</div><div id="lossStreak" class="stat-value red">0</div></div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">📊 LIVE MARKET <span id="selectedMarket" style="font-size:12px;color:#8991ad;">No market</span></div>
            <div class="grid" style="grid-template-columns: 1.5fr 1fr;">
                <div style="background:#05080f;border-radius:10px;padding:20px;min-height:250px;border:1px solid #1a2340;position:relative;">
                    <canvas id="chartCanvas" style="width:100%;height:100%;"></canvas>
                    <div id="chartPrice" style="position:absolute;right:20px;top:20px;background:#0a0e1a;padding:8px 12px;border-radius:6px;font-size:14px;font-weight:bold;">—</div>
                </div>
                <div style="display:flex;flex-direction:column;gap:10px;">
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Symbol</span><strong id="marketName">—</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Price</span><strong id="livePrice">—</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Direction</span><strong style="color:#ed4665;">MULTDOWN</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Status</span><strong id="botStatus">STOPPED</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Trade State</span><strong id="tradeState">IDLE</strong></div>
                    
                    <div class="entry-display">
                        <div class="entry-item">
                            <div class="label">⚪ Entry</div>
                            <div class="value entry" id="entryDisplay">—</div>
                        </div>
                        <div class="entry-item">
                            <div class="label">🟢 TP ($2.00)</div>
                            <div class="value tp" id="tpDisplay">—</div>
                        </div>
                        <div class="entry-item">
                            <div class="label">🔴 SL ($0.10)</div>
                            <div class="value sl" id="slDisplay">—</div>
                        </div>
                        <div class="entry-item">
                            <div class="label">📊 P/L</div>
                            <div class="value pl" id="plDisplay">—</div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">⚙️ SETTINGS & STRATEGY</div>
            <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
                <div class="field"><label>Stake</label><input id="stakeInput" type="number" value="1.00" step="0.01" min="0.01" onchange="updateStake()"></div>
                <div class="field"><label>Interval (sec)</label><input id="tradeInterval" type="number" value="5" min="1" max="60" onchange="updateInterval()"></div>
                <div class="field"><label>Multiplier</label><input id="multiplierInput" type="number" value="300" min="300" max="2000" step="100" onchange="updateMultiplier()"></div>
                <div class="field">
                    <label>Max Loss Streak</label>
                    <input id="maxLossStreak" type="number" value="3" min="1" max="10" onchange="updateRisk()">
                </div>
            </div>
            <div class="strategy-info">
                🎯 <b>STRATEGY:</b> 3 ticks mifanesy midina → <b>MULTDOWN</b> (Multiplier 300x - 2000x)
            </div>
            <div class="btn-group" style="margin-top:15px;">
                <button class="btn btn-green" onclick="startBot()">▶ START</button>
                <button class="btn btn-yellow" onclick="pauseBot()">⏸ PAUSE</button>
                <button class="btn btn-red" onclick="stopBot()">⛔ STOP</button>
                <button class="btn btn-blue" onclick="resetStats()">🔄 RESET STATS</button>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">🔴 BOOM FINDER</div>
            <div class="grid" style="grid-template-columns: 1.5fr 1fr auto;">
                <div class="field">
                    <label>Symbol</label>
                    <select id="symbol" onchange="selectSymbol()">
                        <option value="">Find BOOM first</option>
                    </select>
                </div>
                <div class="field">
                    <label>Live Price</label>
                    <div id="finderPrice" style="padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;font-weight:bold;">—</div>
                </div>
                <div class="field" style="display:flex;align-items:end;">
                    <button class="btn btn-purple" onclick="findBoom()">🔄 FIND</button>
                </div>
            </div>
            <div id="contractInfo" style="margin-top:10px;padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;font-size:12px;color:#8991ad;">Select a symbol to see available contracts</div>
        </div>

        <div class="panel">
            <div class="panel-title">🎯 CURRENT TRADE</div>
            <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
                <div class="field"><label>Symbol</label><div id="tradeSymbol" style="padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;">—</div></div>
                <div class="field"><label>Stake</label><div id="tradeStake" style="padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;">—</div></div>
                <div class="field"><label>Contract</label><div id="contractId" style="padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;">—</div></div>
                <div class="field"><label>Status</label><div id="tradeStatus" style="padding:10px;background:#0a0e1a;border-radius:8px;border:1px solid #1a2340;">WAITING</div></div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">📜 HISTORY <span id="historyCount" style="font-size:12px;color:#8991ad;">0 trades</span></div>
            <div class="table-wrap">
                <table>
                    <thead><tr><th>Time</th><th>Symbol</th><th>Stake</th><th>Result</th><th>Profit</th></tr></thead>
                    <tbody id="historyBody"><tr><td colspan="5" class="empty">No trades yet</td></tr></tbody>
                </table>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">🖥️ SYSTEM LOG <button class="btn btn-blue" style="padding:6px 12px;font-size:11px;" onclick="clearLogs()">CLEAR</button></div>
            <div id="logs" class="log-box"><div class="empty">Waiting...</div></div>
        </div>
    </div>

    <script>
        var priceHistory = [];
        var maxPoints = 70;

        function $(id) { return document.getElementById(id); }
        function money(v) { return "$" + Number(v || 0).toFixed(2); }
        function setMessage(text, type) {
            var box = $("connectionMessage");
            box.textContent = text;
            box.style.color = type === "ok" ? "#19c477" : type === "error" ? "#ed4665" : "#8991ad";
        }

        var currentEntry = null;
        var currentTP = null;
        var currentSL = null;
        var currentPL = null;

        async function connectDeriv() {
            var data = {
                app_id: $("appId").value.trim(),
                token: $("apiToken").value.trim(),
                account_id: $("accountId").value.trim()
            };
            if (!data.token || !data.account_id) {
                setMessage("❌ Token and Account ID required", "error");
                return;
            }
            setMessage("⏳ Connecting...", "");
            try {
                var r = await fetch("/api/connect", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify(data)
                });
                var res = await r.json();
                if (res.ok) {
                    setMessage("✅ Connected!", "ok");
                    updateDashboard();
                } else {
                    setMessage("❌ " + res.error, "error");
                }
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function findBoom() {
            setMessage("⏳ Searching...", "");
            try {
                await fetch("/api/markets", { method: "POST" });
                setMessage("✅ Searching...", "ok");
                setTimeout(updateDashboard, 1000);
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function selectSymbol() {
            var select = $("symbol");
            var symbol = select.value;
            if (!symbol) return;
            var display = select.options[select.selectedIndex].text;
            try {
                var r = await fetch("/api/select-symbol", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ symbol: symbol, display_name: display })
                });
                var res = await r.json();
                if (res.ok) {
                    setMessage("✅ Selected: " + display, "ok");
                    updateDashboard();
                } else {
                    setMessage("❌ " + res.error, "error");
                }
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function updateStake() {
            var stake = parseFloat($("stakeInput").value) || 1.00;
            try {
                var r = await fetch("/api/update-stake", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ stake: stake })
                });
                var res = await r.json();
                if (res.ok) setMessage("✅ Stake: $" + stake, "ok");
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function updateInterval() {
            var interval = parseInt($("tradeInterval").value) || 5;
            try {
                var r = await fetch("/api/update-interval", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ interval: interval })
                });
                var res = await r.json();
                if (res.ok) setMessage("✅ Interval: " + interval + "s", "ok");
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function updateMultiplier() {
            var multiplier = parseInt($("multiplierInput").value) || 300;
            if (multiplier < 300) {
                setMessage("⚠️ Multiplier must be at least 300", "error");
                return;
            }
            try {
                var r = await fetch("/api/update-multiplier", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ multiplier: multiplier })
                });
                var res = await r.json();
                if (res.ok) setMessage("✅ Multiplier: " + multiplier + "x", "ok");
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function updateRisk() {
            var maxLoss = parseInt($("maxLossStreak").value) || 3;
            try {
                var r = await fetch("/api/update-risk", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ max_loss_streak: maxLoss })
                });
                var res = await r.json();
                if (res.ok) setMessage("✅ Max loss streak: " + maxLoss, "ok");
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function startBot() {
            try {
                var r = await fetch("/api/start", { method: "POST" });
                var res = await r.json();
                if (res.ok) setMessage("✅ Bot started!", "ok");
                else setMessage("❌ " + res.error, "error");
                updateDashboard();
            } catch(e) {
                setMessage("❌ " + e.message, "error");
            }
        }

        async function pauseBot() {
            try {
                var r = await fetch("/api/pause", { method: "POST" });
                var res = await r.json();
                if (res.ok) {
                    setMessage("⏸ Paused", "");
                    updateDashboard();
                }
            } catch(e) {}
        }

        async function stopBot() {
            try {
                var r = await fetch("/api/stop", { method: "POST" });
                var res = await r.json();
                if (res.ok) {
                    setMessage("⛔ Stopped", "");
                    updateDashboard();
                }
            } catch(e) {}
        }

        async function resetStats() {
            try {
                var r = await fetch("/api/reset-stats", { method: "POST" });
                var res = await r.json();
                if (res.ok) {
                    setMessage("🔄 Stats reset", "ok");
                    updateDashboard();
                }
            } catch(e) {}
        }

        function updateSymbols(symbols) {
            var select = $("symbol");
            var current = select.value;
            select.innerHTML = '<option value="">-- Select BOOM --</option>';
            if (symbols) {
                symbols.forEach(function(s) {
                    var opt = document.createElement("option");
                    opt.value = s.symbol;
                    opt.textContent = s.display_name + " (" + s.symbol + ")";
                    select.appendChild(opt);
                });
                if (current) select.value = current;
            }
        }

        function updateContractInfo(contracts) {
            var box = $("contractInfo");
            if (!contracts || Object.keys(contracts).length === 0) {
                box.innerHTML = "No contract data available";
                return;
            }
            var html = "<b>Available contracts:</b><br>";
            var hasMultdown = false;
            for (var ctype in contracts) {
                var info = contracts[ctype];
                var expiry = info.expiry_type || "UNKNOWN";
                if (ctype.toUpperCase() === "MULTDOWN") {
                    html += "• <span style='color:#19c477;font-weight:bold;'>✅ " + ctype + "</span> | " + expiry + " <span style='color:#19c477;'>(AVAILABLE)</span><br>";
                    hasMultdown = true;
                } else {
                    html += "• " + ctype + " | " + expiry + "<br>";
                }
            }
            if (!hasMultdown) {
                html += "<br><span style='color:#ed4665;'>⚠️ MULTDOWN is NOT available for this symbol</span>";
            } else {
                html += "<br><span style='color:#19c477;'>✅ MULTDOWN is available for this symbol</span>";
            }
            box.innerHTML = html;
        }

        function updateHistory(history) {
            var body = $("historyBody");
            $("historyCount").textContent = (history || []).length + " trades";
            if (!history || history.length === 0) {
                body.innerHTML = '<tr><td colspan="5" class="empty">No trades yet</td></tr>';
                return;
            }
            body.innerHTML = history.map(function(item) {
                var profit = Number(item.profit || 0);
                var result = item.result || "—";
                return '<tr>' +
                    '<td>' + (item.time || "—") + '</td>' +
                    '<td>' + (item.symbol || "—") + '</td>' +
                    '<td>' + money(item.stake) + '</td>' +
                    '<td class="' + (result === "WIN" ? "win" : "loss") + '">' + result + '</td>' +
                    '<td class="' + (profit >= 0 ? "win" : "loss") + '">' + (profit >= 0 ? "+" : "") + money(profit) + '</td>' +
                '</tr>';
            }).join("");
        }

        function updateLogs(logs) {
            var box = $("logs");
            if (!logs || logs.length === 0) {
                box.innerHTML = '<div class="empty">Waiting...</div>';
                return;
            }
            box.innerHTML = logs.map(function(log) {
                return '<div class="log-line"><span class="log-time">[' + log.time + ']</span>' + log.message + '</div>';
            }).join("");
        }

        async function updateDashboard() {
            try {
                var r = await fetch("/api/status", { cache: "no-store" });
                var data = await r.json();

                var status = $("connectionStatus");
                if (data.connected && data.authorized) {
                    status.textContent = "● CONNECTED";
                    status.className = "status connected";
                } else {
                    status.textContent = "● DISCONNECTED";
                    status.className = "status disconnected";
                }

                $("balance").textContent = money(data.balance);
                $("profit").textContent = (data.profit >= 0 ? "+" : "") + money(data.profit);
                $("profit").className = "stat-value " + (data.profit >= 0 ? "green" : "red");
                $("winRate").textContent = (data.win_rate || 0).toFixed(0) + "%";
                $("totalTrades").textContent = data.total_trades || 0;
                $("winsLosses").innerHTML = '<span class="green">' + (data.wins || 0) + '</span>/<span class="red">' + (data.losses || 0) + '</span>';
                $("lossStreak").textContent = data.loss_streak || 0;
                $("botStatus").textContent = data.running ? "RUNNING" : "STOPPED";
                $("tradeState").textContent = data.trade_state || "IDLE";

                if (data.symbol_display) {
                    $("marketName").textContent = data.symbol_display;
                    $("selectedMarket").textContent = data.symbol_display;
                    $("tradeSymbol").textContent = data.symbol_display;
                } else if (data.symbol) {
                    $("marketName").textContent = data.symbol;
                    $("selectedMarket").textContent = data.symbol;
                    $("tradeSymbol").textContent = data.symbol;
                }

                if (data.last_price) {
                    var price = Number(data.last_price);
                    $("livePrice").textContent = price.toFixed(4);
                    $("finderPrice").textContent = price.toFixed(4);
                    $("chartPrice").textContent = price.toFixed(4);
                    addPrice(price);
                }

                if (data.current_trade) {
                    var trade = data.current_trade;

                    $("tradeStake").textContent = money(trade.stake);
                    $("contractId").textContent = trade.contract_id || "—";
                    $("tradeStatus").textContent = trade.status || "OPEN";

                    currentEntry = trade.entry_price != null ? Number(trade.entry_price) : null;
                    currentTP = trade.tp_price != null ? Number(trade.tp_price) : null;
                    currentSL = trade.sl_price != null
