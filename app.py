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
# ENVIRONMENT
# ============================================================
DEFAULT_APP_ID = os.environ.get("DERIV_APP_ID", "1089")
DEFAULT_PAT_TOKEN = os.environ.get("DERIV_PAT_TOKEN", "")
DEFAULT_ACCOUNT_ID = os.environ.get("DERIV_ACCOUNT_ID", "")

CURRENT_APP_ID = DEFAULT_APP_ID
CURRENT_PAT_TOKEN = DEFAULT_PAT_TOKEN
CURRENT_ACCOUNT_ID = DEFAULT_ACCOUNT_ID

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
    "multiplier": 300,

    # These are MONEY amounts, not price levels.
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
    "last_proposal_id": None,
    "pending_proposal_req_id": None,

    "last_trade_time": 0.0,
    "trade_interval": 5,

    "trade_state": "IDLE",

    "session_start": time.time(),
    "day_start": time.time(),
    "daily_loss_reset_time": time.time(),

    "recovering": False,
    "recovery_pending": False,

    "pending_buy": None,
    "pending_buy_req_id": None,
    "pending_buy_start_time": 0,

    "req_id_counter": 0,

    "last_signal_sequence": None,

    "tick_subscription_id": None,
    "contract_subscription_id": None,
}

state_lock = RLock()

# ============================================================
# WEBSOCKET / RECONNECT
# ============================================================
ws = None
ws_thread = None
ws_should_run = True

reconnect_lock = Lock()
reconnecting = False
reconnect_count = 0
MAX_RECONNECT = 10
RECONNECT_DELAY = 3

trading_thread = None

# ============================================================
# LOGGING
# ============================================================
def add_log(message):
    timestamp = time.strftime("%H:%M:%S")
    with state_lock:
        state["logs"].insert(0, {
            "time": timestamp,
            "message": str(message)
        })
        state["logs"] = state["logs"][:150]
    print(f"[{timestamp}] {message}", flush=True)


# ============================================================
# HELPERS
# ============================================================
def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_req_id():
    with state_lock:
        state["req_id_counter"] += 1
        return state["req_id_counter"]


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


def normalize_symbol_name(item):
    return (
        item.get("underlying_symbol")
        or item.get("symbol")
        or item.get("display_name")
        or ""
    ).strip()


def symbol_is_boom(symbol):
    return "BOOM" in (symbol or "").upper()


def get_price_precision(price):
    """
    Used only for display/local estimate.
    Server-returned TP/SL remains authoritative.
    """
    if price is None:
        return 4
    p = abs(float(price))
    if p >= 1000:
        return 2
    if p >= 100:
        return 3
    if p >= 1:
        return 4
    if p >= 0.1:
        return 5
    return 6


# ============================================================
# TP/SL PRICE ESTIMATE FOR MULTDOWN
# ============================================================
def calculate_multdown_price_levels(entry_price, stake, multiplier, tp_amount, sl_amount):
    """
    IMPORTANT:
    tp_amount and sl_amount are MONEY amounts (USD), not price levels.

    For MULTDOWN, the local estimate is:
        TP = entry * (1 - TP_money / (stake * multiplier))
        SL = entry * (1 + SL_money / (stake * multiplier))

    The returned Deriv `limit_order.*.value` is authoritative and should
    always override this estimate when available.
    """
    entry = safe_float(entry_price)
    stake = safe_float(stake)
    multiplier = safe_float(multiplier)
    tp_amount = safe_float(tp_amount)
    sl_amount = safe_float(sl_amount)

    if not all(v is not None for v in [
        entry, stake, multiplier, tp_amount, sl_amount
    ]):
        return None, None

    if entry <= 0 or stake <= 0 or multiplier <= 0:
        return None, None

    exposure = stake * multiplier
    if exposure <= 0:
        return None, None

    tp_ratio = tp_amount / exposure
    sl_ratio = sl_amount / exposure

    # Invalid if TP would put the theoretical price <= 0.
    if tp_ratio >= 1:
        return None, None

    tp_price = entry * (1.0 - tp_ratio)
    sl_price = entry * (1.0 + sl_ratio)

    return tp_price, sl_price


# ============================================================
# PERSISTENCE
# ============================================================
STATE_FILE = "bot_state.json"


def load_state():
    try:
        if not os.path.exists(STATE_FILE):
            return None

        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    except Exception as exc:
        print(f"⚠️ Error loading state: {exc}")
        return None


def save_state():
    try:
        with state_lock:
            current_trade = state.get("current_trade")

            # Do not persist a live contract as a fake open state.
            if current_trade and current_trade.get("status") == "OPEN":
                current_trade_to_save = None
            else:
                current_trade_to_save = current_trade

            data = {
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

        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    except Exception as exc:
        print(f"⚠️ Error saving state: {exc}")


saved_state = load_state()
if saved_state:
    with state_lock:
        for key, value in saved_state.items():
            if key in state:
                state[key] = value

    # Never restore a fake open trade after process restart.
    with state_lock:
        state["current_trade"] = None
        state["trade_state"] = "IDLE"

    add_log("🔄 State restored from file")


# ============================================================
# DERIV REST HELPERS
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
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text[:300]}"
        )

    data = response.json()
    accounts = data.get("data", [])

    if isinstance(accounts, dict):
        accounts = [accounts]

    return accounts


def get_otp_url(app_id, token, account_id):
    url = (
        "https://api.derivws.com/trading/v1/options/accounts/"
        f"{account_id}/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    response = requests.post(url, headers=headers, timeout=20)

    if not response.ok:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text[:300]}"
        )

    data = response.json()
    ws_url = data.get("data", {}).get("url")

    if not ws_url:
        raise RuntimeError("OTP response did not contain data.url")

    return ws_url


# ============================================================
# UNSUBSCRIBE
# ============================================================
def unsubscribe_old_symbol():
    with state_lock:
        old_symbol = state.get("last_symbol")
        connected = state.get("connected")
        sub_id = state.get("tick_subscription_id")

    if not old_symbol or not connected:
        return False

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

    return True


# ============================================================
# STRATEGY: 3 CONSECUTIVE DOWN TICKS
# ============================================================
def check_strategy():
    with state_lock:
        buffer = list(state.get("ticks_buffer", []))
        last_sequence = state.get("last_signal_sequence")

    if len(buffer) < 3:
        return False

    sequence = tuple(buffer[-3:])

    if not (sequence[2] < sequence[1] < sequence[0]):
        return False

    if last_sequence is not None and last_sequence == sequence:
        return False

    return True


# ============================================================
# CONTRACT AVAILABILITY
# ============================================================
def is_multdown_available(symbol):
    with state_lock:
        contracts = state.get("available_contracts", {})

    if isinstance(contracts, dict):
        return any(
            str(k).upper() == "MULTDOWN"
            for k in contracts.keys()
        )

    if isinstance(contracts, list):
        for contract in contracts:
            ctype = contract.get("contract_type")
            if ctype and ctype.upper() == "MULTDOWN":
                return True

    return False


# ============================================================
# RISK LIMITS
# ============================================================
def check_risk_limits():
    current_time = time.time()

    with state_lock:
        if current_time - state["daily_loss_reset_time"] >= 86400:
            state["daily_loss"] = 0.0
            state["trades_today"] = 0
            state["daily_loss_reset_time"] = current_time
            add_log("🔄 New day: daily statistics reset")

        loss_streak = state["loss_streak"]
        max_loss_streak = state["max_loss_streak"]

        daily_loss = state["daily_loss"]
        max_daily_loss = state["max_daily_loss"]

        trades_today = state["trades_today"]
        max_trades = state["max_trades_per_day"]

    if loss_streak >= max_loss_streak:
        add_log(f"⚠️ Max loss streak reached: {loss_streak}")
        return False

    if daily_loss >= max_daily_loss:
        add_log(f"⚠️ Max daily loss reached: ${daily_loss:.2f}")
        return False

    if trades_today >= max_trades:
        add_log(f"⚠️ Max trades/day reached: {trades_today}")
        return False

    return True


# ============================================================
# CONTRACT RECOVERY
# ============================================================
def recover_active_contract():
    with state_lock:
        if state["recovering"] or state["recovery_pending"]:
            return

        state["recovering"] = True
        state["recovery_pending"] = True
        symbol = state["symbol"]

    add_log("🔄 Recovery started...")

    if not symbol:
        with state_lock:
            state["recovering"] = False
            state["recovery_pending"] = False
        return

    if not state.get("connected"):
        with state_lock:
            state["recovering"] = False
        return

    send_ws({
        "ticks": symbol,
        "subscribe": 1,
        "req_id": get_req_id()
    })

    send_ws({
        "contracts_for": symbol,
        "req_id": get_req_id()
    })

    send_ws({
        "portfolio": 1,
        "req_id": get_req_id()
    })


# ============================================================
# RECONNECT
# ============================================================
def schedule_reconnect():
    global reconnecting, reconnect_count

    with reconnect_lock:
        if reconnecting:
            return
        reconnecting = True

    def do_reconnect():
        global reconnecting, reconnect_count, ws, ws_thread

        try:
            with reconnect_lock:
                reconnect_count += 1
                attempt = reconnect_count

            if attempt > MAX_RECONNECT:
                add_log(
                    f"❌ Max reconnection attempts ({MAX_RECONNECT}) reached"
                )
                return

            add_log(
                f"🔄 Reconnecting ({attempt}/{MAX_RECONNECT})..."
            )

            try:
                if ws:
                    ws.close()
            except Exception:
                pass

            time.sleep(1)

            if not CURRENT_PAT_TOKEN or not CURRENT_ACCOUNT_ID:
                add_log("❌ Missing PAT token or Account ID")
                return

            ws_url = get_otp_url(
                CURRENT_APP_ID,
                CURRENT_PAT_TOKEN,
                CURRENT_ACCOUNT_ID
            )

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws_thread = threading.Thread(
                target=lambda: ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                ),
                daemon=True
            )

            ws_thread.start()
            add_log("✅ Reconnection initiated")

        except Exception as exc:
            add_log(f"❌ Reconnection failed: {exc}")

        finally:
            with reconnect_lock:
                reconnecting = False

    threading.Thread(
        target=do_reconnect,
        daemon=True
    ).start()


# ============================================================
# SET TP/SL ON SERVER AFTER BUY
# ============================================================
def set_server_tp_sl(contract_id, tp_amount, sl_amount):
    """
    Server-side TP/SL.

    IMPORTANT:
    These values are MONEY amounts.
    Deriv returns the actual PRICE LEVEL under:
        contract_update.take_profit.value
        contract_update.stop_loss.value
    """
    tp_amount = safe_float(tp_amount)
    sl_amount = safe_float(sl_amount)

    if not contract_id:
        return False

    if tp_amount is None or sl_amount is None:
        return False

    payload = {
        "contract_update": 1,
        "contract_id": int(contract_id),
        "limit_order": {
            "take_profit": tp_amount,
            "stop_loss": sl_amount,
        },
        "req_id": get_req_id(),
    }

    add_log(
        f"🎯 Setting SERVER TP=${tp_amount:.2f} "
        f"SL=${sl_amount:.2f}"
    )

    return send_ws(payload)


# ============================================================
# TRADING LOOP
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
            if loop_count % 20 == 0:
                add_log(
                    f"🔁 Trading loop alive ({loop_count})"
                )

            with state_lock:
                authenticated = state["connection_authenticated"]
                authorized = state["authorized"]
                symbol = state["symbol"]
                current_trade = state["current_trade"]
                trade_state = state["trade_state"]
                last_trade_time = state["last_trade_time"]
                trade_interval = state["trade_interval"]
                recovering = state["recovering"]
                recovery_pending = state["recovery_pending"]
                pending_buy = state["pending_buy"]

                stake = state["stake"]
                multiplier = state["multiplier"]
                tp_amount = state["take_profit"]
                sl_amount = state["stop_loss"]
                duration_unit = state["duration_unit"]

            if pending_buy:
                if (
                    time.time()
                    - state.get("pending_buy_start_time", 0)
                    > 30
                ):
                    add_log("⚠️ Pending BUY timeout")

                    with state_lock:
                        state["pending_buy"] = None
                        state["pending_buy_req_id"] = None
                        state["trade_state"] = "IDLE"

                time.sleep(0.5)
                continue

            if recovering or recovery_pending:
                time.sleep(0.5)
                continue

            if not authenticated or not authorized:
                time.sleep(1)
                continue

            if not symbol:
                time.sleep(1)
                continue

            if current_trade is not None:
                time.sleep(0.5)
                continue

            if trade_state != "IDLE":
                time.sleep(0.5)
                continue

            if not check_risk_limits():
                with state_lock:
                    state["running"] = False
                add_log("⛔ Risk limits reached. Bot stopped.")
                break

            if time.time() - last_trade_time < trade_interval:
                time.sleep(0.25)
                continue

            if not check_strategy():
                time.sleep(0.25)
                continue

            if not is_multdown_available(symbol):
                add_log(
                    f"⛔ SKIP: MULTDOWN unavailable for {symbol}"
                )
                with state_lock:
                    state["last_trade_time"] = time.time()
                time.sleep(2)
                continue

            with state_lock:
                buffer = list(state["ticks_buffer"])

            sequence = tuple(buffer[-3:])

            with state_lock:
                state["last_signal_sequence"] = sequence

            add_log(
                f"📤 MULTDOWN SIGNAL | {symbol} | "
                f"${stake:.2f} | {multiplier}x | "
                f"TP=${tp_amount:.2f} | SL=${sl_amount:.2f}"
            )

            # ----------------------------------------------------
            # PROPOSAL
            # ----------------------------------------------------
            proposal_req_id = get_req_id()

            with state_lock:
                state["last_proposal"] = None
                state["last_proposal_id"] = None
                state["last_error"] = ""
                state["pending_proposal_req_id"] = proposal_req_id
                state["trade_state"] = "PROPOSAL_PENDING"

            # Do NOT put TP/SL into proposal here.
            # Create proposal first, then buy, then contract_update.
            proposal_payload = {
                "proposal": 1,
                "amount": stake,
                "basis": "stake",
                "contract_type": "MULTDOWN",
                "currency": "USD",
                "duration_unit": duration_unit,
                "multiplier": multiplier,
                "underlying_symbol": symbol,
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

            add_log(
                f"📤 Proposal sent: {json.dumps(proposal_payload)}"
            )

            # Wait for proposal
            wait_start = time.time()
            proposal_received = False

            while time.time() - wait_start < 10:
                with state_lock:
                    proposal = state.get("last_proposal")
                    pending_req = state.get("pending_proposal_req_id")

                if proposal and proposal.get("id") and pending_req == proposal_req_id:
                    proposal_received = True
                    break

                time.sleep(0.2)

            if not proposal_received:
                with state_lock:
                    error_text = state.get(
                        "last_error",
                        "Proposal timeout"
                    )
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None
                    state["last_trade_time"] = time.time()

                add_log(f"❌ PROPOSAL FAILED: {error_text}")
                time.sleep(2)
                continue

            with state_lock:
                proposal_id = state["last_proposal"]["id"]
                ask_price = safe_float(
                    state["last_proposal"].get("ask_price")
                )

            if ask_price is None:
                add_log("❌ Proposal has no valid ask_price")

                with state_lock:
                    state["trade_state"] = "IDLE"
                    state["pending_proposal_req_id"] = None

                time.sleep(1)
                continue

            # ----------------------------------------------------
            # BUY
            # ----------------------------------------------------
            buy_req_id = get_req_id()

            with state_lock:
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

            add_log(
                f"📤 BUY sent: proposal={proposal_id} "
                f"price={ask_price}"
            )

            # Wait for buy
            wait_start = time.time()
            buy_success = False

            while time.time() - wait_start < 10:
                with state_lock:
                    ct = state.get("current_trade")
                    pending_req = state.get("pending_buy_req_id")

                if (
                    ct
                    and ct.get("contract_id")
                    and pending_req == buy_req_id
                ):
                    buy_success = True
                    break

                time.sleep(0.2)

            if not buy_success:
                with state_lock:
                    error_text = state.get(
                        "last_error",
                        "No contract opened"
                    )
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None
                    state["last_trade_time"] = time.time()

                add_log(f"❌ BUY FAILED: {error_text}")
                continue

            with state_lock:
                state["last_trade_time"] = time.time()
                contract_id = state["current_trade"]["contract_id"]

            # ----------------------------------------------------
            # SERVER-SIDE TP/SL
            # ----------------------------------------------------
            set_server_tp_sl(
                contract_id,
                tp_amount,
                sl_amount
            )

            save_state()
            time.sleep(0.5)

        except Exception as exc:
            add_log(f"❌ TRADING LOOP ERROR: {exc}")

            try:
                import traceback
                add_log(traceback.format_exc())
            except Exception:
                pass

            time.sleep(3)

    add_log("=== TRADING LOOP STOPPED ===")


# ============================================================
# WEBSOCKET OPEN
# ============================================================
def on_open(socket):
    global reconnect_count

    with reconnect_lock:
        reconnect_count = 0

    with state_lock:
        state["connected"] = True
        state["connection_authenticated"] = True
        state["authorized"] = False
        state["pending_buy"] = None
        state["pending_buy_req_id"] = None
        state["pending_proposal_req_id"] = None

    add_log("✅ WebSocket connected via OTP")

    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": get_req_id()
    })

    send_ws({
        "active_symbols": "full",
        "req_id": get_req_id()
    })

    time.sleep(0.5)
    recover_active_contract()


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================
def on_message(socket, message):
    try:
        data = json.loads(message)
    except Exception:
        return

    msg_type = data.get("msg_type")
    req_id = data.get("req_id")

    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------
    if msg_type == "error":
        error = data.get("error", {}) or {}
        code = error.get("code", "UNKNOWN")
        text = error.get("message", "Unknown error")

        with state_lock:
            state["last_error"] = f"{code}: {text}"

            if state.get("pending_proposal_req_id") == req_id:
                state["trade_state"] = "IDLE"
                state["pending_proposal_req_id"] = None

            if state.get("pending_buy_req_id") == req_id:
                state["trade_state"] = "IDLE"
                state["pending_buy"] = None
                state["pending_buy_req_id"] = None

        add_log(f"❌ API ERROR: {code} - {text}")

        if "multiplier" in text.lower():
            add_log("⚠️ Check multiplier availability/minimum for this symbol")

        return

    # --------------------------------------------------------
    # BALANCE
    # --------------------------------------------------------
    if msg_type == "balance":
        balance_obj = data.get("balance", {}) or {}
        balance = safe_float(balance_obj.get("balance"), 0.0)

        with state_lock:
            state["balance"] = balance
            state["authorized"] = True

        add_log(f"✅ Authorized | Balance: ${balance:.2f}")
        return

    # --------------------------------------------------------
    # ACTIVE SYMBOLS
    # --------------------------------------------------------
    if msg_type == "active_symbols":
        symbols = data.get("active_symbols", []) or []
        boom = []

        for item in symbols:
            symbol = normalize_symbol_name(item)
            if not symbol:
                continue

            if symbol_is_boom(symbol):
                boom.append({
                    "symbol": symbol,
                    "display_name": (
                        item.get("underlying_symbol_name")
                        or item.get("display_name")
                        or symbol
                    )
                })

        with state_lock:
            state["boom_symbols"] = boom

        add_log(f"✅ BOOM symbols found: {len(boom)}")
        return

    # --------------------------------------------------------
    # TICK
    # --------------------------------------------------------
    if msg_type == "tick":
        tick = data.get("tick", {}) or {}
        price = safe_float(tick.get("quote"))

        if price is None:
            return

        with state_lock:
            state["last_price"] = price
            state["ticks_buffer"].append(price)

            if len(state["ticks_buffer"]) > 30:
                state["ticks_buffer"] = state["ticks_buffer"][-30:]

            subscription = data.get("subscription")
            if subscription and state.get("tick_subscription_id") is None:
                state["tick_subscription_id"] = subscription.get("id")

        return

    # --------------------------------------------------------
    # CONTRACTS FOR
    # --------------------------------------------------------
    if msg_type == "contracts_for":
        result = data.get("contracts_for", {}) or {}
        available = result.get("available", []) or []

        contracts_dict = {}

        for contract in available:
            ctype = contract.get("contract_type")
            if ctype:
                contracts_dict[ctype] = contract

        with state_lock:
            state["available_contracts"] = contracts_dict
            state["contract_info"] = data
            state["recovery_pending"] = False
            state["recovering"] = False

        if "MULTDOWN" in contracts_dict:
            add_log("✅ MULTDOWN available")
        else:
            add_log(
                f"⚠️ MULTDOWN NOT available for {state['symbol']}"
            )

        return

    # --------------------------------------------------------
    # PORTFOLIO
    # --------------------------------------------------------
    if msg_type == "portfolio":
        portfolio = data.get("portfolio", {}) or {}
        contracts = portfolio.get("contracts", []) or []

        open_contract = None

        for contract in contracts:
            if not contract.get("is_sold", True):
                cid = contract.get("contract_id")
                if cid:
                    open_contract = cid
                    break

        if open_contract:
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
                    "current_spot": None,
                    "current_pl": None,
                    "tp_price": None,
                    "sl_price": None,
                    "buy_price": None,
                    "status": "OPEN",
                    "start_time": time.time(),
                    "is_recovered": True,
                    "limit_order_raw": None,
                }
                state["trade_state"] = "OPEN"
                state["recovering"] = False
                state["recovery_pending"] = False

            add_log(
                f"🔄 Recovered active contract: {open_contract}"
            )

            send_ws({
                "proposal_open_contract": 1,
                "contract_id": int(open_contract),
                "subscribe": 1,
                "req_id": get_req_id(),
            })

        else:
            with state_lock:
                state["current_trade"] = None
                state["trade_state"] = "IDLE"
                state["recovering"] = False
                state["recovery_pending"] = False

            add_log("✅ No active contract")
        return

    # --------------------------------------------------------
    # PROPOSAL
    # --------------------------------------------------------
    if msg_type == "proposal":
        proposal = data.get("proposal", {}) or {}
        proposal_id = proposal.get("id")

        with state_lock:
            pending_req = state.get("pending_proposal_req_id")

        if proposal_id and pending_req == req_id:
            with state_lock:
                state["last_proposal"] = {
                    "id": proposal_id,
                    "ask_price": safe_float(
                        proposal.get("ask_price")
                    ),
                }
                state["last_proposal_id"] = proposal_id
                state["trade_state"] = "PROPOSAL_OK"

            add_log(
                f"✅ Proposal OK: {proposal_id}"
            )
        return

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------
    if msg_type == "buy":
        buy = data.get("buy", {}) or {}

        if "error" in data:
            error = data.get("error", {}) or {}

            with state_lock:
                state["last_error"] = (
                    f"BUY ERROR: "
                    f"{error.get('message', 'Unknown')}"
                )

                if state.get("pending_buy_req_id") == req_id:
                    state["trade_state"] = "IDLE"
                    state["pending_buy"] = None
                    state["pending_buy_req_id"] = None

            add_log(
                f"❌ BUY ERROR: "
                f"{error.get('message', 'Unknown')}"
            )
            return

        contract_id = buy.get("contract_id")

        if not contract_id:
            return

        with state_lock:
            pending_req = state.get("pending_buy_req_id")

        if pending_req != req_id:
            add_log(
                f"⚠️ Ignoring BUY response req_id={req_id}"
            )
            return

        buy_price = safe_float(buy.get("buy_price"))

        with state_lock:
            state["current_trade"] = {
                "contract_id": contract_id,
                "symbol": state["symbol"],
                "stake": state["stake"],
                "multiplier": state["multiplier"],

                # MONEY amounts
                "take_profit": state["take_profit"],
                "stop_loss": state["stop_loss"],
                "tp_amount": state["take_profit"],
                "sl_amount": state["stop_loss"],

                # PRICE LEVELS
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

        add_log(
            f"✅ CONTRACT OPENED: {contract_id}"
        )

        # Subscribe to contract
        send_ws({
            "proposal_open_contract": 1,
            "contract_id": int(contract_id),
            "subscribe": 1,
            "req_id": get_req_id(),
        })

        return

    # --------------------------------------------------------
    # CONTRACT UPDATE - SERVER TP/SL CONFIRMATION
    # --------------------------------------------------------
    if msg_type == "contract_update":
        update = data.get("contract_update", {}) or {}

        tp_obj = update.get("take_profit")
        sl_obj = update.get("stop_loss")

        tp_price = None
        sl_price = None
        tp_amount = None
        sl_amount = None

        if isinstance(tp_obj, dict):
            tp_price = safe_float(tp_obj.get("value"))
            tp_amount = safe_float(
                tp_obj.get("order_amount")
            )

        if isinstance(sl_obj, dict):
            sl_price = safe_float(sl_obj.get("value"))
            sl_amount = safe_float(
                sl_obj.get("order_amount")
            )

        with state_lock:
            if state["current_trade"] is not None:

                if tp_price is not None:
                    state["current_trade"]["tp_price"] = tp_price

                if sl_price is not None:
                    state["current_trade"]["sl_price"] = sl_price

                if tp_amount is not None:
                    state["current_trade"]["tp_amount"] = tp_amount

                if sl_amount is not None:
                    state["current_trade"]["sl_amount"] = sl_amount

                state["current_trade"]["limit_order_raw"] = update

        if tp_price is not None:
            add_log(
                f"🟢 SERVER TP PRICE = {tp_price}"
            )

        if sl_price is not None:
            add_log(
                f"🔴 SERVER SL PRICE = {sl_price}"
            )

        save_state()
        return

    # --------------------------------------------------------
    # PROPOSAL OPEN CONTRACT
    # --------------------------------------------------------
    if msg_type == "proposal_open_contract":
        contract = data.get("proposal_open_contract", {}) or {}

        entry_price = safe_float(
            contract.get("entry_price")
        )
        current_spot = safe_float(
            contract.get("current_spot")
        )
        exit_spot = safe_float(
            contract.get("exit_spot")
        )
        profit = safe_float(
            contract.get("profit"),
            0.0
        )

        is_sold = bool(contract.get("is_sold", False))
        is_expired = bool(contract.get("is_expired", False))

        # ----------------------------------------------------
        # Read SERVER TP/SL price levels
        # ----------------------------------------------------
        limit_order = contract.get("limit_order", {}) or {}

        tp_obj = (
            limit_order.get("take_profit")
            if isinstance(limit_order, dict)
            else None
        )
        sl_obj = (
            limit_order.get("stop_loss")
            if isinstance(limit_order, dict)
            else None
        )

        tp_price = None
        sl_price = None
        tp_amount = None
        sl_amount = None

        if isinstance(tp_obj, dict):
            tp_price = safe_float(tp_obj.get("value"))
            tp_amount = safe_float(
                tp_obj.get("order_amount")
            )

        if isinstance(sl_obj, dict):
            sl_price = safe_float(sl_obj.get("value"))
            sl_amount = safe_float(
                sl_obj.get("order_amount")
            )

        # ----------------------------------------------------
        # LIVE CONTRACT
        # ----------------------------------------------------
        if not (is_sold or is_expired):

            with state_lock:
                current_trade = state.get("current_trade")

                if current_trade is not None:

                    if entry_price is not None:
                        current_trade["entry_price"] = entry_price

                    if current_spot is not None:
                        current_trade["current_spot"] = current_spot

                    if profit is not None:
                        current_trade["current_pl"] = profit

                    # Server-confirmed TP/SL
                    if tp_price is not None:
                        current_trade["tp_price"] = tp_price

                    if sl_price is not None:
                        current_trade["sl_price"] = sl_price

                    if tp_amount is not None:
                        current_trade["tp_amount"] = tp_amount

                    if sl_amount is not None:
                        current_trade["sl_amount"] = sl_amount

                    if limit_order:
                        current_trade["limit_order_raw"] = limit_order

                    # ------------------------------------------------
                    # Local estimate ONLY if server has not returned
                    # the actual price yet.
                    # ------------------------------------------------
                    if (
                        entry_price is not None
                        and current_trade.get("tp_price") is None
                    ):
                        est_tp, est_sl = (
                            calculate_multdown_price_levels(
                                entry_price,
                                current_trade.get("stake"),
                                current_trade.get("multiplier"),
                                current_trade.get("tp_amount"),
                                current_trade.get("sl_amount"),
                            )
                        )

                        if est_tp is not None:
                            current_trade["tp_price_estimate"] = est_tp

                        if est_sl is not None:
                            current_trade["sl_price_estimate"] = est_sl

            save_state()
            return

        # ----------------------------------------------------
        # CONTRACT CLOSED
        # ----------------------------------------------------
        with state_lock:
            final_profit = (
                profit if profit is not None else 0.0
            )

            state["profit"] += final_profit

            if final_profit < 0:
                state["daily_loss"] += abs(final_profit)

            if final_profit > 0:
                state["wins"] += 1
                state["loss_streak"] = 0
                result = "WIN"
            else:
                state["losses"] += 1
                state["loss_streak"] += 1
                result = "LOSS"

            trade = state.get("current_trade") or {}

            trade_record = {
                "time": time.strftime("%H:%M:%S"),
                "symbol": (
                    state["symbol_display"]
                    or state["symbol"]
                ),
                "stake": trade.get(
                    "stake",
                    state["stake"]
                ),
                "multiplier": trade.get(
                    "multiplier",
                    state["multiplier"]
                ),

                # Money amounts
                "tp_amount": trade.get(
                    "tp_amount",
                    state["take_profit"]
                ),
                "sl_amount": trade.get(
                    "sl_amount",
                    state["stop_loss"]
                ),

                # Actual price levels
                "entry_price": trade.get("entry_price"),
                "tp_price": trade.get("tp_price"),
                "sl_price": trade.get("sl_price"),
                "buy_price": trade.get("buy_price"),

                "exit_price": exit_spot,
                "result": result,
                "profit": final_profit,
            }

            state["history"].insert(
                0,
                trade_record
            )
            state["history"] = state["history"][:100]

            contract_sub_id = state.get(
                "contract_subscription_id"
            )

            state["current_trade"] = None
            state["trade_state"] = "IDLE"
            state["last_trade_time"] = time.time()
            state["pending_buy"] = None
            state["pending_buy_req_id"] = None
            state["contract_subscription_id"] = None

        add_log(
            f"{result}: {final_profit:+.2f} | "
            f"Loss streak: {state['loss_streak']}"
        )

        if exit_spot is not None:
            add_log(
                f"   Exit Price: {exit_spot}"
            )

        # Free contract stream
        if contract_sub_id:
            send_ws({
                "forget": contract_sub_id,
                "req_id": get_req_id()
            })

        save_state()
        return


# ============================================================
# WS ERROR / CLOSE
# ============================================================
def on_error(socket, error):
    with state_lock:
        state["connected"] = False
        state["connection_authenticated"] = False

    add_log(f"❌ WebSocket error: {error}")

    with state_lock:
        running = state["running"]

    if running:
        schedule_reconnect()


def on_close(socket, code, reason):
    with state_lock:
        state["connected"] = False
        state["connection_authenticated"] = False

    add_log(f"🔒 WebSocket closed: {code} {reason}")

    with state_lock:
        running = state["running"]

    if running:
        schedule_reconnect()


# ============================================================
# CONNECT THREAD
# ============================================================
def start_ws_connection(ws_url):
    global ws, ws_thread

    try:
        add_log("Opening WebSocket...")

        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws_thread = threading.Thread(
            target=lambda: ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            ),
            daemon=True
        )

        ws_thread.start()

    except Exception as exc:
        add_log(f"❌ WS THREAD ERROR: {exc}")


# ============================================================
# HTML
# ============================================================
HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NOVA BOOM BOT - MULTDOWN</title>

<style>
*{box-sizing:border-box}
body{
    margin:0;
    font-family:Arial,Helvetica,sans-serif;
    background:#05070d;
    color:#e8ecf8;
}
.container{
    max-width:1400px;
    margin:auto;
    padding:18px;
}
.header{
    display:flex;
    justify-content:space-between;
    align-items:center;
    gap:15px;
    margin-bottom:18px;
}
.title{
    font-size:24px;
    font-weight:800;
}
.subtitle{
    color:#8991ad;
    font-size:12px;
    margin-top:4px;
}
.status{
    padding:9px 13px;
    border-radius:20px;
    font-size:12px;
    font-weight:700;
}
.connected{
    color:#19c477;
    background:#09261b;
}
.disconnected{
    color:#ed4665;
    background:#2a0b13;
}
.panel{
    background:#0a0e18;
    border:1px solid #18213a;
    border-radius:14px;
    padding:16px;
    margin-bottom:14px;
}
.panel-title{
    font-size:14px;
    font-weight:800;
    margin-bottom:12px;
}
.grid{
    display:grid;
    grid-template-columns:repeat(4,1fr);
    gap:10px;
}
.field label{
    display:block;
    color:#8991ad;
    font-size:11px;
    margin-bottom:6px;
}
input,select{
    width:100%;
    background:#050811;
    color:#e8ecf8;
    border:1px solid #1a2745;
    border-radius:8px;
    padding:10px;
    outline:none;
}
input:focus,select:focus{
    border-color:#725cff;
}
button{
    border:0;
    cursor:pointer;
}
.btn{
    padding:10px 14px;
    border-radius:8px;
    color:white;
    font-weight:700;
}
.btn-purple{background:#694cff}
.btn-green{background:#159a61}
.btn-red{background:#c93656}
.btn-yellow{background:#a77a10}
.btn-blue{background:#245ec4}
.btn-group{
    display:flex;
    gap:8px;
    flex-wrap:wrap;
}
.warning{
    margin-top:10px;
    color:#e7bb59;
    background:#241d09;
    border:1px solid #5d4814;
    border-radius:8px;
    padding:10px;
    font-size:11px;
}
.stat{
    background:#050811;
    border:1px solid #17213a;
    border-radius:10px;
    padding:13px;
}
.stat-label{
    color:#8991ad;
    font-size:11px;
}
.stat-value{
    margin-top:6px;
    font-size:20px;
    font-weight:800;
}
.green{color:#19c477}
.red{color:#ed4665}
.purple{color:#9a84ff}
.market-grid{
    display:grid;
    grid-template-columns:1.5fr 1fr;
    gap:12px;
}
.chart-box{
    height:330px;
    background:#050811;
    border:1px solid #17213a;
    border-radius:10px;
    position:relative;
    overflow:hidden;
}
canvas{
    width:100%;
    height:100%;
}
.price-tag{
    position:absolute;
    right:14px;
    top:14px;
    background:#0c1220;
    border:1px solid #1b2846;
    padding:8px 11px;
    border-radius:7px;
    font-weight:800;
}
.info-box{
    background:#050811;
    border:1px solid #17213a;
    border-radius:9px;
    padding:11px;
    display:flex;
    justify-content:space-between;
    margin-bottom:8px;
}
.label{
    color:#8991ad;
}
.entry-display{
    display:grid;
    grid-template-columns:repeat(4,1fr);
    gap:8px;
    margin-top:10px;
}
.entry-item{
    background:#050811;
    border:1px solid #17213a;
    border-radius:9px;
    padding:10px;
}
.entry-item .label{
    font-size:10px;
}
.entry-item .value{
    font-size:15px;
    font-weight:800;
    margin-top:5px;
}
.entry{color:#dbe1f4}
.tp{color:#19c477}
.sl{color:#ed4665}
.pl{color:#8da7ff}
.strategy{
    margin-top:12px;
    padding:10px;
    background:#101426;
    border:1px solid #27325a;
    border-radius:9px;
    font-size:12px;
}
.table-wrap{
    overflow:auto;
}
table{
    width:100%;
    border-collapse:collapse;
    font-size:12px;
}
th,td{
    padding:9px;
    border-bottom:1px solid #17213a;
    text-align:left;
}
th{color:#8991ad}
.win{color:#19c477}
.loss{color:#ed4665}
.empty{
    color:#8991ad;
    text-align:center;
    padding:20px;
}
.logs{
    max-height:300px;
    overflow:auto;
    background:#050811;
    border:1px solid #17213a;
    border-radius:8px;
    padding:8px;
    font-family:monospace;
    font-size:11px;
}
.log-line{
    padding:4px 0;
    border-bottom:1px solid #0f1628;
}
.log-time{
    color:#65708f;
    margin-right:6px;
}
@media(max-width:900px){
    .grid,.entry-display{
        grid-template-columns:repeat(2,1fr);
    }
    .market-grid{
        grid-template-columns:1fr;
    }
}
@media(max-width:600px){
    .grid,.entry-display{
        grid-template-columns:1fr;
    }
    .header{
        align-items:flex-start;
        flex-direction:column;
    }
}
</style>
</head>

<body>
<div class="container">

<div class="header">
    <div>
        <div class="title">🤖 NOVA BOOM BOT</div>
        <div class="subtitle">
            MULTDOWN • 3 consecutive down ticks • Server-side TP/SL
        </div>
    </div>
    <div id="connectionStatus" class="status disconnected">
        ● DISCONNECTED
    </div>
</div>

<!-- CONNECTION -->
<div class="panel">
    <div class="panel-title">🔐 DERIV CONNECTION</div>

    <div class="grid">
        <div class="field">
            <label>App ID</label>
            <input id="appId" type="text" placeholder="1089">
        </div>

        <div class="field">
            <label>PAT Token</label>
            <input id="apiToken" type="password" placeholder="PAT Token">
        </div>

        <div class="field">
            <label>Account ID</label>
            <input id="accountId" type="text" placeholder="CR...">
        </div>

        <div class="field" style="display:flex;align-items:end">
            <button class="btn btn-purple" onclick="connectDeriv()">
                🔌 CONNECT
            </button>
        </div>
    </div>

    <div id="connectionMessage"
         style="margin-top:10px;color:#8991ad;font-size:12px">
        Enter credentials and connect.
    </div>

    <div class="warning">
        ⚠️ TP/SL amounts are money values. The actual TP/SL PRICE LEVEL
        is read from Deriv after contract_update.
    </div>
</div>

<!-- STATS -->
<div class="panel">
    <div class="grid" style="grid-template-columns:repeat(6,1fr)">
        <div class="stat">
            <div class="stat-label">Balance</div>
            <div id="balance" class="stat-value">$0.00</div>
        </div>
        <div class="stat">
            <div class="stat-label">Profit</div>
            <div id="profit" class="stat-value green">$0.00</div>
        </div>
        <div class="stat">
            <div class="stat-label">Win Rate</div>
            <div id="winRate" class="stat-value purple">0%</div>
        </div>
        <div class="stat">
            <div class="stat-label">Trades</div>
            <div id="totalTrades" class="stat-value">0</div>
        </div>
        <div class="stat">
            <div class="stat-label">W/L</div>
            <div id="winsLosses" class="stat-value">
                <span class="green">0</span>/<span class="red">0</span>
            </div>
        </div>
        <div class="stat">
            <div class="stat-label">Loss Streak</div>
            <div id="lossStreak" class="stat-value red">0</div>
        </div>
    </div>
</div>

<!-- MARKET -->
<div class="panel">
    <div class="panel-title">
        📊 LIVE MARKET
        <span id="selectedMarket"
              style="font-size:12px;color:#8991ad">
            No market
        </span>
    </div>

    <div class="market-grid">

        <div class="chart-box">
            <canvas id="chartCanvas"></canvas>
            <div id="chartPrice" class="price-tag">—</div>
        </div>

        <div>
            <div class="info-box">
                <span class="label">Symbol</span>
                <strong id="marketName">—</strong>
            </div>

            <div class="info-box">
                <span class="label">Price</span>
                <strong id="livePrice">—</strong>
            </div>

            <div class="info-box">
                <span class="label">Direction</span>
                <strong style="color:#ed4665">MULTDOWN</strong>
            </div>

            <div class="info-box">
                <span class="label">Bot</span>
                <strong id="botStatus">STOPPED</strong>
            </div>

            <div class="info-box">
                <span class="label">Trade State</span>
                <strong id="tradeState">IDLE</strong>
            </div>

            <div class="entry-display">
                <div class="entry-item">
                    <div class="label">⚪ Entry</div>
                    <div id="entryDisplay" class="value entry">—</div>
                </div>

                <div class="entry-item">
                    <div class="label">🟢 TP PRICE</div>
                    <div id="tpDisplay" class="value tp">—</div>
                </div>

                <div class="entry-item">
                    <div class="label">🔴 SL PRICE</div>
                    <div id="slDisplay" class="value sl">—</div>
                </div>

                <div class="entry-item">
                    <div class="label">📊 P/L</div>
                    <div id="plDisplay" class="value pl">—</div>
                </div>
            </div>
        </div>
    </div>
</div>

<!-- SETTINGS -->
<div class="panel">
    <div class="panel-title">⚙️ SETTINGS & STRATEGY</div>

    <div class="grid">
        <div class="field">
            <label>Stake ($)</label>
            <input id="stakeInput"
                   type="number"
                   value="1"
                   min="0.01"
                   step="0.01"
                   onchange="updateStake()">
        </div>

        <div class="field">
            <label>Multiplier</label>
            <input id="multiplierInput"
                   type="number"
                   value="300"
                   min="1"
                   max="2000"
                   onchange="updateMultiplier()">
        </div>

        <div class="field">
            <label>Take Profit ($)</label>
            <input id="tpInput"
                   type="number"
                   value="2"
                   min="0.01"
                   step="0.01"
                   onchange="updateTPSL()">
        </div>

        <div class="field">
            <label>Stop Loss ($)</label>
            <input id="slInput"
                   type="number"
                   value="0.10"
                   min="0.01"
                   step="0.01"
                   onchange="updateTPSL()">
        </div>

        <div class="field">
            <label>Interval (sec)</label>
            <input id="tradeInterval"
                   type="number"
                   value="5"
                   min="1"
                   max="60"
                   onchange="updateInterval()">
        </div>

        <div class="field">
            <label>Max Loss Streak</label>
            <input id="maxLossStreak"
                   type="number"
                   value="3"
                   min="1"
                   max="20"
                   onchange="updateRisk()">
        </div>
    </div>

    <div id="strategyInfo" class="strategy">
        🎯 3 consecutive down ticks → MULTDOWN
    </div>

    <div class="btn-group" style="margin-top:14px">
        <button class="btn btn-green" onclick="startBot()">▶ START</button>
        <button class="btn btn-yellow" onclick="pauseBot()">⏸ PAUSE</button>
        <button class="btn btn-red" onclick="stopBot()">⛔ STOP</button>
        <button class="btn btn-blue" onclick="resetStats()">🔄 RESET STATS</button>
    </div>
</div>

<!-- BOOM FINDER -->
<div class="panel">
    <div class="panel-title">🔴 BOOM FINDER</div>

    <div class="grid" style="grid-template-columns:1.5fr 1fr auto">
        <div class="field">
            <label>Symbol</label>
            <select id="symbol" onchange="selectSymbol()">
                <option value="">Find BOOM first</option>
            </select>
        </div>

        <div class="field">
            <label>Live Price</label>
            <div id="finderPrice"
                 style="padding:10px;background:#050811;
                        border-radius:8px;border:1px solid #17213a">
                —
            </div>
        </div>

        <div class="field"
             style="display:flex;align-items:end">
            <button class="btn btn-purple"
                    onclick="findBoom()">
                🔄 FIND
            </button>
        </div>
    </div>

    <div id="contractInfo"
         style="margin-top:10px;padding:10px;
                background:#050811;border-radius:8px;
                border:1px solid #17213a;
                font-size:12px;color:#8991ad">
        Select a symbol to see contracts.
    </div>
</div>

<!-- CURRENT TRADE -->
<div class="panel">
    <div class="panel-title">🎯 CURRENT TRADE</div>

    <div class="grid">
        <div class="field">
            <label>Symbol</label>
            <div id="tradeSymbol"
                 style="padding:10px;background:#050811;
                        border-radius:8px;border:1px solid #17213a">
                —
            </div>
        </div>

        <div class="field">
            <label>Stake</label>
            <div id="tradeStake"
                 style="padding:10px;background:#050811;
                        border-radius:8px;border:1px solid #17213a">
                —
            </div>
        </div>

        <div class="field">
            <label>Contract</label>
            <div id="contractId"
                 style="padding:10px;background:#050811;
                        border-radius:8px;border:1px solid #17213a">
                —
            </div>
        </div>

        <div class="field">
            <label>Status</label>
            <div id="tradeStatus"
                 style="padding:10px;background:#050811;
                        border-radius:8px;border:1px solid #17213a">
                WAITING
            </div>
        </div>
    </div>
</div>

<!-- HISTORY -->
<div class="panel">
    <div class="panel-title">
        📜 HISTORY
        <span id="historyCount"
              style="font-size:12px;color:#8991ad">
            0 trades
        </span>
    </div>

    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Symbol</th>
                    <th>Stake</th>
                    <th>Entry</th>
                    <th>TP</th>
                    <th>SL</th>
                    <th>Result</th>
                    <th>Profit</th>
                </tr>
            </thead>
            <tbody id="historyBody">
                <tr>
                    <td colspan="8" class="empty">No trades yet</td>
                </tr>
            </tbody>
        </table>
    </div>
</div>

<!-- LOGS -->
<div class="panel">
    <div class="panel-title">
        🖥️ SYSTEM LOG
        <button class="btn btn-blue"
                style="float:right;padding:6px 10px;font-size:11px"
                onclick="clearLogs()">
            CLEAR
        </button>
    </div>

    <div id="logs" class="logs">
        <div class="empty">Waiting...</div>
    </div>
</div>

</div>

<script>
var priceHistory = [];
var maxPoints = 80;

function $(id){
    return document.getElementById(id);
}

function money(v){
    var n = Number(v || 0);
    return "$" + n.toFixed(2);
}

function price(v){
    if(v === null || v === undefined || !Number.isFinite(Number(v)))
        return "—";

    var n = Number(v);

    if(n >= 1000) return n.toFixed(2);
    if(n >= 100) return n.toFixed(3);
    if(n >= 1) return n.toFixed(4);
    return n.toFixed(6);
}

function setMessage(text,type){
    var box = $("connectionMessage");
    box.textContent = text;

    if(type === "ok")
        box.style.color = "#19c477";
    else if(type === "error")
        box.style.color = "#ed4665";
    else
        box.style.color = "#8991ad";
}

async function api(url, options){
    var r = await fetch(url, options || {});
    return await r.json();
}

async function connectDeriv(){
    var data = {
        app_id: $("appId").value.trim(),
        token: $("apiToken").value.trim(),
        account_id: $("accountId").value.trim()
    };

    if(!data.token || !data.account_id){
        setMessage("❌ Token and Account ID required","error");
        return;
    }

    setMessage("⏳ Connecting...","");

    try{
        var res = await api("/api/connect",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify(data)
        });

        if(res.ok){
            setMessage("✅ Connection initiated","ok");
            setTimeout(updateDashboard,1000);
        }else{
            setMessage("❌ " + (res.error || "Connection failed"),"error");
        }
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function findBoom(){
    setMessage("⏳ Searching BOOM symbols...","");

    try{
        var res = await api("/api/markets",{
            method:"POST"
        });

        if(res.ok){
            setMessage(
                "✅ Found " + (res.count || 0) + " BOOM symbols",
                "ok"
            );
            updateDashboard();
        }else{
            setMessage("❌ " + res.error,"error");
        }
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function selectSymbol(){
    var symbol = $("symbol").value;
    if(!symbol) return;

    var opt = $("symbol").options[
        $("symbol").selectedIndex
    ];

    try{
        var res = await api("/api/select-symbol",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({
                symbol:symbol,
                display_name:opt.textContent
            })
        });

        if(res.ok){
            setMessage("✅ Selected " + symbol,"ok");
            updateDashboard();
        }else{
            setMessage("❌ " + res.error,"error");
        }
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function updateStake(){
    var stake = parseFloat($("stakeInput").value) || 1;

    try{
        await api("/api/update-stake",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({stake:stake})
        });
    }catch(e){}
}

async function updateMultiplier(){
    var multiplier =
        parseInt($("multiplierInput").value) || 300;

    try{
        var res = await api("/api/update-multiplier",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({multiplier:multiplier})
        });

        if(res.ok){
            updateDashboard();
        }else{
            setMessage("❌ " + res.error,"error");
        }
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function updateTPSL(){
    var tp = parseFloat($("tpInput").value) || 2;
    var sl = parseFloat($("slInput").value) || 0.10;

    try{
        var res = await api("/api/update-tpsl",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({
                take_profit:tp,
                stop_loss:sl
            })
        });

        if(!res.ok){
            setMessage("❌ " + res.error,"error");
        }else{
            updateDashboard();
        }
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function updateInterval(){
    var interval =
        parseInt($("tradeInterval").value) || 5;

    try{
        await api("/api/update-interval",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({interval:interval})
        });
    }catch(e){}
}

async function updateRisk(){
    var maxLoss =
        parseInt($("maxLossStreak").value) || 3;

    try{
        await api("/api/update-risk",{
            method:"POST",
            headers:{"Content-Type":"application/json"},
            body:JSON.stringify({
                max_loss_streak:maxLoss
            })
        });
    }catch(e){}
}

async function startBot(){
    try{
        var res = await api("/api/start",{
            method:"POST"
        });

        if(res.ok){
            setMessage("▶ Bot started","ok");
        }else{
            setMessage("❌ " + res.error,"error");
        }

        updateDashboard();
    }catch(e){
        setMessage("❌ " + e.message,"error");
    }
}

async function pauseBot(){
    try{
        var res = await api("/api/pause",{
            method:"POST"
        });

        if(res.ok){
            setMessage("⏸ Paused","");
        }

        updateDashboard();
    }catch(e){}
}

async function stopBot(){
    try{
        var res = await api("/api/stop",{
            method:"POST"
        });

        if(res.ok){
            setMessage("⛔ Stopped","");
        }

        updateDashboard();
    }catch(e){}
}

async function resetStats(){
    try{
        var res = await api("/api/reset-stats",{
            method:"POST"
        });

        if(res.ok){
            setMessage("🔄 Statistics reset","ok");
        }

        updateDashboard();
    }catch(e){}
}

async function clearLogs(){
    try{
        await api("/api/clear-logs",{
            method:"POST"
        });
        updateDashboard();
    }catch(e){}
}

function updateSymbols(symbols){
    var select = $("symbol");
    var current = select.value;

    select.innerHTML =
        '<option value="">-- Select BOOM --</option>';

    (symbols || []).forEach(function(s){
        var opt = document.createElement("option");
        opt.value = s.symbol;
        opt.textContent =
            s.display_name + " (" + s.symbol + ")";
        select.appendChild(opt);
    });

    if(current){
        select.value = current;
    }
}

function updateContractInfo(contracts){
    var box = $("contractInfo");

    if(!contracts ||
       Object.keys(contracts).length === 0){
        box.textContent = "No contract data available";
        return;
    }

    var html = "<b>Available contracts:</b><br>";
    var mult = false;

    for(var ctype in contracts){
        var info = contracts[ctype] || {};
        var expiry =
            info.expiry_type || "UNKNOWN";

        if(ctype.toUpperCase() === "MULTDOWN"){
            mult = true;
            html +=
                "• <span style='color:#19c477'>" +
                "✅ " + ctype +
                "</span> | " + expiry +
                "<br>";
        }else{
            html +=
                "• " + ctype +
                " | " + expiry + "<br>";
        }
    }

    html += "<br>";

    if(mult){
        html +=
            "<span style='color:#19c477'>" +
            "✅ MULTDOWN AVAILABLE</span>";
    }else{
        html +=
            "<span style='color:#ed4665'>" +
            "⚠️ MULTDOWN NOT AVAILABLE</span>";
    }

    box.innerHTML = html;
}

function updateHistory(history){
    var body = $("historyBody");

    $("historyCount").textContent =
        (history || []).length + " trades";

    if(!history || history.length === 0){
        body.innerHTML =
            '<tr><td colspan="8" class="empty">' +
            'No trades yet</td></tr>';
        return;
    }

    body.innerHTML = history.map(function(item){
        var p = Number(item.profit || 0);
        var result = item.result || "—";

        return "<tr>" +
            "<td>" + (item.time || "—") + "</td>" +
            "<td>" + (item.symbol || "—") + "</td>" +
            "<td>" + money(item.stake) + "</td>" +
            "<td>" + price(item.entry_price) + "</td>" +
            "<td>" + price(item.tp_price) + "</td>" +
            "<td>" + price(item.sl_price) + "</td>" +
            "<td class='" +
                (result === "WIN" ? "win" : "loss") +
                "'>" + result + "</td>" +
            "<td class='" +
                (p >= 0 ? "win" : "loss") +
                "'>" +
                (p >= 0 ? "+" : "") +
                money(p) +
            "</td>" +
            "</tr>";
    }).join("");
}

function updateLogs(logs){
    var box = $("logs");

    if(!logs || logs.length === 0){
        box.innerHTML =
            '<div class="empty">Waiting...</div>';
        return;
    }

    box.innerHTML = logs.map(function(log){
        return (
            '<div class="log-line">' +
            '<span class="log-time">[' +
            log.time +
            ']</span>' +
            log.message +
            '</div>'
        );
    }).join("");
}

function addPrice(p){
    if(!Number.isFinite(Number(p))) return;

    priceHistory.push(Number(p));

    if(priceHistory.length > maxPoints){
        priceHistory.shift();
    }

    drawChart();
}

function drawChart(){
    var canvas = $("chartCanvas");
    var ctx = canvas.getContext("2d");

    var rect = canvas.getBoundingClientRect();

    canvas.width =
        Math.max(300, Math.floor(rect.width * devicePixelRatio));

    canvas.height =
        Math.max(220, Math.floor(rect.height * devicePixelRatio));

    ctx.clearRect(0,0,canvas.width,canvas.height);

    if(priceHistory.length < 2)
        return;

    var pad = 25;
    var w = canvas.width;
    var h = canvas.height;

    var min =
        Math.min.apply(null,priceHistory);

    var max =
        Math.max.apply(null,priceHistory);

    if(max === min){
        max += 1;
        min -= 1;
    }

    ctx.beginPath();

    priceHistory.forEach(function(v,i){
        var x =
            pad +
            (i/(priceHistory.length-1)) *
            (w-pad*2);

        var y =
            h-pad -
            ((v-min)/(max-min)) *
            (h-pad*2);

        if(i === 0)
            ctx.moveTo(x,y);
        else
            ctx.lineTo(x,y);
    });

    ctx.strokeStyle = "#7c6cff";
    ctx.lineWidth = 2 * devicePixelRatio;
    ctx.stroke();
}

function updateDashboard(){
    fetch("/api/status",{cache:"no-store"})
    .then(function(r){ return r.json(); })
    .then(function(data){

        var status = $("connectionStatus");

        if(data.connected && data.authorized){
            status.textContent = "● CONNECTED";
            status.className =
                "status connected";
        }else{
            status.textContent = "● DISCONNECTED";
            status.className =
                "status disconnected";
        }

        $("balance").textContent =
            money(data.balance);

        $("profit").textContent =
            (data.profit >= 0 ? "+" : "") +
            money(data.profit);

        $("profit").className =
            "stat-value " +
            (data.profit >= 0 ? "green" : "red");

        $("winRate").textContent =
            Number(data.win_rate || 0).toFixed(0) + "%";

        $("totalTrades").textContent =
            data.total_trades || 0;

        $("winsLosses").innerHTML =
            '<span class="green">' +
            (data.wins || 0) +
            '</span>/<span class="red">' +
            (data.losses || 0) +
            '</span>';

        $("lossStreak").textContent =
            data.loss_streak || 0;

        $("botStatus").textContent =
            data.running ? "RUNNING" : "STOPPED";

        $("tradeState").textContent =
            data.trade_state || "IDLE";

        var symbol =
            data.symbol_display || data.symbol || "—";

        $("marketName").textContent = symbol;
        $("selectedMarket").textContent = symbol;
        $("tradeSymbol").textContent = symbol;

        if(data.last_price !== null &&
           data.last_price !== undefined){

            var p = Number(data.last_price);

            $("livePrice").textContent = price(p);
            $("finderPrice").textContent = price(p);
            $("chartPrice").textContent = price(p);

            addPrice(p);
        }

        $("stakeInput").value =
            data.stake;

        $("multiplierInput").value =
            data.multiplier;

        $("tpInput").value =
            data.take_profit;

        $("slInput").value =
            data.stop_loss;

        $("tradeInterval").value =
            data.trade_interval;

        $("maxLossStreak").value =
            data.max_loss_streak;

        $("strategyInfo").innerHTML =
            "🎯 3 consecutive down ticks → " +
            "<b>MULTDOWN " +
            data.multiplier +
            "x</b> | TP $" +
            Number(data.take_profit).toFixed(2) +
            " | SL $" +
            Number(data.stop_loss).toFixed(2);

        updateSymbols(data.boom_symbols);
        updateContractInfo(data.available_contracts);
        updateHistory(data.history);
        updateLogs(data.logs);

        var trade = data.current_trade;

        if(trade){
            $("tradeStake").textContent =
                money(trade.stake);

            $("contractId").textContent =
                trade.contract_id || "—";

            $("tradeStatus").textContent =
                trade.status || "OPEN";

            $("entryDisplay").textContent =
                price(trade.entry_price);

            $("tpDisplay").textContent =
                price(
                    trade.tp_price !== null &&
                    trade.tp_price !== undefined
                    ? trade.tp_price
                    : trade.tp_price_estimate
                );

            $("slDisplay").textContent =
                price(
                    trade.sl_price !== null &&
                    trade.sl_price !== undefined
                    ? trade.sl_price
                    : trade.sl_price_estimate
                );

            $("plDisplay").textContent =
                trade.current_pl !== null &&
                trade.current_pl !== undefined
                ? money(trade.current_pl)
                : "—";
        }else{
            $("tradeStake").textContent = "—";
            $("contractId").textContent = "—";
            $("tradeStatus").textContent = "WAITING";
            $("entryDisplay").textContent = "—";
            $("tpDisplay").textContent = "—";
            $("slDisplay").textContent = "—";
            $("plDisplay").textContent = "—";
        }
    })
    .catch(function(e){
        console.log(e);
    });
}

window.addEventListener("resize",drawChart);

setInterval(updateDashboard,1000);
updateDashboard();
</script>
</body>
</html>
"""


# ============================================================
# FLASK ROUTES
# ============================================================
@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/api/connect", methods=["POST"])
def api_connect():
    global CURRENT_APP_ID
    global CURRENT_PAT_TOKEN
    global CURRENT_ACCOUNT_ID
    global ws_should_run

    try:
        payload = request.get_json(silent=True) or {}

        app_id = (
            str(payload.get("app_id") or DEFAULT_APP_ID).strip()
        )
        token = (
            str(payload.get("token") or "").strip()
        )
        account_id = (
            str(payload.get("account_id") or "").strip()
        )

        if not token:
            return jsonify({
                "ok": False,
                "error": "PAT token is required"
            }), 400

        if not account_id:
            return jsonify({
                "ok": False,
                "error": "Account ID is required"
            }), 400

        # Validate account access before starting WS.
        get_accounts(app_id, token)

        CURRENT_APP_ID = app_id
        CURRENT_PAT_TOKEN = token
        CURRENT_ACCOUNT_ID = account_id
        ws_should_run = True

        ws_url = get_otp_url(
            CURRENT_APP_ID,
            CURRENT_PAT_TOKEN,
            CURRENT_ACCOUNT_ID
        )

        threading.Thread(
            target=start_ws_connection,
            args=(ws_url,),
            daemon=True
        ).start()

        return jsonify({
            "ok": True,
            "message": "WebSocket connection initiated"
        })

    except Exception as exc:
        add_log(f"❌ CONNECT ERROR: {exc}")

        return jsonify({
            "ok": False,
            "error": str(exc)
        }), 400


@app.route("/api/markets", methods=["POST"])
def api_markets():
    with state_lock:
        connected = state["connected"]

    if not connected:
        return jsonify({
            "ok": False,
            "error": "Connect to Deriv first"
        }), 400

    send_ws({
        "active_symbols": "full",
        "req_id": get_req_id()
    })

    time.sleep(0.5)

    with state_lock:
        symbols = list(state["boom_symbols"])

    return jsonify({
        "ok": True,
        "count": len(symbols),
        "symbols": symbols
    })


@app.route("/api/select-symbol", methods=["POST"])
def api_select_symbol():
    payload = request.get_json(silent=True) or {}

    symbol = str(
        payload.get("symbol") or ""
    ).strip()

    display_name = str(
        payload.get("display_name") or symbol
    ).strip()

    if not symbol:
        return jsonify({
            "ok": False,
            "error": "Symbol is required"
        }), 400

    with state_lock:
        old_symbol = state["symbol"]

    if old_symbol and old_symbol != symbol:
        unsubscribe_old_symbol()

    with state_lock:
        state["last_symbol"] = old_symbol
        state["symbol"] = symbol
        state["symbol_display"] = display_name
        state["ticks_buffer"] = []
        state["last_price"] = None
        state["last_signal_sequence"] = None
        state["available_contracts"] = {}
        state["contract_info"] = None
        state["tick_subscription_id"] = None

    if state["connected"]:
        send_ws({
            "ticks": symbol,
            "subscribe": 1,
            "req_id": get_req_id()
        })

        send_ws({
            "contracts_for": symbol,
            "req_id": get_req_id()
        })

    add_log(f"🎯 Selected symbol: {symbol}")

    return jsonify({
        "ok": True,
        "symbol": symbol
    })


@app.route("/api/update-stake", methods=["POST"])
def api_update_stake():
    payload = request.get_json(silent=True) or {}
    stake = safe_float(payload.get("stake"))

    if stake is None or stake <= 0:
        return jsonify({
            "ok": False,
            "error": "Invalid stake"
        }), 400

    with state_lock:
        state["stake"] = stake

    save_state()

    return jsonify({
        "ok": True,
        "stake": stake
    })


@app.route("/api/update-multiplier", methods=["POST"])
def api_update_multiplier():
    payload = request.get_json(silent=True) or {}
    multiplier = safe_int(payload.get("multiplier"))

    if multiplier is None or multiplier <= 0:
        return jsonify({
            "ok": False,
            "error": "Invalid multiplier"
        }), 400

    with state_lock:
        state["multiplier"] = multiplier

    save_state()

    return jsonify({
        "ok": True,
        "multiplier": multiplier
    })


@app.route("/api/update-tpsl", methods=["POST"])
def api_update_tpsl():
    payload = request.get_json(silent=True) or {}

    tp = safe_float(payload.get("take_profit"))
    sl = safe_float(payload.get("stop_loss"))

    if tp is None or tp <= 0:
        return jsonify({
            "ok": False,
            "error": "Invalid take profit"
        }), 400

    if sl is None or sl <= 0:
        return jsonify({
            "ok": False,
            "error": "Invalid stop loss"
        }), 400

    with state_lock:
        state["take_profit"] = tp
        state["stop_loss"] = sl

    save_state()

    return jsonify({
        "ok": True,
        "take_profit": tp,
        "stop_loss": sl
    })


@app.route("/api/update-interval", methods=["POST"])
def api_update_interval():
    payload = request.get_json(silent=True) or {}
    interval = safe_int(payload.get("interval"))

    if interval is None:
        interval = 5

    interval = max(1, min(60, interval))

    with state_lock:
        state["trade_interval"] = interval

    save_state()

    return jsonify({
        "ok": True,
        "interval": interval
    })


@app.route("/api/update-risk", methods=["POST"])
def api_update_risk():
    payload = request.get_json(silent=True) or {}
    max_loss = safe_int(
        payload.get("max_loss_streak")
    )

    if max_loss is None:
        max_loss = 3

    max_loss = max(1, min(20, max_loss))

    with state_lock:
        state["max_loss_streak"] = max_loss

    save_state()

    return jsonify({
        "ok": True,
        "max_loss_streak": max_loss
    })


@app.route("/api/start", methods=["POST"])
def api_start():
    global trading_thread

    with state_lock:
        if not state["authorized"]:
            return jsonify({
                "ok": False,
                "error": "Not authorized"
            }), 400

        if not state["symbol"]:
            return jsonify({
                "ok": False,
                "error": "Select a BOOM symbol first"
            }), 400

        if not is_multdown_available(state["symbol"]):
            return jsonify({
                "ok": False,
                "error": "MULTDOWN is not available for this symbol"
            }), 400

        state["running"] = True

    if trading_thread is None or not trading_thread.is_alive():
        trading_thread = threading.Thread(
            target=trading_loop,
            daemon=True
        )
        trading_thread.start()

    add_log("▶ BOT STARTED")

    return jsonify({
        "ok": True
    })


@app.route("/api/pause", methods=["POST"])
def api_pause():
    with state_lock:
        state["running"] = False

    add_log("⏸ BOT PAUSED")

    return jsonify({
        "ok": True
    })


@app.route("/api/stop", methods=["POST"])
def api_stop():
    with state_lock:
        state["running"] = False
        state["trade_state"] = (
            "OPEN"
            if state["current_trade"]
            else "IDLE"
        )

    add_log("⛔ BOT STOPPED")

    return jsonify({
        "ok": True
    })


@app.route("/api/reset-stats", methods=["POST"])
def api_reset_stats():
    with state_lock:
        state["total_trades"] = 0
        state["wins"] = 0
        state["losses"] = 0
        state["profit"] = 0.0
        state["loss_streak"] = 0
        state["daily_loss"] = 0.0
        state["trades_today"] = 0
        state["history"] = []
        state["session_start"] = time.time()
        state["daily_loss_reset_time"] = time.time()

    save_state()
    add_log("🔄 Statistics reset")

    return jsonify({
        "ok": True
    })


@app.route("/api/clear-logs", methods=["POST"])
def api_clear_logs():
    with state_lock:
        state["logs"] = []

    return jsonify({
        "ok": True
    })


@app.route("/api/status")
def api_status():
    with state_lock:
        total = state["total_trades"]
        wins = state["wins"]

        win_rate = (
            (wins / total) * 100
            if total > 0
            else 0.0
        )

        current_trade = state["current_trade"]

        # Copy nested object to avoid race while serializing.
        current_trade_copy = (
            dict(current_trade)
            if isinstance(current_trade, dict)
            else None
        )

        return jsonify({
            "connected": state["connected"],
            "connection_authenticated": (
                state["connection_authenticated"]
            ),
            "authorized": state["authorized"],

            "balance": state["balance"],

            "symbol": state["symbol"],
            "symbol_display": state["symbol_display"],
            "last_price": state["last_price"],

            "running": state["running"],

            "stake": state["stake"],
            "multiplier": state["multiplier"],
            "take_profit": state["take_profit"],
            "stop_loss": state["stop_loss"],

            "total_trades": total,
            "wins": wins,
            "losses": state["losses"],
            "profit": state["profit"],
            "win_rate": win_rate,
            "loss_streak": state["loss_streak"],

            "max_loss_streak": state["max_loss_streak"],
            "daily_loss": state["daily_loss"],
            "max_daily_loss": state["max_daily_loss"],
            "trades_today": state["trades_today"],
            "max_trades_per_day": (
                state["max_trades_per_day"]
            ),

            "trade_interval": state["trade_interval"],

            "current_trade": current_trade_copy,

            "boom_symbols": state["boom_symbols"],
            "available_contracts": (
                state["available_contracts"]
            ),

            "history": state["history"],
            "logs": state["logs"][:100],

            "last_error": state["last_error"],
            "trade_state": state["trade_state"],
        })


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    add_log("========================================")
    add_log("🚀 NOVA BOOM BOT - MULTDOWN")
    add_log("🎯 3 consecutive down ticks")
    add_log("🎯 Server-side TP/SL")
    add_log("========================================")

    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        debug=False,
        threaded=True,
        use_reloader=False
    )
