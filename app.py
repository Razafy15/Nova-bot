import os
import json
import threading
import time
import requests
import websocket
from flask import Flask, jsonify, request, render_template_string
from threading import RLock

app = Flask(__name__)

# ============================================================
# CREDENTIALS DEFAULT
# ============================================================
DEFAULT_APP_ID = "342OQxSDH634DTzSs0Ble"
DEFAULT_PAT_TOKEN = "pat_f9458c289641c9ab5e3da1eb03b46762ed297a5d6c6442905c718f3926a57d5e"
DEFAULT_ACCOUNT_ID = "DOT92654388"

CURRENT_APP_ID = DEFAULT_APP_ID
CURRENT_PAT_TOKEN = DEFAULT_PAT_TOKEN
CURRENT_ACCOUNT_ID = DEFAULT_ACCOUNT_ID

# ============================================================
# STATE
# ============================================================
state = {
    "connected": False,
    "authorized": False,
    "balance": 0.0,
    "symbol": "",
    "symbol_display": "",
    "last_price": None,
    "ticks_buffer": [],
    "running": False,
    "stake": 0.35,
    "duration": 1,
    "duration_unit": "m",
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
    "available_contracts": [],
    "history": [],
    "logs": [],
    "last_error": "",
    "last_proposal": None,
    "last_trade_time": 0,
    "trade_interval": 5,
    "trade_state": "IDLE",
    "contract_info": None,
    "last_proposal_id": None,
    "session_start": time.time(),
    "day_start": time.time(),
}

state_lock = RLock()

ws = None
ws_thread = None
reconnect_count = 0
MAX_RECONNECT = 10
RECONNECT_DELAY = 3
reconnecting = False

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
# WEBSOCKET SEND - Miaraka amin'ny fanamarinana connected
# ============================================================
def send_ws(payload):
    global ws
    
    # Fanamarinana: tsy maintsy misy ny ws sy connected
    with state_lock:
        if not state["connected"]:
            add_log("❌ WebSocket is not connected.")
            return False
    
    if ws is None:
        add_log("❌ WebSocket is not initialized.")
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
# STRATEGIE - 3 TICKS MIDINA IHANY
# ============================================================
def check_strategy():
    """
    Miverina True raha misy 3 ticks mifanesy midina.
    """
    with state_lock:
        buffer = state.get("ticks_buffer", [])
    
    # Tsy ampy ny ticks
    if len(buffer) < 3:
        return False
    
    # 3 ticks mifanesy midina: tick[-1] < tick[-2] < tick[-3]
    if buffer[-1] < buffer[-2] < buffer[-3]:
        return True
    
    return False

# ============================================================
# FANAMARIHANA NY DURATION AZO EKENA
# ============================================================
def find_valid_duration(symbol):
    """
    Mijery ny contracts_for mba hahitana izay duration azo ekena ho an'ny PUT.
    """
    with state_lock:
        contracts = state.get("available_contracts", [])
    
    if not contracts:
        add_log("⚠️ No contract info, using default 1m")
        return 1, "m"
    
    # Mitady ny PUT contract
    put_contracts = []
    for contract in contracts:
        if contract.get("contract_type", "").upper() == "PUT":
            put_contracts.append(contract)
    
    if not put_contracts:
        add_log("⚠️ No PUT contract found, using default 1m")
        return 1, "m"
    
    # Jereo ny expiry_type sy duration azo ekena
    for contract in put_contracts:
        expiry = contract.get("expiry_type", "").lower()
        min_duration = contract.get("min_duration", 0)
        max_duration = contract.get("max_duration", 0)
        
        add_log(f"📋 PUT contract: expiry={expiry}, min={min_duration}, max={max_duration}")
        
        # Mamaritra izay duration azo ekena
        if "intraday" in expiry:
            return 1, "m"
        elif "daily" in expiry:
            return 1, "d"
        elif "tick" in expiry:
            return 5, "t"
        elif "endless" in expiry:
            return 1, "m"
    
    return 1, "m"

# ============================================================
# FANARAHANA NY FEPETRA RISK
# ============================================================
def check_risk_limits():
    # Maka ny sanda ao anaty lock fohy
    with state_lock:
        loss_streak = state["loss_streak"]
        max_loss_streak = state["max_loss_streak"]
        daily_loss = state["daily_loss"]
        max_daily_loss = state["max_daily_loss"]
        trades_today = state["trades_today"]
        max_trades_per_day = state["max_trades_per_day"]
    
    # Manao ny fanaraha-maso
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
# RECONNECT AUTOMATIQUE (Miaraka amin'ny fanatsarana)
# ============================================================
def reconnect_websocket():
    global ws, ws_thread, reconnect_count, reconnecting
    
    # Raha efa misy reconnect
    if reconnecting:
        add_log("⏳ Reconnection already in progress...")
        return False
    
    reconnecting = True
    
    try:
        if reconnect_count > MAX_RECONNECT:
            add_log(f"❌ Max reconnection attempts ({MAX_RECONNECT}) reached")
            return False
        
        reconnect_count += 1
        add_log(f"🔄 Attempting to reconnect ({reconnect_count}/{MAX_RECONNECT})...")
        
        # Manidy ny WebSocket taloha
        if ws:
            try:
                ws.close()
            except:
                pass
            time.sleep(1)
        
        # Mampiasa ny credentials amin'izao fotoana izao
        ws_url = get_otp_url(CURRENT_APP_ID, CURRENT_PAT_TOKEN, CURRENT_ACCOUNT_ID)
        
        # Mamorona WebSocket vaovao
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        
        # Manomboka ny thread
        ws_thread = threading.Thread(
            target=lambda: ws.run_forever(ping_interval=20, ping_timeout=10),
            daemon=True
        )
        ws_thread.start()
        
        add_log("✅ Reconnection initiated")
        return True
        
    except Exception as e:
        add_log(f"❌ Reconnection failed: {e}")
        return False
    finally:
        reconnecting = False

# ============================================================
# TRADING LOOP
# ============================================================
def trading_loop():
    add_log("=== TRADING LOOP STARTED ===")
    loop_count = 0

    while True:
        # Mijery raha mbola mandeha ny bot
        with state_lock:
            if not state["running"]:
                break
        
        loop_count += 1
        try:
            if loop_count % 10 == 0:
                add_log(f"🔁 Trading loop alive (cycle {loop_count})")

            # Maka ireo sanda ilaina
            with state_lock:
                authorized = state["authorized"]
                symbol = state["symbol"]
                current_trade = state["current_trade"]
                trade_state = state["trade_state"]
                last_trade_time = state["last_trade_time"]
                trade_interval = state["trade_interval"]
                stake = state["stake"]
                duration = state["duration"]
                duration_unit = state["duration_unit"]
            
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

            # Jereo ny risk limits
            if not check_risk_limits():
                with state_lock:
                    state["running"] = False
                add_log("⛔ Risk limits reached, bot stopped")
                break

            # Miandry ny fotoana
            if time.time() - last_trade_time < trade_interval:
                time.sleep(0.5)
                continue

            # ==================================================
            # STRATEGIE - 3 TICKS MIDINA IHANY
            # ==================================================
            if not check_strategy():
                time.sleep(0.5)
                continue

            # Mahazo ny duration azo ekena
            if duration_unit == "auto":
                duration, unit = find_valid_duration(symbol)
            else:
                duration = duration
                unit = duration_unit

            add_log(f"📤 STRATEGY TRIGGERED: PUT | {symbol} | ${stake} | {duration}{unit}")

            # Manova ny state
            with state_lock:
                state["last_proposal"] = None
                state["last_proposal_id"] = None
                state["trade_state"] = "PROPOSAL_PENDING"
                state["last_error"] = ""

            # Mandefa PROPOSAL
            proposal_payload = {
                "proposal": 1,
                "amount": stake,
                "basis": "stake",
                "contract_type": "PUT",
                "currency": "USD",
                "duration": duration,
                "duration_unit": unit,
                "underlying_symbol": symbol,
                "subscribe": 1,
                "req_id": 700 + loop_count,
            }

            send_ws(proposal_payload)
            add_log(f"📤 Proposal sent: PUT {duration}{unit}")
            
            # Miandry valiny 10 segondra
            wait_start = time.time()
            proposal_received = False
            while time.time() - wait_start < 10:
                with state_lock:
                    if state.get("last_proposal") and state["last_proposal"].get("id"):
                        proposal_received = True
                        state["trade_state"] = "PROPOSAL_OK"
                        add_log(f"✅ Proposal OK: {state['last_proposal']['id']}")
                        break
                time.sleep(0.5)

            if not proposal_received:
                with state_lock:
                    add_log(f"❌ PROPOSAL FAILED: {state.get('last_error', 'Timeout')}")
                    state["trade_state"] = "IDLE"
                    state["last_trade_time"] = time.time()
                time.sleep(2)
                continue

            # Mahazo ny proposal ID sy price
            with state_lock:
                proposal_id = state["last_proposal"]["id"]
                ask_price = state["last_proposal"]["ask_price"]

            if ask_price is None:
                add_log("❌ No price in proposal")
                with state_lock:
                    state["trade_state"] = "IDLE"
                continue

            try:
                ask_price = float(ask_price)
            except:
                add_log(f"❌ Invalid price: {ask_price}")
                with state_lock:
                    state["trade_state"] = "IDLE"
                continue

            # Mandefa BUY
            buy_payload = {
                "buy": proposal_id,
                "price": ask_price,
                "req_id": 701 + loop_count,
            }

            with state_lock:
                state["trade_state"] = "BUY_PENDING"
                state["last_proposal_id"] = proposal_id
            add_log(f"📤 BUY sent for proposal {proposal_id}")
            send_ws(buy_payload)

            # Miandry ny valin'ny BUY 10 segondra
            wait_start = time.time()
            buy_success = False
            while time.time() - wait_start < 10:
                with state_lock:
                    if state.get("current_trade") and state["current_trade"].get("contract_id"):
                        buy_success = True
                        state["trade_state"] = "OPEN"
                        add_log(f"✅ CONTRACT OPENED: {state['current_trade']['contract_id']}")
                        break
                time.sleep(0.5)

            with state_lock:
                if not buy_success:
                    add_log(f"❌ BUY FAILED: {state.get('last_error', 'No contract opened')}")
                    state["trade_state"] = "IDLE"
                    state["last_trade_time"] = time.time()
                    continue

                state["last_trade_time"] = time.time()

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
    global reconnect_count, reconnecting
    
    with state_lock:
        state["connected"] = True
    reconnect_count = 0
    reconnecting = False
    add_log("✅ WebSocket connected")
    send_ws({"balance": 1, "subscribe": 1, "req_id": 100})
    send_ws({"active_symbols": "full", "req_id": 101})

def on_message(socket, message):
    try:
        data = json.loads(message)
    except:
        return

    msg_type = data.get("msg_type")

    # ============================================================
    # ERROR
    # ============================================================
    if msg_type == "error":
        error = data.get("error", {})
        code = error.get("code", "UNKNOWN")
        message_text = error.get("message", "Unknown error")
        
        with state_lock:
            state["last_error"] = f"{code}: {message_text}"
        
        add_log(f"❌ ERROR: {code} - {message_text}")
        
        if "Duration" in message_text or "duration" in message_text.lower():
            add_log("⚠️ DURATION ERROR: Try changing duration_unit")
            with state_lock:
                state["trade_state"] = "IDLE"
        return

    # ============================================================
    # BALANCE
    # ============================================================
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

    # ============================================================
    # ACTIVE SYMBOLS
    # ============================================================
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

    # ============================================================
    # TICK
    # ============================================================
    if msg_type == "tick":
        tick = data.get("tick", {})
        try:
            price = float(tick.get("quote"))
            with state_lock:
                state["last_price"] = price
                state["ticks_buffer"].append(price)
                if len(state["ticks_buffer"]) > 20:
                    state["ticks_buffer"].pop(0)
        except:
            pass
        return

    # ============================================================
    # CONTRACTS FOR
    # ============================================================
    if msg_type == "contracts_for":
        result = data.get("contracts_for", {})
        with state_lock:
            state["available_contracts"] = result.get("available", [])
            state["contract_info"] = data
        add_log(f"✅ Contract info received for {state['symbol']}")
        
        for contract in state["available_contracts"]:
            ctype = contract.get("contract_type", "UNKNOWN")
            expiry = contract.get("expiry_type", "UNKNOWN")
            add_log(f"  - {ctype} | {expiry}")
        return

    # ============================================================
    # PROPOSAL
    # ============================================================
    if msg_type == "proposal":
        proposal = data.get("proposal", {})
        if proposal.get("id"):
            with state_lock:
                state["last_proposal"] = {
                    "id": proposal["id"],
                    "ask_price": proposal.get("ask_price")
                }
                if state["trade_state"] == "PROPOSAL_PENDING":
                    state["trade_state"] = "PROPOSAL_OK"
            add_log(f"✅ Proposal OK: {proposal['id']}")
        else:
            add_log(f"❌ Proposal FAILED: {json.dumps(data)}")
            with state_lock:
                state["last_proposal"] = None
                state["trade_state"] = "IDLE"
                if "error" in data:
                    state["last_error"] = data["error"].get("message", "Unknown")
        return

    # ============================================================
    # BUY - Miaraka amin'ny fanamarinana error
    # ============================================================
    if msg_type == "buy":
        buy = data.get("buy", {})
        contract_id = buy.get("contract_id")
        
        # Jereo raha misy error
        if "error" in data:
            error = data.get("error", {})
            with state_lock:
                state["last_error"] = f"BUY ERROR: {error.get('message', 'Unknown')}"
                state["trade_state"] = "IDLE"
            add_log(f"❌ BUY ERROR: {error.get('message', 'Unknown')}")
            return
        
        if contract_id:
            with state_lock:
                state["current_trade"] = {
                    "contract_id": contract_id,
                    "symbol": state["symbol"],
                    "stake": state["stake"],
                    "status": "OPEN",
                    "start_time": time.time(),
                }
                state["total_trades"] += 1
                state["trades_today"] += 1
                state["trade_state"] = "OPEN"
            add_log(f"✅ CONTRACT OPENED: {contract_id}")
            
            # SUBSCRIBE amin'ny proposal_open_contract
            send_ws({
                "proposal_open_contract": 1,
                "contract_id": contract_id,
                "subscribe": 1,
                "req_id": 300,
            })
        else:
            add_log(f"❌ BUY FAILED: {json.dumps(data)}")
            with state_lock:
                state["trade_state"] = "IDLE"
                if "error" in data:
                    state["last_error"] = data["error"].get("message", "Unknown")
        return

    # ============================================================
    # OPEN CONTRACT - FANARAHANA NY VOKATRA
    # ============================================================
    if msg_type == "proposal_open_contract":
        contract = data.get("proposal_open_contract", {})
        
        # Raha mbola tsy tapitra
        if not (contract.get("is_sold") or contract.get("is_expired")):
            return

        # Tapitra ny contract
        try:
            profit = float(contract.get("profit", 0))
        except:
            profit = 0.0

        with state_lock:
            state["profit"] += profit
            state["daily_loss"] += profit if profit < 0 else 0
            
            if profit > 0:
                state["wins"] += 1
                state["loss_streak"] = 0
                result = "WIN"
            else:
                state["losses"] += 1
                state["loss_streak"] += 1
                result = "LOSS"

            state["history"].insert(0, {
                "time": time.strftime("%H:%M:%S"),
                "symbol": state["symbol_display"] or state["symbol"],
                "stake": state["stake"],
                "result": result,
                "profit": profit,
            })
            state["history"] = state["history"][:100]
            state["current_trade"] = None
            state["trade_state"] = "IDLE"
            state["last_trade_time"] = time.time()
        
        add_log(f"{result}: {profit:+.2f} | Loss streak: {state['loss_streak']}")
        return

# ============================================================
# WEBSOCKET ERROR & CLOSE
# ============================================================
def on_error(socket, error):
    with state_lock:
        state["connected"] = False
    add_log(f"❌ WebSocket error: {error}")
    with state_lock:
        running = state["running"]
    if running:
        time.sleep(RECONNECT_DELAY)
        reconnect_websocket()

def on_close(socket, code, reason):
    with state_lock:
        state["connected"] = False
        state["authorized"] = False
    add_log(f"🔒 WebSocket closed: {code} {reason}")
    with state_lock:
        running = state["running"]
    if running:
        time.sleep(RECONNECT_DELAY)
        reconnect_websocket()

# ============================================================
# WEBSOCKET THREAD - IRAY IHANY
# ============================================================
def websocket_thread(ws_url):
    global ws
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
        add_log(f"❌ WS THREAD ERROR: {exc}")
        with state_lock:
            running = state["running"]
        if running:
            time.sleep(RECONNECT_DELAY)
            reconnect_websocket()

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
        <title>NOVA BOOM BOT</title>
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
                        <p>Deriv Trading Dashboard - PUT Strategy (3 Ticks Down)</p>
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
                    <input id="appId" type="text" value="{{ APP_ID }}">
                </div>
                <div class="field">
                    <label>PAT Token</label>
                    <input id="apiToken" type="password" value="{{ PAT_TOKEN }}">
                </div>
                <div class="field">
                    <label>Account ID</label>
                    <input id="accountId" type="text" value="{{ ACCOUNT_ID }}">
                </div>
                <div class="field" style="display:flex;align-items:end;">
                    <button class="btn btn-purple" onclick="connectDeriv()">🔌 CONNECT</button>
                </div>
            </div>
            <div id="connectionMessage" style="margin-top:10px;color:#8991ad;font-size:12px;">Enter credentials and connect</div>
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
                <div style="background:#05080f;border-radius:10px;padding:20px;min-height:200px;border:1px solid #1a2340;position:relative;">
                    <canvas id="chartCanvas" style="width:100%;height:100%;"></canvas>
                    <div id="chartPrice" style="position:absolute;right:20px;top:20px;background:#0a0e1a;padding:8px 12px;border-radius:6px;font-size:14px;font-weight:bold;">—</div>
                </div>
                <div style="display:flex;flex-direction:column;gap:10px;">
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Symbol</span><strong id="marketName">—</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Price</span><strong id="livePrice">—</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Direction</span><strong style="color:#ed4665;">PUT</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Status</span><strong id="botStatus">STOPPED</strong></div>
                    <div style="background:#0a0e1a;padding:12px;border-radius:8px;border:1px solid #1a2340;display:flex;justify-content:space-between;"><span style="color:#8991ad;">Trade State</span><strong id="tradeState">IDLE</strong></div>
                </div>
            </div>
        </div>

        <div class="panel">
            <div class="panel-title">⚙️ SETTINGS & STRATEGY</div>
            <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
                <div class="field"><label>Stake</label><input id="stakeInput" type="number" value="0.35" step="0.01" min="0.01" onchange="updateStake()"></div>
                <div class="field"><label>Interval (sec)</label><input id="tradeInterval" type="number" value="5" min="1" max="60" onchange="updateInterval()"></div>
                <div class="field">
                    <label>Duration</label>
                    <div style="display:flex;gap:5px;">
                        <input id="durationInput" type="number" value="1" min="1" max="60" style="width:60%;" onchange="updateDuration()">
                        <select id="durationUnitInput" onchange="updateDuration()" style="width:40%;">
                            <option value="auto">auto</option>
                            <option value="s">s</option>
                            <option value="m" selected>m</option>
                            <option value="h">h</option>
                            <option value="d">d</option>
                            <option value="t">t</option>
                        </select>
                    </div>
                </div>
                <div class="field">
                    <label>Max Loss Streak</label>
                    <input id="maxLossStreak" type="number" value="3" min="1" max="10" onchange="updateRisk()">
                </div>
            </div>
            <div class="strategy-info">
                🎯 <b>STRATEGY:</b> Rehefa misy <b>3 ticks mifanesy midina</b> → <b>PUT</b>
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
            var stake = parseFloat($("stakeInput").value) || 0.35;
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

        async function updateDuration() {
            var duration = parseInt($("durationInput").value) || 1;
            var unit = $("durationUnitInput").value;
            try {
                var r = await fetch("/api/update-duration", {
                    method: "POST",
                    headers: {"Content-Type":"application/json"},
                    body: JSON.stringify({ duration: duration, unit: unit })
                });
                var res = await r.json();
                if (res.ok) setMessage("✅ Duration: " + duration + unit, "ok");
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
                await fetch("/api/pause", { method: "POST" });
                setMessage("⏸ Paused", "");
                updateDashboard();
            } catch(e) {}
        }

        async function stopBot() {
            try {
                await fetch("/api/stop", { method: "POST" });
                setMessage("⛔ Stopped", "");
                updateDashboard();
            } catch(e) {}
        }

        async function resetStats() {
            try {
                await fetch("/api/reset-stats", { method: "POST" });
                setMessage("🔄 Stats reset", "ok");
                updateDashboard();
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
            if (!contracts || contracts.length === 0) {
                box.innerHTML = "No contract data available";
                return;
            }
            var html = "<b>Available contracts:</b><br>";
            contracts.forEach(function(c) {
                html += "• " + (c.contract_type || "UNKNOWN") + " | " + (c.expiry_type || "") + "<br>";
            });
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

                updateSymbols(data.boom_symbols);
                updateContractInfo(data.available_contracts);
                updateHistory(data.history);
                updateLogs(data.logs);

                if (data.current_trade) {
                    $("tradeStake").textContent = money(data.current_trade.stake);
                    $("contractId").textContent = data.current_trade.contract_id || "—";
                    $("tradeStatus").textContent = data.current_trade.status || "OPEN";
                } else {
                    $("tradeStake").textContent = "—";
                    $("contractId").textContent = "—";
                    $("tradeStatus").textContent = "WAITING";
                }

                if (data.last_error) setMessage(data.last_error, "error");
                drawChart();
            } catch(e) {
                console.log("Dashboard error:", e);
            }
        }

        function addPrice(price) {
            priceHistory.push(Number(price));
            if (priceHistory.length > maxPoints) priceHistory.shift();
        }

        function drawChart() {
            var canvas = $("chartCanvas");
            var rect = canvas.parentElement.getBoundingClientRect();
            var dpr = window.devicePixelRatio || 1;
            canvas.width = rect.width * dpr;
            canvas.height = rect.height * dpr;
            var ctx = canvas.getContext("2d");
            ctx.scale(dpr, dpr);
            var width = rect.width;
            var height = rect.height;
            ctx.clearRect(0, 0, width, height);

            if (priceHistory.length < 2) {
                ctx.fillStyle = "#8991ad";
                ctx.font = "12px Arial";
                ctx.fillText("Waiting for price...", 20, 30);
                return;
            }

            var min = Math.min.apply(null, priceHistory);
            var max = Math.max.apply(null, priceHistory);
            if (min === max) { min -= 1; max += 1; }
            var padding = 20;
            var range = max - min;

            ctx.beginPath();
            priceHistory.forEach(function(price, index) {
                var x = padding + (index / (priceHistory.length - 1)) * (width - padding * 2);
                var y = height - padding - ((price - min) / range) * (height - padding * 2);
                if (index === 0) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
            });
            ctx.strokeStyle = "#7654ff";
            ctx.lineWidth = 2;
            ctx.stroke();

            var last = priceHistory[priceHistory.length - 1];
            var lastX = padding + ((priceHistory.length - 1) / (priceHistory.length - 1)) * (width - padding * 2);
            var lastY = height - padding - ((last - min) / range) * (height - padding * 2);
            ctx.beginPath();
            ctx.arc(lastX, lastY, 4, 0, Math.PI * 2);
            ctx.fillStyle = "#19c477";
            ctx.fill();
        }

        function clearLogs() {
            $("logs").innerHTML = '<div class="empty">Cleared</div>';
        }

        setInterval(updateDashboard, 2000);
        window.addEventListener("resize", drawChart);
        updateDashboard();
    </script>
    </body>
    </html>
    """
    return render_template_string(html, APP_ID=CURRENT_APP_ID, PAT_TOKEN=CURRENT_PAT_TOKEN, ACCOUNT_ID=CURRENT_ACCOUNT_ID)

# ============================================================
# API ROUTES
# ============================================================
@app.post("/api/connect")
def api_connect():
    global ws_thread, CURRENT_APP_ID, CURRENT_PAT_TOKEN, CURRENT_ACCOUNT_ID, reconnect_count
    
    data = request.get_json(silent=True) or {}
    app_id = str(data.get("app_id", "")).strip()
    token = str(data.get("token", "")).strip()
    account_id = str(data.get("account_id", "")).strip()

    if not app_id or not token or not account_id:
        return jsonify({"ok": False, "error": "Missing credentials"}), 400

    try:
        add_log("========== CONNECT ==========")
        
        CURRENT_APP_ID = app_id
        CURRENT_PAT_TOKEN = token
        CURRENT_ACCOUNT_ID = account_id
        reconnect_count = 0
        
        accounts = get_accounts(app_id, token)
        selected = None
        for acc in accounts:
            if str(acc.get("account_id", "")).strip() == account_id:
                selected = acc
                break
        if selected is None:
            raise RuntimeError("Account ID not found")

        ws_url = get_otp_url(app_id, token, account_id)

        if ws_thread and ws_thread.is_alive():
            if ws:
                try:
                    ws.close()
                except:
                    pass
            time.sleep(1)

        ws_thread = threading.Thread(
            target=websocket_thread,
            args=(ws_url,),
            daemon=True
        )
        ws_thread.start()

        add_log("✅ Connection started")
        return jsonify({"ok": True})

    except Exception as e:
        add_log(f"❌ CONNECT ERROR: {e}")
        return jsonify({"ok": False, "error": str(e)}), 400

@app.post("/api/markets")
def api_markets():
    if not state["connected"]:
        return jsonify({"ok": False, "error": "Not connected"}), 400
    send_ws({"active_symbols": "full", "req_id": 500})
    return jsonify({"ok": True})

@app.post("/api/select-symbol")
def api_select_symbol():
    data = request.get_json(silent=True) or {}
    symbol = data.get("symbol", "").strip()
    display_name = data.get("display_name", symbol)

    if not symbol:
        return jsonify({"ok": False, "error": "Symbol required"}), 400
    if not symbol.upper().startswith("BOOM"):
        return jsonify({"ok": False, "error": "Not a BOOM symbol"}), 400

    with state_lock:
        state["symbol"] = symbol
        state["symbol_display"] = display_name
        state["available_contracts"] = []
        state["contract_info"] = None
        state["trade_state"] = "IDLE"
        state["ticks_buffer"] = []
        state["last_price"] = None

    send_ws({"ticks": symbol, "subscribe": 1, "req_id": 600})
    send_ws({"contracts_for": symbol, "req_id": 601})

    add_log(f"✅ Selected: {symbol} ({display_name})")
    return jsonify({"ok": True})

@app.post("/api/start")
def api_start():
    with state_lock:
        if not state["authorized"]:
            return jsonify({"ok": False, "error": "Not authorized"}), 400
        if not state["symbol"]:
            return jsonify({"ok": False, "error": "No symbol selected"}), 400

        state["running"] = True
        state["last_trade_time"] = 0
        state["trade_state"] = "IDLE"
        state["day_start"] = time.time()
        state["daily_loss"] = 0.0
        state["trades_today"] = 0

    if not hasattr(app, "trading_thread") or not app.trading_thread.is_alive():
        app.trading_thread = threading.Thread(target=trading_loop, daemon=True)
        app.trading_thread.start()
        add_log("✅ Trading thread started")

    add_log("✅ BOT STARTED")
    return jsonify({"ok": True})

@app.post("/api/pause")
def api_pause():
    with state_lock:
        state["running"] = False
        state["trade_state"] = "IDLE"
    add_log("⏸ BOT PAUSED")
    return jsonify({"ok": True})

@app.post("/api/stop")
def api_stop():
    with state_lock:
        state["running"] = False
        state["trade_state"] = "IDLE"
    add_log("⛔ BOT STOPPED")
    return jsonify({"ok": True})

@app.post("/api/update-stake")
def api_update_stake():
    data = request.get_json(silent=True) or {}
    stake = float(data.get("stake", 0.35))
    if stake < 0.01:
        return jsonify({"ok": False, "error": "Min stake 0.01"}), 400
    with state_lock:
        state["stake"] = stake
    add_log(f"Stake: ${stake}")
    return jsonify({"ok": True})

@app.post("/api/update-interval")
def api_update_interval():
    data = request.get_json(silent=True) or {}
    interval = int(data.get("interval", 5))
    if interval < 1:
        return jsonify({"ok": False, "error": "Min 1 second"}), 400
    with state_lock:
        state["trade_interval"] = interval
    add_log(f"Interval: {interval}s")
    return jsonify({"ok": True})

@app.post("/api/update-duration")
def api_update_duration():
    data = request.get_json(silent=True) or {}
    duration = int(data.get("duration", 1))
    unit = str(data.get("unit", "auto")).strip()

    if duration < 1:
        return jsonify({"ok": False, "error": "Min 1"}), 400
    if unit not in ["auto", "s", "m", "h", "d", "t"]:
        return jsonify({"ok": False, "error": "Invalid unit"}), 400

    with state_lock:
        state["duration"] = duration
        state["duration_unit"] = unit
    add_log(f"Duration: {duration}{unit}")
    return jsonify({"ok": True})

@app.post("/api/update-risk")
def api_update_risk():
    data = request.get_json(silent=True) or {}
    max_loss = int(data.get("max_loss_streak", 3))
    if max_loss < 1:
        return jsonify({"ok": False, "error": "Min 1"}), 400
    with state_lock:
        state["max_loss_streak"] = max_loss
    add_log(f"Max loss streak: {max_loss}")
    return jsonify({"ok": True})

@app.post("/api/reset-stats")
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
        state["current_trade"] = None
        state["trade_state"] = "IDLE"
    add_log("🔄 Stats reset")
    return jsonify({"ok": True})

@app.get("/api/status")
def api_status():
    with state_lock:
        total = state["total_trades"]
        win_rate = (state["wins"] / total * 100) if total else 0.0
        return jsonify({
            "connected": state["connected"],
            "authorized": state["authorized"],
            "balance": state["balance"],
            "symbol": state["symbol"],
            "symbol_display": state["symbol_display"],
            "last_price": state["last_price"],
            "running": state["running"],
            "stake": state["stake"],
            "duration": state["duration"],
            "duration_unit": state["duration_unit"],
            "total_trades": state["total_trades"],
            "wins": state["wins"],
            "losses": state["losses"],
            "profit": state["profit"],
            "loss_streak": state["loss_streak"],
            "current_trade": state["current_trade"],
            "boom_symbols": state["boom_symbols"],
            "available_contracts": state["available_contracts"],
            "history": state["history"],
            "logs": state["logs"][:50],
            "last_error": state["last_error"],
            "win_rate": win_rate,
            "trade_interval": state["trade_interval"],
            "trade_state": state["trade_state"],
            "max_loss_streak": state["max_loss_streak"],
            "daily_loss": state["daily_loss"],
            "max_daily_loss": state["max_daily_loss"],
            "trades_today": state["trades_today"],
            "max_trades_per_day": state["max_trades_per_day"],
        })

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
