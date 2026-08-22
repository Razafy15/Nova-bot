import json
import os
import threading
import time

import requests
import websocket

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

REST_BASE = "https://api.derivws.com"

ws = None
ws_thread = None
ws_lock = threading.Lock()

APP_ID = ""
PAT_TOKEN = ""

state = {
    "connected": False,
    "authorized": False,
    "balance": 0.0,
    "currency": "USD",
    "account_id": "",
    "account_type": "",
    "symbol": "",
    "symbol_display": "",
    "last_price": None,
    "ticks_buffer": [],
    "running": False,
    "stake": 0.35,
    "duration": 3,
    "direction": "PUT",
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss_streak": 0,
    "current_trade": None,
    "boom_symbols": [],
    "available_contracts": [],
    "history": [],
    "logs": [],
    "last_error": "",
    "last_proposal": None,
    "last_trade_time": 0,
    "trade_interval": 5,
}


def add_log(message):
    timestamp = time.strftime("%H:%M:%S")
    state["logs"].insert(0, {"time": timestamp, "message": str(message)})
    state["logs"] = state["logs"][:100]
    print(f"[{timestamp}] {message}", flush=True)


def send_ws(payload):
    global ws
    with ws_lock:
        if ws is None:
            add_log("WebSocket is not initialized.")
            return False
        try:
            ws.send(json.dumps(payload))
            return True
        except Exception as exc:
            state["last_error"] = str(exc)
            add_log(f"WS SEND ERROR: {exc}")
            return False


def api_error(response):
    try:
        data = response.json()
    except Exception:
        return f"HTTP {response.status_code}: {response.text[:500]}"
    errors = data.get("errors")
    if isinstance(errors, list) and errors:
        error = errors[0]
        return f"HTTP {response.status_code} | {error.get('code', 'ERROR')} | {error.get('message', 'Unknown error')}"
    return f"HTTP {response.status_code}: {json.dumps(data)[:500]}"


def get_accounts(app_id, token):
    url = REST_BASE + "/trading/v1/options/accounts"
    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=20)
    if not response.ok:
        raise RuntimeError(api_error(response))
    data = response.json()
    accounts = data.get("data", [])
    if isinstance(accounts, dict):
        accounts = [accounts]
    return accounts


def get_otp_url(app_id, token, account_id):
    url = REST_BASE + "/trading/v1/options/accounts/" + account_id + "/otp"
    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    response = requests.post(url, headers=headers, timeout=20)
    if not response.ok:
        raise RuntimeError(api_error(response))
    data = response.json()
    ws_url = data.get("data", {}).get("url")
    if not ws_url:
        raise RuntimeError("OTP response did not contain data.url")
    return ws_url


def check_strategy():
    interval = state.get("trade_interval", 5)
    last_time = state.get("last_trade_time", 0)
    current_time = time.time()

    if current_time - last_time >= interval:
        state["direction"] = "PUT"
        return True

    return False


def trading_loop():
    add_log("=== TRADING LOOP STARTED ===")
    loop_count = 0

    while state["running"]:
        loop_count += 1
        try:
            if loop_count % 10 == 0:
                add_log(f"Trading loop alive (cycle {loop_count})")

            if not state["authorized"]:
                time.sleep(0.5)
                continue

            if not state["symbol"]:
                time.sleep(0.5)
                continue

            if check_strategy():
                add_log(f"Strategy triggered! Sending proposal (PUT) for {state['symbol']}...")

                proposal_payload = {
                    "proposal": 1,
                    "amount": state["stake"],
                    "contract_type": "PUT",
                    "currency": state["currency"],
                    "duration": state["duration"],
                    "duration_unit": "m",
                    "symbol": state["symbol"]  # <-- Mampiasa ny anarana marina avy amin'ny API
                }
                add_log(f"Proposal payload: {proposal_payload}")
                send_ws(proposal_payload)
                state["last_proposal"] = None

                time.sleep(3)

                if state.get("last_proposal") and state["last_proposal"].get("id"):
                    proposal_id = state["last_proposal"]["id"]
                    price = state["last_proposal"]["price"]
                    buy_payload = {
                        "buy": proposal_id,
                        "price": price
                    }
                    add_log(f"Buy sent for proposal {proposal_id} at price {price}")
                    send_ws(buy_payload)
                    state["last_proposal"] = None
                    state["last_trade_time"] = time.time()
                    time.sleep(5)
                else:
                    add_log("Proposal failed or timed out - no proposal received.")

            time.sleep(0.5)

        except Exception as e:
            add_log(f"TRADING LOOP ERROR: {e}")
            import traceback
            add_log(traceback.format_exc())
            time.sleep(2)

    add_log("=== TRADING LOOP STOPPED ===")


def on_open(socket):
    state["connected"] = True
    state["last_error"] = ""
    add_log("WebSocket connected.")

    send_ws({"balance": 1, "subscribe": 1, "req_id": 100})
    send_ws({"active_symbols": "full", "req_id": 101})


def on_message(socket, message):
    try:
        data = json.loads(message)
    except Exception:
        add_log("Invalid JSON received.")
        return

    msg_type = data.get("msg_type")

    if msg_type == "error":
        error = data.get("error", {})
        code = error.get("code", "UNKNOWN")
        message_text = error.get("message", "Unknown Deriv error")
        state["last_error"] = f"{code}: {message_text}"
        add_log(f"DERIV ERROR: {code}: {message_text}")
        return

    if msg_type == "balance":
        balance = data.get("balance", {})
        try:
            state["balance"] = float(balance.get("balance", 0))
        except Exception:
            state["balance"] = 0.0
        state["currency"] = balance.get("currency") or "USD"
        state["authorized"] = True
        add_log(f"Account authorized. Balance: {state['balance']:.2f} {state['currency']}")
        return

    # --------------------------------------------------------
    # ACTIVE SYMBOLS - FANITSINA LEHIBE ETO
    # --------------------------------------------------------
    if msg_type == "active_symbols":
        symbols = data.get("active_symbols", [])
        boom = []
        for item in symbols:
            symbol = str(item.get("underlying_symbol", "")).strip()
            name = str(item.get("display_name", "")).strip()
            
            # Mitady ny marika rehetra misy BOOM
            text = (symbol + " " + name).upper()
            if "BOOM" in text or "R_" in symbol:
                boom.append({
                    "symbol": symbol,        # <-- Izao no ampiasaina amin'ny API (R_75, R_100, sns)
                    "display_name": name or symbol
                })
        
        state["boom_symbols"] = boom
        add_log(f"BOOM symbols found: {len(boom)}")
        
        # Log ny symbol rehetra mba ho hita
        for b in boom:
            add_log(f"  - {b['symbol']} = {b['display_name']}")
        return

    if msg_type == "tick":
        tick = data.get("tick", {})
        try:
            price = float(tick.get("quote"))
            state["last_price"] = price
            state["ticks_buffer"].append(price)
            if len(state["ticks_buffer"]) > 20:
                state["ticks_buffer"].pop(0)
        except Exception:
            pass
        return

    if msg_type == "contracts_for":
        result = data.get("contracts_for", {})
        state["available_contracts"] = result.get("available", [])
        add_log("Contract information received.")
        return

    if msg_type == "proposal":
        proposal = data.get("proposal", {})
        proposal_id = proposal.get("id")
        if proposal_id:
            state["last_proposal"] = {
                "id": proposal_id,
                "price": proposal.get("price", 0)
            }
            add_log(f"Proposal received: {proposal_id} - Price: {proposal.get('price')}")
        return

    if msg_type == "buy":
        buy = data.get("buy", {})
        contract_id = buy.get("contract_id")
        if contract_id:
            state["current_trade"] = {
                "contract_id": contract_id,
                "symbol": state["symbol"],
                "direction": state["direction"],
                "stake": state["stake"],
                "status": "OPEN",
            }
            state["last_trade_time"] = time.time()
            add_log(f"Contract opened: {contract_id}")
            send_ws({
                "proposal_open_contract": 1,
                "contract_id": contract_id,
                "subscribe": 1,
                "req_id": 300,
            })
        return

    if msg_type == "proposal_open_contract":
        contract = data.get("proposal_open_contract", {})
        if not (contract.get("is_sold") or contract.get("is_expired")):
            return

        try:
            profit = float(contract.get("profit", 0))
        except Exception:
            profit = 0.0

        state["profit"] += profit
        state["total_trades"] += 1

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
            "direction": state["direction"],
            "stake": state["stake"],
            "result": result,
            "profit": profit,
        })
        state["history"] = state["history"][:100]
        state["current_trade"] = None
        add_log(f"{result}: {profit:+.2f}")
        return


def on_error(socket, error):
    state["connected"] = False
    state["authorized"] = False
    state["last_error"] = str(error)
    add_log(f"WebSocket error: {error}")


def on_close(socket, code, reason):
    state["connected"] = False
    state["authorized"] = False
    add_log(f"WebSocket closed: {code} {reason}")


def websocket_thread(ws_url):
    global ws
    try:
        add_log("Opening authenticated WebSocket...")
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
    except Exception as exc:
        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)
        add_log(f"WS THREAD ERROR: {exc}")


# ============================================================
# FLASK ROUTES
# ============================================================

@app.post("/api/connect")
def api_connect():
    global APP_ID, PAT_TOKEN, ws_thread
    data = request.get_json(silent=True) or {}
    app_id = str(data.get("app_id", "")).strip()
    token = str(data.get("token", "")).strip()
    account_id = str(data.get("account_id", "")).strip()

    if not app_id:
        return jsonify({"ok": False, "error": "App ID is required."}), 400
    if not token:
        return jsonify({"ok": False, "error": "PAT/API Token is required."}), 400
    if not account_id:
        return jsonify({"ok": False, "error": "Account ID is required."}), 400

    try:
        add_log("========== CONNECT ==========")

        accounts = get_accounts(app_id, token)
        selected = None
        for account in accounts:
            if str(account.get("account_id", "")).strip() == account_id:
                selected = account
                break
        if selected is None:
            raise RuntimeError("Account ID was not found for this PAT.")

        state["account_id"] = account_id
        state["account_type"] = selected.get("account_type", "")

        ws_url = get_otp_url(app_id, token, account_id)
        APP_ID = app_id
        PAT_TOKEN = token

        if ws_thread and ws_thread.is_alive():
            add_log("Closing old WebSocket thread...")
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
            time.sleep(1)

        ws_thread = threading.Thread(target=websocket_thread, args=(ws_url,), daemon=True)
        ws_thread.start()

        add_log("OTP obtained. WebSocket connection started.")
        return jsonify({"ok": True, "message": "Connection started."})

    except Exception as exc:
        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)
        add_log(f"CONNECT FAILED: {exc}")
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/markets")
def api_markets():
    if not state["connected"]:
        return jsonify({"ok": False, "error": "Connect Deriv first."}), 400
    send_ws({"active_symbols": "full", "req_id": 500})
    return jsonify({"ok": True})


@app.post("/api/select-symbol")
def api_select_symbol():
    data = request.get_json(silent=True) or {}
    symbol = str(data.get("symbol", "")).strip()
    display_name = str(data.get("display_name", symbol)).strip()

    if not symbol:
        return jsonify({"ok": False, "error": "Symbol is required."}), 400
    if not state["connected"]:
        return jsonify({"ok": False, "error": "Connect Deriv first."}), 400

    state["symbol"] = symbol
    state["symbol_display"] = display_name
    state["available_contracts"] = []
    state["ticks_buffer"] = []

    send_ws({"ticks": symbol, "subscribe": 1, "req_id": 600})
    send_ws({"contracts_for": symbol, "req_id": 601})
    
    add_log(f"Selected symbol: {symbol} ({display_name})")
    return jsonify({"ok": True, "symbol": symbol, "display_name": display_name})


@app.post("/api/start")
def api_start():
    add_log("START button pressed.")

    if not state["authorized"]:
        add_log("ERROR: Not authorized yet.")
        return jsonify({"ok": False, "error": "Connect and authorize Deriv first."}), 400

    if not state["symbol"]:
        add_log("ERROR: No symbol selected.")
        return jsonify({"ok": False, "error": "Select a BOOM symbol first."}), 400

    state["running"] = True
    state["last_trade_time"] = 0

    if not hasattr(app, 'trading_thread') or not app.trading_thread.is_alive():
        add_log("Creating new trading thread...")
        app.trading_thread = threading.Thread(target=trading_loop, daemon=True)
        app.trading_thread.start()
        add_log("Trading thread started.")
    else:
        add_log("Trading thread already running.")

    add_log("BOT STARTED.")
    return jsonify({"ok": True})


@app.post("/api/pause")
def api_pause():
    state["running"] = False
    add_log("BOT PAUSED.")
    return jsonify({"ok": True})


@app.post("/api/stop")
def api_stop():
    state["running"] = False
    add_log("BOT STOPPED.")
    return jsonify({"ok": True})


@app.post("/api/update-interval")
def api_update_interval():
    data = request.get_json(silent=True) or {}
    interval = int(data.get("interval", 5))
    if interval < 1:
        return jsonify({"ok": False, "error": "Interval must be at least 1 second."}), 400
    state["trade_interval"] = interval
    add_log(f"Trade interval updated to {interval} seconds.")
    return jsonify({"ok": True, "interval": interval})


@app.get("/api/status")
def api_status():
    total = state["total_trades"]
    win_rate = (state["wins"] / total * 100) if total else 0.0
    return jsonify({**state, "win_rate": win_rate})


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
