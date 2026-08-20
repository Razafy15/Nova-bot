
import os
import json
import time
import threading
from collections import defaultdict, deque

import requests
import websocket
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

API_BASE = "https://api.derivws.com"
PUBLIC_WS = "wss://api.derivws.com/trading/v1/options/ws/public"

state = {
    "app_id": "",
    "api_token": "",
    "account_id": "",
    "account_type": "demo",
    "symbol": "",
    "stake": 1.0,
    "duration": 5,
    "duration_unit": "t",
    "connected": False,
    "bot_running": False,
    "balance": 0.0,
    "currency": "USD",
    "market_price": 0.0,
    "pre_signal": "WAIT",
    "confirmed_signal": "WAIT",
    "confidence": 0,
    "last_signal": "Miandry ny marché...",
    "profit": 0.0,
    "total_trades": 0,
    "active_contract_id": None,
    "chart": [],
    "boom_crash_symbols": [],
    "volatility_symbols": [],
    "scanner": {},
    "logs": [],
    "error": "",
}

lock = threading.RLock()
ws = None
ws_thread = None
stop_event = threading.Event()
req_counter = 100
pending_history = {}
proposal_pending = False


def log(msg):
    print(msg, flush=True)
    with lock:
        state["logs"] = (state["logs"] + [msg])[-80:]


def next_req():
    global req_counter
    with lock:
        req_counter += 1
        return req_counter


def reset_runtime():
    with lock:
        state["connected"] = False
        state["bot_running"] = False
        state["active_contract_id"] = None
        state["balance"] = 0.0
        state["market_price"] = 0.0
        state["pre_signal"] = "WAIT"
        state["confirmed_signal"] = "WAIT"
        state["confidence"] = 0
        state["last_signal"] = "Miandry ny connexion..."
        state["profit"] = 0.0
        state["total_trades"] = 0
        state["chart"] = []
        state["scanner"] = {}
        state["error"] = ""


def get_otp_url():
    account_id = state["account_id"].strip()
    app_id = state["app_id"].strip()
    token = state["api_token"].strip()
    if not account_id or not app_id or not token:
        raise RuntimeError("Fenoy App ID, Account ID ary PAT Token.")

    url = f"{API_BASE}/trading/v1/options/accounts/{account_id}/otp"
    headers = {
        "Authorization": f"Bearer {token}",
        "Deriv-App-ID": app_id,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, timeout=20)
    try:
        body = r.json()
    except Exception:
        body = {"raw": r.text}
    if r.status_code != 200:
        raise RuntimeError(f"OTP {r.status_code}: {json.dumps(body, ensure_ascii=False)[:700]}")
    ws_url = body.get("data", {}).get("url")
    if not ws_url:
        raise RuntimeError(f"OTP tsy namerina WebSocket URL: {json.dumps(body)[:700]}")
    return ws_url


def send(data):
    global ws
    with lock:
        sock = ws
    if sock is None:
        return False
    try:
        sock.send(json.dumps(data))
        return True
    except Exception as e:
        log(f"WS send error: {e}")
        return False


def candles_from_ticks(prices_times, granularity=60):
    # Build TradingView/Lightweight-Charts compatible OHLC candles.
    prices, times = prices_times
    buckets = {}
    for p, ts in zip(prices, times):
        p = float(p)
        ts = int(ts)
        bucket = ts - (ts % granularity)
        if bucket not in buckets:
            buckets[bucket] = [p, p, p, p]  # open, high, low, close
        else:
            c = buckets[bucket]
            c[1] = max(c[1], p)
            c[2] = min(c[2], p)
            c[3] = p
    return [
        {"time": int(t), "open": c[0], "high": c[1], "low": c[2], "close": c[3]}
        for t, c in sorted(buckets.items())
    ][-250:]


def rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    x = [float(v) for v in prices]
    d = [x[i] - x[i-1] for i in range(1, len(x))]
    recent = d[-period:]
    gains = [v for v in recent if v > 0]
    losses = [-v for v in recent if v < 0]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def ema(prices, period):
    if not prices:
        return 0.0
    alpha = 2 / (period + 1)
    value = float(prices[0])
    for p in prices[1:]:
        value = alpha * float(p) + (1 - alpha) * value
    return value


def boom_crash_signal(symbol, ticks):
    if len(ticks) < 30:
        return "WAIT", "WAIT", 0

    prices = [x[0] for x in ticks]
    changes = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    rr = rsi(prices, 14)
    e20 = ema(prices, 20)
    e50 = ema(prices, 50)

    # Generic pre-signal: detect pressure before confirmation.
    recent = changes[-10:]
    up = sum(1 for v in recent if v > 0)
    down = sum(1 for v in recent if v < 0)
    trend_up = e20 > e50
    trend_down = e20 < e50

    name = symbol.lower()
    is_boom = "boom" in name
    is_crash = "crash" in name

    if is_boom:
        # Boom: look for upward pressure/spike, then PUT after confirmation.
        pressure = rr >= 70 and up >= 5
        confirmed = rr >= 78 and up >= 7
        pre = "PUT SOON" if pressure else "WAIT"
        conf = "PUT" if confirmed else "WAIT"
    elif is_crash:
        # Crash: look for downward pressure/spike, then CALL after confirmation.
        pressure = rr <= 30 and down >= 5
        confirmed = rr <= 22 and down >= 7
        pre = "CALL SOON" if pressure else "WAIT"
        conf = "CALL" if confirmed else "WAIT"
    else:
        # Volatility is signal-only.
        pressure = trend_up and rr > 55
        confirmed = (trend_up and rr > 60) or (trend_down and rr < 40)
        pre = "CALL SOON" if trend_up and rr > 55 else ("PUT SOON" if trend_down and rr < 45 else "WAIT")
        conf = "CALL" if trend_up and rr > 60 else ("PUT" if trend_down and rr < 40 else "WAIT")

    confidence = int(min(99, max(0, abs(rr - 50) * 1.8 + max(up, down) * 3)))
    return pre, conf, confidence


def subscribe_symbol(symbol):
    # Historical ticks are used to create the chart and strategy input.
    rid = next_req()
    with lock:
        pending_history[rid] = symbol
    send({
        "ticks_history": symbol,
        "count": 1200,
        "end": "latest",
        "style": "ticks",
        "subscribe": 1,
        "req_id": rid,
    })


def subscribe_all_markets():
    symbols = state["boom_crash_symbols"] + state["volatility_symbols"]
    for symbol in symbols:
        send({"ticks": symbol, "subscribe": 1, "req_id": next_req()})
        rid = next_req()
        with lock:
            pending_history[rid] = symbol
        send({"ticks_history": symbol, "count": 250, "end": "latest",
              "style": "ticks", "subscribe": 0, "req_id": rid})


def request_balance():
    send({"balance": 1, "subscribe": 1, "req_id": next_req()})


def discover_symbols(data):
    items = data.get("active_symbols", [])
    boom, crash, vol = [], [], []
    for item in items:
        sym = item.get("symbol", "")
        display = item.get("display_name", "")
        text = f"{sym} {display}".lower()
        if "boom" in text or "crash" in text:
            if sym not in boom + crash:
                boom.append(sym)
        if "volatility" in text or sym.lower().startswith("1hz") or sym.upper().startswith("R_"):
            if sym not in vol:
                vol.append(sym)
    with lock:
        state["boom_crash_symbols"] = sorted(boom)
        state["volatility_symbols"] = sorted(vol)


def handle_tick(data):
    tick = data.get("tick", {})
    symbol = tick.get("symbol")
    quote = tick.get("quote")
    epoch = tick.get("epoch")
    if not symbol or quote is None:
        return

    with lock:
        state["market_price"] = float(quote) if symbol == state["symbol"] else state["market_price"]
        arr = state["scanner"].setdefault(symbol, [])
        arr.append((float(quote), int(epoch or time.time())))
        state["scanner"][symbol] = arr[-1200:]

    # For selected chart symbol, keep its live tick data.
    if symbol == state["symbol"]:
        update_chart(symbol)


def update_chart(symbol):
    with lock:
        arr = list(state["scanner"].get(symbol, []))
    if not arr:
        return
    candles = candles_from_ticks(([p for p, _ in arr], [t for _, t in arr]), 60)
    with lock:
        state["chart"] = candles
        prices = [p for p, _ in arr]
        state["market_price"] = prices[-1]
        pre, conf, confidence = boom_crash_signal(symbol, arr)
        state["pre_signal"] = pre
        state["confirmed_signal"] = conf
        state["confidence"] = confidence
        state["last_signal"] = f"{symbol}: {conf if conf != 'WAIT' else pre} | RSI {rsi(prices):.1f}"


def maybe_trade(symbol):
    global proposal_pending
    with lock:
        if not state["bot_running"] or state["active_contract_id"] or proposal_pending:
            return
        arr = list(state["scanner"].get(symbol, []))
        stake = float(state["stake"])
        duration = int(state["duration"])
    if "boom" not in symbol.lower() and "crash" not in symbol.lower():
        return

    pre, conf, confidence = boom_crash_signal(symbol, arr)
    if conf not in ("CALL", "PUT") or confidence < 65:
        return

    # Avoid repeated proposals on every tick.
    with lock:
        state["last_signal"] = f"{symbol}: CONFIRMED {conf} ({confidence}%)"
        proposal_pending = True

    send({
        "proposal": 1,
        "amount": stake,
        "basis": "stake",
        "currency": state["currency"],
        "contract_type": conf,
        "duration": duration,
        "duration_unit": "t",
        "underlying_symbol": symbol,
        "subscribe": 1,
        "req_id": next_req(),
    })


def on_message(sock, message):
    try:
        data = json.loads(message)
    except Exception:
        return

    if data.get("error"):
        global proposal_pending
        with lock:
            proposal_pending = False
        err = data["error"]
        msg = err.get("message", str(err))
        log(f"Deriv error: {msg}")
        with lock:
            state["error"] = msg
            state["last_signal"] = f"ERROR: {msg}"
        return

    msg_type = data.get("msg_type")

    if msg_type == "balance":
        b = data.get("balance", {})
        with lock:
            state["balance"] = float(b.get("balance", 0))
            state["currency"] = b.get("currency", "USD")
        return

    if msg_type == "active_symbols":
        discover_symbols(data)
        with lock:
            selected = state["symbol"]
        if selected:
            subscribe_symbol(selected)
        subscribe_all_markets()
        log(f"Symbols: {len(state['boom_crash_symbols'])} Boom/Crash, {len(state['volatility_symbols'])} Volatility")
        return

    if msg_type == "history":
        # Tick history. New API does not guarantee echo_req, so use req_id mapping.
        hist = data.get("history", {})
        prices = hist.get("prices", [])
        times = hist.get("times", [])
        rid = data.get("req_id")
        symbol = pending_history.pop(rid, None) if rid is not None else None
        if symbol and prices and times:
            with lock:
                state["scanner"][symbol] = [(float(p), int(t)) for p, t in zip(prices, times)][-1200:]
            update_chart(symbol)
        return

    if msg_type == "tick":
        handle_tick(data)
        symbol = data.get("tick", {}).get("symbol")
        if symbol:
            maybe_trade(symbol)
        return

    if msg_type == "proposal":
        global proposal_pending
        prop = data.get("proposal", {})
        with lock:
            running = state["bot_running"]
            active = state["active_contract_id"]
            stake = float(state["stake"])
            proposal_pending = False
        if prop.get("id") and running and not active:
            with lock:
                proposal_pending = True
            send({"buy": prop["id"], "price": stake, "req_id": next_req()})
        return

    if msg_type == "buy":
        global proposal_pending
        with lock:
            proposal_pending = False
        buy = data.get("buy", {})
        cid = buy.get("contract_id")
        if cid:
            with lock:
                state["active_contract_id"] = cid
                state["total_trades"] += 1
                state["last_signal"] = f"TRADE OPENED #{cid}"
            send({
                "proposal_open_contract": 1,
                "contract_id": cid,
                "subscribe": 1,
                "req_id": next_req(),
            })
        return

    if msg_type == "proposal_open_contract":
        poc = data.get("proposal_open_contract", {})
        status = poc.get("status")
        if status in ("won", "lost", "sold", "expired"):
            profit = float(poc.get("profit", 0) or 0)
            with lock:
                state["profit"] += profit
                state["active_contract_id"] = None
                state["last_signal"] = f"TRADE {status.upper()} | {profit:.2f} USD"
        return


def on_open(sock):
    log("WebSocket connected (authenticated OTP).")
    with lock:
        state["connected"] = True
        state["error"] = ""
        state["last_signal"] = "Deriv Connected — market data loading..."
    request_balance()
    send({"active_symbols": "brief", "req_id": next_req()})


def on_error(sock, error):
    log(f"WebSocket error: {error}")
    with lock:
        state["connected"] = False
        state["error"] = str(error)


def on_close(sock, code, msg):
    log(f"WebSocket closed: {code} {msg}")
    with lock:
        state["connected"] = False


def websocket_worker():
    global ws
    while not stop_event.is_set():
        try:
            with lock:
                app_id = state["app_id"]
            if not app_id:
                time.sleep(1)
                continue

            url = get_otp_url()
            log("OTP obtained. Connecting...")
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log(f"Connection failed: {e}")
            with lock:
                state["connected"] = False
                state["error"] = str(e)
        finally:
            with lock:
                state["connected"] = False
            if not stop_event.is_set():
                time.sleep(3)


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/connect")
def connect():
    global ws_thread
    data = request.get_json(silent=True) or {}
    required = ["app_id", "api_token", "account_id"]
    if any(not str(data.get(k, "")).strip() for k in required):
        return jsonify({"ok": False, "message": "Fenoy App ID, PAT Token ary Account ID."}), 400

    stop_event.set()
    try:
        if ws:
            ws.close()
    except Exception:
        pass
    time.sleep(0.1)
    stop_event.clear()

    with lock:
        state["app_id"] = str(data["app_id"]).strip()
        state["api_token"] = str(data["api_token"]).strip()
        state["account_id"] = str(data["account_id"]).strip()
        state["account_type"] = str(data.get("account_type", "demo")).lower()
        state["symbol"] = str(data.get("symbol", "")).strip()
        state["stake"] = max(0.35, float(data.get("stake", 1)))
        state["duration"] = max(1, int(data.get("duration", 5)))
        state["last_signal"] = "Maka OTP..."
        state["error"] = ""

    ws_thread = threading.Thread(target=websocket_worker, daemon=True)
    ws_thread.start()
    return jsonify({"ok": True, "message": "Connexion natomboka. Jereo ny status."})


@app.post("/api/start")
def start():
    with lock:
        if not state["connected"]:
            return jsonify({"ok": False, "message": "Connect Deriv aloha."}), 400
        state["bot_running"] = True
        state["last_signal"] = "BOT RUNNING — Boom/Crash auto-trading + Volatility signal-only."
    return jsonify({"ok": True})


@app.post("/api/stop")
def stop():
    with lock:
        state["bot_running"] = False
        state["last_signal"] = "BOT STOPPED"
    return jsonify({"ok": True})


@app.post("/api/symbol")
def change_symbol():
    data = request.get_json(silent=True) or {}
    symbol = str(data.get("symbol", "")).strip()
    if not symbol:
        return jsonify({"ok": False}), 400
    with lock:
        state["symbol"] = symbol
        state["chart"] = []
    subscribe_symbol(symbol)
    return jsonify({"ok": True})


@app.get("/api/stats")
def stats():
    with lock:
        return jsonify({
            "connected": state["connected"],
            "bot_running": state["bot_running"],
            "balance": state["balance"],
            "currency": state["currency"],
            "market_price": state["market_price"],
            "pre_signal": state["pre_signal"],
            "confirmed_signal": state["confirmed_signal"],
            "confidence": state["confidence"],
            "last_signal": state["last_signal"],
            "profit": state["profit"],
            "total_trades": state["total_trades"],
            "active_contract_id": state["active_contract_id"],
            "chart": state["chart"],
            "boom_crash_symbols": state["boom_crash_symbols"],
            "volatility_symbols": state["volatility_symbols"],
            "scanner": {
                k: {"pre": boom_crash_signal(k, v)[0],
                    "confirmed": boom_crash_signal(k, v)[1],
                    "confidence": boom_crash_signal(k, v)[2]}
                for k, v in state["scanner"].items()
            },
            "logs": state["logs"][-30:],
            "error": state["error"],
        })


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
