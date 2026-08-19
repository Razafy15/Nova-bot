import os
import json
import threading
import numpy as np
import websocket
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

bot_config = {
    "app_id": "",
    "api_token": "",
    "account_type": "demo", # Safidy Demo na Real
    "symbol": "1HZ25V",
    "stake": 1.0,
    "is_running": False,
    "balance": 0.0,
    "total_trades": 0,
    "profit": 0.0,
    "ui_candles": [],
    "last_signal": "Miandry ny fepetra rehetra...",
    "paused_for_profit": False
}

ws_app = None
ws_lock = threading.Lock()
active_contract_id = None

def send_ws(data):
    global ws_app
    if ws_app and ws_app.sock and ws_app.sock.connected:
        try:
            with ws_lock:
                ws_app.send(json.dumps(data))
        except Exception as e:
            print(f"Tsy tafita ny fandefasana WS: {e}")

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    if down == 0:
        return 100.0
    rs = up / down
    return 100.0 - (100.0 / (1.0 + rs))

def calculate_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if len(prices) > 0 else 0
    weights = np.exp(np.linspace(-1., 0., period))
    weights /= weights.sum()
    a = np.convolve(prices, weights, mode='valid')
    return a[-1]

def on_message(ws, message):
    global active_contract_id
    data = json.loads(message)
    msg_type = data.get("msg_type")
    
    if msg_type == "authorize":
        print("Tafiditra soa aman-tsara ny Token!")
        send_ws({"balance": 1, "subscribe": 1})
        request_market_data()

    elif msg_type == "balance":
        bal = data.get("balance")
        if bal and "balance" in bal:
            bot_config["balance"] = float(bal["balance"])

    elif msg_type == "candles":
        candles = data.get("candles", [])
        formatted_candles = []
        for c in candles:
            formatted_candles.append({
                "time": c["epoch"],
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"])
            })
        bot_config["ui_candles"] = formatted_candles
        check_strategy_and_trade(formatted_candles)

    elif msg_type == "ohlc":
        ohlc = data.get("ohlc")
        if ohlc:
            new_candle = {
                "time": ohlc["open_time"],
                "open": float(ohlc["open"]),
                "high": float(ohlc["high"]),
                "low": float(ohlc["low"]),
                "close": float(ohlc["close"])
            }
            if bot_config["ui_candles"]:
                if bot_config["ui_candles"][-1]["time"] == new_candle["time"]:
                    bot_config["ui_candles"][-1] = new_candle
                else:
                    bot_config["ui_candles"].append(new_candle)
                    if len(bot_config["ui_candles"]) > 200:
                        bot_config["ui_candles"].pop(0)
            check_strategy_and_trade(bot_config["ui_candles"])

    elif msg_type == "proposal":
        proposal = data.get("proposal")
        if proposal and bot_config["is_running"] and not active_contract_id:
            proposal_id = proposal["id"]
            send_ws({"buy": proposal_id, "price": bot_config["stake"]})

    elif msg_type == "buy":
        buy_res = data.get("buy")
        if buy_res:
            active_contract_id = buy_res["contract_id"]
            bot_config["total_trades"] += 1
            bot_config["last_signal"] = f"Trade nalefa! ID: {active_contract_id}"
            send_ws({
                "proposal_open_contract": 1,
                "contract_id": active_contract_id,
                "subscribe": 1
            })

    elif msg_type == "proposal_open_contract":
        poc = data.get("proposal_open_contract")
        if poc:
            status = poc.get("status")
            if status in ["won", "lost"]:
                profit = float(poc.get("profit", 0.0))
                bot_config["profit"] += profit
                bot_config["last_signal"] = f"Vita ny trade: {status.upper()} ({profit} USD)"
                
                symbol = bot_config["symbol"].lower()
                if ("boom" in symbol or "crash" in symbol) and profit >= 0.05:
                    bot_config["paused_for_profit"] = True
                    bot_config["last_signal"] = "Nahazo $0.05! Miandry Spike matanjaka kokoa (Pause)..."

                active_contract_id = None

def request_market_data():
    symbol = bot_config["symbol"]
    send_ws({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": 150,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": 60,
        "subscribe": 1
    })

def check_strategy_and_trade(candles):
    if not bot_config["is_running"] or active_contract_id or len(candles) < 50:
        return

    symbol = bot_config["symbol"].lower()
    closes = np.array([c["close"] for c in candles])
    current_rsi = calculate_rsi(closes, period=14)
    
    contract_type = None
    signal_msg = ""

    # 1. Volatility Indices (SMC EMA Trend)
    if "1hz" in symbol or "r_" in symbol:
        ema_fast = calculate_ema(closes, period=20)
        ema_slow = calculate_ema(closes, period=50)
        
        if ema_fast > ema_slow:
            contract_type = "CALL"
            signal_msg = f"SMC Trend: EMA20 > EMA50 -> CALL"
        else:
            contract_type = "PUT"
            signal_msg = f"SMC Trend: EMA20 < EMA50 -> PUT"

    # 2. Boom (RSI Spike + Pause -> PUT)
    elif "boom" in symbol:
        recent = candles[-4:]
        green_count = sum(1 for c in recent if c["close"] > c["open"])

        if bot_config["paused_for_profit"]:
            if current_rsi > 85 and green_count >= 4:
                bot_config["paused_for_profit"] = False
                contract_type = "PUT"
                signal_msg = f"BOOM (Pause): RSI > 85 & Maitso {green_count} -> PUT"
            else:
                signal_msg = "BOOM: Miandry Spike lehibe kokoa (RSI > 85)..."
                return
        else:
            if current_rsi > 80 and green_count >= 2:
                contract_type = "PUT"
                signal_msg = f"BOOM Spike: RSI: {current_rsi:.1f} & Maitso {green_count} -> PUT"
            else:
                signal_msg = f"BOOM: Miandry Spike (RSI: {current_rsi:.1f})"

    # 3. Crash (RSI Spike + Pause -> CALL)
    elif "crash" in symbol:
        recent = candles[-4:]
        red_count = sum(1 for c in recent if c["close"] < c["open"])

        if bot_config["paused_for_profit"]:
            if current_rsi < 15 and red_count >= 4:
                bot_config["paused_for_profit"] = False
                contract_type = "CALL"
                signal_msg = f"CRASH (Pause): RSI < 15 & Mena {red_count} -> CALL"
            else:
                signal_msg = "CRASH: Miandry Spike kely kokoa (RSI < 15)..."
                return
        else:
            if current_rsi < 20 and red_count >= 2:
                contract_type = "CALL"
                signal_msg = f"CRASH Spike: RSI: {current_rsi:.1f} & Mena {red_count} -> CALL"
            else:
                signal_msg = f"CRASH: Miandry Spike (RSI: {current_rsi:.1f})"

    bot_config["last_signal"] = signal_msg

    if contract_type:
        duration_val = 5 if ("boom" in symbol or "crash" in symbol) else 3
        duration_unit_val = "t" if ("boom" in symbol or "crash" in symbol) else "m"
        
        send_ws({
            "proposal": 1,
            "amount": bot_config["stake"],
            "basis": "stake",
            "currency": "USD",
            "symbol": bot_config["symbol"].upper(),
            "contract_type": contract_type,
            "duration": duration_val,
            "duration_unit": duration_unit_val
        })

def on_error(ws, error):
    print(f"Diso ny fifandraisana WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Tapaka ny fifandraisana tamin'ny Deriv WebSocket.")
    bot_config["is_running"] = False

def on_open(ws):
    print("Mifandray amin'ny Deriv... Mandefa authorize...")
    auth_data = {"authorize": bot_config["api_token"]}
    send_ws(auth_data)

def run_websocket_bot():
    global ws_app
    app_id = bot_config["app_id"]
    socket_url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    
    websocket.enableTrace(False)
    ws_app = websocket.WebSocketApp(socket_url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws_app.run_forever()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/configure', methods=['POST'])
def configure_bot():
    global ws_app
    data = request.json
    
    bot_config["app_id"] = data.get("app_id")
    bot_config["api_token"] = data.get("api_token")
    bot_config["account_type"] = data.get("account_type", "demo")
    bot_config["symbol"] = data.get("symbol", "1HZ25V")
    bot_config["stake"] = float(data.get("stake", 1.0))
    
    if not bot_config["app_id"] or not bot_config["api_token"]:
        return jsonify({"status": "error", "message": "Fenoina daholo ny App ID sy API Token!"}), 400

    if ws_app:
        try:
            ws_app.close()
        except:
            pass

    threading.Thread(target=run_websocket_bot, daemon=True).start()
    return jsonify({"status": "success", "message": f"Voarindra tsara ny kaonty {bot_config['account_type'].upper()}!"})

@app.route('/api/start', methods=['POST'])
def start_bot():
    data = request.json
    bot_config["stake"] = float(data.get("stake", 1.0))
    bot_config["symbol"] = data.get("symbol", bot_config["symbol"])
    bot_config["is_running"] = True
    bot_config["paused_for_profit"] = False
    bot_config["last_signal"] = "Nalefa ny Bot, mandinika ny tsena..."
    
    request_market_data()
    return jsonify({"status": "success", "message": "Nandeha ny Bot!"})

@app.route('/api/stop', methods=['POST'])
def stop_bot():
    bot_config["is_running"] = False
    bot_config["last_signal"] = "Najanona ny Bot."
    return jsonify({"status": "success", "message": "Nijanona ny Bot."})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "balance": bot_config["balance"],
        "total_trades": bot_config["total_trades"],
        "profit": bot_config["profit"],
        "ui_candles": bot_config["ui_candles"],
        "last_signal": bot_config["last_signal"],
        "is_running": bot_config["is_running"]
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
