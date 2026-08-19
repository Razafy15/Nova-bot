import os
import json
import threading
import websocket
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# Data hitahirizana ny fiasan'ny bot sy ny statistika
bot_state = {
    "app_id": "",
    "api_token": "",
    "symbol": "v25",
    "is_running": False,
    "balance": 0.0,
    "total_trades": 0,
    "profit": 0.0,
    "ui_candles": []
}

ws_app = None

def on_message(ws, message):
    data = json.loads(message)
    msg_type = data.get("msg_type")
    
    # Rehefa tafiditra soa aman-tsara (Authorize)
    if msg_type == "authorize":
        print("Tafiditra soa aman-tsara ny Token!")
        # Mangataka ny balance avy hatrany
        ws.send(json.dumps({"balance": 1, "subscribe": 1}))
        # Mangataka ny sary famantarana (candles) voalohany
        request_candles(ws, bot_state["symbol"])

    elif msg_type == "balance":
        bal = data.get("balance", {})
        if "balance" in bal:
            bot_state["balance"] = float(bal["balance"])

    elif msg_type == "ohlc" or msg_type == "candles":
        # Handraisana ny sary famantarana ho an'ny Lightweight Charts
        candles_data = data.get("candles") or [data.get("ohlc")]
        if candles_data:
            formatted = []
            for c in candles_data:
                if c:
                    formatted.append({
                        "time": c.get("epoch") or c.get("open_time"),
                        "open": float(c.get("open", 0)),
                        "high": float(c.get("high", 0)),
                        "low": float(c.get("low", 0)),
                        "close": float(c.get("close", 0))
                    })
            bot_state["ui_candles"] = formatted

def request_candles(ws, symbol):
    # Fangatahana tabilao (candles) 
    req = {
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": 100,
        "end": "latest",
        "style": "candles",
        "granularity": 60,
        "subscribe": 1
    }
    ws.send(json.dumps(req))

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Tapaka ny fifandraisana WebSocket.")
    bot_state["is_running"] = False

def on_open(ws):
    print("Mifandray amin'ny Deriv WebSocket... Mandefa authorize...")
    auth_data = {"authorize": bot_state["api_token"]}
    ws.send(json.dumps(auth_data))

def run_websocket_bot():
    global ws_app
    app_id = bot_state["app_id"]
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
    
    bot_state["app_id"] = data.get("app_id")
    bot_state["api_token"] = data.get("api_token")
    bot_state["symbol"] = data.get("symbol", "v25")
    
    if not bot_state["app_id"] or not bot_state["api_token"]:
        return jsonify({"status": "error", "message": "Fenoina ny App ID sy API Token!"}), 400

    if bot_state["is_running"] and ws_app:
        ws_app.close()

    bot_state["is_running"] = True
    threading.Thread(target=run_websocket_bot, daemon=True).start()

    return jsonify({"status": "success", "message": "Voarindra sy nanomboka nandeha tsara ny bot!"})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "balance": bot_state["balance"],
        "total_trades": bot_state["total_trades"],
        "profit": bot_state["profit"],
        "ui_candles": bot_state["ui_candles"]
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
