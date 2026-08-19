import os
import json
import threading
import websocket
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

bot_config = {
    "app_id": "",
    "api_token": "",
    "symbol": "R_25",
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
    
    if msg_type == "authorize":
        print("Tafiditra soa aman-tsara ny Token!")
        ws.send(json.dumps({"balance": 1, "subscribe": 1}))
        request_candles(ws)

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

def request_candles(ws):
    req = {
        "ticks_history": bot_config["symbol"],
        "adjust_start_time": 1,
        "count": 100,
        "end": "latest",
        "start": 1,
        "style": "candles",
        "granularity": 300
    }
    ws.send(json.dumps(req))

def on_error(ws, error):
    print(f"Diso ny fifandraisana WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Tapaka ny fifandraisana tamin'ny Deriv WebSocket.")
    bot_config["is_running"] = False

def on_open(ws):
    print("Mifandray amin'ny Deriv... Mandefa authorize...")
    auth_data = {"authorize": bot_config["api_token"]}
    ws.send(json.dumps(auth_data))

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
    bot_config["symbol"] = data.get("symbol", "R_25")
    
    if not bot_config["app_id"] or not bot_config["api_token"]:
        return jsonify({"status": "error", "message": "Fenoina daholo ny App ID sy API Token!"}), 400

    if bot_config["is_running"] and ws_app:
        ws_app.close()

    bot_config["is_running"] = True
    threading.Thread(target=run_websocket_bot, daemon=True).start()

    return jsonify({"status": "success", "message": "Voarindra tsara ny bot!"})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    return jsonify({
        "balance": bot_config["balance"],
        "total_trades": bot_config["total_trades"],
        "profit": bot_config["profit"],
        "ui_candles": bot_config["ui_candles"]
    })

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
