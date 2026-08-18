import os
import json
import threading
import websocket
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# Variable global hitahirizana ny configuration sy ny satan'ny bot
bot_config = {
    "app_id": "",
    "api_token": "",
    "symbol": "R_100",
    "is_running": False
}

current_symbol_index = 0
ws_app = None

def on_message(ws, message):
    global current_symbol_index
    data = json.loads(message)
    msg_type = data.get("msg_type")
    
    print(f"Valiny avy amin'ny Deriv: {msg_type}")

    # Rehefa tafiditra soa aman-tsara (Authorized)
    if msg_type == "authorize":
        print("Tafiditra soa aman-tsara ny Token! Manomboka mangataka proposal...")
        request_proposal(ws)

    # Rehefa voaray ny proposal
    elif msg_type == "proposal":
        proposal = data.get("proposal")
        if proposal:
            proposal_id = proposal.get("id")
            ask_price = proposal.get("ask_price")
            print(f"Proposal voaray. Vola vidiny (ask_price): {ask_price}")
            
            # Jereo ny vidiny alohan'ny hividianana
            if ask_price and float(ask_price) <= 1.0:
                buy_contract(ws, proposal_id, ask_price)
            else:
                print("Mihoatra ny $1 ny vidiny, voàzona ny fividianana.")

    # Rehefa vita ny fividianana contract
    elif msg_type == "buy":
        print("Vita soa aman-tsara ny fividianana contract!")

def on_error(ws, error):
    print(f"Diso ny fifandraisana WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Tapaka ny fifandraisana tamin'ny Deriv WebSocket.")
    bot_config["is_running"] = False

def on_open(ws):
    print("Mifandray amin'ny Deriv... Mandefa authorize...")
    auth_data = {"authorize": bot_config["api_token"]}
    ws.send(json.dumps(auth_data))

def request_proposal(ws):
    proposal_request = {
        "proposal": 1,
        "amount": 1,
        "basis": "stake",
        "currency": "USD",
        "symbol": bot_config["symbol"],
        "duration": 5,
        "duration_unit": "t",
        "contract_type": "CALL"
    }
    ws.send(json.dumps(proposal_request))

def buy_contract(ws, proposal_id, price):
    buy_request = {
        "buy": proposal_id,
        "price": price
    }
    ws.send(json.dumps(buy_request))

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
    bot_config["symbol"] = data.get("symbol", "R_100")
    
    if not bot_config["app_id"] or not bot_config["api_token"]:
        return jsonify({"status": "error", "message": "Fenoina daholo ny angona ilaina!"}), 400

    if bot_config["is_running"] and ws_app:
        ws_app.close()

    bot_config["is_running"] = True
    threading.Thread(target=run_websocket_bot, daemon=True).start()

    return jsonify({"status": "success", "message": "Voarindra sy nanomboka nandeha tsara ny bot!"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
