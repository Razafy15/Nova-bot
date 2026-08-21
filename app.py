import os
import json
import threading
import time

import requests
import websocket
from flask import Flask, jsonify, render_template


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

DERIV_APP_ID = os.getenv("DERIV_APP_ID", "")
DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")
DERIV_ACCOUNT_ID = os.getenv("DERIV_ACCOUNT_ID", "")

# DEMO only for Version 1
ACCOUNT_MODE = "demo"

OTP_URL = (
    "https://api.derivws.com"
    f"/trading/v1/options/accounts/{DERIV_ACCOUNT_ID}/otp"
)


# ============================================================
# BOT STATE
# ============================================================

state = {
    "connected": False,
    "authorized": False,
    "balance": 0.0,
    "currency": "USD",

    "symbol": "BOOM150",
    "direction": "PUT",
    "duration": 3,
    "stake": 0.35,

    "running": False,

    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss_streak": 0,

    "last_error": "",
}

ws = None
ws_lock = threading.Lock()


# ============================================================
# SEND WEBSOCKET MESSAGE
# ============================================================

def ws_send(payload):
    global ws

    with ws_lock:
        if ws is None:
            return False

        try:
            ws.send(json.dumps(payload))
            return True
        except Exception as exc:
            state["last_error"] = str(exc)
            return False


# ============================================================
# GET AUTHENTICATED WEBSOCKET URL
# ============================================================

def get_websocket_url():
    if not DERIV_APP_ID:
        raise RuntimeError("DERIV_APP_ID is missing")

    if not DERIV_TOKEN:
        raise RuntimeError("DERIV_TOKEN is missing")

    if not DERIV_ACCOUNT_ID:
        raise RuntimeError("DERIV_ACCOUNT_ID is missing")

    headers = {
        "Deriv-App-ID": DERIV_APP_ID,
        "Authorization": f"Bearer {DERIV_TOKEN}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        OTP_URL,
        headers=headers,
        timeout=20,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"OTP request failed: HTTP {response.status_code} "
            f"{response.text}"
        )

    data = response.json()

    ws_url = data.get("data", {}).get("url")

    if not ws_url:
        raise RuntimeError(
            f"OTP response did not contain WebSocket URL: {data}"
        )

    return ws_url


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def on_open(socket):
    state["connected"] = True
    state["last_error"] = ""

    print("================================")
    print("DERIV WEBSOCKET CONNECTED")
    print("================================")

    # Get balance
    ws_send({
        "balance": 1,
        "subscribe": 1,
    })


def on_message(socket, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return

    msg_type = data.get("msg_type")

    # --------------------------------------------
    # BALANCE
    # --------------------------------------------

    if msg_type == "balance":
        balance_data = data.get("balance", {})

        try:
            state["balance"] = float(
                balance_data.get("balance", 0)
            )
        except (TypeError, ValueError):
            state["balance"] = 0.0

        state["currency"] = (
            balance_data.get("currency")
            or state["currency"]
        )

        state["authorized"] = True

        print(
            f"BALANCE: "
            f"{state['balance']:.2f} "
            f"{state['currency']}"
        )

    # --------------------------------------------
    # ERROR
    # --------------------------------------------

    elif msg_type == "error":
        error = data.get("error", {})

        message_text = error.get(
            "message",
            "Unknown Deriv API error",
        )

        state["last_error"] = message_text

        print("DERIV ERROR:", message_text)


def on_error(socket, error):
    state["connected"] = False
    state["authorized"] = False
    state["last_error"] = str(error)

    print("WEBSOCKET ERROR:", error)


def on_close(socket, close_status_code, close_msg):
    state["connected"] = False
    state["authorized"] = False

    print(
        "WEBSOCKET CLOSED:",
        close_status_code,
        close_msg,
    )


# ============================================================
# CONNECT
# ============================================================

def connect_deriv():
    global ws

    try:
        print("Getting authenticated WebSocket URL...")

        ws_url = get_websocket_url()

        print("WebSocket URL received.")

        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
        )

    except Exception as exc:
        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)

        print("CONNECT ERROR:", exc)


# ============================================================
# START CONNECTION THREAD
# ============================================================

def start_connection():
    thread = threading.Thread(
        target=connect_deriv,
        daemon=True,
    )

    thread.start()


# ============================================================
# FLASK ROUTES
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    total = state["total_trades"]
    wins = state["wins"]

    if total > 0:
        win_rate = (wins / total) * 100
    else:
        win_rate = 0.0

    return jsonify({
        "connected": state["connected"],
        "authorized": state["authorized"],

        "balance": state["balance"],
        "currency": state["currency"],

        "symbol": state["symbol"],
        "direction": state["direction"],
        "duration": state["duration"],
        "stake": state["stake"],

        "running": state["running"],

        "total_trades": state["total_trades"],
        "wins": state["wins"],
        "losses": state["losses"],
        "win_rate": round(win_rate, 2),

        "profit": round(state["profit"], 2),
        "loss_streak": state["loss_streak"],

        "last_error": state["last_error"],
    })


@app.route("/api/start", methods=["POST"])
def start_bot():
    if not state["authorized"]:
        return jsonify({
            "ok": False,
            "error": "Deriv account is not connected.",
        }), 400

    state["running"] = True

    return jsonify({
        "ok": True,
        "running": True,
    })


@app.route("/api/stop", methods=["POST"])
def stop_bot():
    state["running"] = False

    return jsonify({
        "ok": True,
        "running": False,
    })


@app.route("/api/pause", methods=["POST"])
def pause_bot():
    state["running"] = False

    return jsonify({
        "ok": True,
        "running": False,
    })


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print("================================")
    print("BOOM FALL BOT - VERSION 1")
    print("DEMO MODE")
    print("================================")

    if not DERIV_APP_ID:
        print("WARNING: DERIV_APP_ID is not configured.")

    if not DERIV_TOKEN:
        print("WARNING: DERIV_TOKEN is not configured.")

    if not DERIV_ACCOUNT_ID:
        print("WARNING: DERIV_ACCOUNT_ID is not configured.")

    if (
        DERIV_APP_ID
        and DERIV_TOKEN
        and DERIV_ACCOUNT_ID
    ):
        start_connection()

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv("PORT", "5000")
        ),
        debug=False,
    )
