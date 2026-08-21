import json
import threading

import requests
import websocket

from flask import Flask, jsonify, render_template, request


app = Flask(__name__)

state = {
    "connected": False,
    "authorized": False,
    "balance": 0.0,
    "currency": "USD",
    "account_id": "",
    "account_type": "",
    "app_id": "",
    "running": False,
    "last_error": "",
}

ws = None
ws_lock = threading.Lock()


def send_ws(payload):
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


def get_accounts(app_id, token):
    url = "https://api.derivws.com/trading/v1/options/accounts"

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=20,
    )

    if not response.ok:
        raise RuntimeError(
            f"Accounts request failed: HTTP "
            f"{response.status_code}: {response.text}"
        )

    result = response.json()

    return result.get("data", [])


def get_otp_url(app_id, token, account_id):
    url = (
        "https://api.derivws.com"
        f"/trading/v1/options/accounts/{account_id}/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        timeout=20,
    )

    if not response.ok:
        raise RuntimeError(
            f"OTP request failed: HTTP "
            f"{response.status_code}: {response.text}"
        )

    result = response.json()

    ws_url = result.get("data", {}).get("url")

    if not ws_url:
        raise RuntimeError(
            f"No WebSocket URL in OTP response: {result}"
        )

    return ws_url


def on_open(socket):
    state["connected"] = True
    state["last_error"] = ""

    print("DERIV WEBSOCKET CONNECTED")

    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": 1,
    })


def on_message(socket, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return

    msg_type = data.get("msg_type")

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
            or "USD"
        )

        state["authorized"] = True

        print(
            "BALANCE:",
            state["balance"],
            state["currency"],
        )

    elif msg_type == "error":

        error = data.get("error", {})

        state["last_error"] = (
            error.get("message")
            or "Deriv API error"
        )

        print(
            "DERIV ERROR:",
            state["last_error"],
        )


def on_error(socket, error):
    state["connected"] = False
    state["authorized"] = False
    state["last_error"] = str(error)

    print("WEBSOCKET ERROR:", error)


def on_close(socket, code, message):
    state["connected"] = False
    state["authorized"] = False

    print(
        "WEBSOCKET CLOSED:",
        code,
        message,
    )


def connect_websocket(ws_url):
    global ws

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


@app.route("/")
def index():
    return render_template("index.html")


@app.post("/api/connect")
def api_connect():

    global ws

    data = request.get_json(silent=True) or {}

    app_id = str(
        data.get("app_id", "")
    ).strip()

    token = str(
        data.get("token", "")
    ).strip()

    account_id = str(
        data.get("account_id", "")
    ).strip()

    if not app_id:
        return jsonify({
            "ok": False,
            "error": "App ID is required.",
        }), 400

    if not token:
        return jsonify({
            "ok": False,
            "error": "PAT/API token is required.",
        }), 400

    if not account_id:
        return jsonify({
            "ok": False,
            "error": "Account ID is required.",
        }), 400

    try:

        accounts = get_accounts(
            app_id,
            token,
        )

        selected_account = None

        for account in accounts:

            if (
                account.get("account_id")
                == account_id
            ):
                selected_account = account
                break

        if selected_account is None:
            return jsonify({
                "ok": False,
                "error": (
                    "Account ID was not found "
                    "for this PAT."
                ),
            }), 400

        ws_url = get_otp_url(
            app_id,
            token,
            account_id,
        )

        state["app_id"] = app_id
        state["account_id"] = account_id

        state["account_type"] = (
            selected_account.get(
                "account_type",
                "",
            )
        )

        state["last_error"] = ""

        thread = threading.Thread(
            target=connect_websocket,
            args=(ws_url,),
            daemon=True,
        )

        thread.start()

        return jsonify({
            "ok": True,
            "message": (
                "WebSocket connection "
                "started."
            ),
            "account": {
                "account_id": account_id,
                "account_type":
                    state["account_type"],
            },
        })

    except Exception as exc:

        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)

        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 400


@app.get("/api/status")
def api_status():

    return jsonify({
        "connected":
            state["connected"],

        "authorized":
            state["authorized"],

        "balance":
            state["balance"],

        "currency":
            state["currency"],

        "account_id":
            state["account_id"],

        "account_type":
            state["account_type"],

        "running":
            state["running"],

        "last_error":
            state["last_error"],
    })


@app.post("/api/start")
def api_start():

    if not state["authorized"]:
        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first.",
        }), 400

    state["running"] = True

    return jsonify({
        "ok": True,
    })


@app.post("/api/pause")
def api_pause():

    state["running"] = False

    return jsonify({
        "ok": True,
    })


@app.post("/api/stop")
def api_stop():

    state["running"] = False

    return jsonify({
        "ok": True,
    })


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
    )
