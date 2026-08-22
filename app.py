import json
import threading
import time

import requests
import websocket

from flask import Flask, jsonify, render_template, request


app = Flask(__name__)

REST_BASE = "https://api.derivws.com"

ws = None
ws_lock = threading.Lock()

state = {
    "connected": False,
    "authorized": False,
    "balance": 0,
    "currency": "USD",
    "account_id": "",
    "account_type": "",
    "last_error": "",
    "logs": [],
}


def log(message):
    t = time.strftime("%H:%M:%S")

    state["logs"].insert(
        0,
        {
            "time": t,
            "message": str(message),
        },
    )

    state["logs"] = state["logs"][:100]

    print(
        f"[{t}] {message}",
        flush=True,
    )


def rest_error(response):

    try:
        data = response.json()
    except Exception:
        return (
            f"HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    errors = data.get("errors")

    if isinstance(errors, list) and errors:

        e = errors[0]

        return (
            f"HTTP {response.status_code} | "
            f"{e.get('code', 'Unknown')} | "
            f"{e.get('message', 'Unknown error')}"
        )

    return (
        f"HTTP {response.status_code}: "
        f"{json.dumps(data)[:500]}"
    )


# ==========================================================
# STEP 1 — GET ACCOUNTS
# ==========================================================

def get_accounts(app_id, token):

    url = (
        REST_BASE
        + "/trading/v1/options/accounts"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    log("STEP 1: GET /options/accounts")

    r = requests.get(
        url,
        headers=headers,
        timeout=20,
    )

    log(
        f"Accounts HTTP status: {r.status_code}"
    )

    if not r.ok:

        raise RuntimeError(
            rest_error(r)
        )

    data = r.json()

    log(
        "Accounts response received."
    )

    accounts = data.get(
        "data",
        []
    )

    if isinstance(accounts, dict):
        accounts = [accounts]

    if not accounts:

        raise RuntimeError(
            "PAT accepted but no Options "
            "account was returned."
        )

    return accounts


# ==========================================================
# STEP 2 — OTP
# ==========================================================

def get_otp(
    app_id,
    token,
    account_id,
):

    url = (
        REST_BASE
        + "/trading/v1/options/accounts/"
        + account_id
        + "/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    log(
        "STEP 2: requesting OTP..."
    )

    r = requests.post(
        url,
        headers=headers,
        timeout=20,
    )

    log(
        f"OTP HTTP status: {r.status_code}"
    )

    if not r.ok:

        raise RuntimeError(
            rest_error(r)
        )

    data = r.json()

    payload = data.get(
        "data",
        {}
    )

    ws_url = payload.get(
        "url"
    )

    if not ws_url:

        raise RuntimeError(
            "OTP succeeded but Deriv "
            "did not return data.url."
        )

    log(
        "STEP 2 OK: WebSocket URL received."
    )

    return ws_url


# ==========================================================
# WEBSOCKET
# ==========================================================

def ws_open(socket):

    state["connected"] = True

    log(
        "STEP 3 OK: WebSocket OPEN."
    )

    # Ask balance
    socket.send(
        json.dumps({
            "balance": 1,
            "req_id": 100,
        })
    )

    log(
        "Balance request sent."
    )


def ws_message(
    socket,
    message,
):

    log(
        "WS MESSAGE: "
        + message[:1000]
    )

    try:

        data = json.loads(message)

    except Exception:

        return

    msg_type = data.get(
        "msg_type"
    )

    if msg_type == "error":

        error = data.get(
            "error",
            {}
        )

        code = error.get(
            "code",
            "UNKNOWN"
        )

        msg = error.get(
            "message",
            "Unknown error"
        )

        state["last_error"] = (
            f"{code}: {msg}"
        )

        state["authorized"] = False

        log(
            f"WS ERROR: {code}: {msg}"
        )

        return

    if msg_type == "balance":

        balance = data.get(
            "balance",
            {}
        )

        try:

            state["balance"] = float(
                balance.get(
                    "balance",
                    0
                )
            )

        except Exception:

            state["balance"] = 0

        state["currency"] = (
            balance.get(
                "currency"
            )
            or "USD"
        )

        state["authorized"] = True

        log(
            "STEP 4 OK: ACCOUNT AUTHORIZED."
        )

        log(
            "Balance = "
            f"{state['balance']} "
            f"{state['currency']}"
        )

        return


def ws_error(
    socket,
    error,
):

    state["connected"] = False

    state["authorized"] = False

    state["last_error"] = str(
        error
    )

    log(
        f"WS ERROR CALLBACK: {error}"
    )


def ws_close(
    socket,
    code,
    reason,
):

    state["connected"] = False

    state["authorized"] = False

    log(
        f"WS CLOSED: code={code} "
        f"reason={reason}"
    )


def start_ws(ws_url):

    global ws

    try:

        log(
            "STEP 3: opening WebSocket..."
        )

        socket = websocket.WebSocketApp(
            ws_url,
            on_open=ws_open,
            on_message=ws_message,
            on_error=ws_error,
            on_close=ws_close,
        )

        with ws_lock:
            ws = socket

        socket.run_forever(
            ping_interval=20,
            ping_timeout=10,
        )

    except Exception as e:

        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(e)

        log(
            f"WS THREAD EXCEPTION: {e}"
        )


# ==========================================================
# CONNECT
# ==========================================================

@app.post("/api/connect")
def connect():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    app_id = str(
        data.get(
            "app_id",
            ""
        )
    ).strip()

    token = str(
        data.get(
            "token",
            ""
        )
    ).strip()

    account_id = str(
        data.get(
            "account_id",
            ""
        )
    ).strip()

    if not app_id:

        return jsonify({
            "ok": False,
            "error": "App ID is empty.",
        }), 400

    if not token:

        return jsonify({
            "ok": False,
            "error": "PAT/API Token is empty.",
        }), 400

    if not account_id:

        return jsonify({
            "ok": False,
            "error": "Account ID is empty.",
        }), 400

    try:

        log(
            "=========================="
        )

        log(
            "NEW DERIV CONNECTION"
        )

        # STEP 1
        accounts = get_accounts(
            app_id,
            token,
        )

        found = None

        for account in accounts:

            aid = str(
                account.get(
                    "account_id",
                    ""
                )
            ).strip()

            log(
                f"Account returned: {aid}"
            )

            if aid == account_id:

                found = account

        if found is None:

            raise RuntimeError(
                "Account ID "
                f"{account_id} "
                "was NOT returned by Deriv."
            )

        state["account_id"] = account_id

        state["account_type"] = (
            found.get(
                "account_type",
                ""
            )
        )

        log(
            "STEP 1 OK: account found."
        )

        # STEP 2
        ws_url = get_otp(
            app_id,
            token,
            account_id,
        )

        # IMPORTANT:
        # Don't print the OTP URL because it contains
        # the one-time credential.

        log(
            "OTP URL obtained. "
            "Connecting immediately..."
        )

        # STEP 3
        thread = threading.Thread(
            target=start_ws,
            args=(ws_url,),
            daemon=True,
        )

        thread.start()

        return jsonify({
            "ok": True,
            "message":
                "Connection sequence started.",
        })

    except Exception as e:

        state["connected"] = False
        state["authorized"] = False

        state["last_error"] = str(e)

        log(
            f"CONNECT FAILED: {e}"
        )

        return jsonify({
            "ok": False,
            "error": str(e),
        }), 400


# ==========================================================
# STATUS
# ==========================================================

@app.get("/api/status")
def status():

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

        "last_error":
            state["last_error"],

        "logs":
            state["logs"],
    })


# ==========================================================
# HOME
# ==========================================================

@app.route("/")
def home():

    return render_template(
        "index.html"
    )


# ==========================================================
# RUN
# ==========================================================

if __name__ == "__main__":

    import os

    port = int(
        os.environ.get(
            "PORT",
            "5000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
    )
