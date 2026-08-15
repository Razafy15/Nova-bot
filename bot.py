import requests
import websocket
import json
import threading
import math
import random
import os
import datetime
import time
from flask import Flask, render_template_string, jsonify
from flask_cors import CORS

# ==========================================
# FIDIANA KAONTY DEMO NA REAL
# ==========================================
USE_DEMO = True 
APP_ID = "342OQxSDH634DTzSs0Ble"
AUTH_TOKEN = "pat_f9458c289641c9ab5e3da1eb03b46762ed297a5d6c6442905c718f3926a57d5e"
DEMO_ACCOUNT_ID = "DOT92654388"
REAL_ACCOUNT_ID = "ROT91403302"

if USE_DEMO:
    ACCOUNT_ID = DEMO_ACCOUNT_ID
else:
    ACCOUNT_ID = REAL_ACCOUNT_ID

# ==========================================
# WEB INTERFACE (NovaBot Design)
# ==========================================
app = Flask(__name__)
CORS(app)

ui_symbol = "1HZ25V"
all_ui_candles = {}

web_stats = {
    'balance': 10000.0, 'profit': 0.0,
    'wins': 0, 'losses': 0, 'win_rate': 0, 'total_trades': 0,
    'active_pos': 0, 'waiting_pos': 0,
    'mode': 'DEMO' if USE_DEMO else 'REEL',
    'active': True,
    'ui_candles': [], 'current_price': 0,
    'entry_level': 0, 'sl_level': 0, 'tp1_level': 0, 'tp2_level': 0,
    'signal_markers': []
}

HTML_INTERFACE = """
<!DOCTYPE html>
<html lang="mg">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>NovaBot</title>
<script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
<style>
*{box-sizing:border-box;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
body{background:#0b0d15;color:#e0e0e0;padding:15px;margin:0}
.header{text-align:center;background:linear-gradient(135deg, #00d1ff 0%, #6a11cb 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-weight:800;font-size:22px;letter-spacing:1px;margin-bottom:15px}
.toggle-mode-box{background:#1c1f2b;border-radius:20px;padding:3px;display:flex;justify-content:space-between;align-items:center;margin-bottom:15px;border:1px solid #2c3145}
.toggle-btn{flex:1;text-align:center;padding:8px 0;font-size:13px;font-weight:bold;border-radius:15px;cursor:pointer;color:#666;transition:0.3s}
.toggle-btn.active{background:#00d1ff;color:#0b0d15;box-shadow:0 0 10px rgba(0,209,255,0.3)}
.market-selector{width:100%;padding:10px;background:#1c1f2b;border:1px solid #2c3145;border-radius:12px;color:#00d1ff;font-weight:bold;margin-bottom:15px;text-align:center;outline:none}
.card{background:#131720;border-radius:16px;padding:15px;margin-bottom:15px;border:1px solid #242b3d}
.balance-card{display:flex;justify-content:space-between;align-items:center;background:#0b0e16;border-radius:12px;padding:15px}
.bal-text{font-size:26px;font-weight:bold;color:#00d1ff}
.bal-text span{font-size:13px;color:#777;font-weight:normal}
.status-bar{display:flex;justify-content:space-between;align-items:center;background:#0f1219;border-radius:10px;padding:10px 15px;border:1px solid #242b3d;margin-bottom:15px}
.dot{width:10px;height:10px;border-radius:50%;display:inline-block;margin-right:5px}
.dot-green{background:#28a745;box-shadow:0 0 8px #28a745}
.dot-grey{background:#444}
.stats-grid{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:8px;margin-bottom:15px}
.stat-box{background:#0f1219;padding:10px 5px;text-align:center;border-radius:10px;border:1px solid #242b3d}
.stat-box h4{font-size:11px;color:#777;margin:0 0 5px}
.stat-box .val{font-size:16px;font-weight:bold}
.win-c{color:#00d1ff}.loss-c{color:#ff4976}.rate-c{color:#28a745}.total-c{color:#d3d3d3}
.profit-box{background:#0b0e16;border-radius:10px;padding:15px;margin-bottom:15px;border:1px solid #242b3d}
.profit-box h4{font-size:12px;color:#777;margin:0}
.profit-box .val{font-size:22px;font-weight:bold;color:#00d1ff}
#chart-container{height:210px;width:100%;position:relative;background:#0b0d15;border-radius:0 0 12px 12px}
.chart-header{display:flex;justify-content:space-between;padding:10px 15px;font-size:12px;color:#777}
.position-box{background:#0f1219;border-radius:10px;padding:12px;border:1px solid #242b3d;display:flex;align-items:center;gap:12px;margin-top:10px}
.controls-bot{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:10px;margin-top:15px}
.btn-ctrl{background:#1c1f2b;border:1px solid #2c3145;padding:12px 0;border-radius:10px;color:#888;font-weight:bold;cursor:pointer;text-align:center;font-size:12px;transition:0.2s}
.btn-start{background:#00d1ff;color:#0b0d15;border:none}
.btn-stop{background:#ff4976;color:#fff;border:none}
.btn-ctrl:active{transform:scale(0.95)}
</style>
</head>
<body>
<div class="header">TRADING NOVABOT</div>

<!-- Mode Toggle & Market Select -->
<div class="toggle-mode-box">
    <div class="toggle-btn" id="btn-reel" onclick="switchMode('REEL')">REEL</div>
    <div class="toggle-btn active" id="btn-demo" onclick="switchMode('DEMO')">DEMO</div>
</div>
<select class="market-selector" id="symbol-selector" onchange="changeChart()">
    <option value="1HZ25V">📊 Volatility 25</option>
    <option value="1HZ50V">📊 Volatility 50</option>
    <option value="BOOM150">💥 Boom 150</option>
</select>

<!-- Balance -->
<div class="card">
    <div class="balance-card">
        <div>
            <div style="font-size:11px;color:#777">BALANCE</div>
            <div class="bal-text" id="balance">0.00<span> USD</span></div>
            <div style="font-size:10px;color:#aaa;margin-top:4px">Account: <span id="account-mode">DEMO</span></div>
        </div>
        <div style="font-size:35px;opacity:0.6">💳</div>
    </div>
</div>

<!-- Active / Desactive Status -->
<div class="status-bar">
    <div style="font-size:13px;font-weight:bold">
        <span class="dot dot-green" id="status-dot"></span> <span id="status-label">ACTIVE</span>
    </div>
</div>

<!-- Stats Grid -->
<div class="stats-grid">
    <div class="stat-box"><h4>🏆 WIN</h4><div class="val win-c" id="win">0</div></div>
    <div class="stat-box"><h4>📉 LOS</h4><div class="val loss-c" id="los">0</div></div>
    <div class="stat-box"><h4>🎯 WIN RATE</h4><div class="val rate-c" id="win-rate">0%</div></div>
    <div class="stat-box"><h4>📊 TOTAL</h4><div class="val total-c" id="total">0</div></div>
</div>

<!-- Profit -->
<div class="profit-box">
    <h4>PROFIT</h4>
    <div class="val" id="profit">+0.00 USD</div>
</div>

<!-- CHART -->
<div class="card" style="padding:0;overflow:hidden;border-radius:16px">
    <div class="chart-header">
        <span>POSITION ATTENTE (1/2) <span id="pos-wait" style="background:#1c1f2b;padding:2px 8px;border-radius:10px;font-size:10px">0 / 2</span></span>
    </div>
    <div id="chart-container"></div>
</div>

<!-- Position Active -->
<div class="card">
    <div style="font-size:12px;color:#777;display:flex;justify-content:space-between;margin-bottom:10px">
        <span>POSITION ACTIVE <span id="pos-active-badge" style="background:#1c1f2b;padding:2px 8px;border-radius:10px;font-size:10px;color:#aaa">0 / 2</span></span>
    </div>
    <div class="position-box">
        <div style="background:#1a1f2a;padding:8px;border-radius:8px;text-align:center;font-size:10px;font-weight:bold;color:#00d1ff" id="pos-type">CALL</div>
        <div style="flex:1;font-size:12px">
            <div style="font-weight:bold" id="pos-symbol">Volatility 25</div>
            <div style="color:#777;font-size:11px">Entry: <b id="pos-entry">0.00</b> | Profit: <b id="pos-profit" style="color:#00d1ff">0.00 USD</b></div>
        </div>
        <div style="font-size:16px;color:#00d1ff;font-weight:bold" id="pos-timer">00:00</div>
    </div>
</div>

<!-- Bottom Controls -->
<div class="controls-bot">
    <button class="btn-ctrl btn-start" id="btn-start" onclick="controlBot('start')">▶ START BOT</button>
    <button class="btn-ctrl btn-stop" id="btn-stop" onclick="controlBot('stop')">⏹ STOP BOT</button>
    <button class="btn-ctrl">⚙️ SETTINGS</button>
    <button class="btn-ctrl">📄 LOGS</button>
</div>

<script>
// Chart Setup
const container = document.getElementById('chart-container');
const chart = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 210,
    layout: { background: {color: 'transparent'}, textColor: '#a0a0a0' },
    grid: { vertLines: {color: 'rgba(255,255,255,0.03)'}, horzLines: {color: 'rgba(255,255,255,0.03)'} },
    timeScale: { timeVisible: true, secondsVisible: false, borderColor: 'rgba(255,255,255,0.1)' }
});
// Candlestick Series (Bougie)
const candleSeries = chart.addCandlestickSeries({
    upColor: '#00d1ff', downColor: '#ff4976',
    borderUpColor: '#00d1ff', borderDownColor: '#ff4976',
    wickUpColor: '#00d1ff', wickDownColor: '#ff4976'
});
// Price Line
const priceLine = chart.addLineSeries({ color: '#ffffff', lineWidth: 1, lineStyle: 3 });
// SMC Levels (SL, TP, Entry)
const entryLine = chart.addLineSeries({ color: '#ffffff', lineWidth: 1, lineStyle: 2 });
const slLine = chart.addLineSeries({ color: '#ff4976', lineWidth: 1, lineStyle: 2 });
const tp1Line = chart.addLineSeries({ color: '#00d1ff', lineWidth: 1, lineStyle: 2 });
const tp2Line = chart.addLineSeries({ color: '#9d4edd', lineWidth: 1, lineStyle: 2 });

async function updateUI() {
    try {
        const res = await fetch('/api/stats'); const data = await res.json();
        document.getElementById('balance').innerHTML = data.balance.toFixed(2) + '<span> USD</span>';
        document.getElementById('account-mode').innerText = data.mode;
        document.getElementById('profit').innerText = (data.profit >= 0 ? '+' : '') + data.profit.toFixed(2) + ' USD';
        document.getElementById('win').innerText = data.wins; document.getElementById('los').innerText = data.losses;
        document.getElementById('total').innerText = data.total_trades;
        document.getElementById('win-rate').innerText = data.win_rate + '%';
        document.getElementById('pos-wait').innerText = data.waiting_pos + ' / 2';
        document.getElementById('pos-active-badge').innerText = data.active_pos + ' / 2';
        document.getElementById('pos-profit').innerText = data.profit.toFixed(2) + ' USD';

        if (data.ui_candles && data.ui_candles.length > 0) {
            candleSeries.setData(data.ui_candles);
            if (data.signal_markers && data.signal_markers.length > 0) {
                candleSeries.setMarkers(data.signal_markers);
            }
            if (data.entry_level > 0) {
                const t = Math.floor(Date.now()/1000);
                entryLine.setData([{time: t-100, value: data.entry_level}, {time: t, value: data.entry_level}]);
                slLine.setData([{time: t-100, value: data.sl_level}, {time: t, value: data.sl_level}]);
                tp1Line.setData([{time: t-100, value: data.tp1_level}, {time: t, value: data.tp1_level}]);
                tp2Line.setData([{time: t-100, value: data.tp2_level}, {time: t, value: data.tp2_level}]);
            } else {
                entryLine.setData([]); slLine.setData([]); tp1Line.setData([]); tp2Line.setData([]);
            }
            const lastCandle = data.ui_candles[data.ui_candles.length - 1];
            const t = Math.floor(Date.now()/1000);
            priceLine.setData([{time: t-60, value: lastCandle.close}, {time: t, value: lastCandle.close}]);
        }
    } catch(e) {}
}
setInterval(updateUI, 1000); updateUI();

async function switchMode(mode) {
    document.getElementById('btn-reel').classList.remove('active');
    document.getElementById('btn-demo').classList.remove('active');
    if(mode === 'REEL') document.getElementById('btn-reel').classList.add('active');
    else document.getElementById('btn-demo').classList.add('active');
    await fetch('/api/toggle');
    updateUI();
}

async function changeChart() {
    const symbol = document.getElementById('symbol-selector').value;
    await fetch('/api/select_chart/'+symbol);
    updateUI();
}

async function controlBot(action) {
    const res = await fetch('/api/state/'+action);
    const data = await res.json();
    if(data.status === 'active'){
        document.getElementById('status-label').innerText = 'ACTIVE';
        document.getElementById('status-dot').className = 'dot dot-green';
    } else {
        document.getElementById('status-label').innerText = 'DESACTIVE';
        document.getElementById('status-dot').className = 'dot dot-grey';
    }
}
</script>
</body>
</html>
"""
@app.route('/')
def index():
    return render_template_string(HTML_INTERFACE)

@app.route('/api/stats')
def get_stats():
    return jsonify(web_stats)

@app.route('/api/toggle')
def toggle_mode():
    global USE_DEMO, ACCOUNT_ID
    USE_DEMO = not USE_DEMO
    if USE_DEMO:
        ACCOUNT_ID = DEMO_ACCOUNT_ID
    else:
        ACCOUNT_ID = REAL_ACCOUNT_ID
    web_stats['mode'] = 'DEMO' if USE_DEMO else 'REEL'
    try:
        ws.close()
    except:
        pass
    return jsonify({'status': 'ok'})

@app.route('/api/select_chart/<symbol>')
def select_chart(symbol):
    global ui_symbol
    if symbol in ["1HZ25V", "1HZ50V", "BOOM150"]:
        ui_symbol = symbol
        web_stats['ui_candles'] = all_ui_candles.get(ui_symbol, [])
    return jsonify({'status': 'ok'})

@app.route('/api/state/<state>')
def bot_state(state):
    global bot_active
    bot_active = (state == 'start')
    web_stats['active'] = bot_active
    return jsonify({'status': 'active' if bot_active else 'desactive'})

def start_web():
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# ==========================================
# BOT CORE & STRATEGY (SMC & BOOM)
# ==========================================
def get_websocket_url():
    url = f"https://api.derivws.com/trading/v1/options/accounts/{ACCOUNT_ID}/otp"
    headers = {"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {AUTH_TOKEN}"}
    try:
        response = requests.post(url, headers=headers, timeout=15)
        return response.json()['data']['url']
    except:
        return None

MAX_PROFIT_PERCENT = 20
MAX_LOSS_PERCENT = 20
MAX_TRADES = 2
CONTRACT_DURATION = 2
SYMBOLS = ["1HZ25V", "1HZ50V", "BOOM150"]

current_symbol_index = 0
balance = 10000
start_balance = 10000
tf_data = {"1m": [], "5m": [], "15m": [], "30m": [], "1h": []}
last_resample_time = {"5m": 0, "15m": 0, "30m": 0, "1h": 0}
trade_active = False
decision = None
active_trades = 0
trades_today = 0
profit_win = 0
profit_loss = 0
bot_active = True
entry_found = False
current_symbol = SYMBOLS[0]
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 10
DATA_FILE = "bot_data.txt"
stake_amount = 0.35
boom_tp_hit = False
boom_stake = 0.35

def get_today():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def load_data():
    if not os.path.exists(DATA_FILE):
        return None
    with open(DATA_FILE, "r") as f:
        lines = f.readlines()
        if len(lines) >= 2:
            return lines[0].strip(), float(lines[1].strip())
    return None

def save_data(balance):
    with open(DATA_FILE, "w") as f:
        f.write(f"{get_today()}\n{balance}\n")

def check_daily_limit(start_bal, current_bal):
    profit_pct = ((current_bal - start_bal) / start_bal) * 100
    return profit_pct >= MAX_PROFIT_PERCENT or profit_pct <= -MAX_LOSS_PERCENT

def ema(data, period):
    if len(data) < period:
        return None
    multiplier = 2 / (period + 1)
    ema_value = data[0]
    for price in data[1:]:
        ema_value = (price * multiplier) + (ema_value * (1 - multiplier))
    return ema_value

def find_swing(closes, period=10):
    if len(closes) < period:
        return None, None
    return max(closes[-period:]), min(closes[-period:])

def detect_bos_choch(closes):
    if len(closes) < 5:
        return None
    recent_high, recent_low = find_swing(closes)
    if recent_high is None:
        return None
    last_high = max(closes[-5:-1]) if len(closes) > 5 else closes[-1]
    last_low = min(closes[-5:-1]) if len(closes) > 5 else closes[-1]
    current = closes[-1]
    if current > recent_high:
        return "BOS_UPTREND"
    elif current < recent_low:
        return "BOS_DOWNTREND"
    elif current < last_low and current < recent_low:
        return "CHoCH_DOWN"
    elif current > last_high and current > recent_high:
        return "CHoCH_UP"
    return None

def detect_fvg(closes):
    if len(closes) < 3:
        return None
    if closes[-1] > closes[-3] and closes[-2] < closes[-3]:
        return "FVG_UP"
    elif closes[-1] < closes[-3] and closes[-2] > closes[-3]:
        return "FVG_DOWN"
    return None

def detect_liquidity_sweep(closes_5, highs_list, lows_list):
    if len(closes_5) < 5:
        return None
    recent_high = max(highs_list[-5:]) if len(highs_list) >= 5 else max(highs_list)
    recent_low = min(lows_list[-5:]) if len(lows_list) >= 5 else min(lows_list)
    current = closes_5[-1]
    if current > recent_high:
        return "SWEEP_HIGH"
    elif current < recent_low:
        return "SWEEP_LOW"
    return None

def resample_candles():
    if len(tf_data["1m"]) < 2:
        return
    cur = time.time()
    tfs = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}
    for k, mins in tfs.items():
        if cur - last_resample_time[k] > (mins * 60):
            last_resample_time[k] = cur
            if len(tf_data["1m"]) >= mins:
                g = tf_data["1m"][-mins:]
                tf_data[k].append({
                    'time': g[-1]['time'],
                    'open': g[0]['open'],
                    'high': max([x['high'] for x in g]),
                    'low': min([x['low'] for x in g]),
                    'close': g[-1]['close']
                })

def rsi(data, period=14):
    if len(data) < period + 1:
        return 50
    gains = 0
    losses = 0
    for i in range(-period, 0):
        diff = data[i] - data[i-1]
        if diff > 0:
            gains += diff
        else:
            losses += abs(diff)
    if losses == 0:
        return 100
    rs = gains / losses
    return 100 - (100 / (1 + rs))

def smc_strategy_mtf():
    global entry_found, web_stats
    if not (8 <= datetime.datetime.now().hour <= 16):
        return None
    if len(tf_data["5m"]) < 5 or len(tf_data["15m"]) < 10 or len(tf_data["1h"]) < 15:
        return None

    e1h = ema([c['close'] for c in tf_data["1h"]], 20)
    if e1h is None:
        return None

    c15 = [c['close'] for c in tf_data["15m"]]
    h15 = [c['high'] for c in tf_data["15m"]]
    l15 = [c['low'] for c in tf_data["15m"]]
    str15 = detect_bos_choch(c15)
    fvg15 = detect_fvg(c15)

    c5 = [c['close'] for c in tf_data["5m"]]
    h5 = [c['high'] for c in tf_data["5m"]]
    l5 = [c['low'] for c in tf_data["5m"]]
    sweep = detect_liquidity_sweep(c5, h5, l5)

    buy = False
    sell = False
    score = 0
    if c15[-1] > e1h:
        if sweep == "SWEEP_HIGH":
            score += 1
        if str15 in ["BOS_UPTREND", "CHoCH_UP"]:
            score += 1
        if fvg15 == "FVG_UP":
            score += 1
        if score >= 3:
            buy = True
            swing_high, swing_low = find_swing(c15)
            web_stats['sl_level'] = swing_low or c15[-1] * 0.95
            web_stats['tp1_level'] = swing_high or c15[-1] * 1.05
    elif c15[-1] < e1h:
        if sweep == "SWEEP_LOW":
            score += 1
        if str15 in ["BOS_DOWNTREND", "CHoCH_DOWN"]:
            score += 1
        if fvg15 == "FVG_DOWN":
            score += 1
        if score >= 3:
            sell = True
            swing_high, swing_low = find_swing(c15)
            web_stats['sl_level'] = swing_high or c15[-1] * 1.05
            web_stats['tp1_level'] = swing_low or c15[-1] * 0.95

    if buy or sell:
        entry_found = True
        return "CALL" if buy else "PUT"
    return None

def boom_strategy():
    global entry_found, boom_tp_hit
    if len(tf_data["1m"]) < 15:
        return None
    c = [x['close'] for x in tf_data["1m"]]
    r = rsi(c)
    if boom_tp_hit:
        if r > 85 and c[-1] > c[-2] and c[-2] > c[-3] and c[-3] > c[-4]:
            boom_tp_hit = False
            entry_found = True
            return "PUT"
        return None
    if r > 80 and c[-1] > c[-2] and c[-2] > c[-3]:
        entry_found = True
        return "PUT"
    return None

def on_message(ws, message):
    global balance, start_balance, tf_data, trade_active, active_trades, trades_today, profit_win, profit_loss, bot_active, entry_found, current_symbol, web_stats, decision, boom_tp_hit, ui_symbol, all_ui_candles
    try:
        data = json.loads(message)
        msg_type = data.get('msg_type')
        if msg_type == 'balance':
            balance = float(data['balance']['balance'])
            web_stats['balance'] = balance
            web_stats['profit'] = balance - start_balance
            if check_daily_limit(start_balance, balance):
                save_data(balance)
                print("Daily limit reached.")
                ws.close()
                return

        elif msg_type == 'candles':
            if not bot_active:
                return
            sym = data['candles']['symbol']
            for c in data['candles']['data']:
                ts = int(c['epoch'] / 1000)
                tf_data["1m"].append({
                    'time': ts,
                    'open': c['open'],
                    'high': c['high'],
                    'low': c['low'],
                    'close': c['close']
                })
                if sym not in all_ui_candles:
                    all_ui_candles[sym] = []
                all_ui_candles[sym].append({
                    'time': ts,
                    'open': c['open'],
                    'high': c['high'],
                    'low': c['low'],
                    'close': c['close']
                })
            if len(tf_data["1m"]) > 200:
                tf_data["1m"] = tf_data["1m"][-200:]
            if len(all_ui_candles.get(sym, [])) > 200:
                all_ui_candles[sym] = all_ui_candles[sym][-200:]
            if ui_symbol == sym:
                web_stats['ui_candles'] = all_ui_candles[sym][-50:]
            resample_candles()

            if not trade_active and active_trades < MAX_TRADES:
                if current_symbol == "BOOM150":
                    decision = boom_strategy()
                    sl_val = 0.15
                    stake = boom_stake
                    tp_is_amount = True
                    tp_val = 0.05
                else:
                    decision = smc_strategy_mtf()
                    rm = {
                        "1HZ25V": {"sl": 0.28, "tp": 0.45},
                        "1HZ50V": {"sl": 0.28, "tp": 0.50}
                    }
                    p = rm.get(current_symbol, {"sl": 0.28, "tp": 0.45})
                    sl_val = p["sl"]
                    stake = stake_amount
                    tp_is_amount = False
                    tp_val = p["tp"]

                if decision and entry_found:
                    current_price = tf_data["1m"][-1]['close']
                    if decision == "CALL":
                        sl_price = current_price * (1 - sl_val)
                        tp_price = current_price + tp_val if tp_is_amount else current_price * (1 + tp_val)
                    else:
                        sl_price = current_price * (1 + sl_val)
                        tp_price = current_price - tp_val if tp_is_amount else current_price * (1 - tp_val)
                    web_stats['entry_level'] = current_price
                    web_stats['sl_level'] = sl_price
                    web_stats['tp1_level'] = tp_price
                    web_stats['signal_markers'] = [{
                        'time': tf_data["1m"][-1]['time'],
                        'position': 'belowBar' if decision == 'CALL' else 'aboveBar',
                        'color': '#00d1ff' if decision == 'CALL' else '#ff4976',
                        'shape': 'arrowUp' if decision == 'CALL' else 'arrowDown',
                        'text': decision
                    }]
                    print(f"✅ {current_symbol} - {decision} | SL: {sl_val*100}%, TP: {tp_val}{'$' if tp_is_amount else '%'}")
                    ws.send(json.dumps({
                        "proposal": 1,
                        "amount": stake,
                        "basis": "stake",
                        "contract_type": decision,
                        "currency": "USD",
                        "duration": CONTRACT_DURATION,
                        "duration_unit": "m",
                        "underlying_symbol": current_symbol,
                        "req_id": 3
                    }))
                    trade_active = True
                    entry_found = False

        elif msg_type == 'proposal':
            proposal_id = data.get('proposal', {}).get('id')
            if proposal_id:
                ws.send(json.dumps({"buy": proposal_id, "price": 1, "req_id": 4}))

        elif msg_type == 'buy' and data.get('buy', {}).get('status') == 'pending':
            contract_id = data.get('buy', {}).get('contract_id')
            active_trades += 1
            trades_today += 1
            web_stats['active_pos'] = active_trades
            if contract_id and web_stats['entry_level'] > 0:
                if current_symbol == "BOOM150":
                    ws.send(json.dumps({
                        "contract_update": 1,
                        "contract_id": contract_id,
                        "stop_loss": round(web_stats['sl_level'], 2),
                        "take_profit": round(web_stats['tp1_level'], 2)
                    }))
                    print(f"🛡️ BOOM - SL: {round(web_stats['sl_level'], 2)}, TP: {round(web_stats['tp1_level'], 2)}")
                else:
                    ws.send(json.dumps({
                        "contract_update": 1,
                        "contract_id": contract_id,
                        "stop_loss": round(web_stats['sl_level'], 2),
                        "take_profit": round(web_stats['tp1_level'], 2),
                        "trailing_stop": 1.5,
                        "trailing_stop_unit": "percent"
                    }))
                    print(f"🛡️ Volatility - SL: {round(web_stats['sl_level'], 2)}, TP: {round(web_stats['tp1_level'], 2)}, Trailing 1.5%")
            current_symbol_index = (current_symbol_index + 1) % len(SYMBOLS)
            current_symbol = SYMBOLS[current_symbol_index]
            trade_active = False
            ws.send(json.dumps({"forget_all": 1}))
            ws.send(json.dumps({
                "candles": current_symbol,
                "adjust_start_time": 1,
                "count": 200,
                "subscribe": 1,
                "req_id": 2
            }))

        elif msg_type == 'proposal_open_contract' and data.get('proposal_open_contract', {}).get('status') == 'sold':
            profit = data['proposal_open_contract']['profit']
            if profit > 0:
                profit_win += 1
                web_stats['wins'] = profit_win
            else:
                profit_loss += 1
                web_stats['losses'] = profit_loss
            web_stats['total_trades'] = profit_win + profit_loss
            web_stats['win_rate'] = int((profit_win / web_stats['total_trades']) * 100) if web_stats['total_trades'] > 0 else 0
            if current_symbol == "BOOM150" and profit >= 0.05:
                boom_tp_hit = True
                print(f"🎯 BOOM 150 - TP 0.05$ VOATRATRA! ({profit:.2f}$)")
            web_stats['signal_markers'] = []
            web_stats['entry_level'] = 0
            web_stats['sl_level'] = 0
            web_stats['tp1_level'] = 0
            active_trades -= 1
            trade_active = False

    except Exception as e:
        print(f"Error: {e}")
def on_error(ws, error):
    print(f"WS Error: {error}")
    ws.close()

def on_close(ws, close_status_code, close_msg):
    global reconnect_attempts
    if bot_active and reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        reconnect_attempts += 1
        print(f"Reconnecting in 10s... ({reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})")
        time.sleep(10)

def on_open(ws):
    global reconnect_attempts, current_symbol
    reconnect_attempts = 0
    def send_ping():
        try:
            if bot_active:
                ws.send(json.dumps({"ping": 1, "req_id": 999}))
                threading.Timer(30, send_ping).start()
        except:
            pass
    send_ping()
    ws.send(json.dumps({"balance": 1, "subscribe": 1, "req_id": 1}))
    ws.send(json.dumps({"candles": current_symbol, "adjust_start_time": 1, "count": 200, "subscribe": 1, "req_id": 2}))

def main():
    global start_balance, balance
    data = load_data()
    today = get_today()
    if data and data[0] == today:
        print(f"Already ran today.")
        return
    ws_url = get_websocket_url()
    if not ws_url:
        return
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    try:
        ws.run_forever()
    except:
        pass

if __name__ == "__main__":
    import os
    port = int(os.environ.get('PORT', 5000))
    threading.Thread(target=start_web, daemon=True).start()
    while True:
        main()
        print("Bot paused. Restarting in 60s...")
        time.sleep(60)
