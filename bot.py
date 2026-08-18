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
# WEB INTERFACE (Professional Dashboard)
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
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Nova — Bot Trading Deriv</title>
<script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
<style>
  :root{
    --bg:#0F1115; --panel:#171A20; --border:#262B34; --text:#E8EAED; --muted:#8A93A3;
    --accent:#6E56CF; --up:#46E39B; --down:#F0546B; --pending:#E8B23A;
  }
  *{box-sizing:border-box;}
  body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,Segoe UI,Inter,sans-serif;}
  main{max-width:480px;margin:0 auto;min-height:100vh;display:flex;flex-direction:column;}
  header{padding:20px;display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--border);}
  header .dot{width:8px;height:8px;background:var(--accent);border-radius:2px;transform:rotate(45deg);}
  header b{font-size:15px;}
  .view{display:none;padding:24px 20px 60px;flex:1;}
  .view.active{display:block;}
  h1{font-size:24px;margin:0 0 10px;line-height:1.2;}
  h2{font-size:19px;margin:0 0 4px;}
  h3{font-size:14.5px;margin:0 0 2px;}
  p.sub{color:var(--muted);font-size:13.5px;line-height:1.55;margin:0 0 24px;}
  .btn{display:block;width:100%;text-align:center;padding:13px;border-radius:9px;border:1px solid transparent;font-size:14.5px;font-weight:600;text-decoration:none;color:#fff;background:var(--accent);margin-top:10px;cursor:pointer;}
  label{display:block;font-size:12.5px;color:var(--muted);margin:16px 0 6px;}
  input,select{width:100%;background:#101319;border:1px solid var(--border);color:var(--text);padding:11px 12px;border-radius:8px;font-size:14px;}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:14px;}
  .pill{display:inline-block;font-size:11px;padding:3px 9px;border-radius:100px;border:1px solid var(--border);color:var(--muted);}
  .pill.on{color:var(--up);border-color:rgba(70,227,155,.3);}
  .stats{display:flex;gap:10px;margin-bottom:14px;}
  .stat{flex:1;background:#101319;border:1px solid var(--border);border-radius:8px;padding:10px 12px;}
  .stat .l{font-size:10.5px;color:var(--muted);}
  .stat .v{font-size:16px;font-weight:600;}
  .v.up{color:var(--up);} .v.down{color:var(--down);}
  .log{background:#000;border:1px solid var(--border);border-radius:8px;height:150px;overflow-y:auto;padding:10px;font-family:monospace;font-size:11.5px;color:var(--muted);}
  .nav{display:flex;justify-content:space-around;border-top:1px solid var(--border);padding:10px 0;position:sticky;bottom:0;background:var(--bg);}
  .nav a{color:var(--muted);font-size:11.5px;text-decoration:none;display:flex;flex-direction:column;align-items:center;gap:3px;}
  .nav a.active{color:var(--accent);}
  .toggle{width:42px;height:24px;border-radius:100px;background:#101319;border:1px solid var(--border);position:relative;cursor:pointer;}
  .toggle.on{background:var(--accent);}
  .toggle .k{position:absolute;top:2px;left:2px;width:18px;height:18px;border-radius:50%;background:#fff;transition:.2s;}
  .toggle.on .k{left:20px;}
  .row{display:flex;align-items:center;justify-content:space-between;margin-top:14px;}
  .mtabs{display:flex;gap:5px;margin-bottom:8px;flex-wrap:wrap;}
  .mtab{flex:1 1 30%;min-width:0;text-align:center;padding:8px 3px;border-radius:8px;border:1px solid var(--border);font-size:10.5px;color:var(--muted);cursor:pointer;background:#101319;}
  .mtab.active{border-color:var(--accent);color:var(--text);background:rgba(110,86,207,.12);}
  .tf-row{display:flex;gap:6px;margin-bottom:12px;}
  .tf{flex:1;text-align:center;padding:7px 2px;border-radius:7px;border:1px solid var(--border);font-size:11.5px;color:var(--muted);cursor:pointer;background:#101319;}
  .tf.active{border-color:var(--accent);color:#fff;background:var(--accent);}
  .chart-wrap{position:relative;background:#131722;border:1px solid var(--border);border-radius:10px;padding:6px 0 4px;margin-bottom:6px;}
  canvas{display:block;width:100%;height:230px;touch-action:none;}
  .chart-legend{display:flex;gap:12px;font-size:10px;color:var(--muted);padding:5px 10px 0;flex-wrap:wrap;}
  .dotL{width:8px;height:8px;border-radius:50%;display:inline-block;}
  .stake-row{display:flex;align-items:center;gap:8px;}
  .stake-row input{text-align:center;font-size:16px;font-weight:600;}
  .stepbtn{width:38px;height:38px;flex:none;border-radius:8px;border:1px solid var(--border);background:#101319;color:var(--text);font-size:18px;cursor:pointer;}
  .quick-row{display:flex;gap:6px;margin-top:8px;}
  .qbtn{flex:1;text-align:center;padding:6px 2px;border-radius:6px;border:1px solid var(--border);font-size:11.5px;color:var(--muted);cursor:pointer;background:#101319;}
  .sig-item{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border);}
  .sig-item:last-child{border-bottom:none;}
  .sig-badge{width:34px;height:34px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:700;flex:none;}
  .sig-badge.call{background:rgba(70,227,155,.12);color:var(--up);}
  .sig-badge.put{background:rgba(240,84,107,.12);color:var(--down);}
  .sig-info{flex:1;}
  .sig-info .m{font-size:13px;font-weight:600;}
  .sig-info .d{font-size:11.5px;color:var(--muted);}
  .sig-status{font-size:10.5px;padding:3px 8px;border-radius:100px;white-space:nowrap;}
  .sig-status.pend{color:var(--pending);background:rgba(232,178,58,.12);}
  .sig-status.conf{color:var(--up);background:rgba(70,227,155,.12);}
</style>
</head>
<body>
<main>
  <header><span class="dot"></span><b>Nova</b></header>
  <section class="view active" id="v-landing">
    <span class="pill">Deriv synthetic indices</span>
    <h1 style="margin-top:12px;">Bot trading ho an'ny Volatility sy Boom.</h1>
    <a class="btn" href="#" onclick="go('dash');return false;">Hiditra amin'ny bot</a>
  </section>

  <section class="view" id="v-dash">
    <div class="row" style="margin-top:0;">
      <h2 style="margin:0;">Dashboard</h2>
      <span class="pill on" id="connPill">Mandeha</span>
    </div>
    
    <div class="card">
      <div class="mtabs" id="mtabs"></div>
      <div class="tf-row" id="tfRow"></div>
      <div class="chart-wrap">
        <canvas id="chart" width="440" height="230"></canvas>
      </div>
    </div>

    <div class="card">
      <h3>Signaly farany</h3>
      <div id="sigList"><div class="note">Mbola tsy misy signal.</div></div>
    </div>

    <div class="card">
      <label style="margin-top:0;">Stake (USD)</label>
      <div class="stake-row">
        <button class="stepbtn" onclick="stepStake(-1)">−</button>
        <input type="number" id="stakeInput" value="1" step="0.1" min="0.35" oninput="onStakeInput()">
        <button class="stepbtn" onclick="stepStake(1)">+</button>
      </div>
      <div class="quick-row">
        <div class="qbtn" onclick="setStake(0.35)">0.35</div>
        <div class="qbtn" onclick="setStake(1)">1</div>
        <div class="qbtn" onclick="setStake(2)">2</div>
        <div class="qbtn" onclick="setStake(5)">5</div>
        <div class="qbtn" onclick="setStake(10)">10</div>
      </div>
      <div class="row">
        <div style="font-size:13.5px;">Alefaso ny bot</div>
        <div class="toggle" id="runT" onclick="toggleRun()"><div class="k"></div></div>
      </div>
    </div>

    <div class="stats">
      <div class="stat"><div class="l">VOLA</div><div class="v up" id="stBal">—</div></div>
      <div class="stat"><div class="l">TRADE</div><div class="v" id="stTr">0</div></div>
      <div class="stat"><div class="l">P/L</div><div class="v" id="stPl">0.00</div></div>
    </div>

    <div class="log" id="logBox"><div>Miandry ny fanombohan'ny Bot...</div></div>
  </section>

  <nav class="nav">
    <a href="#" class="active" data-v="landing" onclick="go('landing');return false;">Fitaovana</a>
    <a href="#" data-v="dash" onclick="go('dash');return false;">Dashboard</a>
  </nav>
</main>
<script>
function go(v){
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v-'+v).classList.add('active');
  document.querySelectorAll('.nav a').forEach(a=>a.classList.toggle('active', a.dataset.v===v));
}

const MARKETS = ['v25','v50','boom150','boom300','boom500','boom1000','crash150','crash300','crash500','crash1000']; 
const TIMEFRAMES = ['1m','5m','15m','30m','1h'];
let activeMarket = 'v25', activeTf = '5m';
let stakeVal = 1, running = false;

function renderTabs(){
  const wrap = document.getElementById('mtabs'); wrap.innerHTML = '';
  MARKETS.forEach(m => {
    const el = document.createElement('div');
    el.className = 'mtab' + (m===activeMarket?' active':'');
    el.innerHTML = m;
    el.onclick = ()=>{ activeMarket=m; renderTabs(); };
    wrap.appendChild(el);
  });
}
function renderTf(){
  const wrap = document.getElementById('tfRow'); wrap.innerHTML = '';
  TIMEFRAMES.forEach(tf => {
    const el = document.createElement('div');
    el.className = 'tf' + (tf===activeTf?' active':'');
    el.textContent = tf;
    el.onclick = ()=>{ activeTf=tf; renderTf(); };
    wrap.appendChild(el);
  });
}
renderTabs(); renderTf();

function onStakeInput(){ stakeVal = parseFloat(document.getElementById('stakeInput').value)||1; }
function stepStake(dir){ stakeVal = Math.max(0.35, stakeVal + dir*(stakeVal<1?0.1:1)); document.getElementById('stakeInput').value=stakeVal; }
function setStake(v){ stakeVal=v; document.getElementById('stakeInput').value=v; }

async function toggleRun(){
  running = !running;
  document.getElementById('runT').classList.toggle('on', running);
  document.getElementById('logBox').innerHTML = running ? '<div>🚀 Bot nalefa. Mikaroka signal...</div>' : '<div>⏹ Bot najanona.</div>';
}

const container = document.getElementById('chart');
const initialWidth = container.clientWidth > 0 ? container.clientWidth : 380;
const chart = LightweightCharts.createChart(container, {
    width: initialWidth, height: 230,
    layout: { background: {color: 'transparent'}, textColor: '#8b95a6' },
    grid: { vertLines: {color: 'rgba(255,255,255,0.04)'}, horzLines: {color: 'rgba(255,255,255,0.04)'} }
});
const candleSeries = chart.addCandlestickSeries({
    upColor: '#46E39B', downColor: '#F0546B', borderUpColor: '#46E39B', borderDownColor: '#F0546B'
});

window.addEventListener('resize', () => {
    if(container.clientWidth > 0) {
        chart.resize(container.clientWidth, 230);
    }
});

async function updateUI() {
  try {
    const res = await fetch('/api/stats'); 
    const data = await res.json();
    document.getElementById('stBal').innerText = data.balance.toFixed(2);
    document.getElementById('stTr').innerText = data.total_trades;
    document.getElementById('stPl').innerText = data.profit.toFixed(2);
    
    if (data.ui_candles && data.ui_candles.length > 0) {
      candleSeries.setData(data.ui_candles);
      setTimeout(() => {
          if(container.clientWidth > 0) chart.resize(container.clientWidth, 230);
      }, 100);
    }
  } catch(e) { console.log("UI Error:", e); }
}
setInterval(updateUI, 1000); updateUI();
</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_INTERFACE)

@app.route('/api/stats')
def get_stats():
    # MOCK DATA (Raha mbola tsy nahazo tena angona ny WebSocket)
    if not web_stats['ui_candles']:
        mock_candles = []
        current_t = int(time.time())
        price = 300.0
        for i in range(50):
            price += random.uniform(-0.5, 0.5)
            mock_candles.append({'time': current_t - (50 - i)*60, 'open': price - 0.5, 'high': price + 0.8, 'low': price - 0.8, 'close': price})
        web_stats['ui_candles'] = mock_candles
    
    return jsonify(web_stats)

@app.route('/api/state/<state>')
def bot_state(state):
    global bot_active
    bot_active = (state == 'start')
    web_stats['active'] = bot_active
    return jsonify({'status': 'active' if bot_active else 'desactive'})

# ==========================================
# BOT CORE & STRATEGY
# ==========================================
def get_websocket_url():
    url = f"https://api.derivws.com/trading/v1/options/accounts/{ACCOUNT_ID}/otp"
    headers = {"Deriv-App-ID": APP_ID, "Authorization": f"Bearer {AUTH_TOKEN}"}
    try:
        response = requests.post(url, headers=headers, timeout=15)
        return response.json()['data']['url']
    except Exception as e:
        print(f"⚠️ Error WS URL: {e}")
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
            
            if sym not in all_ui_candles:
                all_ui_candles[sym] = []
                
            for c in data['candles']['data']:
                ts = int(c['epoch'] / 1000)
                candle_obj = {
                    'time': ts,
                    'open': c['open'],
                    'high': c['high'],
                    'low': c['low'],
                    'close': c['close']
                }
                tf_data["1m"].append(candle_obj)
                all_ui_candles[sym].append(candle_obj)
                
            if len(tf_data["1m"]) > 200:
                tf_data["1m"] = tf_data["1m"][-200:]
            if len(all_ui_candles[sym]) > 200:
                all_ui_candles[sym] = all_ui_candles[sym][-200:]
                
            if current_symbol in all_ui_candles and len(all_ui_candles[current_symbol]) > 0:
                web_stats['ui_candles'] = all_ui_candles[current_symbol][-50:]
                web_stats['current_price'] = all_ui_candles[current_symbol][-1]['close']
            
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
                        'color': '#22c55e' if decision == 'CALL' else '#ef4444',
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
                "ticks_history": current_symbol, 
                "style": "candles", 
                "granularity": 60, 
                "count": 200, 
                "end": "latest", 
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
    
    # Authorize (OAuth 2.0)
    ws.send(json.dumps({"authorize": AUTH_TOKEN, "req_id": 0}))
    ws.send(json.dumps({"balance": 1, "subscribe": 1, "req_id": 1}))
    # Ticks_history ho an'ny tena chart
    ws.send(json.dumps({"ticks_history": current_symbol, "style": "candles", "granularity": 60, "count": 200, "end": "latest", "subscribe": 1, "req_id": 2}))

def main():
    global start_balance, balance
    data = load_data()
    today = get_today()
    if data and data[0] == today:
        print("⚠️ Efa nisy varotra androany. Mampifandray fotsiny ny WebSocket mba hahazoana ny Chart.")
    ws_url = get_websocket_url()
    if not ws_url:
        return
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    try:
        ws.run_forever()
    except:
        pass

def start_bot_loop():
    while True:
        main()
        print("Bot paused. Restarting in 60s...")
        time.sleep(60)

if __name__ == "__main__":
    import os
    port = int(os.environ.get('PORT', 5000))
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False), daemon=True).start()
    threading.Thread(target=start_bot_loop, daemon=True).start()
else:
    threading.Thread(target=start_bot_loop, daemon=True).start()
