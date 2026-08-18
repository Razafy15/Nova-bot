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
# WEB INTERFACE (Professional Dashboard - Render Optimized)
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
  .btn{display:block;width:100%;text-align:center;padding:13px;border-radius:9px;border:1px solid transparent;
    font-size:14.5px;font-weight:600;text-decoration:none;color:#fff;background:var(--accent);margin-top:10px;cursor:pointer;}
  .btn.ghost{background:transparent;border-color:var(--border);color:var(--text);}
  label{display:block;font-size:12.5px;color:var(--muted);margin:16px 0 6px;}
  input,select{width:100%;background:#101319;border:1px solid var(--border);color:var(--text);
    padding:11px 12px;border-radius:8px;font-size:14px;}
  .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:14px;}
  .pill{display:inline-block;font-size:11px;padding:3px 9px;border-radius:100px;border:1px solid var(--border);color:var(--muted);}
  .pill.on{color:var(--up);border-color:rgba(70,227,155,.3);}
  .pill.err{color:var(--down);border-color:rgba(240,84,107,.3);}
  .stats{display:flex;gap:10px;margin-bottom:14px;}
  .stat{flex:1;background:#101319;border:1px solid var(--border);border-radius:8px;padding:10px 12px;}
  .stat .l{font-size:10.5px;color:var(--muted);}
  .stat .v{font-size:16px;font-weight:600;}
  .v.up{color:var(--up);} .v.down{color:var(--down);}
  .log{background:#000;border:1px solid var(--border);border-radius:8px;height:150px;overflow-y:auto;padding:10px;font-family:monospace;font-size:11.5px;color:var(--muted);}
  .log div{margin-bottom:5px;}
  .log .buy{color:var(--accent);} .log .win{color:var(--up);} .log .loss{color:var(--down);} .log .err{color:var(--down);}
  .nav{display:flex;justify-content:space-around;border-top:1px solid var(--border);padding:10px 0;position:sticky;bottom:0;background:var(--bg);}
  .nav a{color:var(--muted);font-size:11.5px;text-decoration:none;display:flex;flex-direction:column;align-items:center;gap:3px;}
  .nav a.active{color:var(--accent);}
  .toggle{width:42px;height:24px;border-radius:100px;background:#101319;border:1px solid var(--border);position:relative;cursor:pointer;}
  .toggle.on{background:var(--accent);}
  .toggle .k{position:absolute;top:2px;left:2px;width:18px;height:18px;border-radius:50%;background:#fff;transition:.2s;}
  .toggle.on .k{left:20px;}
  .row{display:flex;align-items:center;justify-content:space-between;margin-top:14px;}
  .warn{font-size:12px;color:#f0899a;background:rgba(240,84,107,.08);border:1px solid rgba(240,84,107,.25);border-radius:8px;padding:10px 12px;margin-bottom:16px;line-height:1.5;}

  .mtabs{display:flex;gap:5px;margin-bottom:8px;flex-wrap:wrap;}
  .mtab{flex:1 1 30%;min-width:0;text-align:center;padding:8px 3px;border-radius:8px;border:1px solid var(--border);font-size:10.5px;color:var(--muted);cursor:pointer;background:#101319;}
  .mtab .px{display:block;font-size:9px;margin-top:2px;font-family:monospace;}
  .mtab.active{border-color:var(--accent);color:var(--text);background:rgba(110,86,207,.12);}
  .mtab .alert-dot{position:absolute;top:-4px;right:-4px;width:9px;height:9px;border-radius:50%;background:var(--pending);box-shadow:0 0 0 2px var(--panel);animation:blink 1.1s ease-in-out infinite;}
  .mtab .alert-dot.confirmed{background:var(--up);}
  @keyframes blink{0%,100%{opacity:1;}50%{opacity:.35;}}
  .strat-active{background:#101319;border:1px solid var(--border);border-radius:8px;padding:9px 12px;font-size:13px;font-weight:600;}

  .tf-row{display:flex;gap:6px;margin-bottom:12px;}
  .tf{flex:1;text-align:center;padding:7px 2px;border-radius:7px;border:1px solid var(--border);font-size:11.5px;color:var(--muted);cursor:pointer;background:#101319;}
  .tf.active{border-color:var(--accent);color:#fff;background:var(--accent);}

  .chart-wrap{position:relative;background:#131722;border:1px solid var(--border);border-radius:10px;padding:6px 0 4px;margin-bottom:6px;}
  canvas{display:block;width:100%;height:230px;touch-action:none;}
  .chart-legend{display:flex;gap:12px;font-size:10px;color:var(--muted);padding:5px 10px 0;flex-wrap:wrap;}
  .chart-legend span{display:flex;align-items:center;gap:5px;}
  .dotL{width:8px;height:8px;border-radius:50%;display:inline-block;}
  .crosshair-info{position:absolute;top:6px;left:10px;font-family:monospace;font-size:11px;color:#D1D4DC;background:rgba(19,23,34,.85);padding:4px 8px;border-radius:5px;display:none;border:1px solid var(--border);line-height:1.5;pointer-events:none;}

  .stake-row{display:flex;align-items:center;gap:8px;}
  .stake-row input{text-align:center;font-size:16px;font-weight:600;}
  .stepbtn{width:38px;height:38px;flex:none;border-radius:8px;border:1px solid var(--border);background:#101319;color:var(--text);font-size:18px;cursor:pointer;}
  .unit-row{display:flex;gap:6px;margin-bottom:4px;}
  .unit-btn{flex:1;text-align:center;padding:8px 4px;border-radius:7px;border:1px solid var(--border);font-size:12px;color:var(--muted);cursor:pointer;background:#101319;}
  .unit-btn.active{border-color:var(--accent);color:#fff;background:var(--accent);}
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
    <p class="sub">Dashboard tokana ahafahanao mifehy ny bot sy mijery ny graphique avy ao ambadiky ny Render.</p>
    <a class="btn" href="#" onclick="go('dash');return false;">Hiditra amin'ny bot</a>
  </section>

  <section class="view" id="v-dash">
    <div class="row" style="margin-top:0;">
      <h2 style="margin:0;">Dashboard</h2>
      <span class="pill on" id="connPill">Mandeha</span>
    </div>
    <div style="font-size:11.5px;color:var(--muted);margin-top:2px;margin-bottom:14px;">Kaonty: DEMO</div>

    <div class="card">
      <div class="mtabs" id="mtabs"></div>
      <div class="tf-row" id="tfRow"></div>
      <div class="chart-wrap">
        <canvas id="chart" width="440" height="230"></canvas>
        <div class="crosshair-info" id="crosshair"></div>
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
        <div>
          <div style="font-size:13.5px;">Alefaso ny bot</div>
        </div>
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
/* ================= NAV ================= */
function go(v){
  document.querySelectorAll('.view').forEach(x=>x.classList.remove('active'));
  document.getElementById('v-'+v).classList.add('active');
  document.querySelectorAll('.nav a').forEach(a=>a.classList.toggle('active', a.dataset.v===v));
  window.scrollTo(0,0);
}

const MARKETS = ['v25','v50','boom150','boom300','boom500','boom1000','crash150','crash300','crash500','crash1000'];
const TIMEFRAMES = ['1m','5m','15m','30m','1h'];
let activeMarket = 'v25', activeTf = '5m', activeStrat = 'SMC';
let stakeVal = 1, running = false;
let candleStore = {}, signalStore = {};

function renderTabs(){
  const wrap = document.getElementById('mtabs'); wrap.innerHTML = '';
  MARKETS.forEach(m => {
    const el = document.createElement('div');
    el.className = 'mtab' + (m===activeMarket?' active':'');
    el.innerHTML = m + '<span class="px">Strategy</span>';
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
  document.getElementById('logBox').innerHTML = running ? '<div class="buy">🚀 Bot nalefa. Mikaroka signal...</div>' : '<div>⏹ Bot najanona.</div>';
}

/* ================= CHART & UI POLLING ================= */
const container = document.getElementById('chart');
const chart = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 230,
    layout: { background: {color: 'transparent'}, textColor: '#8b95a6' },
    grid: { vertLines: {color: 'rgba(255,255,255,0.04)'}, horzLines: {color: 'rgba(255,255,255,0.04)'} }
});
const candleSeries = chart.addCandlestickSeries({
    upColor: '#46E39B', downColor: '#F0546B', borderUpColor: '#46E39B', borderDownColor: '#F0546B'
});
const priceLine = chart.addLineSeries({ color: '#ffffff', lineWidth: 1, lineStyle: 3 });
const entryLine = chart.addLineSeries({ color: '#6E56CF', lineWidth: 1, lineStyle: 2 });

async function updateUI() {
  try {
    const res = await fetch('/api/stats'); 
    const data = await res.json();
    
    document.getElementById('stBal').innerText = data.balance.toFixed(2);
    document.getElementById('stTr').innerText = data.total_trades;
    document.getElementById('stPl').innerText = data.profit.toFixed(2);

    if (data.ui_candles && data.ui_candles.length > 0) {
      candleSeries.setData(data.ui_candles);
      if(data.entry_level > 0){
        const t = Math.floor(Date.now()/1000);
        entryLine.setData([{time: t-100, value: data.entry_level}, {time: t, value: data.entry_level}]);
      } else {
        entryLine.setData([]);
      }
      const lastCandle = data.ui_candles[data.ui_candles.length - 1];
      const t = Math.floor(Date.now()/1000);
      priceLine.setData([{time: t-60, value: lastCandle.close}, {time: t, value: lastCandle.close}]);
    }
  } catch(e) {}
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
    return jsonify(web_stats)

@app.route('/api/state/<state>')
def bot_state(state):
    global bot_active
    bot_active = (state == 'start')
    web_stats['active'] = bot_active
    return jsonify({'status': 'active' if bot_active else 'desactive'})

# ==========================================
# BOT CORE & STRATEGY (Deriv WebSocket)
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

MAX_TRADES = 2
SYMBOLS = ["1HZ25V", "1HZ50V", "BOOM150"]
tf_data = {"1m": []}
balance = 10000
start_balance = 10000
trade_active = False
active_trades = 0
profit_win = 0
profit_loss = 0
bot_active = True
entry_found = False
current_symbol = SYMBOLS[0]
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 10

def ema(data, period):
    if len(data) < period: return None
    mult = 2 / (period + 1)
    ema_val = data[0]
    for price in data[1:]: ema_val = (price * mult) + (ema_val * (1 - mult))
    return ema_val

def smc_strategy():
    if len(tf_data["1m"]) < 20: return None
    c1 = [x['close'] for x in tf_data["1m"]]
    e1 = ema(c1, 20)
    if e1 is None: return None
    if c1[-1] > e1: return "CALL"
    elif c1[-1] < e1: return "PUT"
    return None

def on_message(ws, message):
    global balance, start_balance, tf_data, web_stats, all_ui_candles, ui_symbol, current_symbol
    try:
        data = json.loads(message)
        msg_type = data.get('msg_type')
        if msg_type == 'balance':
            balance = float(data['balance']['balance'])
            web_stats['balance'] = balance
            web_stats['profit'] = balance - start_balance

        elif msg_type == 'candles':
            sym = data['candles']['symbol']
            for c in data['candles']['data']:
                ts = int(c['epoch'] / 1000)
                candle_obj = {'time': ts, 'open': c['open'], 'high': c['high'], 'low': c['low'], 'close': c['close']}
                tf_data["1m"].append(candle_obj)
                if sym not in all_ui_candles: all_ui_candles[sym] = []
                all_ui_candles[sym].append(candle_obj)
            
            if len(tf_data["1m"]) > 150: tf_data["1m"] = tf_data["1m"][-150:]
            if len(all_ui_candles.get(sym, [])) > 150: all_ui_candles[sym] = all_ui_candles[sym][-150:]
            
            # Alefaso any amin'ny UI ilay sary vao farany
            web_stats['ui_candles'] = all_ui_candles.get(sym, [])[-50:]

            if not trade_active and bot_active:
                decision = smc_strategy()
                if decision:
                    current_price = tf_data["1m"][-1]['close']
                    if decision == "CALL":
                        sl_price = current_price * 0.98
                        tp_price = current_price * 1.02
                    else:
                        sl_price = current_price * 1.02
                        tp_price = current_price * 0.98
                    
                    web_stats['entry_level'] = current_price
                    web_stats['sl_level'] = sl_price
                    web_stats['tp1_level'] = tp_price
                    
                    print(f"📊 Signal {decision} hita...")
                    # (Test ihany, afaka ampianao ny trade raha tianao)

    except Exception as e: print(f"Error: {e}")

def on_error(ws, error): print(f"WS Error: {error}")
def on_close(ws, close_status_code, close_msg):
    global reconnect_attempts
    if bot_active and reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        reconnect_attempts += 1
        print(f"Reconnecting in 10s... ({reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})")
        time.sleep(10)

def on_open(ws):
    global reconnect_attempts, current_symbol
    reconnect_attempts = 0
    print("✅ WebSocket mifandray! Maka Balance sy Candles...")
    ws.send(json.dumps({"authorize": AUTH_TOKEN, "req_id": 0}))
    ws.send(json.dumps({"balance": 1, "subscribe": 1, "req_id": 1}))
    ws.send(json.dumps({"candles": current_symbol, "adjust_start_time": 1, "count": 150, "subscribe": 1, "req_id": 2}))

def main():
    ws_url = get_websocket_url()
    if not ws_url:
        print("❌ TSY NAHAZO NY URL. Mete malemy na lany ny Token.")
        return
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
    try: ws.run_forever()
    except: pass

def start_bot_loop():
    while True:
        main()
        print("Bot reboot ao anatin'ny 60 segondra...")
        time.sleep(60)

if __name__ == "__main__":
    import os
    port = int(os.environ.get('PORT', 5000))
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False), daemon=True).start()
    threading.Thread(target=start_bot_loop, daemon=True).start()
else:
    threading.Thread(target=start_bot_loop, daemon=True).start()
