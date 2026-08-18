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
const MARKETS = ['v25','v50','boom150']; 
const TIMEFRAMES = ['1m','5m','15m'];
let activeMarket = 'v25', activeTf = '5m';
let stakeVal = 1, running = false;
let candleStore = [];

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
  document.getElementById('logBox').innerHTML = running ? '<div>🚀 Bot nalefa.</div>' : '<div>⏹ Bot najanona.</div>';
}

const container = document.getElementById('chart');
const chart = LightweightCharts.createChart(container, {
    width: container.clientWidth, height: 230,
    layout: { background: {color: 'transparent'}, textColor: '#8b95a6' },
    grid: { vertLines: {color: 'rgba(255,255,255,0.04)'}, horzLines: {color: 'rgba(255,255,255,0.04)'} }
});
const candleSeries = chart.addCandlestickSeries({
    upColor: '#46E39B', downColor: '#F0546B', borderUpColor: '#46E39B', borderDownColor: '#F0546B'
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

SYMBOLS = ["1HZ25V"]
tf_data = {"1m": []}
balance = 10000
start_balance = 10000
bot_active = True
current_symbol = SYMBOLS[0]

def on_message(ws, message):
    global balance, start_balance, tf_data, web_stats, all_ui_candles, current_symbol
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
            # Rehefa tonga ny tena angona, dia hosoloany ilay Mock data
            web_stats['ui_candles'] = all_ui_candles.get(sym, [])[-50:]

    except Exception as e: 
        print(f"WS Error: {e}")

def on_error(ws, error): print(f"WS Error: {error}")
def on_open(ws):
    print("✅ WebSocket mifandray! Maka angona...")
    ws.send(json.dumps({"authorize": AUTH_TOKEN, "req_id": 0}))
    ws.send(json.dumps({"balance": 1, "subscribe": 1, "req_id": 1}))
    # Namboarina ho 'ticks_history' (ny marina an'i Deriv)
    ws.send(json.dumps({"ticks_history": current_symbol, "style": "candles", "granularity": 60, "count": 100, "end": "latest", "subscribe": 1, "req_id": 2}))

def main():
    ws_url = get_websocket_url()
    if not ws_url:
        print("❌ TSY NAHAZO NY WEB SOCKET URL. Mety malemy na lany ny AUTH_TOKEN.")
        return
    ws = websocket.WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
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
