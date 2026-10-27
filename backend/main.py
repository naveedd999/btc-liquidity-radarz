import asyncio, json, math, os, sqlite3, time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import httpx, websockets

SYMBOL = 'BTCUSDT'
REST = 'https://fapi.binance.com'
WS = 'wss://fstream.binance.com/stream'
DB_PATH = Path(os.getenv('DB_PATH', '/data/radar.db'))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

state = {
    'price': 0.0, 'change24h': 0.0, 'high24h': 0.0, 'low24h': 0.0, 'volume24h': 0.0,
    'bidPressure': 50.0, 'sellVolume': 0.0, 'buyVolume': 0.0, 'delta': 0.0,
    'obi': 0.0, 'spread': 0.0, 'oi': 0.0, 'funding': 0.0,
    'longLiquidations': 0.0, 'shortLiquidations': 0.0, 'dominance': 0.0,
    'orderBook': [], 'trades': [], 'levels': [], 'health': {
        'connected': False, 'latency': 0, 'lastMessage': 0, 'bookSynced': False,
        'sequenceOk': True, 'lastSync': 0, 'status': 'STARTING'
    }, 'history': [], 'events': [], 'resyncHistory': [], 'alerts': []
}
clients = set(); trades = deque(maxlen=500); history = deque(maxlen=240); events = deque(maxlen=80); resync_history = deque(maxlen=30)
book = {'bids': {}, 'asks': {}}; lock = asyncio.Lock(); resync_event = asyncio.Event(); last_depth_id = None

def now(): return int(time.time() * 1000)
def num(x):
    try: return float(x)
    except Exception: return 0.0

def db():
    c = sqlite3.connect(DB_PATH); c.execute('PRAGMA journal_mode=WAL'); return c

def init_db():
    c = db()
    c.executescript('''
    CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, type TEXT, detail TEXT, side TEXT);
    CREATE TABLE IF NOT EXISTS trades(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, side TEXT, usd REAL, price REAL);
    CREATE TABLE IF NOT EXISTS resyncs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, mode TEXT, status TEXT, detail TEXT);
    CREATE TABLE IF NOT EXISTS metric_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, price REAL, obi REAL, pressure REAL, delta REAL, spread REAL);
    '''); c.commit(); c.close()

def db_event(e):
    try:
        c=db(); c.execute('INSERT INTO events(ts,type,detail,side) VALUES(?,?,?,?)',(e['time'],e['type'],e['detail'],e['side'])); c.commit(); c.close()
    except Exception: pass

def add_event(kind, detail, side='INFO'):
    e={'time':now(),'type':kind,'detail':detail,'side':side}; events.appendleft(e); db_event(e)

def save_trade(t):
    try:
        c=db(); c.execute('INSERT INTO trades(ts,side,usd,price) VALUES(?,?,?,?)',(t['time'],t['side'],t['usd'],t['price'])); c.commit(); c.close()
    except Exception: pass

def save_metrics():
    try:
        c=db(); c.execute('INSERT INTO metric_snapshots(ts,price,obi,pressure,delta,spread) VALUES(?,?,?,?,?,?)',(now(),state['price'],state['obi'],state['bidPressure'],state['delta'],state['spread'])); c.commit(); c.close()
    except Exception: pass

def record_resync(mode,status,detail):
    x={'time':now(),'mode':mode,'status':status,'detail':detail}; resync_history.appendleft(x)
    try:
        c=db(); c.execute('INSERT INTO resyncs(ts,mode,status,detail) VALUES(?,?,?,?)',(x['time'],mode,status,detail)); c.commit(); c.close()
    except Exception: pass

def metrics():
    bids, asks = book['bids'], book['asks']; bestb=max(bids) if bids else 0; besta=min(asks) if asks else 0
    bidq=sum(q for p,q in bids.items() if bestb and p >= bestb*0.995)
    askq=sum(q for p,q in asks.items() if besta and p <= besta*1.005)
    obi=(bidq-askq)/(bidq+askq) if bidq+askq else 0.0
    bv=sum(t['usd'] for t in trades if t['side']=='BUY'); sv=sum(t['usd'] for t in trades if t['side']=='SELL')
    pressure=bv/(bv+sv)*100 if bv+sv else 50.0; delta=bv-sv
    levels=([{'price':p,'size':q,'side':'BID'} for p,q in sorted(bids.items(),reverse=True)[:30]] +
            [{'price':p,'size':q,'side':'ASK'} for p,q in sorted(asks.items())[:30]])
    prices=sorted(set(list(bids.keys())[:50] + list(asks.keys())[:50]), reverse=True)[:24]
    order=[{'price':p,'bid':bids.get(p,0),'ask':asks.get(p,0)} for p in prices]
    return obi,pressure,delta,order,levels

def snapshot():
    return {**state, 'trades':list(trades)[:100], 'history':list(history), 'events':list(events), 'resyncHistory':list(resync_history)}

async def rest_json(path, params=None):
    async with httpx.AsyncClient(timeout=8) as c:
        r=await c.get(REST+path, params=params); r.raise_for_status(); return r.json()

async def load_snapshot():
    global book, last_depth_id
    d=await rest_json('/fapi/v1/depth', {'symbol':SYMBOL,'limit':1000})
    async with lock:
        book={'bids':{num(p):num(q) for p,q in d['bids'] if num(q)>0}, 'asks':{num(p):num(q) for p,q in d['asks'] if num(q)>0}}
        last_depth_id=d.get('lastUpdateId'); state['health']['bookSynced']=True; state['health']['sequenceOk']=True
    add_event('ORDER BOOK RESYNC','Fresh REST depth snapshot loaded','INFO')

def calc_book_metrics():
    obi,pressure,delta,order,levels=metrics(); state['obi']=obi; state['bidPressure']=pressure; state['delta']=delta; state['orderBook']=order; state['levels']=levels
    if book['bids'] and book['asks']: state['spread']=min(book['asks'])-max(book['bids'])

async def load_stats():
    try:
        tick,prem,oi=await asyncio.gather(
            rest_json('/fapi/v1/ticker/24hr',{'symbol':SYMBOL}),
            rest_json('/fapi/v1/premiumIndex',{'symbol':SYMBOL}),
            rest_json('/fapi/v1/openInterest',{'symbol':SYMBOL}))
        state['price']=num(tick['lastPrice']); state['change24h']=num(tick['priceChangePercent']); state['high24h']=num(tick['highPrice']); state['low24h']=num(tick['lowPrice']); state['volume24h']=num(tick['quoteVolume']); state['funding']=num(prem.get('lastFundingRate')); state['oi']=num(oi['openInterest'])*state['price']
    except Exception as e: add_event('REST WARNING',str(e)[:100],'INFO')
    try:
        async with httpx.AsyncClient(timeout=6) as c:
            r=await c.get('https://api.coingecko.com/api/v3/global'); r.raise_for_status(); state['dominance']=num(r.json().get('data',{}).get('market_cap_percentage',{}).get('btc',0))
    except Exception: pass

def detect_events(prev_obi=None):
    # Conservative heuristics: flags are anomalies, never claims of manipulation.
    now_ms=now()
    def throttled(key, ms=10000):
        if now_ms-last_event_ms.get(key,0)<ms: return True
        last_event_ms[key]=now_ms; return False
    if abs(state['obi']) >= 0.45 and not throttled('wall'):
        add_event('LIQUIDITY WALL', f"{'Bid' if state['obi']>0 else 'Ask'} depth is unusually concentrated", 'BULL' if state['obi']>0 else 'BEAR')
    if abs(state['delta']) > max(500_000, (state['buyVolume']+state['sellVolume'])*0.25) and not throttled('flow'):
        add_event('FLOW IMBALANCE', f"Aggressive {'buy' if state['delta']>0 else 'sell'} delta elevated", 'BULL' if state['delta']>0 else 'BEAR')
    near=[x for x in state['levels'] if state['price'] and abs(x['price']-state['price'])/state['price']<0.002]
    if near and sum(x['size'] for x in near)>20 and not throttled('cluster'):
        add_event('LIQUIDITY CLUSTER','Dense resting liquidity detected near price','INFO')

async def apply_trade(data):
    price=num(data['p']); qty=num(data['q']); usd=price*qty; side='SELL' if data.get('m') else 'BUY'
    t={'time':int(data['T']),'side':side,'usd':usd,'price':price}; trades.appendleft(t); save_trade(t)
    state['price']=price; state['buyVolume']=sum(t['usd'] for t in trades if t['side']=='BUY'); state['sellVolume']=sum(t['usd'] for t in trades if t['side']=='SELL')
    calc_book_metrics(); state['health']['lastMessage']=now(); state['health']['latency']=max(0,now()-int(data['E']))
    history.append({'t':int(data['E']),'p':price,'delta':state['delta']}); state['history']=list(history)
    if usd>=1_000_000: add_event('LARGE ORDER',f'{side} {usd/1e6:.2f}M @ ${price:,.0f}','BULL' if side=='BUY' else 'BEAR')
    if len(history)%10==0: save_metrics()

def apply_depth(data):
    global last_depth_id
    # Binance Futures diff-depth stream: apply deltas only after the REST snapshot
    # has been synchronized. A gap in the update chain marks the book DESYNCED
    # and requests an automatic resync.
    U, u = data.get('U'), data.get('u')
    if U is None or u is None:
        return
    if last_depth_id is None:
        return
    if U > last_depth_id + 1 or u < last_depth_id:
        state['health']['sequenceOk'] = False
        state['health']['bookSynced'] = False
        state['health']['status'] = 'DESYNCED'
        resync_event.set()
        return
    for p, q in data.get('b', []):
        p, q = num(p), num(q)
        if q == 0: book['bids'].pop(p, None)
        else: book['bids'][p] = q
    for p, q in data.get('a', []):
        p, q = num(p), num(q)
        if q == 0: book['asks'].pop(p, None)
        else: book['asks'][p] = q
    last_depth_id = u
    calc_book_metrics(); state['health']['lastMessage']=now(); state['health']['bookSynced']=True; state['health']['sequenceOk']=True

async def broadcast():
    if not clients:return
    msg=json.dumps({'type':'state','data':snapshot()}); dead=[]
    for ws in list(clients):
        try: await ws.send_text(msg)
        except Exception: dead.append(ws)
    for ws in dead: clients.discard(ws)

async def perform_resync(mode='quick'):
    state['health']['status']='SYNCING'; record_resync(mode,'STARTED','Resync requested'); await broadcast()
    try:
        await load_snapshot(); await load_stats(); calc_book_metrics(); state['health']['lastSync']=now(); state['health']['status']='SYNCED'; state['health']['lastMessage']=now(); record_resync(mode,'SUCCESS','Snapshot, statistics and derived metrics refreshed'); add_event('RESYNC COMPLETE',f'{mode.title()} resync completed','INFO')
    except Exception as e:
        state['health']['status']='ERROR'; record_resync(mode,'ERROR',str(e)[:160]); add_event('RESYNC ERROR',str(e)[:120],'INFO')
    await broadcast()

async def market_loop():
    backoff=1
    while True:
        try:
            await perform_resync('startup')
            streams=f'{SYMBOL.lower()}@aggTrade/{SYMBOL.lower()}@depth@100ms/{SYMBOL.lower()}@markPrice@1s/{SYMBOL.lower()}@forceOrder'
            async with websockets.connect(f'{WS}?streams={streams}', ping_interval=20, ping_timeout=20, max_queue=2048) as sock:
                state['health']['connected']=True; state['health']['status']='SYNCED'; add_event('CONNECTION','Binance Futures market stream connected','INFO'); await broadcast(); backoff=1
                async for raw in sock:
                    msg=json.loads(raw); data=msg.get('data',msg); et=data.get('e')
                    if et=='aggTrade': await apply_trade(data)
                    elif et=='depthUpdate': apply_depth(data)
                    elif et=='markPriceUpdate': state['price']=num(data.get('p',state['price'])); state['health']['lastMessage']=now()
                    elif et=='forceOrder':
                        o=data.get('o',{}); usd=num(o.get('ap') or o.get('p'))*num(o.get('q')); side='SELL' if o.get('S')=='SELL' else 'BUY'; state['longLiquidations']+=usd if side=='SELL' else 0; state['shortLiquidations']+=usd if side=='BUY' else 0; add_event('LIQUIDATION',f'{side} {usd/1e6:.2f}M','BEAR' if side=='SELL' else 'BULL')
                    if resync_event.is_set(): resync_event.clear(); await perform_resync('automatic')
                    detect_events(); await broadcast()
        except Exception as e:
            state['health']['connected']=False; state['health']['bookSynced']=False; state['health']['status']='RECONNECTING'; add_event('STREAM ERROR',str(e)[:120],'INFO'); await broadcast(); await asyncio.sleep(backoff); backoff=min(backoff*2,30)

async def stats_loop():
    while True:
        await asyncio.sleep(15)
        await load_stats(); await broadcast()

@asynccontextmanager
async def lifespan(app):
    init_db(); tasks=[asyncio.create_task(market_loop()),asyncio.create_task(stats_loop())]; yield
    for t in tasks: t.cancel()

app=FastAPI(title='BTC Liquidity Radar API', version='2.0.0', lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=['*'], allow_headers=['*'])

@app.get('/health')
async def health(): return {'ok':True,'symbol':SYMBOL,'connected':state['health']['connected'],'status':state['health']['status'],'last_message':state['health']['lastMessage']}
@app.get('/api/v1/btc/state')
async def get_state(): return snapshot()
@app.get('/api/v1/btc/events')
async def get_events(limit:int=50): return list(events)[:max(1,min(limit,80))]
@app.get('/api/v1/btc/resync-history')
async def get_resync_history(): return list(resync_history)
@app.post('/api/v1/btc/resync')
async def resync(mode:str='quick'):
    asyncio.create_task(perform_resync(mode if mode in {'quick','full','orderbook','market'} else 'quick')); return {'status':'SYNCING','mode':mode}

@app.get('/api/v1/btc/klines')
async def klines(interval:str='15m', limit:int=120):
    allowed={'1m','3m','5m','15m','30m','1h','2h','4h','6h','12h','1d'}
    if interval not in allowed: interval='15m'
    limit=max(20,min(limit,500))
    return await rest_json('/fapi/v1/klines', {'symbol':SYMBOL,'interval':interval,'limit':limit})

@app.websocket('/ws/market')
async def market_ws(ws:WebSocket):
    await ws.accept(); clients.add(ws); await ws.send_text(json.dumps({'type':'state','data':snapshot()}))
    try:
        while True:
            raw=await ws.receive_text()
            try:
                action=json.loads(raw).get('action')
                if action=='resync': asyncio.create_task(perform_resync(json.loads(raw).get('mode','quick')))
            except Exception: pass
    except (WebSocketDisconnect, Exception): clients.discard(ws)
