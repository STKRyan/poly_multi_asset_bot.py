#!/usr/bin/env python3
"""Polymarket multi-asset adaptive paper scanner.

Paper trading only: this program uses public market data and never submits orders.
It discovers active BTC/ETH 5m+15m and Gold 15m markets via Gamma search,
uses independent reference-price feeds, and learns only from resolved paper calls.
"""
from __future__ import annotations
import argparse, json, math, os, re, sqlite3, sys, time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart"
DB_PATH = os.getenv("POLY_BOT_DB", "poly_multi_asset.sqlite3")
WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
SCAN_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "15"))
STAKE = float(os.getenv("PAPER_STAKE_USD", "2"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT_USD", "8"))
MAX_OPEN = int(os.getenv("MAX_OPEN_PAPER_TRADES", "4"))
MIN_DEPTH = float(os.getenv("MIN_BOOK_DEPTH_USD", "500"))
MAX_SPREAD = float(os.getenv("MAX_SPREAD", "0.035"))
MIN_ASK, MAX_ASK = 0.20, 0.82
TIMEFRAMES = {"BTC": (5, 15), "ETH": (5, 15), "GOLD": (15,)}
SYMBOLS = {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}
S = requests.Session(); S.headers.update({"User-Agent": "Poly-Multi-Asset-Paper-Scanner/1.0"})


def now(): return datetime.now(timezone.utc)
def iso(): return now().isoformat()
def jlist(v):
    if isinstance(v, list): return v
    if isinstance(v, str):
        try:
            x=json.loads(v); return x if isinstance(x,list) else []
        except Exception: return []
    return []
def get(url, params=None):
    r=S.get(url, params=params, timeout=12); r.raise_for_status(); return r.json()
def send(text, embed=None):
    if not WEBHOOK.startswith(("https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/")):
        print("Discord webhook missing or invalid; message not sent."); return False
    payload={"content":text[:1900], "allowed_mentions":{"parse":[]}}
    if embed: payload["embeds"]=[embed]
    try:
        r=S.post(WEBHOOK, json=payload, timeout=12); r.raise_for_status(); return True
    except Exception as e: print(f"Discord send failed: {type(e).__name__}: {e}"); return False


def db():
    c=sqlite3.connect(DB_PATH); c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS calls(
      id INTEGER PRIMARY KEY, market_id TEXT NOT NULL, slug TEXT NOT NULL, asset TEXT NOT NULL,
      timeframe INTEGER NOT NULL, side TEXT NOT NULL, token_id TEXT NOT NULL,
      entry REAL NOT NULL, stake REAL NOT NULL, qty REAL NOT NULL, score INTEGER NOT NULL,
      features TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
      remaining_qty REAL NOT NULL DEFAULT 0, realized_pnl REAL NOT NULL DEFAULT 0,
      partial_taken INTEGER NOT NULL DEFAULT 0, last_action TEXT NOT NULL DEFAULT '',
      result TEXT, pnl REAL, resolved_at TEXT, UNIQUE(market_id,side));
    CREATE TABLE IF NOT EXISTS observations(
      id INTEGER PRIMARY KEY, asset TEXT NOT NULL, timeframe INTEGER NOT NULL,
      market_id TEXT NOT NULL, side TEXT NOT NULL, score INTEGER NOT NULL,
      features TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS bot_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    """)
    # Safe additive migrations for databases created by an earlier draft.
    cols={r[1] for r in c.execute("PRAGMA table_info(calls)")}
    for name,decl in {
        "remaining_qty":"REAL NOT NULL DEFAULT 0", "realized_pnl":"REAL NOT NULL DEFAULT 0",
        "partial_taken":"INTEGER NOT NULL DEFAULT 0", "last_action":"TEXT NOT NULL DEFAULT ''"
    }.items():
        if name not in cols: c.execute(f"ALTER TABLE calls ADD COLUMN {name} {decl}")
    c.execute("UPDATE calls SET remaining_qty=qty WHERE status='OPEN' AND remaining_qty=0 AND qty>0")
    c.commit(); return c


def discover(asset, tf):
    """Use Gamma public search; never manufacture a market slug."""
    terms={"BTC":"Bitcoin", "ETH":"Ethereum", "GOLD":"Gold"}[asset]
    queries=[f"{terms} Up or Down {tf}m", f"{terms} {tf} minute"]
    found=[]
    for q in queries:
        try:
            data=get(f"{GAMMA}/public-search", {"q":q})
            events=data.get("events",[]) if isinstance(data,dict) else []
            for ev in events:
                found.extend(ev.get("markets",[]) or [])
            if isinstance(data,dict): found.extend(data.get("markets",[]) or [])
        except Exception as e: print(f"Market search {asset} {tf}m: {type(e).__name__}")
    # Deduplicate and demand a clear asset + timeframe match.
    unique={str(m.get("id") or m.get("slug")):m for m in found if isinstance(m,dict)}
    candidates=[]
    for m in unique.values():
        label=" ".join(str(m.get(k,"")) for k in ("question","slug","description")).lower()
        asset_ok=("bitcoin" in label or re.search(r"\bbtc\b",label)) if asset=="BTC" else (("ethereum" in label or re.search(r"\beth\b",label)) if asset=="ETH" else ("gold" in label or "xau" in label))
        tf_ok=bool(re.search(rf"\b{tf}\s*(?:m|min|minute|minutes)\b|{tf}m",label))
        active=m.get("active",True) not in (False,"false",0) and m.get("closed",False) not in (True,"true",1)
        tokens=jlist(m.get("clobTokenIds")); outcomes=jlist(m.get("outcomes"))
        if asset_ok and tf_ok and active and len(tokens)>=2 and len(outcomes)>=2:
            m["_tokens"]=tokens; m["_outcomes"]=outcomes; candidates.append(m)
    if not candidates: return None
    # Prefer the market ending soonest, but still in the future.
    def end_ts(m):
        try: return datetime.fromisoformat(str(m.get("endDate")).replace("Z","+00:00")).timestamp()
        except Exception: return float("inf")
    future=[m for m in candidates if end_ts(m)>time.time()]
    return min(future or candidates, key=end_ts)


def tokens(m):
    out={}
    for label, tid in zip(m.get("_outcomes",jlist(m.get("outcomes"))), m.get("_tokens",jlist(m.get("clobTokenIds")))):
        x=str(label).strip().lower()
        if x in ("up","yes"): out["UP"]=str(tid)
        elif x in ("down","no"): out["DOWN"]=str(tid)
    return out


def reference_candles(asset):
    if asset in SYMBOLS:
        rows=get("https://data-api.binance.vision/api/v3/klines", {"symbol":SYMBOLS[asset],"interval":"1m","limit":100})
        return [(float(r[4]),float(r[5])) for r in rows[:-1]]
    # XAUUSD=X is an indicative public reference feed, not the Polymarket settlement oracle.
    data=get(f"{YAHOO}/XAUUSD=X", {"range":"1d","interval":"1m"})
    result=data["chart"]["result"][0]; q=result["indicators"]["quote"][0]
    closes=q.get("close") or []; volumes=q.get("volume") or [0]*len(closes)
    return [(float(p),float(v or 0)) for p,v in zip(closes,volumes) if p is not None]


def indicators(candles):
    closes=[x[0] for x in candles]; vols=[x[1] for x in candles]
    if len(closes)<55: raise RuntimeError(f"Reference feed only returned {len(closes)} completed candles")
    def ema(vals,n):
        a=2/(n+1); out=[vals[0]]
        for x in vals[1:]: out.append(a*x+(1-a)*out[-1])
        return out
    changes=[closes[i]-closes[i-1] for i in range(1,len(closes))]
    gains=[max(x,0) for x in changes[-14:]]; losses=[max(-x,0) for x in changes[-14:]]
    ag=sum(gains)/14; al=sum(losses)/14; rsi=100-(100/(1+ag/al)) if al else (100 if ag else 50)
    e9,e21,e50=ema(closes,9),ema(closes,21),ema(closes,50)
    macd=ema(closes,12)[-1]-ema(closes,26)[-1]
    prev=ema(closes,12)[-2]-ema(closes,26)[-2]
    move1=(closes[-1]/closes[-2]-1)*100; move3=(closes[-1]/closes[-4]-1)*100; move10=(closes[-1]/closes[-11]-1)*100
    vavg=sum(vols[-21:-1])/max(1,len(vols[-21:-1])); vr=vols[-1]/vavg if vavg else 0
    votes=[1 if e9[-1]>e21[-1]>e50[-1] else -1 if e9[-1]<e21[-1]<e50[-1] else 0,
           1 if 52<=rsi<=68 else -1 if 32<=rsi<=48 else 0,
           1 if macd>prev else -1 if macd<prev else 0,
           1 if move3>=0.035 and move10>0 else -1 if move3<=-0.035 and move10<0 else 0,
           1 if vr>=1.1 and move3>0 else -1 if vr>=1.1 and move3<0 else 0]
    return {"score":sum(votes),"side":"UP" if sum(votes)>0 else "DOWN" if sum(votes)<0 else "WAIT",
            "rsi":rsi,"move1":move1,"move3":move3,"move10":move10,"volume_ratio":vr,"price":closes[-1],"votes":votes}


def book_stats(token):
    b=get(f"{CLOB}/book",{"token_id":token}); bids=sorted(b.get("bids",[]),key=lambda x:float(x["price"]),reverse=True); asks=sorted(b.get("asks",[]),key=lambda x:float(x["price"]))
    if not bids or not asks:return None
    bid=float(bids[0]["price"]); ask=float(asks[0]["price"])
    bd=sum(float(x["price"])*float(x["size"]) for x in bids[:10]); ad=sum(float(x["price"])*float(x["size"]) for x in asks[:10])
    return {"bid":bid,"ask":ask,"spread":ask-bid,"depth":min(bd,ad),"imbalance":(bd-ad)/max(1,bd+ad)}


def learning_adjustment(c, asset, tf):
    rows=c.execute("SELECT pnl FROM calls WHERE asset=? AND timeframe=? AND status='RESOLVED' ORDER BY id DESC LIMIT 30",(asset,tf)).fetchall()
    n=len(rows)
    if n<12:return {"sample":n,"threshold":4,"pause":False,"win_rate":None}
    wins=sum(float(r["pnl"])>0 for r in rows); wr=100*wins/n
    # Learning can tighten risk, but never loosen the base safety filters.
    return {"sample":n,"threshold":5 if wr<52 else 4,"pause":wr<42,"win_rate":wr}


def save_observation(c, asset, tf, m, tech):
    c.execute("INSERT INTO observations(asset,timeframe,market_id,side,score,features,created_at) VALUES(?,?,?,?,?,?,?)",
              (asset,tf,str(m.get("id")),tech["side"],tech["score"],json.dumps(tech),iso())); c.commit()


def maybe_enter(c, asset, tf, m, tech):
    side=tech["side"]
    if asset=="GOLD":
        return "SIGNAL ONLY · Gold feed is indicative; paper entries stay disabled until it is matched to the market's settlement reference"
    if side not in ("UP","DOWN"):return "WAIT · mixed indicators"
    mapping=tokens(m); tid=mapping.get(side)
    if not tid:return "WAIT · outcome tokens not mapped safely"
    stats=book_stats(tid)
    if not stats:return "WAIT · order book unavailable"
    learn=learning_adjustment(c,asset,tf)
    # Gold's external spot feed is indicative only; use stricter confirmation and smaller confidence claims.
    required=learn["threshold"]+(1 if asset=="GOLD" else 0)
    signed_imb=stats["imbalance"]*(1 if side=="UP" else -1)
    confirm=(tech["move1"]>0 and tech["move3"]>0) if side=="UP" else (tech["move1"]<0 and tech["move3"]<0)
    reasons=[]
    if abs(tech["score"])<required:reasons.append(f"signal votes {abs(tech['score'])}/5; need {required}")
    if not confirm:reasons.append("short momentum not aligned")
    if signed_imb< -0.10:reasons.append("order book opposes signal")
    if stats["spread"]>MAX_SPREAD:reasons.append("spread too wide")
    if stats["depth"]<MIN_DEPTH:reasons.append("book too thin")
    if not MIN_ASK<=stats["ask"]<=MAX_ASK:reasons.append("contract price outside entry band")
    if learn["pause"]:reasons.append("learning safety pause after weak recent results")
    pnl=c.execute("SELECT COALESCE(SUM(pnl),0) n FROM calls WHERE status='RESOLVED' AND substr(resolved_at,1,10)=?",(now().date().isoformat(),)).fetchone()["n"]
    if pnl<=-DAILY_LOSS_LIMIT:reasons.append("daily paper loss limit reached")
    open_n=c.execute("SELECT COUNT(*) n FROM calls WHERE status='OPEN'").fetchone()["n"]
    if open_n>=MAX_OPEN:reasons.append("maximum open paper calls reached")
    if reasons:return "WAIT · "+"; ".join(reasons)
    qty=STAKE/stats["ask"]
    try:
        c.execute("INSERT INTO calls(market_id,slug,asset,timeframe,side,token_id,entry,stake,qty,remaining_qty,score,features,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (str(m.get("id")),str(m.get("slug","")),asset,tf,side,tid,stats["ask"],STAKE,qty,qty,tech["score"],json.dumps(tech),iso()))
        c.commit()
    except sqlite3.IntegrityError:return "Already paper-tracked this market/side"
    return f"PAPER ENTRY {side} @ {stats['ask']:.3f} · ${STAKE:.2f} stake · book spread {stats['spread']:.3f}"


def resolve_calls(c):
    rows=c.execute("SELECT * FROM calls WHERE status='OPEN'").fetchall()
    for r in rows:
        try:
            data=get(f"{GAMMA}/markets/{r['market_id']}")
            if not data.get("closed"):continue
            outs=jlist(data.get("outcomes")); prices=jlist(data.get("outcomePrices"))
            mapping={str(o).lower():float(p) for o,p in zip(outs,prices)}
            winning=None
            if mapping.get("up",0)>=0.99 or mapping.get("yes",0)>=0.99:winning="UP"
            elif mapping.get("down",0)>=0.99 or mapping.get("no",0)>=0.99:winning="DOWN"
            if winning is None:continue
            won=(winning==r["side"]); rem=float(r["remaining_qty"] or 0.0)
            payout=rem if won else 0.0
            pnl=float(r["realized_pnl"] or 0.0)+payout-(rem*float(r["entry"]))
            c.execute("UPDATE calls SET status='RESOLVED',result=?,pnl=?,remaining_qty=0,resolved_at=? WHERE id=?",("WIN" if pnl>0 else "LOSS",pnl,iso(),r["id"]))
            c.commit()
            send(f"{'🟢' if won else '🔴'} **PAPER RESULT · {r['asset']} {r['timeframe']}m {r['side']}**\nResult: {'WIN' if won else 'LOSS'} · Simulated P/L: **${pnl:+.2f}**\nLearning sample updated. No real order was placed.")
        except Exception as e:print(f"Resolve check {r['market_id']}: {type(e).__name__}: {e}")



def manage_open_calls(c, asset, tech):
    rows=c.execute("SELECT * FROM calls WHERE status='OPEN' AND asset=?",(asset,)).fetchall()
    for r in rows:
        try:
            stats=book_stats(r["token_id"])
            if not stats: continue
            entry=float(r["entry"]); bid=float(stats["bid"])
            if entry<=0: continue
            roi=bid/entry-1.0; rem=float(r["remaining_qty"] or 0.0)
            realized=float(r["realized_pnl"] or 0.0); partial=int(r["partial_taken"] or 0)
            aligned=int(tech["score"])*(1 if r["side"]=="UP" else -1)
            exit_qty=0.0; action="HOLD"
            if roi<=-0.08: action="PROTECTIVE EXIT"; exit_qty=rem
            elif roi>=0.10: action="TARGET EXIT"; exit_qty=rem
            elif aligned<=-3 and stats["imbalance"]*(1 if r["side"]=="UP" else -1)<-0.10:
                action="FLIP CONFIRMATION EXIT"; exit_qty=rem
            elif roi>=0.05 and not partial and rem>0:
                action="PARTIAL PROFIT · SELL 50%"; exit_qty=rem/2
            if exit_qty>0:
                realized+=(bid-entry)*exit_qty; rem-=exit_qty
                closed=rem<=0.000001
                c.execute("UPDATE calls SET remaining_qty=?,realized_pnl=?,partial_taken=?,last_action=?,status=?,pnl=?,result=?,resolved_at=? WHERE id=?",
                  (max(0,rem),realized,1 if "PARTIAL" in action else partial,action,
                   "RESOLVED" if closed else "OPEN",realized if closed else None,
                   "EXIT" if closed else None,iso() if closed else None,r["id"]))
                c.commit()
            if action!=r["last_action"]:
                send(f"📣 **{asset} {r['timeframe']}m {r['side']} PAPER POSITION**\n**{action}** · entry {entry:.3f} · bid {bid:.3f} · ROI {roi*100:+.2f}%\nHold/flip check: {aligned:+d} aligned votes · simulated only")
                c.execute("UPDATE calls SET last_action=? WHERE id=?",(action,r["id"])); c.commit()
        except Exception as e: print(f"Position manager {r['id']}: {type(e).__name__}: {e}")


def dashboard(c, lines):
    rows=c.execute("SELECT asset,timeframe,pnl FROM calls WHERE status='RESOLVED' ORDER BY id DESC LIMIT 50").fetchall()
    wins=sum(float(x["pnl"])>0 for x in rows); n=len(rows); pnl=sum(float(x["pnl"]) for x in rows)
    opens=c.execute("SELECT COUNT(*) n FROM calls WHERE status='OPEN'").fetchone()["n"]
    desc="\n".join(lines)[:3800] or "No matching active markets found."
    embed={"title":"POLYMARKET · ADAPTIVE PAPER DESK","description":desc,
      "color":5793266,"fields":[
       {"name":"PAPER TRACK RECORD","value":f"{wins}W / {n-wins}L · {100*wins/n:.1f}%" if n else "Building first resolved sample","inline":True},
       {"name":"REALIZED PAPER P/L","value":f"${pnl:+.2f}","inline":True},
       {"name":"OPEN PAPER CALLS","value":str(opens),"inline":True}],
      "footer":{"text":"Paper only · gross P/L before protocol fees · indicators are not calibrated probabilities · Gold reference feed is indicative"},"timestamp":iso()}
    send("",embed)


def cycle(c):
    resolve_calls(c); lines=[]
    for asset, tfs in TIMEFRAMES.items():
        try: tech=indicators(reference_candles(asset))
        except Exception as e:
            msg=f"**{asset}** · FEED ERROR · {type(e).__name__}: {str(e)[:100]}"; print(msg); lines.append(msg); continue
        manage_open_calls(c,asset,tech)
        for tf in tfs:
            try:
                m=discover(asset,tf)
                if not m:
                    lines.append(f"**{asset} {tf}m** · MARKET NOT FOUND · no call"); continue
                save_observation(c,asset,tf,m,tech)
                action=maybe_enter(c,asset,tf,m,tech)
                lr=learning_adjustment(c,asset,tf)
                wr=f" · recent {lr['win_rate']:.0f}%/{lr['sample']}" if lr["win_rate"] is not None else f" · learning sample {lr['sample']}/12"
                lines.append(f"**{asset} {tf}m** · **{tech['side']}** ({tech['score']:+d}/5) · RSI {tech['rsi']:.0f} · 1m {tech['move1']:+.3f}% · 3m {tech['move3']:+.3f}%\n{action}{wr}")
            except Exception as e:
                msg=f"**{asset} {tf}m** · SCAN ERROR · {type(e).__name__}: {str(e)[:100]}"; print(msg); lines.append(msg)
    dashboard(c,lines)


def main():
    p=argparse.ArgumentParser(description="Polymarket multi-asset paper scanner")
    p.add_argument("--duration",type=int,default=0,help="0 runs continuously")
    args=p.parse_args(); c=db()
    send("🤖 **MULTI-ASSET PAPER DESK ONLINE**\nBTC + ETH: 5m/15m · Gold: 15m only\nAdaptive feedback begins after resolved paper calls. Real-money trading is disabled.")
    start=time.time()
    try:
        while True:
            try:cycle(c)
            except Exception as e:
                print(f"Cycle error: {type(e).__name__}: {e}")
                send(f"⚠️ **Scanner cycle error** · `{type(e).__name__}: {str(e)[:180]}`")
            if args.duration and time.time()-start>=args.duration:break
            time.sleep(max(5,SCAN_SECONDS))
    finally:c.close()
    return 0

if __name__=="__main__":sys.exit(main())
