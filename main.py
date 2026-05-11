from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional
import httpx


http_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=15.0)
    yield
    await http_client.aclose()


app = FastAPI(title="DeFi Llama Wrapper", lifespan=lifespan)

BASE_URL = "https://api.llama.fi"
YIELDS_URL = "https://yields.llama.fi"
STABLECOINS_URL = "https://stablecoins.llama.fi"
COINS_URL = "https://coins.llama.fi"


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Common slug mapping for top protocols ────────────────────────────────

PROTOCOL_SLUGS = {
    "AAVE": "aave",
    "LIDO": "lido",
    "MAKERDAO": "makerdao",
    "MAKER": "makerdao",
    "UNISWAP": "uniswap",
    "CURVE": "curve-dex",
    "COMPOUND": "compound-finance",
    "EIGENLAYER": "eigenlayer",
    "ROCKET POOL": "rocket-pool",
    "ROCKETPOOL": "rocket-pool",
    "PENDLE": "pendle",
    "MORPHO": "morpho",
    "SPARK": "spark",
    "JITO": "jito",
    "MARINADE": "marinade-finance",
    "RAYDIUM": "raydium",
    "ORCA": "orca",
    "JUPITER": "jupiter",
    "KAMINO": "kamino",
    "DRIFT": "drift-protocol",
    "GMX": "gmx",
    "DYDX": "dydx",
    "PANCAKESWAP": "pancakeswap",
    "SUSHISWAP": "sushi",
    "SUSHI": "sushi",
    "BALANCER": "balancer",
    "YEARN": "yearn-finance",
    "CONVEX": "convex-finance",
    "INSTADAPP": "instadapp",
    "BENQI": "benqi-lending",
    "TRADER JOE": "trader-joe",
    "VENUS": "venus",
    "AERODROME": "aerodrome",
    "VELODROME": "velodrome",
    "STARGATE": "stargate",
    "ACROSS": "across",
    "ETHENA": "ethena",
    "SKY": "sky",
    "FLUID": "fluid-dex",
    "MAPLE": "maple",
    "SOLAYER": "solayer",
    "SANCTUM": "sanctum-infinity",
    "HYPERLIQUID": "hyperliquid",
}


def _resolve_slug(name: str) -> str:
    """Resolve a protocol name to a DeFi Llama slug."""
    upper = name.upper().strip()
    if upper in PROTOCOL_SLUGS:
        return PROTOCOL_SLUGS[upper]
    # If it looks like a slug already (lowercase, has dashes), pass through
    return name.lower().strip()


async def _llama_get(url: str) -> dict | list:
    """Make a request to DeFi Llama API."""
    try:
        r = await http_client.get(url)
        if r.status_code == 429:
            raise HTTPException(status_code=429, detail="DeFi Llama rate limit exceeded")
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="Not found on DeFi Llama")
        r.raise_for_status()
        return r.json()
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Network error: {e}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unexpected error: {e}")


# ── HTML home page ───────────────────────────────────────────────────────

HOME_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DeFi Llama · Chekk</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{background:#0a0a0a;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;display:flex;justify-content:center;padding:32px 16px}
.w{max-width:640px;width:100%;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:32px;height:fit-content}
.hd{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
.t{font-family:'Courier New',monospace;font-size:28px;font-weight:700;color:#0080FF}
.st{font-family:'Courier New',monospace;font-size:13px;color:#555;display:flex;align-items:center;gap:6px}
.st .d{width:8px;height:8px;border-radius:50%;background:#555;transition:background .3s}
.st .d.on{background:#4CAF50}
.sub{color:#666;font-size:14px;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid rgba(255,255,255,.06)}
.stats{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px}
.sc{background:rgba(0,128,255,.06);border:1px solid rgba(0,128,255,.15);border-radius:12px;padding:14px 16px;opacity:0;animation:fi .4s ease forwards}
.sc:nth-child(1){animation-delay:.1s}.sc:nth-child(2){animation-delay:.15s}.sc:nth-child(3){animation-delay:.2s}.sc:nth-child(4){animation-delay:.25s}
.sl{color:#0080FF;font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:6px}
.sv{font-family:'Courier New',monospace;font-size:22px;font-weight:700;color:#fff}
.tbl{margin:14px 0;opacity:0;animation:fi .4s ease .35s forwards}
.tbl-title{color:#0080FF;font-size:10px;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;margin-bottom:10px}
.tr{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04)}
.tr:last-child{border-bottom:none}
.tr .nm{font-size:13px}.tr .val{font-family:'Courier New',monospace;font-size:13px;color:#0080FF}
hr.dv{border:none;border-top:1px solid rgba(255,255,255,.06);margin:18px 0}
.fm{display:flex;gap:10px;margin-bottom:8px}
.ip{flex:1;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);border-radius:10px;padding:13px 16px;color:#fff;font-family:'Courier New',monospace;font-size:15px;outline:none;transition:border-color .2s}
.ip:focus{border-color:rgba(0,128,255,.5)}
.bt{background:#0080FF;color:#fff;border:none;border-radius:10px;padding:13px 22px;font-weight:700;font-size:14px;cursor:pointer;font-family:'Courier New',monospace;white-space:nowrap;transition:opacity .15s}
.bt:hover{opacity:.85}
.try{color:#555;font-size:12px}.try a{color:#666;text-decoration:none;cursor:pointer;transition:color .15s}.try a:hover{color:#0080FF}
#res{margin-top:14px;padding:14px 16px;background:rgba(0,128,255,.06);border:1px solid rgba(0,128,255,.15);border-radius:10px;display:none;font-family:'Courier New',monospace;font-size:13px;line-height:1.6;white-space:pre-wrap;word-break:break-all;max-height:300px;overflow-y:auto}
@keyframes fi{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@media(max-width:480px){.stats{grid-template-columns:1fr}.w{padding:20px}}
</style>
</head>
<body>
<div class="w">
 <div class="hd"><div class="t">DeFi Llama</div><div class="st"><span class="d" id="dot"></span><span id="stx">connecting...</span></div></div>
 <div class="sub">TVL, protocol revenue, DeFi yields, and chain analytics</div>
 <div class="stats" id="stats"></div>
 <div class="tbl" id="topsec" style="display:none"><div class="tbl-title">TOP PROTOCOLS BY TVL</div><div id="toplist"></div></div>
 <hr class="dv">
 <div class="fm"><input class="ip" id="proto" placeholder="aave" value="aave" onkeydown="if(event.key==='Enter')fetchP()"><button class="bt" onclick="fetchP()">&rarr; tvl</button></div>
 <div class="try">Try: <a onclick="ts('lido')">Lido</a> &middot; <a onclick="ts('uniswap')">Uniswap</a> &middot; <a onclick="ts('makerdao')">MakerDAO</a> &middot; <a onclick="ts('eigenlayer')">Eigenlayer</a> &middot; <a onclick="ts('aave')">Aave</a></div>
 <div id="res"></div>
</div>
<script>
function fmt(n){if(n==null)return'--';if(n>=1e12)return'$'+(n/1e12).toFixed(2)+'T';if(n>=1e9)return'$'+(n/1e9).toFixed(2)+'B';if(n>=1e6)return'$'+(n/1e6).toFixed(1)+'M';return'$'+n.toLocaleString('en-US',{maximumFractionDigits:0})}
async function init(){
 const t0=Date.now();
 try{await fetch('/health');const ms=Date.now()-t0;document.getElementById('dot').classList.add('on');document.getElementById('stx').textContent='online \\u00B7 '+ms+'ms'}catch(e){document.getElementById('stx').textContent='offline'}
 try{
  const ch=await fetch('/chains').then(r=>r.json());
  const totalTvl=ch.chains?ch.chains.reduce((s,c)=>s+(c.tvl||0),0):0;
  const chainCount=ch.chains?ch.chains.length:0;
  const st=document.getElementById('stats');
  st.innerHTML='<div class="sc"><div class="sl">TOTAL TVL</div><div class="sv">'+fmt(totalTvl)+'</div></div>'
   +'<div class="sc"><div class="sl">CHAINS TRACKED</div><div class="sv">'+chainCount+'</div></div>';
 }catch(e){}
 try{
  const p=await fetch('/top?limit=5').then(r=>r.json());
  if(p.protocols&&p.protocols.length){const sec=document.getElementById('topsec');sec.style.display='';const list=document.getElementById('toplist');
  p.protocols.forEach(pr=>{const row=document.createElement('div');row.className='tr';row.innerHTML='<span class="nm">'+pr.name+'</span><span class="val">'+fmt(pr.tvl)+'</span>';list.appendChild(row)})}
 }catch(e){}
}
function ts(s){document.getElementById('proto').value=s;fetchP()}
async function fetchP(){
 const s=document.getElementById('proto').value.trim().toLowerCase();if(!s)return;
 const res=document.getElementById('res');res.style.display='block';res.textContent='Fetching '+s+'...';
 try{const d=await fetch('/protocol?name='+encodeURIComponent(s)).then(r=>{if(!r.ok)throw new Error(r.status);return r.json()});
  res.textContent=JSON.stringify(d,null,2)}
 catch(e){res.innerHTML='<span style="color:#ef5350">Error fetching '+s+'</span>'}
}
init();
</script>
</body></html>"""


# ── Endpoints ────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return HTMLResponse(content=HOME_HTML)


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": _ts()}


@app.get("/protocol")
async def get_protocol(name: str = Query(..., description="Protocol name or slug (e.g., aave, lido, uniswap)")):
    """
    Get TVL and metadata for a DeFi protocol.
    Example: /protocol?name=aave
    """
    slug = _resolve_slug(name)
    data = await _llama_get(f"{BASE_URL}/protocol/{slug}")

    if isinstance(data, dict) and data.get("statusCode"):
        raise HTTPException(status_code=404, detail=f"Protocol '{name}' not found")

    # Extract chain TVLs from currentChainTvls
    chain_tvls = {}
    for chain, tvl in (data.get("currentChainTvls") or {}).items():
        # Skip staking/pool2/borrowed keys (they contain dashes like "Ethereum-staking")
        if "-" not in chain and chain not in ("staking", "pool2", "borrowed"):
            chain_tvls[chain] = tvl

    # Current TVL = sum of chain TVLs, or last entry in tvl history array
    current_tvl = sum(chain_tvls.values()) if chain_tvls else None
    if current_tvl is None:
        tvl_history = data.get("tvl")
        if isinstance(tvl_history, list) and tvl_history:
            current_tvl = tvl_history[-1].get("totalLiquidityUSD")

    return {
        "name": data.get("name"),
        "slug": data.get("slug"),
        "tvl": current_tvl,
        "chain_tvls": chain_tvls,
        "category": data.get("category"),
        "chains": data.get("chains", []),
        "change_1h_pct": data.get("change_1h"),
        "change_1d_pct": data.get("change_1d"),
        "change_7d_pct": data.get("change_7d"),
        "mcap": data.get("mcap"),
        "symbol": data.get("symbol"),
        "url": data.get("url"),
        "description": data.get("description"),
        "audits": data.get("audits"),
        "audit_links": data.get("audit_links"),
        "timestamp": _ts(),
    }


@app.get("/tvl")
async def get_tvl(name: str = Query(..., description="Protocol name or slug")):
    """
    Get just the current TVL for a protocol.
    Example: /tvl?name=aave
    """
    slug = _resolve_slug(name)
    data = await _llama_get(f"{BASE_URL}/tvl/{slug}")

    # /tvl/{protocol} returns a raw number
    if isinstance(data, (int, float)):
        return {"name": name, "slug": slug, "tvl": data, "timestamp": _ts()}

    raise HTTPException(status_code=404, detail=f"Protocol '{name}' not found")


@app.get("/chains")
async def get_chains():
    """
    Get TVL for all chains.
    Example: /chains
    """
    data = await _llama_get(f"{BASE_URL}/v2/chains")

    chains = []
    for chain in data if isinstance(data, list) else []:
        chains.append({
            "name": chain.get("name"),
            "tvl": chain.get("tvl"),
            "token_symbol": chain.get("tokenSymbol"),
            "gecko_id": chain.get("gecko_id"),
        })

    # Sort by TVL descending
    chains.sort(key=lambda c: c.get("tvl") or 0, reverse=True)

    return {"chains": chains, "count": len(chains), "timestamp": _ts()}


@app.get("/top")
async def get_top_protocols(
    limit: int = Query(10, description="Number of protocols to return", ge=1, le=100),
):
    """
    Get top DeFi protocols ranked by TVL.
    Example: /top?limit=10
    """
    data = await _llama_get(f"{BASE_URL}/protocols")

    protocols = []
    for p in (data if isinstance(data, list) else [])[:limit]:
        protocols.append({
            "name": p.get("name"),
            "slug": p.get("slug"),
            "tvl": p.get("tvl"),
            "category": p.get("category"),
            "chains": p.get("chains", []),
            "change_1d_pct": p.get("change_1d"),
            "change_7d_pct": p.get("change_7d"),
            "mcap": p.get("mcap"),
            "symbol": p.get("symbol"),
        })

    return {"protocols": protocols, "count": len(protocols), "timestamp": _ts()}


@app.get("/yields")
async def get_yields(
    limit: int = Query(20, description="Number of pools to return", ge=1, le=100),
    chain: Optional[str] = Query(None, description="Filter by chain (e.g., Ethereum, Solana)"),
    project: Optional[str] = Query(None, description="Filter by project (e.g., aave-v3)"),
    stablecoin: Optional[bool] = Query(None, description="Filter stablecoin-only pools"),
):
    """
    Get top DeFi yield pools sorted by TVL.
    Example: /yields?limit=10&chain=Ethereum
    """
    data = await _llama_get(f"{YIELDS_URL}/pools")
    pools = data.get("data", []) if isinstance(data, dict) else []

    # Apply filters
    if chain:
        chain_lower = chain.lower()
        pools = [p for p in pools if (p.get("chain") or "").lower() == chain_lower]
    if project:
        project_lower = project.lower()
        pools = [p for p in pools if (p.get("project") or "").lower() == project_lower]
    if stablecoin is not None:
        pools = [p for p in pools if p.get("stablecoin") == stablecoin]

    # Sort by TVL descending and take top N
    pools.sort(key=lambda p: p.get("tvlUsd") or 0, reverse=True)
    pools = pools[:limit]

    results = []
    for p in pools:
        results.append({
            "pool_id": p.get("pool"),
            "project": p.get("project"),
            "chain": p.get("chain"),
            "symbol": p.get("symbol"),
            "tvl_usd": p.get("tvlUsd"),
            "apy": p.get("apy"),
            "apy_base": p.get("apyBase"),
            "apy_reward": p.get("apyReward"),
            "stablecoin": p.get("stablecoin"),
            "il_risk": p.get("ilRisk"),
            "exposure": p.get("exposure"),
        })

    return {"pools": results, "count": len(results), "timestamp": _ts()}


@app.get("/stablecoins")
async def get_stablecoins():
    """
    Get circulating supply data for all stablecoins.
    Example: /stablecoins
    """
    data = await _llama_get(f"{STABLECOINS_URL}/stablecoins?includePrices=true")
    peggedAssets = data.get("peggedAssets", []) if isinstance(data, dict) else []

    stables = []
    for s in peggedAssets[:30]:
        circulating = s.get("circulating", {}).get("peggedUSD")
        stables.append({
            "name": s.get("name"),
            "symbol": s.get("symbol"),
            "circulating_usd": circulating,
            "peg_type": s.get("pegType"),
            "peg_mechanism": s.get("pegMechanism"),
            "chains": s.get("chains", []),
            "price": s.get("price"),
        })

    return {"stablecoins": stables, "count": len(stables), "timestamp": _ts()}


@app.get("/chain-tvl")
async def get_chain_tvl(
    chain: str = Query(..., description="Chain name (e.g., Ethereum, Solana, Arbitrum)"),
):
    """
    Get historical TVL for a specific chain.
    Example: /chain-tvl?chain=Ethereum
    """
    data = await _llama_get(f"{BASE_URL}/v2/historicalChainTvl/{chain}")

    if not isinstance(data, list):
        raise HTTPException(status_code=404, detail=f"Chain '{chain}' not found")

    # Return last 90 data points (daily)
    points = []
    for item in data[-90:]:
        points.append({
            "date": datetime.fromtimestamp(item.get("date", 0), tz=timezone.utc).strftime("%Y-%m-%d"),
            "tvl": item.get("tvl"),
        })

    return {"chain": chain, "history": points, "count": len(points), "timestamp": _ts()}
