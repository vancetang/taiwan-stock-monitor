# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "cn_stock_list_cache.json")
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

THREADS_CN = 4 
DATA_EXPIRY_SECONDS = 3600 # 1 小時內抓過則跳過

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def init_audit_db():
    conn = sqlite3.connect(AUDIT_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_time TEXT,
        market_id TEXT,
        total_count INTEGER,
        success_count INTEGER,
        fail_count INTEGER,
        success_rate REAL
    )''')
    conn.close()

def get_cn_list():
    """獲取 A 股清單 (含多重備援)"""
    ensure_pkg("akshare")
    import akshare as ak
    threshold = 4500
    
    # 檢查今日快取
    if os.path.exists(CACHE_LIST_PATH):
        if datetime.fromtimestamp(os.path.getmtime(CACHE_LIST_PATH)).date() == datetime.now().date():
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if len(data) >= threshold: return data

    log("📡 更新清單中...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        res = [f"{row['code']}&{row['名称' if '名称' in df.columns else '名稱']}" for _, row in df.iterrows()]
        if len(res) >= threshold:
            with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False)
            return res
    except: pass
    return ["600519&貴州茅台", "000001&平安銀行"] if not os.path.exists(CACHE_LIST_PATH) else json.load(open(CACHE_LIST_PATH))

def download_one(item):
    """單檔下載：具備時效檢查"""
    try:
        code, name = item.split('&', 1)
        symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        # ✨ 智慧快取邏輯
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "tkr": code}

        time.sleep(random.uniform(0.7, 1.5))
        tk = yf.Ticker(symbol)
        hist = tk.history(period="2y", timeout=25)
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "tkr": code}
        return {"status": "empty", "tkr": code}
    except:
        return {"status": "error", "tkr": code}

def main():
    start_time = time.time()
    init_audit_db()
    items = get_cn_list()
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="CN 下載進度")
        for f in as_completed(futures):
            res = f.result()
            s = res["status"]
            stats[s] += 1
            if s in ["error", "empty"]: fail_list.append(res["tkr"])
            pbar.update(1)
        pbar.close()

    total = len(items)
    success = stats['success'] + stats['exists']
    fail = stats['error'] + stats['empty']
    
    # 紀錄 Audit DB
    conn = sqlite3.connect(AUDIT_DB_PATH)
    now_ts = (datetime.utcnow() + pd.Timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    conn.execute('INSERT INTO sync_audit (execution_time, market_id, total_count, success_count, fail_count, success_rate) VALUES (?,?,?,?,?,?)',
                 (now_ts, MARKET_CODE, total, success, fail, round(success/total*100, 2) if total>0 else 0))
    conn.commit()
    conn.close()

    return {"total": total, "success": success, "fail": fail, "fail_list": fail_list}

if __name__ == "__main__":
    main()
