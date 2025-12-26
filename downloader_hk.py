# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import urllib3

# 忽略 SSL 警告 (港交所官網有時會報憑證錯誤)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能調優
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, 
                            name TEXT, 
                            sector TEXT, 
                            market TEXT,
                            updated_at TEXT)''')
        
        # 自動升級舊資料庫
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 HK 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取港股清單 (強化穩定性) ==========

def get_hk_stock_list():
    """獲取港股清單並確保寫入 stock_info"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    # 模擬完整瀏覽器 Header
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }
    
    log(f"📡 正在從港交所獲取名單...")
    try:
        # 使用 verify=False 避免 SSL 阻擋
        r = requests.get(url, headers=headers, timeout=20, verify=False)
        r.raise_for_status()
        
        # 讀取 Excel
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找包含 "Stock Code" 的正確起始行
        hdr_idx = None
        for i in range(len(df_raw)):
            row_str = " ".join([str(x) for x in df_raw.iloc[i].values])
            if "Stock Code" in row_str:
                hdr_idx = i
                break
        
        if hdr_idx is None: 
            log("❌ 找不到 Excel 表頭，請檢查網址是否有變。")
            return []
        
        # 重新整理 DataFrame
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].values
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 💡 先清空舊 info 數據確保重新同步
        conn.execute("DELETE FROM stock_info")

        for _, row in df.iterrows():
            raw_code = str(row['Stock Code']).strip()
            # 港股名稱可能在不同欄位名下 (English Stock Short Name)
            name_col = [c for c in df.columns if 'Short Name' in str(c) and 'English' in str(c)]
            name = str(row[name_col[0]]).strip() if name_col else "Unknown"
            
            # 港股普通股邏輯：數字且長度 <= 4 (或是 5 位但前幾位是 0)
            if raw_code.isdigit() and int(raw_code) < 10000:
                symbol = f"{raw_code.zfill(4)}.HK"
                market = "HKEX"
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, "Unknown", market, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
        conn.commit()
        conn.close()
        log(f"✅ 港股清單同步完成：{len(stock_list)} 檔")
        return stock_list
        
    except Exception as e:
        log(f"⚠️ 港股名單獲取異常: {e}")
        # 萬一失敗，返回基本的藍籌股名單確保程序不崩潰
        return [("0700.HK", "TENCENT"), ("09988.HK", "BABA-SW"), ("00005.HK", "HSBC HOLDINGS")]

# ========== 4. 下載邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            wait_time = random.uniform(2.0, 4.0) if IS_GITHUB_ACTIONS else random.uniform(0.2, 0.5)
            time.sleep(wait_time)
            
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 12))
                continue
            return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_hk_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始港股同步 | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="HK同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    log("🧹 資料庫 VACUUM...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    
    return {
        "success": stats['success'],
        "error": stats['error'],
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
