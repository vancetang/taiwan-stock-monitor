# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝/匯入必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# 路徑定義 (與您的 main.py 結構對接)
MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 狀態管理檔案
MANIFEST_CSV = Path(LIST_DIR) / "jp_manifest.csv"
LIST_ALL_CSV = Path(LIST_DIR) / "jp_list_all.csv"
THREADS = 4

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_tse_list():
    """獲取日股清單：自動識別日文標頭 (コード/銘柄名) 或英文標頭"""
    log("📡 正在讀取 tokyo-stock-exchange 套件資料...")
    try:
        # 讀取套件內建 CSV
        df = pd.read_csv(tse.csv_file_path)
        log(f"📋 偵測到 CSV 原始欄位: {list(df.columns)}")

        # 1. 尋找代碼欄位 (優先匹配日文版本，再嘗試英文)
        code_col = None
        for cand in ['コード', 'Code', 'code', 'Local Code', 'ticker']:
            if cand in df.columns:
                code_col = cand
                break
        
        # 2. 尋找名稱欄位
        name_col = None
        for cand in ['銘柄名', 'Name', 'name', 'Company Name']:
            if cand in df.columns:
                name_col = cand
                break

        if not code_col:
            raise KeyError(f"無法在 CSV 中定位代碼欄位。現有欄位: {list(df.columns)}")

        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            # 日股通常是 4 位數字代碼
            if len(code) >= 4 and code[:4].isdigit():
                res.append({
                    "code": code[:4], 
                    "name": str(row[name_col]) if name_col else code[:4], 
                    "board": "T"
                })
        
        final_df = pd.DataFrame(res).drop_duplicates(subset=['code'])
        # 使用 utf-8-sig 存檔，確保 Excel 開啟不亂碼
        final_df.to_csv(LIST_ALL_CSV, index=False, encoding='utf-8-sig')
        log(f"✅ 成功獲取 {len(final_df)} 檔日股清單")
        return final_df

    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return pd.DataFrame()

def build_manifest(df_list):
    """建立或載入續跑清單 (Manifest)"""
    if df_list.empty:
        log("⚠️ 傳入清單為空，無法建立 Manifest")
        return pd.DataFrame()

    if MANIFEST_CSV.exists():
        log(f"📄 載入現有續跑紀錄：{MANIFEST_CSV}")
        return pd.read_csv(MANIFEST_CSV)
    
    log("🆕 正在建立全新的 jp_manifest.csv...")
    df_list["status"] = "pending"
    
    # 掃描本地已存在的檔案 (.T.csv 格式)
    existing_files = {f.split(".")[0] for f in os.listdir(DATA_DIR) if f.endswith(".T.csv")}
    if existing_files:
        log(f"🔍 偵測到本地已有 {len(existing_files)} 份檔案，自動標記為 done")
        df_list.loc[df_list['code'].astype(str).isin(existing_files), "status"] = "done"
    
    df_list.to_csv(MANIFEST_CSV, index=False)
    return df_list

def download_one(row_tuple):
    """單檔下載邏輯，轉換為標準格式"""
    idx, row = row_tuple
    code = str(row['code']).zfill(4)
    symbol = f"{code}.T"
    out_path = os.path.join(DATA_DIR, f"{code}.T.csv")
    
    try:
        tk = yf.Ticker(symbol)
        # 抓取 2 年資料供分析
        df_raw = tk.history(period="2y", interval="1d", auto_adjust=False)
        if df_raw is not None and not df_raw.empty:
            df_raw.reset_index(inplace=True)
            df_raw.columns = [c.lower() for c in df_raw.columns]
            
            # 標準化日期格式 (移除時區)
            if 'date' in df_raw.columns:
                df_raw['date'] = pd.to_datetime(df_raw['date'], utc=True).dt.tz_localize(None)
            
            # 只要核心 6 欄位
            cols = ['date','open','high','low','close','volume']
            df_final = df_raw[[c for c in cols if c in df_raw.columns]]
            df_final.to_csv(out_path, index=False)
            return idx, "done"
        return idx, "empty"
    except Exception:
        return idx, "failed"

def main():
    log("🇯🇵 日本股市 K 線下載器啟動 (TSE 版)")
    
    # 1. 獲取清單
    df_list = get_tse_list()
    if df_list.empty:
        log("❌ 終止執行：無法取得股票清單。")
        return

    # 2. 建立/讀取續跑清單
    mf = build_manifest(df_list)
    if mf.empty: return

    # 3. 過濾待處理標的
    todo = mf[mf["status"] != "done"]
    if todo.empty:
        log("✅ 所有日股資料已是最新。")
        return

    log(f"📝 待處理標的數：{len(todo)} 檔")

    # 4. 多執行緒並行下載
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
        pbar = tqdm(total=len(todo), desc="日股下載進度")
        
        count = 0
        for f in as_completed(futures):
            idx, status = f.result()
            mf.at[idx, "status"] = status
            count += 1
            pbar.update(1)
            
            # 每 50 筆儲存一次狀態，防止意外中斷進度遺失
            if count % 50 == 0:
                mf.to_csv(MANIFEST_CSV, index=False)
        
        pbar.close()

    # 5. 任務結算
    mf.to_csv(MANIFEST_CSV, index=False)
    success_count = len(mf[mf['status'] == 'done'])
    log(f"🏁 任務結束。成功下載：{success_count} 檔，失敗/無資料：{len(mf)-success_count} 檔")

if __name__ == "__main__":
    main()
