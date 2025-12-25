# -*- coding: utf-8 -*-
import os, io, re, time, random, json, requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數與路徑 ==========
MARKET_CODE = "hk-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
CACHE_LIST_PATH = os.path.join(BASE_DIR, "hk_stock_list_cache.json")

# GitHub Actions 建議執行緒設為 4，避免被 Yahoo 封鎖 IP
MAX_WORKERS = 4
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 工具：代碼與安全過濾 ==========
def normalize_code5(s: str) -> str:
    """確保為 5 位數補零格式 (用於存檔名稱)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits else ""

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (例如 0700.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    if not digits: return ""
    # 取後四位或五位並加上 .HK
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品、牛熊證與非普通股標的"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證", "衍生", "界內證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

# ========== 核心：雙重保險清單獲取 ==========
def get_full_stock_list():
    """
    🛡️ 雙重保險機制：優先使用 Akshare，若數據異常則切換至 HKEX 官網 Excel
    """
    if os.path.exists(CACHE_LIST_PATH):
        file_mtime = os.path.getmtime(CACHE_LIST_PATH)
        if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
            log("📦 偵測到今日已緩存港股清單，直接載入...")
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    final_list = []

    # --- 方案 A: 使用 Akshare (API 方式) ---
    log("📡 [方案 A] 嘗試從 Akshare 獲取港股清單...")
    try:
        import akshare as ak
        df_ak = ak.stock_hk_spot_em()
        if df_ak is not None and len(df_ak) > 500:
            for _, row in df_ak.iterrows():
                name = str(row['名称'])
                if classify_security(name) == "Common Stock":
                    code = str(row['代码'])
                    final_list.append(f"{code}&{name}")
            log(f"✅ 方案 A 成功，初步獲取 {len(final_list)} 檔標的。")
    except Exception as e:
        log(f"⚠️ 方案 A 失敗: {e}")

    # --- 方案 B: 使用 HKEX 官網 (Excel 下載方式) ---
    if len(final_list) < 500:
        log("📡 [方案 B] Akshare 數據不足，嘗試從 HKEX 官網獲取清單...")
        try:
            url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            r = requests.get(url, headers=headers, timeout=30)
            df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
            
            # 定位表頭
            hdr_idx = 0
            for i in range(25):
                row_str = "".join([str(x) for x in df_raw.iloc[i]]).lower()
                if "stock code" in row_str and "short name" in row_str:
                    hdr_idx = i
                    break
            
            df_hkex = df_raw.iloc[hdr_idx+1:].copy()
            df_hkex.columns = df_raw.iloc[hdr_idx].tolist()
            
            # 尋找代碼與名稱欄位
            col_code = [c for c in df_hkex.columns if "Stock Code" in str(c)][0]
            col_name = [c for c in df_hkex.columns if "Short Name" in str(c)][0]
            
            for _, row in df_hkex.iterrows():
                raw_name = str(row[col_name])
                if classify_security(raw_name) == "Common Stock":
                    code5 = normalize_code5(str(row[col_code]))
                    if code5 and int(code5) >= 1:
                        final_list.append(f"{code5}&{raw_name}")
            log(f"✅ 方案 B 成功，目前累積 {len(final_list)} 檔標的。")
        except Exception as e:
            log(f"❌ 方案 B 獲取失敗: {e}")

    # 最終處理與存儲快取
    if final_list:
        final_list = list(set(final_list)) # 去重
        with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False)
        log(f"🎉 最終確定港股監控清單: {len(final_list)} 檔。")
        return final_list
    else:
        log("🚨 [錯誤] 無法從任何來源獲取港股清單！")
        return []

# ========== 數據下載邏輯 ==========
def download_stock_data(item):
    try:
        code5, name = item.split('&', 1)
        yf_sym = to_symbol_yf(code5)
        out_path = os.path.join(DATA_DIR, f"{code5}.HK.csv")
        
        # 檢查是否今日已更新且檔案有效
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            mtime = datetime.fromtimestamp(os.path.getmtime(out_path)).date()
            if mtime == datetime.now().date():
                return {"status": "exists", "tkr": code5}

        # 延遲避免被 Yahoo 封鎖
        time.sleep(random.uniform(0.5, 1.2))
        tk = yf.Ticker(yf_sym)
        hist = tk.history(period="2y", timeout=20)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "tkr": code5}
            
        return {"status": "empty", "tkr": code5}
    except:
        return {"status": "error"}

# ========== 主程式入口 (對接 main.py) ==========
def main():
    items = get_full_stock_list()
    if not items:
        return {"total": 0, "success": 0, "fail": 0}
    
    log(f"🚀 啟動港股 K 線下載 (執行緒: {MAX_WORKERS})")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="港股進度", unit="檔")
        
        for future in as_completed(futures):
            res = future.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
            
            # 每成功下載 100 檔額外休息，防止被封 IP
            if res.get("status") == "success" and stats["success"] % 100 == 0:
                time.sleep(random.uniform(3, 7))
        pbar.close()

    # 封裝結果傳回 main.py
    report_stats = {
        "total": len(items),
        "success": stats["success"] + stats["exists"],
        "fail": stats["error"] + stats["empty"]
    }
    
    print("\n" + "="*50)
    log(f"📊 港股任務總結: {report_stats}")
    print("="*50 + "\n")
    
    return report_stats

if __name__ == "__main__":
    main()
