# 🌍 Global Stock Multi-Matrix Monitor 
### 全球股市六國矩陣監控
[![Build Status](https://github.com/你的帳號/global-stock-data-warehouse/actions/workflows/main.yml/badge.svg)](https://github.com/你的帳號/global-stock-data-warehouse/actions)
[![Python Version](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[English](#english) | [中文](#中文)

---

<a name="english"></a>
## 🚀 Project Overview

A professional-grade, multi-market automated monitoring and data warehousing system. It performs large-scale data scraping and matrix analysis across **6 major global markets**. The system visualizes market breadth and momentum through a **3x3 Rolling Distribution Matrix**, delivering interactive daily reports via Resend API.

### 🌎 Monitored Markets
- 🇹🇼 **Taiwan (TW)**: TWSE/TPEx All-share (Stock, ETF, Emerging).
- 🇺🇸 **United States (US)**: NYSE & NASDAQ Common Stocks.
- 🇭🇰 **Hong Kong (HK)**: HKEX Main Board & GEM ordinary shares.
- 🇨🇳 **China (CN)**: SSE/SZSE A-shares (via Akshare).
- 🇯🇵 **Japan (JP)**: Tokyo Stock Exchange (TSE) coverage.
- 🇰🇷 **South Korea (KR)**: KOSPI & KOSDAQ (via PyKRX).

### 🛠️ Key Features
- **Rolling Window Logic**: Indicators are calculated based on **Rolling Trading Days (5D / 20D / 250D)** instead of calendar dates. This eliminates weekend/holiday gaps and ensures statistical continuity for **1,000-day high breakouts** and momentum backtesting.
- **Parallel Matrix Strategy**: Utilizes GitHub Actions to run 6 independent market tasks simultaneously, reducing total runtime from 90 mins to **15 mins**.
- **Resilient Pipeline**: Includes randomized jitter to prevent IP blocking and threshold guards to ensure data integrity.
- **Smart Reporting**: Integrated **Resend API** for HTML reports featuring 10% return bins and direct technical chart links.

---

<a name="中文"></a>
## 🚀 專案概述

這是一個專業級的多國自動化監控與數據倉庫系統，針對 **全球 6 大主要市場** 執行大規模數據爬取與矩陣分析。系統透過 **3x3 滾動分佈矩陣** 視覺化市場寬度與動能，每日透過 Resend API 寄送互動式電子郵件報表。

### 🌎 監控市場
- 🇹🇼 **台灣 (TW)**：上市、上櫃、興櫃、ETF 全股票覆蓋。
- 🇺🇸 **美國 (US)**：NYSE、NASDAQ 普通股。
- 🇭🇰 **香港 (HK)**：港交所主板與創業板普通股。
- 🇨🇳 **中國 (CN)**：滬深 A 股（透過 Akshare）。
- 🇯🇵 **日本 (JP)**：東京證券交易所 (TSE) 全股票。
- 🇰🇷 **韓國 (KR)**：KOSPI 與 KOSDAQ (透過 PyKRX)。

### 🛠️ 核心功能
- **滾動交易日邏輯**：所有分析指標與矩陣均基於 **滾動交易日 (Rolling Trading Days)** 計算（5日/20日/250日），而非日曆日期。此方法排除了非交易日的干擾，確保留存數據在 **「千日新高」** 或動能回測中的統計連續性。
- **平行矩陣策略 (Matrix Strategy)**：利用 GitHub Actions 同時啟動 6 台虛擬機並行運算，將總執行時間從 1.5 小時縮短至 **15 分鐘**。
- **強韌下載管線**：內建隨機延遲 (Jitter) 防止 IP 封鎖，並具備數量門檻防護確保清單獲取完整。
- **互動式報表**：整合 **Resend API**，自動生成包含 10% 報酬分箱圖表的 HTML 報表，並提供直達券商線圖的超連結。

---

## ⚡ Architecture & Efficiency (運算架構)



- **Sequential Load (本機初次)**: 10y+ historical data is seeded locally to build the initial warehouse.
- **Incremental Update (雲端每日)**: GitHub Actions fetches only the last **7 rolling days** to update the database, ensuring zero-cost maintenance.
- **Fault Isolation**: Each market runner is independent. If one market API fails, the others remain unaffected.

## 🧰 Tech Stack (技術棧)
- **Language**: Python 3.10
- **Database**: SQLite3 (with optimized indexing)
- **Data Sources**: Yfinance, Akshare, PyKRX, Tokyo-Stock-Exchange
- **Visualization**: Matplotlib, Numpy
- **Automation**: GitHub Actions (Matrix Strategy)
- **Cloud Sync**: Google Drive API

## 🚀 快速上手教學 (方格子專文)

如果你是初學者，或是想在 5 分鐘內快速完成設定，請參考這篇詳細的圖文教學：
👉 [**【教學】如何獲取 Resend API Key 並設定 GitHub 變數**](https://vocus.cc/article/694a52fcfd89780001b9f09b)

### 只要三步即可運作：
1. **註冊 Resend**：獲取 API Key（初學者可直接使用預設的 `onboarding@resend.dev` 寄件帳號）。
2. **設定 GitHub Secrets**：在 Repo 的 Settings 中新增變數 `RESEND_API_KEY` 與 `REPORT_RECEIVER_EMAIL`。
3. **啟動 Actions**：手動或依排程啟動，即可在信箱收到精美的台股監控報表。





------------------------------------------------------------------------------------------------------------------------------

⚠️ Data Integrity & Learning Note (資料完整性與練習說明)
[!IMPORTANT] [English] Raw Data vs. Production Ready This repository currently provides Raw Data without deep post-processing. Users may encounter anomalies such as Corporate Action Gaps (unadjusted splits) or Resume Jumps (suspension noise) that may lead to distorted return calculations (e.g., ghost >100% monthly returns).

We deliberately keep the data "raw" to make it a perfect Data Cleaning Practice ground. To learn how to implement professional-grade cleaning logic, refer to my guide: 👉 Technical Guide: Modern Data Cleaning Pipeline

[!IMPORTANT] [中文] 原始數據與實戰差異說明 本專案目前產出的數據為 原始數據 (Raw Data)，尚未經過深度清洗。分析時可能會遇到除權息缺口、停牌復牌跳空或殭屍 K 線，這可能導致報酬率統計異常（例如：明明沒漲卻顯示月 K 報酬 > 100%）。

為什麼這是絕佳的練習素材？ 我們刻意保留數據的「原始狀態」，適合作為 Pandas 資料處理 的實戰練習。如果你想學習如何實作專業級的清洗邏輯（如：乒乓模式偵測、物理價格防護、復牌標記），請參考我的技術專文： 👉 方格子技術導引：現代化資料清洗管線實作https://vocus.cc/article/6949c6a9fd8978000142a5c6

🛠️ Data Cleaning Goals for Learners (給學習者的練習目標)
如果你想利用此專案練手，建議嘗試實作以下邏輯：

[NaN Handling]：識別並處理缺失的價格點。

[Ping-Pong Filter]：過濾單日 +/- 40% 的異常反轉（通常是數據雜訊）。

[Volume Guard]：剔除成交量極低、不具代表性的「殭屍股」。
---------------------------------------------------------------------------------------------------------------------------------
## ⚖️ License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
![googlesheet1](image/6job1.png)


![googlesheet1](image/6job.png)


![googlesheet1](image/6job3.png)


![googlesheet1](image/6job4.png)


![googlesheet1](image/week_close.png)



![googlesheet1](image/week_high.png)



![googlesheet1](image/week_low.png)


![googlesheet1](image/month_high.png)


![googlesheet1](image/month_low.png)


![googlesheet1](image/month_close.png)


![googlesheet1](image/year_close.png)



![googlesheet1](image/year_high.png)


![googlesheet1](image/year_low.png)



![googlesheet1](image/1.png)


![googlesheet1](image/2.png)


![googlesheet1](image/3.png)


![googlesheet1](image/4.png)



![googlesheet1](image/5.png)

---
📢 **系統更新 (2025/12/23)**：優化數據抓取管線與快取機制，詳情內容請[按此連結查看完整更新日誌](https://vocus.cc/article/694b1b63fd89780001e66192)。

<img width="901" height="585" alt="image" src="https://github.com/user-attachments/assets/ba307ab0-73cc-49ae-a33e-d8de5e4454b5" />












