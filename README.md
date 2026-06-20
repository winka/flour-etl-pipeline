# 🌾 Flour Sales ETL Pipeline (麵粉批發大客戶資料管線)

這是一個自動化的資料工程 ETL (Extract, Transform, Load) 專案。主要用於處理麵粉批發商的每月訂單資料，進行清洗、商業邏輯計算，並將最終的「每月大客戶排行榜」安全地寫入雲端關聯式資料庫。

## 🛠️ 技術標籤 (Tech Stack)
* **語言:** Python 3.x
* **資料處理:** Pandas
* **雲端資料庫:** AWS RDS (PostgreSQL)
* **套件管理:** `psycopg2-binary`, `python-dotenv`

## 🏗️ 系統架構與特色 (Architecture & Features)
1. **Extract (萃取):** 從本地端/原始來源讀取訂單資料 (`orders.csv`) 與客戶名單 (`clients.json`)。
2. **Transform (轉換):**
   * 執行資料清洗，過濾異常值。
   * 分類資料，含異常值資料(Dirty Data)以及乾淨資料(Clean Data)。
   * Clean Data套用商業邏輯，彙總每位客戶的總採購重量。
   * 使用視窗函數邏輯計算客戶的每月銷售排名 (Sales Rank)。
3. **Load (載入):**
   * 透過聯合主鍵 (Order Month, Client Name) 實作 **Upsert (Insert On Conflict Do Update)** 機制。
   * 確保資料庫不會產生重複資料，且支援歷史數據重跑 (Idempotent)。
   * 採用環境變數 (`.env`) 隔離雲端連線憑證，符合資安規範。

graph LR
    subgraph Source [原始資料 Data]
        A[orders.csv]
        B[clients.json]
    end

    subgraph Pipeline [Python ETL Pipeline]
        direction LR
        C(Op 1: 清洗資料) --> D(Op 2: 計算總量與排名) --> E(Op 3: 轉換英文欄位)
    end

    subgraph Target [目標資料庫 Data]
        F[(AWS RDS PostgreSQL)]
    end

    Source ==> Pipeline
    Pipeline ==> Target
