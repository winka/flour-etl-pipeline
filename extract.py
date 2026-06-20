import pandas as pd
import json
import sqlite3
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def extract_data():
    """
    從不同來源（CSV, JSON）萃取原始資料，
    並轉換為 Pandas DataFrame 以供後續處理。
    """
    print("🚀 開始執行 Extract (萃取) 階段...")

    # 1. 讀取 CSV 訂單資料
    try:
        # pd.read_csv 是處理表格式檔案的神器
        df_orders = pd.read_csv('orders.csv')
        print(f"✅ 成功讀取 orders.csv，共 {len(df_orders)} 筆資料")
    except FileNotFoundError:
        print("❌ 找不到 orders.csv，請確認檔案是否存在。")
        return None, None

    # 2. 讀取 JSON 客戶資料
    try:
        # 使用內建的 json 模組讀取檔案
        with open('clients.json', 'r', encoding='utf-8') as file:
            clients_raw_data = json.load(file)
            
        # 將 JSON 結構轉換為 Pandas DataFrame 格式，方便後續與訂單進行 SQL 般的 Join 操作
        df_clients = pd.DataFrame(clients_raw_data)
        print(f"✅ 成功讀取 clients.json，共 {len(df_clients)} 筆資料")
    except FileNotFoundError:
        print("❌ 找不到 clients.json，請確認檔案是否存在。")
        return None, None

    return df_orders, df_clients
# DLQ 髒資料隔離區塊 (Dead Letter Queue)
def clean_error(order):#, client):
    """
    加入三道防線的進階 Transform 函式
    """
    print("\n🛡️ [Step 2] 啟動防禦機制與 Transform 清洗...")

    # ---------------------------------------------------------
    #【第一道防線：資料合約 (Data Contract)】
    # 在開始處理前，先檢查「絕對不能少」的欄位是否存在 (Fail-fast 機制)
    # 一旦發現問題就立刻停止，避免後續的處理浪費時間在錯誤資料上
    #【第二道防線：強制轉型 (Type Coercion)】
    # 不管塞了什麼亂碼，都強制轉成正確型態。轉失敗的一律變成空值 (NaN/NaT)
    # ---------------------------------------------------------
    expected_columns = ['order_id', 'client_id', 'flour_type', 'weight_kg', 'order_date']
    for col in expected_columns:
        if col not in orders.columns:
            raise ValueError(f"🚨 資料合約破裂：上游傳來的 CSV 缺少重要欄位 '{col}'！管線已緊急停止。")
    print("   ➔ (防線 3 通過) 資料欄位結構符合合約。")
    
    #【第二道防線：強制轉型 (Type transform)】
    orders['weight_kg'] = pd.to_numeric(orders['weight_kg'], errors='coerce')
    
    # 強制把日期轉為時間格式 (遇到 "2026-99-99" 會變成 NaT)
    orders['order_date'] = orders['order_date'].astype(str).str.replace('/', '-')
    orders['order_date'] = pd.to_datetime(orders['order_date'], errors='coerce')
    orders['order_date'] = orders['order_date'].dt.strftime('%Y-%m-%d')
    print("   ➔ (防線 1 通過) 型態強制轉換完畢，亂碼已標記為空值。")


    # 髒資料隔離
    # 建立一個存在「記憶體 (memory)」中的暫時資料庫，程式關閉就會消失
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE raw_orders (
            order_id text,
            client_id text,
            flour_type TEXT,
            weight_kg REAL,
            order_date TEXT
        )
    """)
    # 將 Pandas 資料寫入我們剛剛定義好的表中 (if_exists='append' 表示附加進去)
    order.to_sql('raw_orders', conn, if_exists='append', index=False)
    print("   ➔ (防線 2 通過) 資料已安全進入具備 Schema 的資料表中。")

    # clean data 
    sql_dlg = """
    SELECT 
        order_id,
        client_id,
        flour_type,
        weight_kg,
        order_date
    FROM raw_orders
    WHERE 
        order_id is null
        or client_id is null
        or  flour_type is null
        or  weight_kg is null
        or  order_date is null
    """
    # dirty data
    sql_clean = """
    SELECT 
        order_id,
        client_id,
        flour_type,
        weight_kg,
        order_date
    FROM raw_orders
    WHERE 
        order_id is not null
        and client_id is not null
        and  flour_type is not null
        and  weight_kg is not null
        and  order_date is not null
    """
    clean_df = pd.read_sql(sql_clean, conn)
    dirty_df = pd.read_sql(sql_dlg, conn)
    print("✅ SQL 分類完成！")
    print("\n--- 🧹 髒資料 (Dirty Data) ---" )
    print(dirty_df)
    print("\n--- 🧼 清洗後的資料 (Clean Data) ---")    
    print(clean_df)
    conn.close()    
    return clean_df, dirty_df

def apply_business_logic(df_clean_orders, df_clients):
    """
    執行商業邏輯：將訂單與客戶資料關聯，聚合計算月銷售額，並產生大客戶排名。
    """
    print("\n📈 [Step 3] 開始執行商業邏輯計算 (Data Aggregation)...")
    
    # 1. 建立記憶體資料庫，將兩張表都倒進去準備進行 Join
    conn = sqlite3.connect(':memory:')
    df_clean_orders.to_sql('clean_orders', conn, index=False)
    df_clients.to_sql('clients', conn, index=False)
    
    # 2. 撰寫核心商業邏輯 SQL
    # 第一段 (CTE)：先將兩張表 Join 起來，並以「月份」和「客戶」為維度進行加總
    # 第二段 (主查詢)：針對加總後的結果，使用 RANK() 進行分月排名
    sql_business_logic = """
    WITH monthly_sales AS (
        select a.client_id, b.name as client_name, 
        strftime('%Y-%m', a.order_date) as order_month,
        sum(weight_kg) as  total_weight_kg
        from clean_orders a 
        left join clients b on a.client_id = b.client_id
        group by  a.client_id, b.name, strftime('%Y-%m', a.order_date)
        ) 
    SELECT  *,RANK() OVER (PARTITION BY order_month ORDER BY total_weight_kg DESC) as sales_rank
    FROM 
    monthly_sales
    """
    
    # 3. 執行 SQL 並將最終報表存回 DataFrame
    final_report_df = pd.read_sql(sql_business_logic, conn)
    conn.close()
    
    return final_report_df

def load_to_aws_rds(df_report):
    """
    將 Pandas DataFrame 安全載入至 AWS RDS (PostgreSQL)
    """
    print("\n☁️ [Step 4] 開始執行 Load (安全載入至 AWS RDS)...")
    
    try:
        # 完全沿用之前的讀取環境變數邏輯
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS monthly_top_clients (
                order_month VARCHAR(10),
                sales_rank INTEGER,
                client_name VARCHAR(100),
                total_weight_kg REAL,
                PRIMARY KEY (order_month, client_name)
            );
        """)
        
        # 完美沿用的 PostgreSQL Upsert 語法
        upsert_sql = """
            INSERT INTO monthly_top_clients (order_month, sales_rank, client_name, total_weight_kg)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_month, client_name) DO nothing;
        """
        
        data_list = df_report[['order_month', 'sales_rank', 'client_name', 'total_weight_kg']].values.tolist()
        cursor.executemany(upsert_sql, data_list)
        
        conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM monthly_top_clients;")
        total_rows = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ 安全寫入 AWS RDS 成功！目前總計：{total_rows} 筆紀錄。")
        
    except Exception as e:
        print(f"❌ 資料庫連線或寫入失敗，原因: {e}")

# ... 前面是你原本的 extract_data() 程式碼 ...

if __name__ == "__main__":
    orders, clients = extract_data()
    
    if orders is not None and clients is not None:
        print("\n--- 🛠️ 開始使用 SQL 進行 Transform 轉換 ---")
    

        clean_data, dirty_data = clean_error(orders)

        df_client = pd.read_json('clients.json')
        final_report = apply_business_logic(clean_data, df_client)
        print("\n--- 📊 最終報表 ---")


        print(final_report)

        load_to_aws_rds(final_report)
