import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# 建立連線
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

# 查詢資料表
cursor.execute("SELECT * FROM monthly_top_clients ORDER BY order_month DESC, sales_rank ASC;")
rows = cursor.fetchall()

print("\n📦 AWS 資料庫裡的最新內容：")
for row in rows:
    print(row)

cursor.close()
conn.close()