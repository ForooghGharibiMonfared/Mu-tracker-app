from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
# وارد کردن تابع اسکرپر از فایل scrap.py
from scrap import scrape_to_cloud 

load_dotenv()

app = FastAPI()

# تنظیمات CORS برای اینکه مرورگر اجازه بده فرانت‌ند به بک‌اِند وصل بشه
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URI = os.getenv("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DB_URI, cursor_factory=RealDictCursor)

@app.get("/")
def read_root():
    return {"message": "Mac-Tracker API is Online!"}

@app.get("/products")
def get_products():
    """گرفتن لیست محصولات به همراه آخرین قیمت هر کدام"""
    conn = get_db_connection()
    cur = conn.cursor()
    # این کوئری آخرین قیمت ثبت شده برای هر محصول رو برمی‌گردونه
    cur.execute('''
        SELECT DISTINCT ON (p.uuid) 
               p.name, p.category, p.image_url, h.price, h.currency, h.scraped_at
        FROM products p
        LEFT JOIN price_history h ON p.uuid = h.product_uuid
        ORDER BY p.uuid, h.scraped_at DESC;
    ''')
    products = cur.fetchall()
    cur.close()
    conn.close()
    return products

@app.get("/stats")
def get_stats():
    """محاسبه میانگین قیمت برای هر دسته‌بندی (مخصوص نمودار)"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        SELECT category, ROUND(AVG(price), 2) as avg_price 
        FROM products p 
        JOIN price_history h ON p.uuid = h.product_uuid 
        WHERE price > 0
        GROUP BY category
        ORDER BY avg_price DESC;
    ''')
    stats = cur.fetchall()
    cur.close()
    conn.close()
    return stats

@app.post("/run-scraper")
async def run_scraper(background_tasks: BackgroundTasks):
    """اجرای اسکرپر در پس‌زمینه (بدون اینکه سایت کند بشه)"""
    try:
        background_tasks.add_task(scrape_to_cloud)
        return {"message": "the scrapping runs in the background and it can be can be more than 30 min., until the database becomes updated."}
    except Exception as e:
        return {"error": str(e)}