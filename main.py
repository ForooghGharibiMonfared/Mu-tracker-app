from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
# وارد کردن تابع اسکرپر از فایل scrap.py
from scrap import scrape_to_cloud 

load_dotenv()

app = FastAPI()

# تنظیمات CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# اتصال به فایل‌های استاتیک (HTML, CSS, JS)
# نکته: فایل index.html باید درون پوشه ای به نام static باشد
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

DB_URI = os.getenv("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DB_URI, cursor_factory=RealDictCursor)

@app.get("/")
def read_root():
    """نمایش صفحه اصلی سایت (ویترین)"""
    index_path = os.path.join("static", "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Mac-Tracker API is Online! (Put your index.html in 'static' folder to see the UI)"}

@app.get("/products")
def get_products():
    """گرفتن لیست محصولات به همراه آخرین قیمت هر کدام"""
    conn = get_db_connection()
    cur = conn.cursor()
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
    """اجرای اسکرپر در پس‌زمینه"""
    try:
        background_tasks.add_task(scrape_to_cloud)
        return {"message": "The scraping runs in the background. It might take some time to update the database."}
    except Exception as e:
        return {"error": str(e)}