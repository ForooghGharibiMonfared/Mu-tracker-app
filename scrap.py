import requests
import psycopg2
import time
import os
from dotenv import load_dotenv

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")

def setup_database():
    """ساخت جدول‌ها در صورت عدم وجود"""
    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS products (
            uuid UUID PRIMARY KEY,
            name TEXT,
            sku TEXT,
            category TEXT,
            image_url TEXT,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS price_history (
            id SERIAL PRIMARY KEY,
            product_uuid UUID REFERENCES products(uuid),
            price NUMERIC,
            currency TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    cur.close()
    conn.close()

def scrape_to_cloud():
    # آدرس API واقعی که پیدا کردی
    base_url = "https://backend.gomula.com/api/v1/warehouse/category_tree/get_catalog_articles/"
    limit = 18
    offset = 0
    total_count = 1

    conn = psycopg2.connect(DB_URI)
    cur = conn.cursor()

    print("🚀 شروع استخراج و انتقال داده به دیتابیس ابری...")

    while offset < total_count:
        params = {"filter": "clothing-fashion", "limit": limit, "offset": offset}
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                total_count = data.get('count', 0)
                results = data.get('results', [])

                for item in results:
                    u_id = item.get('uuid')
                    name = item.get('name')
                    sku = item.get('sku')
                    category = item['categories'][0]['name'] if item.get('categories') else "N/A"
                    img = item.get('image_url')
                    
                    price_info = item.get('price_points', [])
                    price = price_info[0].get('selling_price') if price_info else 0
                    currency = price_info[0].get('currency') if price_info else "EUR"

                    # وارد کردن محصول
                    cur.execute('''
                    INSERT INTO products (uuid, name, sku, category, image_url)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (uuid) DO UPDATE SET
                    image_url = EXCLUDED.image_url,
                    last_update = CURRENT_TIMESTAMP;
                    ''', (u_id, name, sku, category, img))

                    # وارد کردن قیمت در تاریخچه
                    cur.execute('''
                    INSERT INTO price_history (product_uuid, price, currency)
                    VALUES (%s, %s, %s);
                    ''', (u_id, price, currency))

                conn.commit()
                print(f"✅ {len(results)} محصول سینک شد. (Offset: {offset})")
                offset += limit
                time.sleep(0.5)
            else:
                break
        except Exception as e:
            print(f"❌ Error: {e}")
            break

    cur.close()
    conn.close()
    print("✨ دیتابیس آپدیت شد!")

if __name__ == "__main__":
    setup_database()
    scrape_to_cloud()