# coding: utf-8
import sqlite3
import time
import re
import os
from dotenv import load_dotenv
from tqdm import tqdm
import traceback

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

def get_driver():
    """Selenium WebDriverを初期化して返す"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def get_html(driver, url):
    """指定されたURLからHTMLを取得する"""
    try:
        driver.get(url)
        time.sleep(1) # 負荷軽減
        if "エラー" in driver.title or "ご指定のページは見つかりませんでした" in driver.page_source:
            return None
        return driver.page_source
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

# --- Jockey Scraping ---

def get_jockeys_to_scrape():
    """所属や誕生日が未入力の騎手IDを取得する"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT jockey_id FROM jockeys WHERE belonging IS NULL OR birth_date IS NULL")
        return [row[0] for row in cursor.fetchall()]

def parse_person_profile(soup):
    """騎手または調教師のプロフィールページを解析する"""
    details = {}
    prof_table = soup.select_one('table.db_prof_table')
    if not prof_table:
        return None
    
    rows = prof_table.select('tr')
    for row in rows:
        th = row.select_one('th').text.strip()
        td = row.select_one('td').text.strip()
        
        if '生年月日' in th:
            match = re.search(r'(\d{4}年\d{1,2}月\d{1,2}日)', td)
            if match:
                details['birth_date'] = match.group(1).replace('年', '-').replace('月', '-').replace('日', '')
        elif '所属' in th:
            details['belonging'] = td.split(' ')[0] # "美浦" や "栗東" などを取得
            
    return details

def update_jockey_details(jockey_id, details):
    """騎手情報を更新する"""
    if not details or not details.get('birth_date'):
        return
        
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE jockeys 
            SET belonging = ?, birth_date = ? 
            WHERE jockey_id = ? AND (belonging IS NULL OR birth_date IS NULL)
        """, (details.get('belonging'), details.get('birth_date'), jockey_id))
        conn.commit()

def scrape_jockeys(driver):
    """騎手の詳細情報をスクレイピングする"""
    jockey_ids = get_jockeys_to_scrape()
    if not jockey_ids:
        print("No new jockeys to scrape.")
        return
        
    print(f"Scraping details for {len(jockey_ids)} jockeys...")
    for jockey_id in tqdm(jockey_ids, desc="Jockeys"):
        url = f"https://db.netkeiba.com/jockey/prof/{jockey_id}/"
        html = get_html(driver, url)
        if not html: continue
        
        soup = BeautifulSoup(html, 'lxml')
        details = parse_person_profile(soup)
        if details:
            update_jockey_details(jockey_id, details)

# --- Trainer Scraping ---

def get_trainers_to_scrape():
    """所属や誕生日が未入力の調教師IDを取得する"""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT trainer_id FROM trainers WHERE belonging IS NULL OR birth_date IS NULL")
        return [row[0] for row in cursor.fetchall()]

def update_trainer_details(trainer_id, details):
    """調教師情報を更新する"""
    if not details or not details.get('birth_date'):
        return

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE trainers 
            SET belonging = ?, birth_date = ? 
            WHERE trainer_id = ? AND (belonging IS NULL OR birth_date IS NULL)
        """, (details.get('belonging'), details.get('birth_date'), trainer_id))
        conn.commit()

def scrape_trainers(driver):
    """調教師の詳細情報をスクレイピングする"""
    trainer_ids = get_trainers_to_scrape()
    if not trainer_ids:
        print("No new trainers to scrape.")
        return

    print(f"Scraping details for {len(trainer_ids)} trainers...")
    for trainer_id in tqdm(trainer_ids, desc="Trainers"):
        url = f"https://db.netkeiba.com/trainer/prof/{trainer_id}/"
        html = get_html(driver, url)
        if not html: continue
        
        soup = BeautifulSoup(html, 'lxml')
        details = parse_person_profile(soup)
        if details:
            update_trainer_details(trainer_id, details)

def main():
    print("Initializing Selenium Driver...")
    driver = get_driver()
    
    try:
        scrape_jockeys(driver)
        scrape_trainers(driver)
        print("Scraping of person details completed.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
    finally:
        print("Closing driver...")
        driver.quit()

if __name__ == "__main__":
    main()
