import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from tqdm import tqdm
import traceback
import os
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

# 絶対パス指定
DB_PATH = r'C:\Users\T123085\github\horseRacing\first\keiba.db'
BASE_URL = "https://db.netkeiba.com/horse/"

def get_driver():
    """Selenium WebDriverを初期化して返す"""
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080") # ウィンドウサイズを指定
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def get_html_with_selenium(driver, horse_id):
    """Seleniumを使ってページを取得しHTMLを返す"""
    url = f"{BASE_URL}{horse_id}"
    try:
        driver.get(url)
        time.sleep(1)
        
        if "エラー" in driver.title or "ご指定のページは見つかりませんでした" in driver.page_source:
            return None
            
        return driver.page_source
    except Exception as e:
        print(f"Error fetching {url} with Selenium: {e}")
        return None

def get_unscraped_horse_ids():
    """resultsテーブルにあってhorsesテーブルにないhorse_idを取得する"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # resultsにある全horse_id - horsesにある全horse_id
    query = '''
    SELECT DISTINCT r.horse_id 
    FROM results r
    LEFT JOIN horses h ON r.horse_id = h.horse_id
    WHERE h.horse_id IS NULL
    '''
    cursor.execute(query)
    ids = [row[0] for row in cursor.fetchall() if row[0]] # Noneや空文字を除外
    conn.close()
    return ids

def parse_pedigree(soup):
    """5代血統表を解析して祖先IDの辞書を返す"""
    pedigree = {}
    table = soup.select_one('table.blood_table')
    if not table:
        return {}

    # 父ID (f_id) の取得強化
    # 父は通常 rowspan="32" のtd内にある
    f_id = None
    father_td = table.select_one('td[rowspan="32"]')
    if father_td:
        a_tag = father_td.select_one('a')
        if a_tag and 'href' in a_tag.attrs:
            m = re.search(r'/horse/(\d+)', a_tag['href'])
            if m:
                f_id = m.group(1)
    
    # バックアップ: もしrowspanで見つからなければ、最初の有効なリンクを採用
    if not f_id:
        tds = table.select('td')
        for td in tds:
            a_tag = td.select_one('a')
            if a_tag and 'href' in a_tag.attrs and '/horse/' in a_tag['href']:
                m = re.search(r'/horse/(\d+)', a_tag['href'])
                if m:
                    f_id = m.group(1)
                    break # 最初に見つかったものを父とする

    return {'f_id': f_id} 

def parse_horse_page(soup, horse_id):
    """馬の個別ページを解析"""
    try:
        name_elem = soup.select_one('div.horse_title h1')
        name = name_elem.text.strip() if name_elem else ""
        
        prof_table = soup.select_one('table.db_prof_table')
        birth_year = None
        sex = None
        
        if prof_table:
            rows = prof_table.select('tr')
            for row in rows:
                th = row.select_one('th').text.strip()
                td = row.select_one('td').text.strip()
                
                if th == '生年月日':
                    m = re.search(r'(\d+)年', td)
                    if m: 
                        birth_year = int(m.group(1))
                    
                if th == '性別':
                    sex = td.strip() 
                    
        pedigree_data = parse_pedigree(soup)
        
        return {
            'horse_id': horse_id,
            'name': name,
            'birth_year': birth_year,
            'sex': sex,
            'sire_line': '', 
            **pedigree_data
        }
            
    except Exception as e:
        print(f"Error parsing horse page {horse_id}: {e}")
        return None

def save_horse_to_db(data):
    if not data: return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT OR IGNORE INTO horses (horse_id, name, birth_year, sex, sire_line, f_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            data['horse_id'], data['name'], data['birth_year'], data['sex'], 
            data['sire_line'], data.get('f_id')
        ))
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

def scrape_missing_horses(driver):
    ids = get_unscraped_horse_ids()
    print(f"Found {len(ids)} horses to scrape.")
    
    # デバッグ: 1頭だけ処理
    ids = ids[:1]
    
    for hid in tqdm(ids):
        html = get_html_with_selenium(driver, hid)
        if not html: continue
        
        # デバッグ: HTMLを保存
        with open('debug_horse.html', 'w', encoding='utf-8') as f:
            f.write(html)
            
        soup = BeautifulSoup(html, 'lxml')
        data = parse_horse_page(soup, hid)
        
        if data:
            save_horse_to_db(data)

if __name__ == "__main__":
    print("Initializing Selenium Driver...")
    driver = get_driver()
    
    try:
        scrape_missing_horses(driver)
        print("Scraping completed.")
            
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        print("Closing driver...")
        driver.quit()