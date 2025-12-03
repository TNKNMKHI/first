import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from tqdm import tqdm
import traceback
import os
from dotenv import load_dotenv
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

BASE_URL = "https://db.netkeiba.com/horse/"

def get_driver():
    """Selenium WebDriverを初期化して返す"""
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
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
        # JavaScriptの読み込み待ち時間を延長
        time.sleep(5)
        
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

    ancestors = []
    links = table.select('a[href*="/horse/ped/"]')
    
    for link in links:
        match = re.search(r'/horse/ped/(\w+)', link['href'])
        if match:
            ancestors.append(match.group(1))

    # 5代血統のカラム名を生成 (f, m, ff, fm, mf, mm, ...)
    # 2 + 4 + 8 + 16 + 32 = 62 祖先
    cols = []
    base = ['f', 'm']
    
    # 1代から5代までループ
    current_gen = ['']
    for i in range(5):
        next_gen_labels = []
        for label in current_gen:
            next_gen_labels.append(label + 'f')
            next_gen_labels.append(label + 'm')
        cols.extend(next_gen_labels)
        current_gen = next_gen_labels
    
    # 抽出したIDをカラムにマッピング (最大62カラム)
    for i, col_name in enumerate(cols):
        if i < len(ancestors):
            pedigree[f"{col_name}_id"] = ancestors[i]
        else:
            pedigree[f"{col_name}_id"] = None # 足りない場合はNone
            
    return pedigree 

def parse_horse_page(soup, horse_id):
    """馬の個別ページを解析し、(horse_data, owner_data, breeder_data) を返す"""
    try:
        horse_data = {'horse_id': horse_id}
        owner_data = None
        breeder_data = None

        name_elem = soup.select_one('div.horse_title h1')
        horse_data['name'] = name_elem.text.strip() if name_elem else ""
        
        prof_table = soup.select_one('table.db_prof_table')
        if prof_table:
            rows = prof_table.select('tr')
            for row in rows:
                th = row.select_one('th').text.strip()
                td = row.select_one('td')
                
                if th == '生年月日':
                    m = re.search(r'(\d+)年', td.text)
                    horse_data['birth_year'] = int(m.group(1)) if m else None
                    
                elif th == '性別':
                    horse_data['sex'] = td.text.strip()
                    
                elif th == '調教師':
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/trainer/prof/(\w+)/', a_tag['href'])
                        horse_data['trainer_id'] = match.group(1) if match else None
                
                elif th == '馬主':
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/owner/prof/(\w+)/', a_tag['href'])
                        owner_id = match.group(1) if match else None
                        owner_name = a_tag.text.strip()
                        if owner_id:
                            horse_data['owner_id'] = owner_id
                            owner_data = {'owner_id': owner_id, 'name': owner_name}
                
                elif th == '生産者':
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/breeder/prof/(\w+)/', a_tag['href'])
                        breeder_id = match.group(1) if match else None
                        breeder_name = a_tag.text.strip()
                        if breeder_id:
                            horse_data['breeder_id'] = breeder_id
                            breeder_data = {'breeder_id': breeder_id, 'name': breeder_name}
                
                elif th == 'サイアーライン':
                    horse_data['sire_line'] = td.text.strip()
                    
        pedigree_data = parse_pedigree(soup)
        horse_data.update(pedigree_data)
        
        return horse_data, owner_data, breeder_data
            
    except Exception as e:
        print(f"Error parsing horse page {horse_id}: {e}")
        traceback.print_exc()
        return None, None, None

def save_horse_to_db(horse_data, owner_data, breeder_data):
    if not horse_data: return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # OwnerとBreederを先に保存
        if owner_data:
            cursor.execute("INSERT OR IGNORE INTO owners (owner_id, name) VALUES (?, ?)",
                           (owner_data['owner_id'], owner_data['name']))
        if breeder_data:
            cursor.execute("INSERT OR IGNORE INTO breeders (breeder_id, name) VALUES (?, ?)",
                           (breeder_data['breeder_id'], breeder_data['name']))

        # 5代血統のカラム名を動的に生成
        pedigree_cols = []
        current_gen = ['']
        for _ in range(5):
            next_gen = []
            for label in current_gen:
                next_gen.extend([label + 'f', label + 'm'])
            pedigree_cols.extend(next_gen)
            current_gen = next_gen
        
        pedigree_col_names = [f"{col}_id" for col in pedigree_cols]
        
        # SQL文の準備
        columns = [
            'horse_id', 'name', 'birth_year', 'sex', 
            'trainer_id', 'owner_id', 'breeder_id', 'sire_line',
            *pedigree_col_names
        ]
        
        placeholders = ', '.join(['?'] * len(columns))
        sql = f"INSERT OR IGNORE INTO horses ({', '.join(columns)}) VALUES ({placeholders})"
        
        # 値のリストを準備
        values = [
            horse_data.get('horse_id'), horse_data.get('name'), horse_data.get('birth_year'),
            horse_data.get('sex'), horse_data.get('trainer_id'), horse_data.get('owner_id'),
            horse_data.get('breeder_id'), horse_data.get('sire_line', '')
        ]
        # 血統IDを追加
        for col in pedigree_col_names:
            values.append(horse_data.get(col))
            
        cursor.execute(sql, tuple(values))
        conn.commit()

    except sqlite3.Error as e:
        print(f"DB Error: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def scrape_missing_horses(driver):
    ids = get_unscraped_horse_ids()
    print(f"Found {len(ids)} horses to scrape.")
    
    if not ids:
        print("No new horses to scrape.")
        return

    for hid in tqdm(ids):
        html = get_html_with_selenium(driver, hid)
        if not html:
            print(f"Failed to fetch HTML for {hid}. Skipping.")
            continue
        
        soup = BeautifulSoup(html, 'lxml')
        horse_data, owner_data, breeder_data = parse_horse_page(soup, hid)
        
        if horse_data:
            save_horse_to_db(horse_data, owner_data, breeder_data)
        else:
            print(f"Failed to parse data for {hid}. Skipping.")

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
