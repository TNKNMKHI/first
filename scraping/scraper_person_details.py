import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from tqdm import tqdm
import traceback
import os
from dotenv import load_dotenv
from datetime import datetime
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

BASE_URL = "https://www.jbis.or.jp"

def get_html_from_jbis(url):
    """指定されたJBISのURLからHTMLを取得する"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, verify=False, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding

        if "該当するデータが見つかりませんでした" in response.text:
            print(f"Page not found or no data for URL: {url}")
            return None
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_unscraped_person_ids(table_name, id_column):
    """指定されたテーブルで詳細情報が欠けているIDのリストを取得する"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # birth_dateがNULLまたは空のレコードを対象とする
    query = f"SELECT {id_column} FROM {table_name} WHERE birth_date IS NULL OR birth_date = ''"
    cursor.execute(query)
    ids = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()
    return ids

def parse_person_page(soup):
    """騎手または調教師のページを解析し、詳細情報を辞書で返す"""
    data = {'birth_date': None, 'belonging': None}
    try:
        prof_table = soup.select_one('table.tbl-data-04')
        if not prof_table:
            return data

        rows = prof_table.select('tr')
        for row in rows:
            th = row.select_one('th').text.strip()
            td = row.select_one('td')

            if '生年月日' in th:
                # 例: "1988年10月29日"
                date_text = td.text.strip()
                match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_text)
                if match:
                    year, month, day = match.groups()
                    data['birth_date'] = f"{year}-{int(month):02d}-{int(day):02d}"

            elif '所属' in th:
                data['belonging'] = td.text.strip()

    except Exception as e:
        print(f"Error parsing person page: {e}")
        traceback.print_exc()

    return data

def update_person_in_db(table_name, id_column, person_id, data):
    """DBの騎手または調教師情報を更新する"""
    if not data or not data.get('birth_date'):
        print(f"Not enough data to update for {person_id}. Skipping.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            UPDATE {table_name}
            SET birth_date = ?, belonging = ?
            WHERE {id_column} = ?
            """,
            (data['birth_date'], data['belonging'], person_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"DB Error updating {person_id}: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def scrape_person_details(person_type):
    """騎手または調教師の詳細情報をスクレイピングする"""
    if person_type == 'jockey':
        table_name, id_column = 'jockeys', 'jockey_id'
        url_path = '/jockey/'
    elif person_type == 'trainer':
        table_name, id_column = 'trainers', 'trainer_id'
        url_path = '/trainer/'
    else:
        print(f"Invalid person_type: {person_type}")
        return

    ids_to_scrape = get_unscraped_person_ids(table_name, id_column)
    if not ids_to_scrape:
        print(f"No new {person_type}s to scrape.")
        return

    print(f"Found {len(ids_to_scrape)} {person_type}s to scrape.")

    for person_id in tqdm(ids_to_scrape, desc=f"Scraping {person_type}s"):
        try:
            url = f"{BASE_URL}{url_path}{person_id}/"
            html = get_html_from_jbis(url)
            if not html:
                print(f"Failed to fetch HTML for {person_type} {person_id}. Skipping.")
                continue

            soup = BeautifulSoup(html, 'lxml')
            person_data = parse_person_page(soup)

            update_person_in_db(table_name, id_column, person_id, person_data)

            time.sleep(1) # サーバー負荷軽減

        except Exception as e:
            print(f"An unexpected error occurred for {person_type} {person_id}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    print("--- Starting to scrape Jockey details ---")
    scrape_person_details('jockey')
    print("--- Finished scraping Jockey details ---")

    print("\n--- Starting to scrape Trainer details ---")
    scrape_person_details('trainer')
    print("--- Finished scraping Trainer details ---")

    print("\nScraping for person details completed.")