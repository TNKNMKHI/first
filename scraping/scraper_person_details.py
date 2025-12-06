import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import re
from tqdm import tqdm
import traceback
import os
from dotenv import load_dotenv

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

NETKEIBA_DB_BASE_URL = "https://db.netkeiba.com"

def get_html(url):
    """指定されたURLからHTMLを取得する"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_person_ids_to_update(table_name):
    """詳細情報が未入力の人物IDリストを取得する"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        # birth_dateがNULLのレコードを取得
        cursor.execute(f"SELECT {table_name[:-1]}_id FROM {table_name} WHERE birth_date IS NULL")
        ids = [row[0] for row in cursor.fetchall()]
        return ids
    finally:
        conn.close()

def scrape_and_update_person_details(person_id, person_type):
    """netkeiba.comから人物詳細をスクレイピングしてDBを更新する"""
    # JBIS ID (e.g., J01191) から netkeiba ID (e.g., 01191) へ変換
    netkeiba_id = person_id.lstrip('J')
    url = f"{NETKEIBA_DB_BASE_URL}/{person_type}/{netkeiba_id}/"
    
    html = get_html(url)
    if not html:
        return

    soup = BeautifulSoup(html, 'lxml')
    profile_table = soup.select_one('table.db_prof_table')
    if not profile_table:
        print(f"Could not find profile table for {person_type} {person_id} at {url}")
        return

    details = {}
    for row in profile_table.find_all('tr'):
        th = row.find('th')
        td = row.find('td')
        if th and td:
            key = th.text.strip()
            value = td.text.strip()
            if '生年月日' in key:
                date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', value)
                if date_match:
                    details['birth_date'] = f"{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}"
            elif '所属' in key:
                details['belonging'] = value

    if not details:
        print(f"Could not parse details for {person_type} {person_id}")
        return

    # DB更新
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        table_name = f"{person_type}s" # jockey -> jockeys, trainer -> trainers
        cursor.execute(f'''
        UPDATE {table_name}
        SET birth_date = ?, belonging = ?
        WHERE {person_type}_id = ?
        ''', (details.get('birth_date'), details.get('belonging'), person_id))
        conn.commit()
        # print(f"Updated details for {person_type} {person_id}")
    except Exception as e:
        print(f"DB Error updating {person_type} {person_id}: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def main():
    """騎手と調教師の詳細情報を更新するメイン処理"""
    # 騎手情報の更新
    print("Fetching jockey IDs to update...")
    jockey_ids = get_person_ids_to_update('jockeys')
    print(f"Found {len(jockey_ids)} jockeys to update.")
    if jockey_ids:
        for jockey_id in tqdm(jockey_ids, desc="Updating jockeys"):
            try:
                scrape_and_update_person_details(jockey_id, 'jockey')
                time.sleep(1)
            except Exception as e:
                print(f"An error occurred while processing jockey {jockey_id}: {e}")

    # 調教師情報の更新
    print("\nFetching trainer IDs to update...")
    trainer_ids = get_person_ids_to_update('trainers')
    print(f"Found {len(trainer_ids)} trainers to update.")
    if trainer_ids:
        for trainer_id in tqdm(trainer_ids, desc="Updating trainers"):
            try:
                scrape_and_update_person_details(trainer_id, 'trainer')
                time.sleep(1)
            except Exception as e:
                print(f"An error occurred while processing trainer {trainer_id}: {e}")

    print("\nPerson details update process finished.")

if __name__ == "__main__":
    main()