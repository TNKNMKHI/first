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

BASE_URL = "https://www.jbis.or.jp/horse/"

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

def parse_pedigree(pedigree_soup):
    """5代血統表を解析して祖先IDの辞書を返す"""
    # 変更後: (ancestor_id, generation, position) のタプルのリストを返す
    pedigree_list = []
    # 旧実装: pedigree = {}

    table = pedigree_soup.select_one('table.tbl-pedigree')
    if not table:
        return {}

    ancestors = []
    # 1代から5代までの祖先を取得 (62頭)
    # 父、母、父母、母父、母母... の順
    rows = table.select('tr')
    
    # 1代 (父、母)
    sire_a = rows[0].select_one('td a')
    dam_a = rows[16].select_one('td a')
    ancestors.append(re.search(r'/horse/(\w+)/', sire_a['href']).group(1) if sire_a else None)
    ancestors.append(re.search(r'/horse/(\w+)/', dam_a['href']).group(1) if dam_a else None)

    # 2代 (FF, FM, MF, MM)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[0].select('td a')[1]['href']).group(1) if len(rows[0].select('td a')) > 1 else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[8].select_one('td a')['href']).group(1) if rows[8].select_one('td a') else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[16].select('td a')[1]['href']).group(1) if len(rows[16].select('td a')) > 1 else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[24].select_one('td a')['href']).group(1) if rows[24].select_one('td a') else None)

    # 3代以降 (8 + 16 + 32 = 56頭)
    # 各世代の先頭インデックス
    gen3_indices = [0, 4, 8, 12, 16, 20, 24, 28]
    gen4_indices = [i for i in range(32) if i % 2 == 0]
    gen5_indices = [i for i in range(32)]

    # 3代
    for i in gen3_indices:
        a_tags = rows[i].select('td a')
        ancestors.append(re.search(r'/horse/(\w+)/', a_tags[2]['href']).group(1) if len(a_tags) > 2 else None)
    # 4代
    for i in gen4_indices:
        a_tags = rows[i].select('td a')
        ancestors.append(re.search(r'/horse/(\w+)/', a_tags[3]['href']).group(1) if len(a_tags) > 3 else None)
    # 5代
    for i in gen5_indices:
        a_tags = rows[i].select('td a')
        ancestors.append(re.search(r'/horse/(\w+)/', a_tags[4]['href']).group(1) if len(a_tags) > 4 else None)

    cols = []
    current_gen = ['']
    for i in range(5):
        next_gen_labels = []
        for label in current_gen:
            next_gen_labels.append(label + 'f')
            next_gen_labels.append(label + 'm')
        cols.extend(sorted(next_gen_labels)) # f, m, ff, fm, mf, mm... の順にする
        current_gen = next_gen_labels
    
    for i, position in enumerate(cols):
        if i < len(ancestors):
            ancestor_id = ancestors[i]
            if ancestor_id:
                generation = len(position)
                pedigree_list.append((ancestor_id, generation, position))

    return pedigree_list

def parse_horse_page(profile_soup, horse_id):
    """馬の個別ページを解析し、(horse_data, owner_data, breeder_data) を返す"""
    try:
        horse_data = {'horse_id': horse_id}
        owner_data = None
        breeder_data = None

        name_elem = profile_soup.select_one('h1.heading-level2-bold')
        horse_data['name'] = name_elem.text.strip() if name_elem else ""
        
        prof_table = profile_soup.select_one('table.tbl-data-04')
        if prof_table:
            rows = prof_table.select('tr')
            for row in rows:
                th = row.select_one('th').text.strip()
                td = row.select_one('td')
                
                if '生年月日' in th:
                    # e.g., "2021/04/14"
                    date_text = td.text.strip()
                    try:
                        # JBISの "YYYY/MM/DD" 形式をパース
                        dt_obj = datetime.strptime(date_text, '%Y/%m/%d')
                        horse_data['birth_date'] = dt_obj.strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        horse_data['birth_date'] = None
                    
                elif '性別' in th:
                    horse_data['sex'] = td.text.strip()
                    
                elif '調教師' in th:
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/trainer/(\w+)/', a_tag['href'])
                        horse_data['trainer_id'] = match.group(1) if match else None
                
                elif '馬主' in th:
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/owner/(\w+)/', a_tag['href'])
                        owner_id = match.group(1) if match else None
                        owner_name = a_tag.text.strip()
                        if owner_id:
                            horse_data['owner_id'] = owner_id
                            owner_data = {'owner_id': owner_id, 'name': owner_name}
                
                elif '生産者' in th:
                    a_tag = td.select_one('a')
                    if a_tag:
                        match = re.search(r'/breeder/(\w+)/', a_tag['href'])
                        breeder_id = match.group(1) if match else None
                        breeder_name = a_tag.text.strip()
                        if breeder_id:
                            horse_data['breeder_id'] = breeder_id
                            breeder_data = {'breeder_id': breeder_id, 'name': breeder_name}
        
        return horse_data, owner_data, breeder_data
            
    except Exception as e:
        print(f"Error parsing horse page {horse_id}: {e}")
        traceback.print_exc()
        return None, None, None

def save_horse_to_db(horse_data, owner_data, breeder_data, pedigree_list):
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

        # 1. horsesテーブルに基本情報を保存
        cursor.execute(
            """
            INSERT OR IGNORE INTO horses (horse_id, name, birth_date, sex, trainer_id, owner_id, breeder_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                horse_data.get('horse_id'), horse_data.get('name'), horse_data.get('birth_date'),
                horse_data.get('sex'), horse_data.get('trainer_id'), horse_data.get('owner_id'),
                horse_data.get('breeder_id')
            )
        )

        # 2. pedigreesテーブルに血統情報を保存
        if pedigree_list:
            pedigree_insert_data = [
                (horse_data['horse_id'], ancestor_id, generation, position)
                for ancestor_id, generation, position in pedigree_list
            ]
            cursor.executemany(
                "INSERT OR IGNORE INTO pedigrees (horse_id, ancestor_id, generation, position) VALUES (?, ?, ?, ?)",
                pedigree_insert_data
            )

        conn.commit()

    except sqlite3.Error as e:
        print(f"DB Error: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def scrape_missing_horses():
    ids = get_unscraped_horse_ids()
    print(f"Found {len(ids)} horses to scrape.")
    
    if not ids:
        print("No new horses to scrape.")
        return

    for horse_id in tqdm(ids, desc="Scraping Horses"):
        try:
            # 1. プロフィールページの取得と解析
            profile_url = f"{BASE_URL}{horse_id}/"
            profile_html = get_html_from_jbis(profile_url)
            if not profile_html:
                print(f"Failed to fetch profile for {horse_id}. Skipping.")
                continue
            
            profile_soup = BeautifulSoup(profile_html, 'lxml')
            horse_data, owner_data, breeder_data = parse_horse_page(profile_soup, horse_id)
            
            if not horse_data:
                print(f"Failed to parse profile for {horse_id}. Skipping.")
                continue

            # 2. 血統ページの取得と解析
            pedigree_url = f"{BASE_URL}{horse_id}/pedigree/"
            pedigree_html = get_html_from_jbis(pedigree_url)
            if not pedigree_html:
                print(f"Failed to fetch pedigree for {horse_id}. Skipping.")
                continue
            
            pedigree_soup = BeautifulSoup(pedigree_html, 'lxml')
            pedigree_list = parse_pedigree(pedigree_soup)

            # 3. DBへの保存
            save_horse_to_db(horse_data, owner_data, breeder_data, pedigree_list)
            time.sleep(1) # サーバー負荷軽減

        except Exception as e:
            print(f"An unexpected error occurred for horse {horse_id}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    scrape_missing_horses()
    print("Scraping completed.")
