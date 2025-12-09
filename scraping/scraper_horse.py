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
from get_html import get_html_from_jbis

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

BASE_URL = "https://www.jbis.or.jp/horse/"


def get_incomplete_horse_ids():
    """horsesテーブルでnameがNULLまたは空文字のhorse_idを取得する"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 馬の基本情報(name)がまだ埋まっていないレコードを取得
    query = '''
    SELECT horse_id 
    FROM horses 
    WHERE (name IS NULL OR name = '') AND horse_id IS NOT NULL AND horse_id != ''
    '''
    cursor.execute(query)
    ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return ids

def get_missing_pedigree_horse_ids():
    """horsesテーブルに存在するが、pedigreesテーブルにデータがない馬のIDを取得する"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    query = '''
    SELECT h.horse_id
    FROM horses h
    LEFT JOIN pedigrees p ON h.horse_id = p.horse_id
    WHERE p.horse_id IS NULL AND h.horse_id IS NOT NULL AND h.horse_id != '' AND h.name IS NOT NULL AND h.name != ''
    '''
    cursor.execute(query)
    ids = [row[0] for row in cursor.fetchall() if row[0]]
    conn.close()
    return ids

def parse_pedigree(pedigree_soup):
    """5代血統表を解析して祖先IDの辞書を返す"""
    pedigree_list = []
    table = pedigree_soup.select_one('table.tbl-pedigree')
    if not table:
        return []

    ancestors = []
    rows = table.select('tr')
    
    # This parsing logic seems complex and specific to the website structure.
    # We will assume it is correct for now.
    sire_a = rows[0].select_one('td a')
    dam_a = rows[16].select_one('td a')
    ancestors.append(re.search(r'/horse/(\w+)/', sire_a['href']).group(1) if sire_a else None)
    ancestors.append(re.search(r'/horse/(\w+)/', dam_a['href']).group(1) if dam_a else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[0].select('td a')[1]['href']).group(1) if len(rows[0].select('td a')) > 1 else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[8].select_one('td a')['href']).group(1) if rows[8].select_one('td a') else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[16].select('td a')[1]['href']).group(1) if len(rows[16].select('td a')) > 1 else None)
    ancestors.append(re.search(r'/horse/(\w+)/', rows[24].select_one('td a')['href']).group(1) if rows[24].select_one('td a') else None)
    gen3_indices = [0, 4, 8, 12, 16, 20, 24, 28]
    for i in gen3_indices:
        a_tags = rows[i].select('td a')
        ancestors.append(re.search(r'/horse/(\w+)/', a_tags[2]['href']).group(1) if len(a_tags) > 2 else None)
    gen4_indices = [i for i in range(32) if i % 2 == 0]
    for i in gen4_indices:
        a_tags = rows[i].select('td a')
        ancestors.append(re.search(r'/horse/(\w+)/', a_tags[3]['href']).group(1) if len(a_tags) > 3 else None)
    gen5_indices = [i for i in range(32)]
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
        cols.extend(sorted(next_gen_labels))
    
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
            for row in prof_table.select('tr'):
                th = row.select_one('th').text.strip()
                td = row.select_one('td')
                if not td: continue
                
                if '生年月日' in th:
                    date_text = td.text.strip()
                    try:
                        dt_obj = datetime.strptime(date_text, '%Y/%m/%d')
                        horse_data['birth_date'] = dt_obj.strftime('%Y-%m-%d')
                    except (ValueError, TypeError): horse_data['birth_date'] = None
                elif '性別' in th:
                    horse_data['sex'] = td.text.strip()
                elif '調教師' in th:
                    a_tag = td.select_one('a[href*="/trainer/"]')
                    if a_tag: horse_data['trainer_id'] = re.search(r'/trainer/(\w+)/', a_tag['href']).group(1)
                elif '馬主' in th:
                    a_tag = td.select_one('a[href*="/owner/"]')
                    if a_tag:
                        owner_id = re.search(r'/owner/(\w+)/', a_tag['href']).group(1)
                        horse_data['owner_id'] = owner_id
                        owner_data = {'owner_id': owner_id, 'name': a_tag.text.strip()}
                elif '生産者' in th:
                    a_tag = td.select_one('a[href*="/breeder/"]')
                    if a_tag:
                        breeder_id = re.search(r'/breeder/(\w+)/', a_tag['href']).group(1)
                        horse_data['breeder_id'] = breeder_id
                        breeder_data = {'breeder_id': breeder_id, 'name': a_tag.text.strip()}
        
        return horse_data, owner_data, breeder_data
            
    except Exception as e:
        print(f"Error parsing horse page {horse_id}: {e}")
        traceback.print_exc()
        return None, None, None

def update_horse_in_db(horse_data, owner_data, breeder_data, pedigree_list):
    """DBの既存の馬情報を更新し、血統情報を追加する"""
    if not horse_data or not horse_data.get('name'):
        return
    
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

        # 1. horsesテーブルの既存のレコードを更新
        cursor.execute(
            """
            UPDATE horses 
            SET name = ?, birth_date = ?, sex = ?, trainer_id = ?, owner_id = ?, breeder_id = ?
            WHERE horse_id = ?
            """,
            (
                horse_data.get('name'), horse_data.get('birth_date'),
                horse_data.get('sex'), horse_data.get('trainer_id'), 
                horse_data.get('owner_id'), horse_data.get('breeder_id'),
                horse_data.get('horse_id')
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
        print(f"DB Error on update: {e}")
    finally:
        conn.close()

def save_pedigree_to_db(horse_id, pedigree_list):
    """指定されたhorse_idの血統情報のみをDBに保存する"""
    if not horse_id or not pedigree_list: return
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.executemany(
            "INSERT OR IGNORE INTO pedigrees (horse_id, ancestor_id, generation, position) VALUES (?, ?, ?, ?)",
            [(horse_id, anc_id, gen, pos) for anc_id, gen, pos in pedigree_list]
        )
        conn.commit()
    finally:
        conn.close()

def scrape_incomplete_horses():
    """馬の基本情報が欠けているデータを補完する"""
    ids = get_incomplete_horse_ids()
    print(f"Found {len(ids)} incomplete horse records to update.")
    
    if not ids:
        print("No incomplete horses to scrape.")
        return

    for horse_id in tqdm(ids, desc="Scraping Incomplete Horses"):
        try:
            profile_url = f"{BASE_URL}{horse_id}/"
            profile_html = get_html_from_jbis(profile_url)
            if not profile_html: continue
            
            profile_soup = BeautifulSoup(profile_html, 'lxml')
            horse_data, owner_data, breeder_data = parse_horse_page(profile_soup, horse_id)
            if not horse_data: continue

            pedigree_url = f"{BASE_URL}{horse_id}/pedigree/"
            pedigree_html = get_html_from_jbis(pedigree_url)
            pedigree_list = []
            if pedigree_html:
                pedigree_soup = BeautifulSoup(pedigree_html, 'lxml')
                pedigree_list = parse_pedigree(pedigree_soup)

            update_horse_in_db(horse_data, owner_data, breeder_data, pedigree_list)
            time.sleep(1)

        except Exception as e:
            print(f"An unexpected error occurred for horse {horse_id}: {e}")

def scrape_missing_pedigrees():
    """血統情報が欠けている馬のデータを補完する"""
    ids = get_missing_pedigree_horse_ids()
    print(f"\nFound {len(ids)} horses with missing pedigrees to update.")

    if not ids:
        print("No missing pedigrees to scrape.")
        return

    for horse_id in tqdm(ids, desc="Scraping Missing Pedigrees"):
        try:
            pedigree_url = f"{BASE_URL}{horse_id}/pedigree/"
            pedigree_html = get_html_from_jbis(pedigree_url)
            if not pedigree_html: continue

            pedigree_soup = BeautifulSoup(pedigree_html, 'lxml')
            pedigree_list = parse_pedigree(pedigree_soup)
            save_pedigree_to_db(horse_id, pedigree_list)
            time.sleep(1)
        except Exception as e:
            print(f"An unexpected error occurred for horse {horse_id}: {e}")

if __name__ == "__main__":
    scrape_incomplete_horses()
    scrape_missing_pedigrees()
    print("\nScraping completed.")
