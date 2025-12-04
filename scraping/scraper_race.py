import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import time
import re
from tqdm import tqdm
import traceback
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import os
import datetime
from dotenv import load_dotenv

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

# .env読み込み
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

BASE_URL = "https://www.jbis.or.jp/race/"

def get_html_from_jbis_url(url):
    """指定されたURLからHTMLを取得する"""
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

def parse_race_info(soup, race_id):
    """レース情報を解析して辞書で返す"""
    try:
        # JBISのページ構造に合わせて解析
        header = soup.select_one('div.header-inner')
        if not header:
            return None

        # 日付と競馬場
        date_venue_text = header.select_one('h1.heading-level2-bold').text.strip()
        date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_venue_text)
        date_str = f"{date_match.group(1)}-{int(date_match.group(2)):02d}-{int(date_match.group(3)):02d}" if date_match else ""
        venue = date_venue_text.split(' ')[-1].strip()

        # レース番号とレース名
        race_title_elem = header.select_one('h2.heading-level2')
        race_title_text = race_title_elem.text.strip()
        round_match = re.search(r'(\d{1,2})R', race_title_text)
        race_round = int(round_match.group(1)) if round_match else 0
        race_name = race_title_elem.find(text=True, recursive=False).strip()
        race_class_elem = race_title_elem.select_one('span.icon-grade')
        race_class = race_class_elem.text.strip() if race_class_elem else ""

        # レース条件
        race_spec_text = soup.select_one('div.box-note-01').text.strip()
        course_match = re.search(r'(芝|ダート|障害)\s*(左|右|直線)?\s*(\d+)m', race_spec_text)
        course_type, rotation, distance = "Unknown", "Unknown", 0
        if course_match:
            course_type = course_match.group(1)
            rotation = course_match.group(2) if course_match.group(2) else "直線" if "直線" in race_spec_text else "Unknown"
            distance = int(course_match.group(3))

        weather_match = re.search(r'天候：(.*?)\s', race_spec_text)
        weather = weather_match.group(1).strip() if weather_match else ""

        state_match = re.search(r'(芝|ダート)：(.*?)\s', race_spec_text)
        state = state_match.group(2).strip() if state_match else ""

        return {
            'race_id': race_id,
            'date': date_str,
            'venue': venue,
            'race_name': race_name,
            'race_class': race_class,
            'race_round': race_round,
            'course_type': course_type,
            'distance': distance,
            'rotation': rotation,
            'weather': weather,
            'state': state,
            'entries': 0 # 後でresultsの長さで更新
        }
    except Exception as e:
        print(f"Error parsing race info for {race_id}: {e}")
        traceback.print_exc()
        return None

def parse_race_results(soup, race_id):
    """レース結果テーブルを解析して (results, jockeys, trainers) のタプルを返す"""
    results, jockeys, trainers = [], [], []
    try:
        table = soup.select_one('table.table-data-01')
        if not table:
            return [], [], []
        
        rows = table.select('tr')[1:] # ヘッダーを除く
        for row in rows:
            cols = row.select('td')
            if len(cols) < 19: continue
            
            # 抽出処理
            rank_text = cols[0].text.strip()
            if not rank_text.isdigit(): continue
            rank = int(rank_text)
            
            frame_no = int(cols[1].text.strip()) if cols[1].text.strip().isdigit() else 0
            horse_no = int(cols[2].text.strip()) if cols[2].text.strip().isdigit() else 0
            
            horse_a = cols[3].select_one('a')
            horse_id = ""
            if horse_a and 'href' in horse_a.attrs:
                horse_id_match = re.search(r'/horse/(\w+)/', horse_a['href'])
                if horse_id_match:
                    horse_id = horse_id_match.group(1)
            
            sex_age = cols[4].text.strip() # "牡3"
            age_match = re.search(r'\d+', sex_age)
            age = int(age_match.group()) if age_match else 0

            weight = float(cols[5].text.strip()) if cols[5].text.strip() else 0.0
            
            jockey_a = cols[6].select_one('a')
            jockey_id = ""
            jockey_name = ""
            if jockey_a:
                jockey_id_match = re.search(r'/jockey/(\w+)/', jockey_a['href'])
                jockey_id = jockey_id_match.group(1) if jockey_id_match else ""
                jockey_name = jockey_a.text.strip()
                if jockey_id:
                    jockeys.append({'jockey_id': jockey_id, 'name': jockey_name})

            time_str = cols[7].text.strip() # "1:58.2"
            try:
                if ':' in time_str:
                    m, s = time_str.split(':')
                    time_seconds = int(m) * 60 + float(s)
                else:
                    time_seconds = float(time_str)
            except (ValueError, TypeError):
                time_seconds = None
            
            margin = cols[8].text.strip() # "クビ"
            
            passing = cols[10].text.strip() # "2-2-2-2"

            last_3f_text = cols[11].text.strip() # "34.6"
            try:
                last_3f = float(last_3f_text)
            except (ValueError, TypeError):
                last_3f = None
            
            odds_text = cols[12].text.strip()
            odds = float(odds_text) if odds_text else None

            pop_text = cols[13].text.strip()
            popularity = int(pop_text) if pop_text.isdigit() else None

            weight_text = cols[14].text.strip() # "484(-2)"
            hw_match = re.search(r'(\d+)\((.*?)\)', weight_text)
            horse_weight = int(hw_match.group(1)) if hw_match else None
            weight_diff_str = hw_match.group(2) if hw_match else "0"
            try:
                weight_diff = int(weight_diff_str)
            except (ValueError, TypeError):
                weight_diff = 0

            trainer_a = cols[15].select_one('a')
            trainer_id = ""
            trainer_name = ""
            if trainer_a:
                trainer_id_match = re.search(r'/trainer/(\w+)/', trainer_a['href'])
                trainer_id = trainer_id_match.group(1) if trainer_id_match else ""
                trainer_name = trainer_a.text.strip()
                if trainer_id:
                    trainers.append({'trainer_id': trainer_id, 'name': trainer_name})

            weight_text = cols[14].text.strip()
            hw_match = re.search(r'(\d+)\((.*?)\)', weight_text)
            results.append({
                'race_id': race_id, 'horse_id': horse_id, 'rank': rank,
                'frame_no': frame_no, 'horse_no': horse_no,
                'jockey_id': jockey_id, 'trainer_id': trainer_id,
                'age': age, 'weight': weight, 'time_seconds': time_seconds,
                'margin': margin, 'passing': passing, 'last_3f': last_3f,
                'odds': odds, 'popularity': popularity,
                'horse_weight': horse_weight, 'weight_diff': weight_diff
            })
            
    except Exception as e:
        print(f"Error parsing results for {race_id}: {e}")
        traceback.print_exc()
        return [], [], []
    
    return results, jockeys, trainers

def save_to_db(race_info, results, jockeys, trainers):
    """DBに保存する"""
    if not race_info or not results:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Racesテーブルへの挿入
        cursor.execute('''
        INSERT OR IGNORE INTO races (race_id, date, venue, race_name, race_class, race_round, course_type, distance, rotation, weather, state, entries)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            race_info['race_id'], race_info['date'], race_info['venue'], race_info['race_name'],
            race_info['race_class'], race_info['race_round'], race_info['course_type'],
            race_info['distance'], race_info['rotation'], race_info['weather'], race_info['state'],
            len(results)
        ))
        
        # Jockeysテーブルへの挿入
        jockey_data = [(j['jockey_id'], j['name']) for j in jockeys]
        cursor.executemany('''
        INSERT OR IGNORE INTO jockeys (jockey_id, name) VALUES (?, ?)
        ''', jockey_data)

        # Trainersテーブルへの挿入
        trainer_data = [(t['trainer_id'], t['name']) for t in trainers]
        cursor.executemany('''
        INSERT OR IGNORE INTO trainers (trainer_id, name) VALUES (?, ?)
        ''', trainer_data)

        # Resultsテーブルへの挿入
        for res in results:
            cursor.execute('''
            INSERT OR IGNORE INTO results (race_id, horse_id, rank, frame_no, horse_no, jockey_id, trainer_id, age, weight, time_seconds, margin, passing, last_3f, odds, popularity, horse_weight, weight_diff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                res['race_id'], res['horse_id'], res['rank'], res['frame_no'], res['horse_no'],
                res['jockey_id'], res['trainer_id'], res['age'], res['weight'],
                res['time_seconds'], res['margin'], res['passing'], res['last_3f'], res['odds'],
                res['popularity'], res['horse_weight'], res['weight_diff']
            ))
            
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
        traceback.print_exc()
    finally:
        conn.close()

import argparse
# get_race_ids.pyから関数をインポート
# この変更に伴い、get_race_ids.pyが直接実行されるだけでなく、
# インポート可能な関数(例: get_race_ids_for_year)を提供する必要があります。
try:
    from get_race_ids import get_race_ids_for_year as fetch_race_ids_from_netkeiba
except ImportError:
    print("Error: Could not import 'get_race_ids_for_year' from get_race_ids.py.")
    print("Please ensure get_race_ids.py exists and contains the function.")
    # 依存関係が解決できない場合は、ダミー関数を定義してクラッシュを防ぐ
    def fetch_race_ids_from_netkeiba(year):
        return []

def get_existing_race_ids(year):
    """指定した年の既に保存されているレースIDのセットを返す"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # race_idは文字列なので、先頭が year と一致するものを取得
    cursor.execute("SELECT race_id FROM races WHERE race_id LIKE ?", (f'{year}%',))
    ids = set(row[0] for row in cursor.fetchall())
    conn.close()
    return ids

def construct_jbis_url(race_id, date_str):
    """netkeibaのrace_idと日付オブジェクトからJBISのURLを構築する"""
    venue_code_nk = race_id[4:6]
    race_num = int(race_id[10:12])

    venue_map_nk_to_jbis = {
        '01': '101', '02': '102', '03': '103', '04': '104', '05': '105',
        '06': '106', '07': '107', '08': '108', '09': '109', '10': '110'
    }
    venue_code_jbis = venue_map_nk_to_jbis.get(venue_code_nk)
    if not venue_code_jbis:
        return None

    date_yyyymmdd = date_str.replace('-', '')
    return f"{BASE_URL}{date_yyyymmdd}/{venue_code_jbis}/{race_num}.html"

def scrape_year(year):
    """指定した年の全レースをスクレイピングする"""
    print(f"Starting scrape for year {year}...")

    # 既存データの取得
    existing_ids = get_existing_race_ids(year)
    print(f"Found {len(existing_ids)} existing races in DB. These will be skipped.")

    # get_race_ids.pyからレースIDと日付のタプルリストを取得
    race_id_date_pairs = fetch_race_ids_from_netkeiba(year)
    if not race_id_date_pairs:
        print("No race IDs found to scrape.")
        return
        
    print(f"Found {len(race_id_date_pairs)} race IDs to scrape for {year}.")

    # スキップするIDを除外
    races_to_process = [pair for pair in race_id_date_pairs if pair[0] not in existing_ids]
    print(f"After skipping existing ones, {len(races_to_process)} races will be processed.")
    
    if not races_to_process:
        print("No new races to process.")
        return

    for race_id, date_str in tqdm(races_to_process, desc=f"Scraping races for {year}"):
        try:
            url = construct_jbis_url(race_id, date_str)
            if not url:
                print(f"Could not construct URL for race_id {race_id}. Skipping.")
                continue

            html = get_html_from_jbis_url(url)
            if not html:
                print(f"Failed to get HTML for {race_id} from {url}. Skipping.")
                continue

            soup = BeautifulSoup(html, 'lxml')
            race_info = parse_race_info(soup, race_id)
            if not race_info:
                print(f"Failed to parse race info for {race_id}. Skipping.")
                continue

            results, jockeys, trainers = parse_race_results(soup, race_id)
            if not results:
                print(f"No results found for {race_id}. Skipping.")
                continue

            save_to_db(race_info, results, jockeys, trainers)
            time.sleep(1) # サーバーへの負荷を軽減するための待機

        except Exception as e:
            print(f"An unexpected error occurred for race {race_id}: {e}")
            traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape race data from netkeiba')
    parser.add_argument('year', type=int, help='Year to scrape (e.g., 2023)')
    args = parser.parse_args()

    scrape_year(args.year)