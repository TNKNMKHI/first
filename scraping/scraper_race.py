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
from dotenv import load_dotenv

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

BASE_URL = "https://db.netkeiba.com/race/"

def get_driver():
    """Selenium WebDriverを初期化して返す"""
    chrome_options = Options()
    chrome_options.add_argument("--headless") # ヘッドレスモード
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") # 自動操作フラグを隠す
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # WebDriver Managerでドライバを自動取得・設定
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def get_html_with_selenium(driver, race_id):
    """Seleniumを使ってページを取得しHTMLを返す"""
    url = f"{BASE_URL}{race_id}"
    try:
        driver.get(url)
        time.sleep(0.8) # ページ読み込み待機 + 負荷軽減
        
        # タイトルに "エラー" が含まれていないかチェック
        if "エラー" in driver.title or "ご指定のページは見つかりませんでした" in driver.page_source:
            return None
            
        return driver.page_source
    except Exception as e:
        print(f"Error fetching {url} with Selenium: {e}")
        return None

def parse_race_info(soup, race_id):
    """レース情報を解析して辞書で返す"""
    try:
        # レース名: div.data_intro 内の h1 を探す
        race_name_elem = soup.select_one('div.data_intro h1')
        if not race_name_elem:
            # バックアッププラン: 単純に h1
            race_name_elem = soup.select_one('h1')
            
        title = race_name_elem.text.strip() if race_name_elem else ""
        
        # レース詳細 (R, 距離, 天候など) が含まれるエリア
        data_intro = soup.select_one('.data_intro')
        if not data_intro:
            return None
            
        r_text = data_intro.select_one('dt').text.strip() # 例: "1 R"
        r_match = re.search(r'(\d+)', r_text)
        race_round = int(r_match.group(1)) if r_match else 0
        
        details = data_intro.select_one('p').text.strip()
        # 例: "芝右1600m / 天候 : 晴 / 芝 : 良 / 発走 : 10:05"
        
        # 抽出ロジック (正規表現などで簡易的に)
        course_type = "Unknown"
        if "芝" in details: course_type = "芝"
        elif "ダ" in details: course_type = "ダート"
        elif "障" in details: course_type = "障害"
        
        distance_match = re.search(r'(\d+)m', details)
        distance = int(distance_match.group(1)) if distance_match else 0
        
        rotation = "Unknown"
        if "右" in details: rotation = "右"
        elif "左" in details: rotation = "左"
        elif "直線" in details: rotation = "直線"
        
        weather_match = re.search(r'天候\s*:\s*(.*?)(/|$)', details)
        weather = weather_match.group(1).strip() if weather_match else ""
        
        state_match = re.search(r'(芝|ダート|障害)\s*:\s*(.*?)(/|$)', details)
        state = state_match.group(2).strip() if state_match else ""
        
        date_text = data_intro.select_one('.smalltxt').text.strip().split(' ')[0]
        # 例: 2024年1月6日
        date_str = date_text.replace('年', '-').replace('月', '-').replace('日', '')
        
        venue_mapping = {
            '01': '札幌', '02': '函館', '03': '福島', '04': '新潟', '05': '東京', 
            '06': '中山', '07': '中京', '08': '京都', '09': '阪神', '10': '小倉'
        }
        venue_code = race_id[4:6]
        venue = venue_mapping.get(venue_code, "Unknown")

        
        # レースクラス（G1, G2など）
        race_class_elem = soup.select_one('p.race_grade > span.grade')
        race_class = race_class_elem.text.strip() if race_class_elem else ""

        return {
            'race_id': race_id,
            'date': date_str,
            'venue': venue,
            'race_name': title,
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
        # print(f"Error parsing race info for {race_id}: {e}")
        return None

def parse_race_results(soup, race_id):
    """レース結果テーブルを解析して (results, jockeys, trainers) のタプルを返す"""
    results, jockeys, trainers = [], [], []
    try:
        table = soup.select_one('table.race_table_01')
        if not table:
            return [], [], []
        
        rows = table.select('tr')[1:] # ヘッダーを除く
        for row in rows:
            cols = row.select('td')
            if len(cols) < 14: continue
            
            # 抽出処理
            rank_text = cols[0].text.strip()
            if not rank_text.isdigit(): continue
            rank = int(rank_text)
            
            frame_no = int(cols[1].text.strip())
            horse_no = int(cols[2].text.strip())
            
            horse_a = cols[3].select_one('a')
            horse_id = re.search(r'/horse/(\d+)', horse_a['href']).group(1) if horse_a else ""
            
            sex_age = cols[4].text.strip()
            age = int(re.search(r'\d+', sex_age).group()) if re.search(r'\d+', sex_age) else 0

            weight = float(cols[5].text.strip())
            
            jockey_a = cols[6].select_one('a')
            jockey_id = ""
            jockey_name = ""
            if jockey_a:
                jockey_id_match = re.search(r'/jockey/result/recent/(\w+)', jockey_a['href'])
                jockey_id = jockey_id_match.group(1) if jockey_id_match else ""
                jockey_name = jockey_a.text.strip()
                if jockey_id:
                    jockeys.append({'jockey_id': jockey_id, 'name': jockey_name})

            time_str = cols[7].text.strip()
            try:
                if ':' in time_str:
                    m, s = time_str.split(':')
                    time_seconds = int(m) * 60 + float(s)
                else:
                    time_seconds = float(time_str)
            except (ValueError, TypeError):
                time_seconds = None
            
            margin = cols[8].text.strip()
            
            # 9番人気が単勝オッズと同じtd内にある場合がある
            pop_odds_text = cols[9].text.strip()
            try:
                popularity = int(cols[9].text.strip())
                odds = float(cols[10].text.strip())
            except ValueError:
                try: # 同一セルに人気とオッズが含まれるパターン
                    pop_match = re.search(r'^(\d+)', pop_odds_text)
                    popularity = int(pop_match.group(1)) if pop_match else None
                    
                    odds_match = re.search(r'\((\d+\.\d+)\)$', pop_odds_text)
                    odds = float(odds_match.group(1)) if odds_match else None
                except (ValueError, TypeError):
                    popularity = None
                    odds = None

            last_3f_text = cols[11].text.strip()
            try:
                last_3f = float(last_3f_text)
            except (ValueError, TypeError):
                last_3f = None
            
            passing = cols[12].text.strip()
            
            trainer_a = cols[13].select_one('a')
            trainer_id = ""
            trainer_name = ""
            if trainer_a:
                trainer_id_match = re.search(r'/trainer/result/recent/(\w+)', trainer_a['href'])
                trainer_id = trainer_id_match.group(1) if trainer_id_match else ""
                trainer_name = trainer_a.text.strip()
                if trainer_id:
                    trainers.append({'trainer_id': trainer_id, 'name': trainer_name})

            weight_text = cols[14].text.strip()
            hw_match = re.search(r'(\d+)\((.*?)\)', weight_text)
            horse_weight = int(hw_match.group(1)) if hw_match else None
            weight_diff_str = hw_match.group(2) if hw_match else "0"
            try:
                weight_diff = int(weight_diff_str)
            except (ValueError, TypeError):
                weight_diff = 0
            
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

# ... (中略) ...

def get_existing_race_ids(year):
    """指定した年の既に保存されているレースIDのセットを返す"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # race_idは文字列なので、先頭が year と一致するものを取得
    cursor.execute("SELECT race_id FROM races WHERE race_id LIKE ?", (f'{year}%',))
    ids = set(row[0] for row in cursor.fetchall())
    conn.close()
    return ids

def scrape_year(year):
    """指定した年の全レースをスクレイピングする"""
    print(f"Starting scrape for year {year}...")
    
    # 既存データの取得
    existing_ids = get_existing_race_ids(year)
    print(f"Found {len(existing_ids)} existing races in DB. These will be skipped.")
    
    for place in range(1, 11):
        place_id = f"{place:02}"
        
        # WebDriverを会場（place）ごとに初期化
        print(f"--- Initializing driver for place {place_id} ---")
        driver = get_driver()
        
        try:
            for kai in range(1, 7): 
                kai_id = f"{kai:02}"
                
                for day in range(1, 13): 
                    day_id = f"{day:02}"
                    consecutive_failures = 0
                    
                    for r in range(1, 13):
                        r_id = f"{r:02}"
                        race_id = f"{year}{place_id}{kai_id}{day_id}{r_id}"
                        
                        # スキップ判定
                        if race_id in existing_ids:
                            # print(f"Skipping {race_id}: Already exists.") # ログが多すぎる場合があるため非表示化を推奨
                            continue
                        
                        # ここで 'invalid session id' エラーが発生する可能性がある
                        html = get_html_with_selenium(driver, race_id)
                        
                        if not html:
                            consecutive_failures += 1
                            continue
                            
                        soup = BeautifulSoup(html, 'lxml')
                        race_info = parse_race_info(soup, race_id)
                        
                        if not race_info:
                            consecutive_failures += 1
                            continue 
                        
                        consecutive_failures = 0 
                        
                        results, jockeys, trainers = parse_race_results(soup, race_id)
                        save_to_db(race_info, results, jockeys, trainers)
                        print(f"Saved {race_id}: {race_info['race_name']}")
                    
                    # 12レース連続で失敗した場合、その日のループを抜ける
                    if consecutive_failures >= 12:
                         break
                
                # consecutive_failuresが引き継がれている場合、開催回のループも抜ける
                if consecutive_failures >= 12:
                     break
        
        except Exception as e:
            # 会場ごとのループでエラーが発生しても、次の会場へ進む
            print(f"An error occurred while scraping place {place_id}: {e}")
            traceback.print_exc()
        finally:
            # 各会場の処理が終わったら、必ずドライバーを閉じる
            print(f"--- Closing driver for place {place_id} ---")
            driver.quit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape race data from netkeiba')
    parser.add_argument('year', type=int, help='Year to scrape (e.g., 2023)')
    args = parser.parse_args()

    # scrape_yearの中でトライバーの管理を行う
    try:
        scrape_year(args.year)
        print("Scraping completed.")
    except Exception as e:
        print(f"A general error occurred outside the scraping loop: {e}")
        traceback.print_exc()