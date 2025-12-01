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

# SSL警告を抑制
urllib3.disable_warnings(InsecureRequestWarning)

DB_PATH = '/DB/keiba.db'
BASE_URL = "https://db.netkeiba.com/race/"

def get_page(race_id):
    """指定されたrace_idのページを取得する"""
    url = f"{BASE_URL}{race_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://db.netkeiba.com/"
    }
    try:
        # verify=Falseを追加
        response = requests.get(url, headers=headers, verify=False)
        response.encoding = 'euc-jp' # netkeibaの文字コード
        if response.status_code == 200:
            return response.text
        else:
            print(f"Status Code: {response.status_code} for {url}")
            return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_race_info(soup, race_id):
    """レース情報を解析して辞書で返す"""
    try:
        title = soup.select_one('h1').text.strip() if soup.select_one('h1') else ""
        
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
        
        weather_match = re.search(r'天候 : (.*?)( /|$)', details)
        weather = weather_match.group(1).strip() if weather_match else ""
        
        state_match = re.search(r'(芝|ダート) : (.*?)( /|$)', details)
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

        return {
            'race_id': race_id,
            'date': date_str,
            'venue': venue,
            'race_name': title,
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
    """レース結果テーブルを解析してリストで返す"""
    results = []
    try:
        table = soup.select_one('table.race_table_01')
        if not table:
            return []
        
        rows = table.select('tr')[1:] # ヘッダーを除く
        for row in rows:
            cols = row.select('td')
            if len(cols) < 10: continue
            
            # 抽出処理
            rank_text = cols[0].text.strip()
            if not rank_text.isdigit(): continue # 失格・除外などは一旦スキップ(必要ならハンドリング追加)
            rank = int(rank_text)
            
            frame_no = int(cols[1].text.strip())
            horse_no = int(cols[2].text.strip())
            
            # 馬ID取得
            horse_a = cols[3].select_one('a')
            horse_id = re.search(r'/horse/(\d+)', horse_a['href']).group(1) if horse_a else ""
            
            # 性齢 (例: 牡3)
            sex_age = cols[4].text.strip()
            sex = sex_age[0]
            age = int(sex_age[1:])
            
            weight = float(cols[5].text.strip())
            
            # 騎手ID
            jockey_a = cols[6].select_one('a')
            jockey_id = re.search(r'/jockey/result/recent/(\d+)', jockey_a['href']).group(1) if jockey_a else ""
            
            # タイム (例: 1:35.2)
            time_str = cols[7].text.strip()
            try:
                if ':' in time_str:
                    m, s = time_str.split(':')
                    time_seconds = int(m) * 60 + float(s)
                else:
                    time_seconds = float(time_str) # 秒のみの場合や無効値対策が必要
            except:
                time_seconds = None
            
            margin = cols[8].text.strip()
            
            # 通過順, 上がり, オッズ, 人気, 馬体重, 調教師...
            # サイトのカラム位置は固定だが、念のため確認が必要。
            # netkeiba dbページの標準的な並び:
            # 着順, 枠, 馬番, 馬名, 性齢, 斤量, 騎手, タイム, 着差, 人気, 単勝オッズ, 後3F, コーナー通過, 厩舎(調教師), 馬体重
            
            popularity = cols[9].text.strip()
            popularity = int(popularity) if popularity.isdigit() else None
            
            odds = cols[10].text.strip()
            try: odds = float(odds)
            except: odds = None
            
            last_3f = cols[11].text.strip()
            try: last_3f = float(last_3f)
            except: last_3f = None
            
            passing = cols[12].text.strip()
            
            trainer_a = cols[13].select_one('a')
            trainer_id = re.search(r'/trainer/result/recent/(\d+)', trainer_a['href']).group(1) if trainer_a else ""
            
            weight_text = cols[14].text.strip()
            # 例: 480(+2)
            hw_match = re.search(r'(\d+)\((.*?)\)', weight_text)
            horse_weight = int(hw_match.group(1)) if hw_match else None
            weight_diff_str = hw_match.group(2) if hw_match else "0"
            try:
                weight_diff = int(weight_diff_str)
            except:
                weight_diff = 0
            
            results.append({
                'race_id': race_id,
                'horse_id': horse_id,
                'rank': rank,
                'frame_no': frame_no,
                'horse_no': horse_no,
                'jockey_id': jockey_id,
                'trainer_id': trainer_id,
                'age': age,
                'sex': sex,
                'weight': weight,
                'time_seconds': time_seconds,
                'margin': margin,
                'passing': passing,
                'last_3f': last_3f,
                'odds': odds,
                'popularity': popularity,
                'horse_weight': horse_weight,
                'weight_diff': weight_diff
            })
            
    except Exception as e:
        print(f"Error parsing results for {race_id}: {e}")
        traceback.print_exc()
        return []
    
    return results

def save_to_db(race_info, results):
    """DBに保存する"""
    if not race_info or not results:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Race
        cursor.execute('''
        INSERT OR IGNORE INTO races (race_id, date, venue, race_name, race_round, course_type, distance, rotation, weather, state, entries)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            race_info['race_id'], race_info['date'], race_info['venue'], race_info['race_name'],
            race_info['race_round'], race_info['course_type'], race_info['distance'],
            race_info['rotation'], race_info['weather'], race_info['state'], len(results)
        ))
        
        # Results
        for res in results:
            cursor.execute('''
            INSERT OR IGNORE INTO results (race_id, horse_id, rank, frame_no, horse_no, jockey_id, trainer_id, age, sex, weight, time_seconds, margin, passing, last_3f, odds, popularity, horse_weight, weight_diff)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                res['race_id'], res['horse_id'], res['rank'], res['frame_no'], res['horse_no'],
                res['jockey_id'], res['trainer_id'], res['age'], res['sex'], res['weight'],
                res['time_seconds'], res['margin'], res['passing'], res['last_3f'], res['odds'],
                res['popularity'], res['horse_weight'], res['weight_diff']
            ))
            
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

def scrape_year(year):
    """指定した年の全レースをスクレイピングする"""
    print(f"Starting scrape for year {year}...")
    
    # 競馬場コード: 01-10
    for place in range(1, 11):
        place_id = f"{place:02}"
        print(f"Scraping place {place_id}...")
        
        for kai in range(1, 7): # 開催回 (通常最大5,6回)
            kai_id = f"{kai:02}"
            
            for day in range(1, 13): # 日数 (通常最大12日)
                day_id = f"{day:02}"
                
                # この日のレースが存在するか確認するために1Rだけチェックしても良いが、
                # 途中抜け（中止など）もあり得るので、全ラウンド回すのが確実。
                # ただし、1Rが存在しなければその日(またはその開催回)は終了の可能性が高い。
                
                consecutive_failures = 0
                
                for r in range(1, 13): # 1-12R
                    r_id = f"{r:02}"
                    race_id = f"{year}{place_id}{kai_id}{day_id}{r_id}"
                    
                    # DBに既に存在するかチェックしてスキップするロジックを入れると再開時に便利
                    # ここでは簡易化のため省略(INSERT OR IGNOREで対応)
                    
                    html = get_page(race_id)
                    time.sleep(1) # Wait
                    
                    if not html:
                        consecutive_failures += 1
                        continue
                        
                    soup = BeautifulSoup(html, 'lxml') # lxmlパーサーを使用
                    race_info = parse_race_info(soup, race_id)
                    
                    if not race_info:
                        consecutive_failures += 1
                        continue # 情報が取れないページ
                    
                    consecutive_failures = 0 # 成功したらリセット
                    
                    results = parse_race_results(soup, race_id)
                    save_to_db(race_info, results)
                    print(f"Saved {race_id}: {race_info['race_name']}")
                
                # 1Rから12Rまで全部失敗したら、その開催回の日数は終了とみなして次の開催回へ
                if consecutive_failures >= 12:
                     # print(f"No races found for {year}-{place_id}-{kai_id}-{day_id}. Skipping to next kai.")
                     break
            
            # 1日目すら存在しなければ、その開催回は終了とみなして次の場所へ？
            # NOTE: 開催回が飛ぶことは稀だが、場所ごとにループしているので、
            # ここでのbreak判定は「その開催回の1日目もデータがない場合」にするなど工夫が必要。
            # 今回は全探索気味にループさせる（無駄なリクエストも走るが安全）

if __name__ == "__main__":
    # ダミーデータによるDB保存テスト
    print("Starting dummy data test...")
    
    dummy_race_info = {
        'race_id': '209901010101',
        'date': '2099-01-01',
        'venue': '東京',
        'race_name': 'テスト記念',
        'race_round': 11,
        'course_type': '芝',
        'distance': 2400,
        'rotation': '左',
        'weather': '晴',
        'state': '良',
        'entries': 16
    }
    
    dummy_results = [
        {
            'race_id': '209901010101',
            'horse_id': '2020123456',
            'rank': 1,
            'frame_no': 1,
            'horse_no': 1,
            'jockey_id': '00001',
            'trainer_id': '00001',
            'age': 3,
            'sex': '牡',
            'weight': 56.0,
            'time_seconds': 145.5,
            'margin': '0.0',
            'passing': '2-2-2',
            'last_3f': 34.5,
            'odds': 2.5,
            'popularity': 1,
            'horse_weight': 500,
            'weight_diff': 2
        },
        {
            'race_id': '209901010101',
            'horse_id': '2020654321',
            'rank': 2,
            'frame_no': 2,
            'horse_no': 2,
            'jockey_id': '00002',
            'trainer_id': '00002',
            'age': 4,
            'sex': '牝',
            'weight': 54.0,
            'time_seconds': 145.6,
            'margin': 'クビ',
            'passing': '1-1-1',
            'last_3f': 35.0,
            'odds': 5.0,
            'popularity': 2,
            'horse_weight': 460,
            'weight_diff': -4
        }
    ]
    
    try:
        save_to_db(dummy_race_info, dummy_results)
        print("Dummy data saved successfully.")
        
        # 保存確認
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        print("\n--- Races Table ---")
        for row in cursor.execute("SELECT * FROM races WHERE race_id='209901010101'"):
            print(row)
            
        print("\n--- Results Table ---")
        for row in cursor.execute("SELECT * FROM results WHERE race_id='209901010101'"):
            print(row)
            
        conn.close()
        
    except Exception as e:
        print(f"Error saving dummy data: {e}")
        traceback.print_exc()

