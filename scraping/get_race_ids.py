import requests
from bs4 import BeautifulSoup
import time
import re
from tqdm import tqdm
from datetime import datetime
import argparse

NETKEIBA_BASE_URL = "https://race.netkeiba.com"

def get_html(url, params=None, method='GET'):
    """指定されたURLからHTMLを取得する"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = requests.request(method, url, headers=headers, data=params)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_race_dates_from_netkeiba_calendar(year):
    """netkeibaのカレンダーページから指定された年の全レース開催日リストを取得する"""
    dates = set()
    print(f"Fetching race dates for {year} from netkeiba calendar...")
    for month in tqdm(range(1, 13), desc="Fetching calendar months"):
        calendar_url = f"{NETKEIBA_BASE_URL}/top/calendar.html?year={year}&month={month}"
        html = get_html(calendar_url)
        if not html:
            continue
        
        soup = BeautifulSoup(html, 'lxml')
        
        # 開催日が含まれるリンクを探す
        for link in soup.select('a[href*="kaisai_date="]'):
            match = re.search(r'kaisai_date=(\d{8})', link['href'])
            if match:
                dates.add(match.group(1))
        time.sleep(0.5) # サーバー負荷軽減
    return sorted(list(dates))

def get_race_ids_for_date(date_str):
    """指定された日付の全レースIDを取得する"""
    race_list_url = f"{NETKEIBA_BASE_URL}/top/race_list.html?kaisai_date={date_str}"
    html = get_html(race_list_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    race_links = soup.select('a[href*="/race/result.html?race_id="]')
    
    race_ids = []
    for link in race_links:
        match = re.search(r'race_id=(\d+)', link['href'])
        if match:
            race_ids.append(match.group(1))
            
    return race_ids

def get_race_ids_for_year(year):
    """
    指定された年の全レースIDと日付のタプルのリストを取得する。
    
    Returns:
        list[tuple[str, str]]: [(race_id, 'YYYY-MM-DD'), ...]
    """
    all_races = []
    race_dates = get_race_dates_from_netkeiba_calendar(year)
    if not race_dates:
        print(f"Could not fetch any race dates for {year} from netkeiba calendar.")
        return []
    
    print(f"Fetching race IDs for {len(race_dates)} days from netkeiba.com...")
    for date_yyyymmdd in tqdm(race_dates, desc=f"Processing race days for {year}"):
        race_ids = get_race_ids_for_date(date_yyyymmdd)
        date_obj = datetime.strptime(date_yyyymmdd, '%Y%m%d')
        date_formatted = date_obj.strftime('%Y-%m-%d')
        for race_id in race_ids:
            all_races.append((race_id, date_formatted))
        time.sleep(1) # サーバー負荷軽減
    return all_races

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get race IDs and dates for a specific year from netkeiba.com.')
    parser.add_argument('year', type=int, help='The year to fetch race data for (e.g., 2023).')
    args = parser.parse_args()

    race_id_date_pairs = get_race_ids_for_year(args.year)
    
    for race_id, date_str in race_id_date_pairs:
        print(f"{race_id},{date_str}")