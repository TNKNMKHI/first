from bs4 import BeautifulSoup
import time
import re
from tqdm import tqdm
from datetime import datetime
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

NETKEIBA_BASE_URL = "https://race.netkeiba.com"

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

def get_race_ids_for_year(year):
    """
    指定された年の全レースIDと日付のタプルのリストを取得する。
    
    Returns:
        list[tuple[str, str]]: [(race_id, 'YYYY-MM-DD'), ...]
    """
    all_races = []
    print(f"Fetching race IDs for {year} from netkeiba calendar using Selenium...")

    driver = get_driver()
    try:
        # 初回アクセスでCookie同意バナーを処理
        print("Accessing netkeiba to handle cookie consent...")
        driver.get(NETKEIBA_BASE_URL)
        # try:
        #     # 10秒待機してCookie同意ボタンを探す
        #     accept_button = WebDriverWait(driver, 10).until(
        #         EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        #     )
        #     accept_button.click()
        #     print("Cookie consent accepted.")
        #     time.sleep(1) # クリック後の待機
        # except TimeoutException:
        #     print("Cookie consent banner not found or timed out, proceeding.")

        for month in tqdm(range(1, 13), desc=f"Fetching calendar for {year}"):
            calendar_url = f"{NETKEIBA_BASE_URL}/top/calendar.html?year={year}&month={month}"
            driver.get(calendar_url)
            try:
                # ページの主要な要素(カレンダーセル)が表示されるまで最大10秒待機
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "td.RaceCellBox"))
                )
            except TimeoutException:
                # この月にレースがなければタイムアウトするので、スキップする
                print(f"No races found for {year}-{month:02d}, skipping.")
                continue
            html = driver.page_source # JS実行後のHTMLを取得
            soup = BeautifulSoup(html, 'lxml')
            
            # 開催日が含まれるリンクをすべて取得
            date_links = soup.select('a[href*="race_list.html?kaisai_date="]')
            
            for link in date_links:
                date_match = re.search(r'kaisai_date=(\d{8})', link['href'])
                if not date_match:
                    continue
                
                date_yyyymmdd = date_match.group(1)
                race_list_url = f"{NETKEIBA_BASE_URL}/top/race_list.html?kaisai_date={date_yyyymmdd}"
                driver.get(race_list_url)
                time.sleep(1) # ページ遷移を待つ
                
                # レース一覧ページからレースIDを抽出
                list_html = driver.page_source
                list_soup = BeautifulSoup(list_html, 'lxml')
                
                for race_link in list_soup.select('a[href*="/race/result.html?race_id="]'):
                    race_id_match = re.search(r'race_id=(\d{12})', race_link['href'])
                    if race_id_match:
                        race_id = race_id_match.group(1)
                        date_obj = datetime.strptime(date_yyyymmdd, '%Y%m%d')
                        date_formatted = date_obj.strftime('%Y-%m-%d')
                        all_races.append((race_id, date_formatted))

            time.sleep(1) # 次の月へのリクエスト前に待機
    finally:
        driver.quit()

    if not all_races:
        print(f"No race IDs found for {year}.")
        return []

    # 重複を除去してソート
    unique_races = sorted(list(set(all_races)))

    return unique_races

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get race IDs and dates for a specific year from netkeiba.com.')
    parser.add_argument('year', type=int, help='The year to fetch race data for (e.g., 2023).')
    args = parser.parse_args()

    race_id_date_pairs = get_race_ids_for_year(args.year)
    
    for race_id, date_str in race_id_date_pairs:
        print(f"{race_id},{date_str}")