from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
from bs4 import BeautifulSoup
import re

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def search_jbis(driver, horse_name):
    print(f"Searching Google for JBIS page of: {horse_name}")
    
    # Google検索で "site:jbis.or.jp {馬名}" を検索
    driver.get("https://www.google.com/")
    time.sleep(2)
    
    try:
        # 検索ボックスを探す (name="q")
        search_box = driver.find_element(By.NAME, "q")
        search_box.send_keys(f"site:jbis.or.jp {horse_name}")
        search_box.send_keys(Keys.RETURN)
        time.sleep(3)
        
        print(f"Google Result Title: {driver.title}")
        
        # 検索結果のリンクを探す
        # h3タグの親のaタグなどが一般的
        links = driver.find_elements(By.TAG_NAME, "a")
        for link in links:
            href = link.get_attribute('href')
            if href and 'jbis.or.jp/horse/' in href and 'pedigree' not in href:
                # IDが含まれているか確認
                if re.search(r'/horse/\d+/$', href):
                    print(f"Found JBIS link via Google: {href}")
                    return href
                    
    except Exception as e:
        print(f"Error during Google search: {e}")
        
    return None

def get_pedigree_jbis(driver, horse_url):
    # horse_url: https://www.jbis.or.jp/horse/0001237587/
    # pedigree_url: https://www.jbis.or.jp/horse/0001237587/pedigree/
    
    if not horse_url.endswith('/'):
        horse_url += '/'
    pedigree_url = horse_url + 'pedigree/'
    
    print(f"Fetching pedigree from: {pedigree_url}")
    driver.get(pedigree_url)
    time.sleep(3)
    
    soup = BeautifulSoup(driver.page_source, 'lxml')
    
    # 血統表のパース
    # 父のセルを探す
    # JBISの血統表構造はまだ不明だが、"父"という文字の近くか、テーブルの特定位置
    # とりあえずテキストダンプ
    
    # 父名を取得してみる
    # 通常、血統表の左上
    # <table class="tbl-pedigree"> ...
    
    table = soup.select_one('table') # クラス名不明なので最初のテーブル
    if table:
        print("Found table.")
        # 最初のtd (rowspanあり) が父のはず
        tds = table.select('td')
        for i, td in enumerate(tds[:5]):
            print(f"TD {i}: {td.text.strip().replace(chr(10), '')}")
            
if __name__ == "__main__":
    driver = get_driver()
    try:
        # テスト馬: アンモシエラ
        horse_name = "アンモシエラ"
        horse_url = search_jbis(driver, horse_name)
        
        if horse_url:
            get_pedigree_jbis(driver, horse_url)
        else:
            print("Could not find horse on JBIS.")
            
    finally:
        driver.quit()
