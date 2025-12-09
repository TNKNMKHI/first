
import sqlite3
import pandas as pd
import os
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
# このスクリプトはプロジェクトのルートに配置される想定
load_dotenv()

# .envからデータベースのパスを取得
DB_PATH = os.getenv('DB_FILE_PATH')

def analyze_database():
    """
    データベースに接続し、簡単な分析を行う。
    """
    if not DB_PATH:
        print("エラー: .envファイルにDB_FILE_PATHが設定されていません。")
        return

    if not os.path.exists(DB_PATH):
        print(f"エラー: データベースファイル '{DB_PATH}' が見つかりません。")
        print("スクレイピングスクリプトを実行して、データベースを生成・移入してください。")
        return

    try:
        # データベースに接続
        conn = sqlite3.connect(DB_PATH)
        print(f"'{DB_PATH}'に正常に接続しました。")

        # --- 1. テーブル一覧の取得 ---
        print("\n--- データベース内のテーブル一覧 ---")
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
        if tables.empty:
            print("テーブルが見つかりません。")
            return
        print(tables['name'].tolist())

        # --- 2. 'races'テーブルの分析 ---
        if 'races' in tables['name'].values:
            print("\n--- 'races' テーブルの最初の5件 ---")
            races_df = pd.read_sql_query("SELECT * FROM races LIMIT 5", conn)
            if races_df.empty:
                print("'races' テーブルにデータがありません。")
            else:
                print(races_df.to_string())

            print("\n--- 'races' テーブルの基本統計 ---")
            # 全てのレースを読み込んで統計情報を計算
            all_races_df = pd.read_sql_query("SELECT * FROM races", conn)
            if all_races_df.empty:
                print("'races' テーブルにデータがありません。")
            else:
                # 'distance' 列に絞って統計情報を表示
                if 'distance' in all_races_df.columns:
                    print(all_races_df['distance'].describe())
                else:
                    print("'distance'列が'races'テーブルにありません。")
        else:
            print("\n'races' テーブルが見つかりません。")
            
        # --- 3. 'results'テーブルのデータ件数 ---
        if 'results' in tables['name'].values:
            print("\n--- 'results' テーブルのデータ件数 ---")
            results_count = pd.read_sql_query("SELECT COUNT(*) as count FROM results", conn).iloc[0]['count']
            print(f"合計 {results_count} 件のレース結果が格納されています。")

        else:
            print("\n'results' テーブルが見つかりません。")

        # --- 4. 'horses'テーブルの分析 ---
        if 'horses' in tables['name'].values:
            print("\n--- 'horses' テーブルの最初の5件 ---")
            horses_df = pd.read_sql_query("SELECT * FROM horses LIMIT 5", conn)
            if horses_df.empty:
                print("'horses' テーブルにデータがありません。")
            else:
                print(horses_df.to_string())
            
            print("\n--- 'horses' テーブルのデータ件数 ---")
            horses_count = pd.read_sql_query("SELECT COUNT(*) as count FROM horses", conn).iloc[0]['count']
            print(f"合計 {horses_count} 頭の馬データが格納されています。")
        else:
            print("\n'horses' テーブルが見つかりません。")

        # --- 5. 'pedigrees'テーブルの分析 ---
        if 'pedigrees' in tables['name'].values:
            print("\n--- 'pedigrees' テーブルの最初の5件 ---")
            pedigrees_df = pd.read_sql_query("SELECT * FROM pedigrees LIMIT 5", conn)
            if pedigrees_df.empty:
                print("'pedigrees' テーブルにデータがありません。")
            else:
                print(pedigrees_df.to_string())

            print("\n--- 'pedigrees' テーブルのデータ件数 ---")
            pedigrees_count = pd.read_sql_query("SELECT COUNT(*) as count FROM pedigrees", conn).iloc[0]['count']
            print(f"合計 {pedigrees_count} 件の血統データが格納されています。")
        else:
            print("\n'pedigrees' テーブルが見つかりません。")

    except sqlite3.Error as e:
        print(f"データベースエラー: {e}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print(f"\n'{DB_PATH}'との接続を閉じました。")

if __name__ == "__main__":
    analyze_database()
