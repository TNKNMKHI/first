import sqlite3
import os
from dotenv import load_dotenv

# .envファイルを読み込む
# スクリプトのディレクトリの親ディレクトリ(ルート)にある.envを探す
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

def generate_pedigree_columns():
    """5代血統までのカラム名を生成する"""
    columns = []
    # 1代 (Parents): f, m
    # 2代 (Grandparents): ff, fm, mf, mm
    # ...
    # 再帰的または反復的に生成
    
    def expand_generation(current_gen_labels):
        next_gen = []
        for label in current_gen_labels:
            next_gen.append(label + 'f') # 父
            next_gen.append(label + 'm') # 母
        return next_gen

    generations = []
    current = ['']
    for _ in range(5): # 5代
        current = expand_generation(current)
        generations.extend(current)
    
    # カラム定義文字列のリストを返す
    return [f"{col}_id TEXT" for col in generations]

def create_tables():
    if os.path.exists(DB_PATH):
        print(f"Database {DB_PATH} already exists.")
        # 必要に応じて削除して作り直す場合はコメントアウトを外す
        # os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Races Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS races (
        race_id TEXT PRIMARY KEY,
        date TEXT,
        venue TEXT,
        race_name TEXT,
        race_round INTEGER,
        course_type TEXT,
        distance INTEGER,
        rotation TEXT,
        weather TEXT,
        state TEXT,
        entries INTEGER
    )
    ''')

    # 2. Results Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS results (
        race_id TEXT,
        horse_id TEXT,
        rank INTEGER,
        frame_no INTEGER,
        horse_no INTEGER,
        jockey_id TEXT,
        trainer_id TEXT,
        age INTEGER,
        sex TEXT,
        weight REAL,
        time_seconds REAL,
        margin TEXT,
        passing TEXT,
        last_3f REAL,
        odds REAL,
        popularity INTEGER,
        horse_weight INTEGER,
        weight_diff INTEGER,
        PRIMARY KEY (race_id, horse_id),
        FOREIGN KEY (race_id) REFERENCES races (race_id)
    )
    ''')

    # 3. Horses Table
    pedigree_cols = generate_pedigree_columns()
    pedigree_sql_part = "\n        ".join(pedigree_cols)
    
    create_horses_sql = f'''
    CREATE TABLE IF NOT EXISTS horses (
        horse_id TEXT PRIMARY KEY,
        name TEXT,
        birth_year INTEGER,
        sex TEXT,
        sire_line TEXT,
        {pedigree_sql_part}
    )
    '''
    cursor.execute(create_horses_sql)

    conn.commit()
    conn.close()
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()
