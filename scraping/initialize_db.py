import sqlite3
import os
from dotenv import load_dotenv

# .envファイルを読み込む
# スクリプトのディレクトリの親ディレクトリ(ルート)にある.envを探す
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_PATH = os.getenv('DB_FILE_PATH')
if not DB_PATH:
    raise ValueError("DB_FILE_PATH is not set in .env file")

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
        race_class TEXT,
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
        FOREIGN KEY (race_id) REFERENCES races (race_id),
        FOREIGN KEY (horse_id) REFERENCES horses (horse_id),
        FOREIGN KEY (jockey_id) REFERENCES jockeys (jockey_id),
        FOREIGN KEY (trainer_id) REFERENCES trainers (trainer_id)
    )
    ''')

    # 3. Horses Table
    create_horses_sql = f'''
    CREATE TABLE IF NOT EXISTS horses (
        horse_id TEXT PRIMARY KEY,
        name TEXT,
        birth_date TEXT,
        sex TEXT,
        trainer_id TEXT,
        owner_id TEXT,
        breeder_id TEXT,
        FOREIGN KEY (trainer_id) REFERENCES trainers (trainer_id),
        FOREIGN KEY (owner_id) REFERENCES owners (owner_id),
        FOREIGN KEY (breeder_id) REFERENCES breeders (breeder_id)
    )
    '''
    cursor.execute(create_horses_sql)

    # 3.5. Pedigrees Table (New)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedigrees (
        horse_id TEXT NOT NULL,
        ancestor_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        position TEXT NOT NULL,
        PRIMARY KEY (horse_id, position),
        FOREIGN KEY (horse_id) REFERENCES horses (horse_id),
        FOREIGN KEY (ancestor_id) REFERENCES horses (horse_id)
    )
    ''')

    # 4. Jockeys Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS jockeys (
        jockey_id TEXT PRIMARY KEY,
        name TEXT,
        belonging TEXT,
        birth_date TEXT
    )
    ''')

    # 5. Trainers Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trainers (
        trainer_id TEXT PRIMARY KEY,
        name TEXT,
        belonging TEXT,
        birth_date TEXT
    )
    ''')

    # 6. Owners Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS owners (
        owner_id TEXT PRIMARY KEY,
        name TEXT,
        country TEXT
    )
    ''')

    # 7. Breeders Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS breeders (
        breeder_id TEXT PRIMARY KEY,
        name TEXT
    )
    ''')

    conn.commit()
    conn.close()
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()
