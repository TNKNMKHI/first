## 競馬予想AI

多数の説明変数が存在し、複雑な関係を持つ競馬データをサンプルに、機械学習でアプローチを行い、AI作成の練習を行うプロジェクト。

### 環境
- 言語: Python 3.11.9
- OS: Windows (win32)
- データソース: 
    - netkeiba.com (スクレイピング)
    - jbis.com (スクレイピング)

### データ管理
データの整合性と効率的な管理のため、**SQLite** (`keiba.db`) を使用する。

#### データベース設計 (Schema)
詳細は[設計ドキュメント](design_doc.md)を参照。

### 各ソースの役割
#### スクレイピング・データ収集
| ソース | 概要 |
| :--- | :--- |
| `get_race_ids.py` | 年ごとのレーシングカレンダーを取得しidと日付をcsvで出力 |
| `initialize_db.py` | データベースとテーブルの初期化 |
| `scraper_race.py` | csvに存在するレースIDからレースの詳細を取得 |
| `scraper_horse.py` | 馬IDから馬の詳細を取得 |
| `scraper_person_detail.py` |  騎手・調教師の詳細情報を取得 |

