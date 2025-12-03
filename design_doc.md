# 競馬予想AIプロジェクト 設計ドキュメント

## 1. プロジェクト概要
*   **目的**: 競馬予想AIの作成
*   **言語**: Python 3.11.9
*   **環境**: Windows (win32)
*   **データソース**: netkeiba.com (スクレイピング)

## 2. データ管理
データの整合性と効率的な管理のため、**SQLite** (`keiba.db`) を使用する。

### 2.1 データベース設計 (Schema)

#### `races` テーブル (レース情報)
レース自体のメタデータ。

| カラム名 | 型 | 説明 | 備考 |
| :--- | :--- | :--- | :--- |
| `race_id` | TEXT | レースID | **PK** (例: `202406010101`) |
| `date` | TEXT | 開催日 | ISO8601形式 (YYYY-MM-DD) |
| `venue` | TEXT | 開催場所 | 例: 中山 |
| `race_class` | TEXT | レースクラス | 例: G1, G2, OP, 3勝C |
| `race_name` | TEXT | レース名 | |
| `race_round` | INTEGER | ラウンド | 例: 11 |
| `course_type` | TEXT | コース種別 | 芝/ダート/障害 |
| `distance` | INTEGER | 距離(m) | |
| `rotation` | TEXT | 回り | 右/左/直線 |
| `weather` | TEXT | 天候 | |
| `state` | TEXT | 馬場状態 | 良/稍/重/不 |
| `entries` | INTEGER | 出走頭数 | |

#### `results` テーブル (レース結果)
レースと馬の関連データ（各レースにおける各馬の成績）。

| カラム名 | 型 | 説明 | 備考 |
| :--- | :--- | :--- | :--- |
| `race_id` | TEXT | レースID | **PK, FK** (`races.race_id`) |
| `horse_id` | TEXT | 馬ID | **PK, FK** (`horses.horse_id`) |
| `rank` | INTEGER | 着順 | 失格等は除外または別値 |
| `frame_no` | INTEGER | 枠番 | |
| `horse_no` | INTEGER | 馬番 | |
| `jockey_id` | TEXT | 騎手ID | |
| `trainer_id` | TEXT | 調教師ID | |
| `age` | INTEGER | 年齢 | |
| `sex` | TEXT | 性別 | 牡/牝/セ |
| `weight` | REAL | 斤量 | |
| `time_seconds` | REAL | タイム(秒) | 変換済みデータ |
| `margin` | TEXT | 着差 | |
| `passing` | TEXT | 通過順 | |
| `last_3f` | REAL | 上がり3F | |
| `odds` | REAL | 単勝オッズ | |
| `popularity` | INTEGER | 人気 | |
| `horse_weight` | INTEGER | 馬体重 | |
| `weight_diff` | INTEGER | 体重増減 | |

#### `horses` テーブル (競走馬・血統情報)
馬の静的データ。5代血統までの全祖先IDをカラムとして保持する。

| カラム名 | 型 | 説明 | 備考 |
| :--- | :--- | :--- | :--- |
| `horse_id` | TEXT | 馬ID | **PK** (例: `2021100123`) |
| `name` | TEXT | 馬名 | |
| `birth_year` | INTEGER | 生年 | |
| `sex` | TEXT | 性別 | |
| `trainer_id` | TEXT | 担当調教師ID | |
| `sire_line` | TEXT | 牡系 | サイアーライン |
| `f_id` | TEXT | 父ID | |
| `m_id` | TEXT | 母ID | |
| `ff_id` | TEXT | 父父ID | |
| `fm_id` | TEXT | 父母ID | |
| `mf_id` | TEXT | 母父ID | |
| `mm_id` | TEXT | 母母ID | |
| ... | ... | ... | (3代〜5代も同様にカラム展開。計62カラム) |
| `mmmmm_id` | TEXT | 5代母母 | |

#### `jockeys` テーブル (騎手情報)
騎手の基本情報。
| カラム名 | 型 | 説明 | 備考 |
| :--- | :--- | :--- | :--- |
| `jockey_id` | TEXT | 騎手ID | **PK** |
| `name` | TEXT | 騎手名 | |
| `belonging` | TEXT | 所属 | 栗東 or 美浦 |
| `birth_date` | TEXT | 生年月日 | ISO8601形式 (YYYY-MM-DD) |
| `first` | INTEGER | 通算勝利数 | １着回数 |
| `second` | INTEGER | 通算２着数 | ２着回数 |
| `third` | INTEGER | 通算３着数 | ３着回数 |
| `career_races` | INTEGER | 通算出走数 | |

#### `trainer` テーブル（調教師情報）
騎手の基本情報。
| カラム名 | 型 | 説明 | 備考 |
| :--- | :--- | :--- | :--- |
| `trainer_id` | TEXT | 騎手ID | **PK** |
| `name` | TEXT | 騎手名 | |
| `belonging` | TEXT | 所属 | 栗東 or 美浦 |
| `birth_date` | TEXT | 生年月日 | ISO8601形式 (YYYY-MM-DD) |
| `first` | INTEGER | 通算勝利数 | １着回数 |
| `second` | INTEGER | 通算２着数 | ２着回数 |
| `third` | INTEGER | 通算３着数 | ３着回数 |
| `career_races` | INTEGER | 通算出走数 | |

## 3. 開発フロー

### Phase 1: データ収集基盤の構築
1.  **DB初期化**: SQLiteテーブル作成スクリプトの実装。
    *   `horses` テーブルは5代血統を展開したカラムを持つ。
2.  **レース結果スクレイピング**: 指定期間のレース結果を取得し、`races`, `results` に保存。
    *   ※風情報の取得はnetkeibaに含まれないため、現段階では実装しない（将来的に気象庁データ連携を検討）。
    *   この段階では `horses` テーブルは空だが、`results` に `horse_id` は記録される。
3.  **馬情報スクレイピング**: `results` に存在する全 `horse_id` をリストアップし、未取得の馬の血統情報を取得して `horses` に保存。

### Phase 2: 特徴量エンジニアリング & モデル構築
(今後の予定)
