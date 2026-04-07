# scripts/etl_pipeline.py
import pandas as pd
import sqlite3
from pathlib import Path

# ========================= CONFIG =========================
DATA_DIR = Path("data")
DB_PATH = Path("database/production.db")
DB_PATH.parent.mkdir(exist_ok=True)

# Key mining files
FILES = {
    "facilities": "facilities.csv",
    "production": "production.csv",      # change to coal.csv or minerals.csv if your main file has a different name
    "waste": "waste.csv"
}

def load_csv(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    print(f"Loading {filename} ...")
    return pd.read_csv(path, low_memory=False)

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df

def run_etl(force_reload: bool = False):
    print("🚀 Starting Mining ETL Pipeline...\n")
    
    if DB_PATH.exists() and not force_reload:
        print("Database already exists. Use force_reload=True to overwrite.")
        return

    # 1. Extract
    facilities = load_csv(FILES["facilities"])
    production = load_csv(FILES["production"])
    waste = load_csv(FILES["waste"])

    # 2. Clean & Standardize
    facilities = clean_column_names(facilities)
    production = clean_column_names(production)
    waste = clean_column_names(waste)

    # Rename common key for merging
    production = production.rename(columns={'facility_id': 'mine_id'})
    waste = waste.rename(columns={'facility_id': 'mine_id'})
    facilities = facilities.rename(columns={'facility_id': 'mine_id'})

    # 3. Merge core tables
    df = production.merge(
        facilities[['mine_id', 'country', 'facility_name', 'primary_commodity']],
        on='mine_id', how='left'
    )

    # Add waste data
    df = df.merge(
        waste[['mine_id', 'year', 'value_tonnes', 'total_material_tonnes', 'stripping_ratio']],
        on=['mine_id', 'year'], how='left', suffixes=('', '_waste')
    )

    # 4. Calculate Mining KPIs
    df['production_tonnes'] = df['value_tonnes'].fillna(0)
    df['waste_tonnes'] = df['value_tonnes_waste'].fillna(0)
    
    df['waste_ratio'] = df['waste_tonnes'] / df['production_tonnes'].replace(0, float('nan'))
    df['efficiency_score'] = 1 / (df['waste_ratio'] + 1)
    
    df['risk_level'] = pd.cut(
        df['waste_ratio'],
        bins=[0, 0.5, 1.5, float('inf')],
        labels=['Low', 'Medium', 'High']
    )
    
    df = df.sort_values(['mine_id', 'year'])
    df['yoy_growth'] = df.groupby('mine_id')['production_tonnes'].pct_change() * 100

    # Final columns
    final_columns = [
        'mine_id', 'year', 'country', 'facility_name', 'primary_commodity',
        'production_tonnes', 'waste_tonnes', 'waste_ratio',
        'efficiency_score', 'risk_level', 'yoy_growth'
    ]
    df_final = df[final_columns].copy()

    # 5. Load to SQLite
    conn = sqlite3.connect(DB_PATH)
    df_final.to_sql('fact_production', conn, if_exists='replace', index=False)
    
    # Dimension table
    dim_facilities = facilities[['mine_id', 'country', 'facility_name', 'primary_commodity']].copy()
    dim_facilities.to_sql('dim_facilities', conn, if_exists='replace', index=False)
    
    conn.close()

    print(f"\n ETL completed successfully!")
    print(f"   • Rows loaded: {len(df_final):,}")
    print(f"   • Database created: {DB_PATH}")
    print(f"   • Mining KPIs added: waste_ratio, efficiency_score, risk_level, yoy_growth")

if __name__ == "__main__":
    run_etl(force_reload=True)