# rf_train_and_project_2026.py

import mysql.connector
import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

FEATURES = [
    "games_played",
    "pass_attempts", "pass_completions", "pass_yards", "pass_tds", "interceptions",
    "rush_attempts", "rush_yards", "rush_tds",
    "targets", "receptions", "rec_yards", "rec_tds",
]

TARGET = "fantasy_points_ppr"

import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

def read_sql_df(query: str) -> pd.DataFrame:
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df
def upsert_projections_ppr(proj_df):
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()

    sql = """
    INSERT INTO projections (
        player_id,
        season,
        format,
        games,
        pass_attempts,
        pass_completions,
        pass_yards,
        pass_tds,
        interceptions,
        rush_attempts,
        rush_yards,
        rush_tds,
        targets,
        receptions,
        rec_yards,
        rec_tds,
        fantasy_points,
        model_name,
        model_version
    )
    VALUES (
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s
    )
    ON DUPLICATE KEY UPDATE
        fantasy_points = VALUES(fantasy_points),
        model_name = VALUES(model_name),
        model_version = VALUES(model_version),
        created_at = CURRENT_TIMESTAMP
    """

    rows = []

    for _, row in proj_df.iterrows():

        player_id = int(row["player_id"])

        fantasy_points = float(row["proj_fantasy_points_ppr"])

        rows.append((
            player_id,
            2026,
            "PPR",

            None,  # games
            None,  # pass_attempts
            None,  # pass_completions
            None,  # pass_yards
            None,  # pass_tds
            None,  # interceptions

            None,  # rush_attempts
            None,  # rush_yards
            None,  # rush_tds

            None,  # targets
            None,  # receptions
            None,  # rec_yards
            None,  # rec_tds

            fantasy_points,

            "random_forest",
            "v1_basic"
        ))

    cur.executemany(sql, rows)
    conn.commit()

    print(f"[STEP 8] Inserted/updated {len(rows)} projections")

    cur.close()
    conn.close()

def main():
    season_stats = read_sql_df("""
        SELECT
          player_id,
          season,
          games_played,
          pass_attempts, pass_completions, pass_yards, pass_tds, interceptions,
          rush_attempts, rush_yards, rush_tds,
          targets, receptions, rec_yards, rec_tds,
          fantasy_points_ppr
        FROM player_season_stats
        WHERE season IN (2024, 2025);
    """)

    players = read_sql_df("""
        SELECT player_id,full_name, position
        FROM players
        WHERE status = 'active';
    """)

    df = season_stats.merge(players, on="player_id", how="left")
    
    df_2024 = df[df["season"] == 2024].copy()
    df_2025 = df[df["season"] == 2025].copy()

    rename_2024 = {col: f"{col}_2024" for col in FEATURES + [TARGET]}
    df_2024 = df_2024.rename(columns=rename_2024)

    df_2025 = df_2025.rename(columns={
        TARGET: f"{TARGET}_2025"
    })

    train = df_2024.merge(
        df_2025[["player_id", f"{TARGET}_2025"]],
        on="player_id",
        how="inner"
    )


    print(train.columns)

    X = train[[f"{c}_2024" for c in FEATURES]].copy()

    y = train[f"{TARGET}_2025"].copy()

    X = X.fillna(0)
    y = y.fillna(0)

    print("[INFO] X shape:", X.shape)
    print("[INFO] y shape:", y.shape)

    print("\n[INFO] X sample (first 5 rows):")
    print(X.head(5).to_string(index=False))

    print("\n[INFO] y sample (first 10 values):")
    print(y.head(10).to_string(index=False))
    
    model = RandomForestRegressor(
        n_estimators=500,    
        random_state=42,      
        min_samples_leaf=2,    
        n_jobs=-1              
    )

    model.fit(X, y)

    train_preds = model.predict(X)
    mae = mean_absolute_error(y, train_preds)

    print("\n[STEP 6] RandomForest trained.")
    print("[STEP 6] Train MAE (sanity check):", round(mae, 2))

    X_2025 = df_2025[FEATURES].copy().fillna(0)
    X_2025.columns = [f"{c}_2024" for c in FEATURES]
    proj_2026_ppr = model.predict(X_2025)

    proj_2026 = pd.DataFrame({
        "player_id": df_2025["player_id"].values,
        "full_name": df_2025["full_name"].values,
        "position": df_2025["position"].values,
        "season": 2026,
        "proj_fantasy_points_ppr": proj_2026_ppr
    })

    print("\n[STEP 7] Top 50 projected PPR fantasy points for 2026:")
    print(
        proj_2026.sort_values("proj_fantasy_points_ppr", ascending=False)
                .head(50)
                .to_string(index=False)
    )
    upsert_projections_ppr(proj_2026)




if __name__ == "__main__":
    main()
