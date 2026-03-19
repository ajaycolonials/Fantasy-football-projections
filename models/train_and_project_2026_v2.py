# rf_train_and_project_2026_v2_allstats_bypos.py

import mysql.connector
import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split

TARGET = "fantasy_points_ppr"
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

POSITIONS = ["QB", "RB", "WR", "TE"]


def read_sql_df(query: str) -> pd.DataFrame:
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    return df


def upsert_projections_ppr(proj_df: pd.DataFrame):
    """
    Inserts/updates 2026 PPR projections into `projections`.
    NOTE: Your enum is ('ppr','half_ppr','standard') so we use 'ppr' (lowercase).
    Also: ON DUPLICATE KEY UPDATE only works if you have a unique key that conflicts.
    If you don't have one, this will behave like normal inserts.
    """
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
            "ppr",

            None, None, None, None, None, None,   # passing
            None, None, None,                     # rushing
            None, None, None, None,               # receiving

            fantasy_points,
            "random_forest",
            "v2_allstats_bypos"
        ))

    cur.executemany(sql, rows)
    conn.commit()
    print(f"[STEP 8] Inserted/updated {len(rows)} projections")

    cur.close()
    conn.close()


def main():
    # Pull all season stats (all columns) for 2024 and 2025
    season_stats = read_sql_df("""
        SELECT *
        FROM player_season_stats
        WHERE season IN (2024, 2025);
    """)

    players = read_sql_df("""
        SELECT player_id, full_name, position
        FROM players
        WHERE status = 'active';
    """)

    df = season_stats.merge(players, on="player_id", how="left")

    # Ensure we have our needed columns
    if TARGET not in df.columns:
        raise ValueError(f"Target column '{TARGET}' not found in merged dataframe.")

    # Determine FEATURES dynamically: all numeric columns except ids/season/target
    exclude = {"player_id", "season", TARGET, "fantasy_points_half",
    "fantasy_points_std"}
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    FEATURES = [c for c in numeric_cols if c not in exclude]

    print("[INFO] Using FEATURES (count={}):".format(len(FEATURES)))
    print(FEATURES)

    # Split by season
    df_2024 = df[df["season"] == 2024].copy()
    df_2025 = df[df["season"] == 2025].copy()

    # Rename 2024 feature columns
    rename_2024 = {col: f"{col}_2024" for col in FEATURES + [TARGET]}
    df_2024 = df_2024.rename(columns=rename_2024)

    # Rename 2025 target column
    df_2025 = df_2025.rename(columns={TARGET: f"{TARGET}_2025"})

    # Build training pairs: players who have both seasons
    train = df_2024.merge(
    df_2025[["player_id", f"{TARGET}_2025"]],
    on="player_id",
    how="inner"
)


    print("[INFO] Training rows (players with both 2024 & 2025):", len(train))

    models = {}
    val_rows = []

    # Train per position
    for pos in POSITIONS:
        train_pos = train[train["position"] == pos].copy()

        if len(train_pos) < 20:
            print(f"[SKIP] Not enough training rows for {pos}: {len(train_pos)}")
            continue

        X = train_pos[[f"{c}_2024" for c in FEATURES]].copy().fillna(0)
        y = train_pos[f"{TARGET}_2025"].copy().fillna(0)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = RandomForestRegressor(
            n_estimators=700,
            random_state=42,
            min_samples_leaf=2,
            n_jobs=-1
        )

        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae = mean_absolute_error(y_test, preds)

        print(f"[VAL] {pos} MAE={mae:.2f}  n_train={len(X_train)}  n_test={len(X_test)}")
        importances = pd.Series(
        model.feature_importances_,
        index=[f"{c}_2024" for c in FEATURES]
        ).sort_values(ascending=False)

        print(f"\n[IMPORTANCE] Top 10 features for {pos}:")
        print(importances.head(10))


        models[pos] = model
        val_rows.append((pos, mae, len(train_pos)))

    # If nothing trained, stop
    if not models:
        print("[ERROR] No models trained (not enough data).")
        return

    # Project 2026 using 2025 features, per position
    proj_parts = []
    df_2025_features = df_2025[["player_id", "full_name", "position"] + FEATURES].copy().fillna(0)

    for pos, model in models.items():
        mask = df_2025_features["position"] == pos
        if mask.sum() == 0:
            continue

        X_2025 = df_2025_features.loc[mask, FEATURES].copy()
        X_2025.columns = [f"{c}_2024" for c in FEATURES]  # match training column names

        proj_vals = model.predict(X_2025)

        proj_parts.append(pd.DataFrame({
            "player_id": df_2025_features.loc[mask, "player_id"].values,
            "full_name": df_2025_features.loc[mask, "full_name"].values,
            "position": pos,
            "season": 2026,
            "proj_fantasy_points_ppr": proj_vals
        }))

    proj_2026 = pd.concat(proj_parts, ignore_index=True)

    print("\n[STEP 7] Top 50 projected PPR fantasy points for 2026:")
    print(
        proj_2026.sort_values("proj_fantasy_points_ppr", ascending=False)
                .head(50)
                .to_string(index=False)
    )

    upsert_projections_ppr(proj_2026)


if __name__ == "__main__":
    main()
