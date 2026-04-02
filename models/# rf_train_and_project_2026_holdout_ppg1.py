# rf_train_and_project_2026_holdout_ppg17_v2.py
#
# Holdout-year training:
#   Train on:   2021->2022, 2022->2023, 2023->2024
#   Holdout on: 2024->2025
#
# Model target:
#   Predict PPR fantasy points PER GAME (PPG) for next season.
#
# Projection:
#   2026 projected total points = predicted_2026_ppg * 17
#   with a light calibration factor to reduce RF conservatism.

import os
import mysql.connector
import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

# ----------------------------
# Config
# ----------------------------
TARGET_PTS = "fantasy_points_ppr"
GAMES_COL = "games_played"

MIN_GAMES_FOR_PPG = 4
MIN_TARGET_PPG = 6
ASSUMED_GAMES_2026 = 17
CALIBRATION_FACTOR = 1.08

POSITIONS = ["QB", "RB", "WR", "TE"]

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
}

# Manual projected 2026 starting QBs
# Update this as needed before each season.
PROJECTED_STARTING_QBS = {
    "Josh Allen",
    "Patrick Mahomes",
    "Joe Burrow",
    "Lamar Jackson",
    "Jalen Hurts",
    "Justin Herbert",
    "Dak Prescott",
    "C.J. Stroud",
    "Jordan Love",
    "Jared Goff",
    "Brock Purdy",
    "Trevor Lawrence",
    "Kyler Murray",
    "Tua Tagovailoa",
    "Caleb Williams",
    "Jayden Daniels",
    "Drake Maye",
    "Bo Nix",
    "Bryce Young",
    "Daniel Jones",
    "Matthew Stafford",
    "Geno Smith",
    "Sam Darnold",
    "Aaron Rodgers",
    "malik willis",
    "Baker Mayfield",
    "Jaxson Dart",
    "Tyler Shough",
    "Shedeur Sanders",
    "Cam Ward",
    "Jacoby Brissett",
    "Kenny Pickett",
}

# ----------------------------
# DB helpers
# ----------------------------
def read_sql_df(query: str) -> pd.DataFrame:
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def upsert_projections_ppr(proj_df: pd.DataFrame):
    """
    Inserts/updates 2026 PPR projections into `projections`.

    Recommended unique key:
      ALTER TABLE projections
      ADD UNIQUE KEY uniq_proj (player_id, season, format, model_name, model_version);
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
    for _, r in proj_df.iterrows():
        rows.append((
            int(r["player_id"]),
            2026,
            "ppr",
            None, None, None, None, None, None,
            None, None, None,
            None, None, None, None,
            float(r["proj_fantasy_points_ppr"]),
            "random_forest",
            "holdout_ppg17_v2"
        ))

    cur.executemany(sql, rows)
    conn.commit()
    print(f"[STEP] Inserted/updated {len(rows)} projections into projections table")

    cur.close()
    conn.close()


# ----------------------------
# Pair builder (year_from -> year_to)
# ----------------------------
def build_pairs(df: pd.DataFrame, features: list[str], year_from: int, year_to: int) -> pd.DataFrame:
    """
    Build paired dataset:
      X from year_from -> y_ppg from year_to for same player_id.

    y_ppg = fantasy_points_ppr / games_played in year_to
    """
    a = df[df["season"] == year_from].copy()
    b = df[df["season"] == year_to].copy()

    # X: features from year_from
    a = a[["player_id", "position"] + features].copy()
    a = a.rename(columns={c: f"{c}_x" for c in features})

    # y: compute PPG from year_to
    b = b[["player_id", TARGET_PTS, GAMES_COL]].copy()
    b = b.rename(columns={TARGET_PTS: "y_pts", GAMES_COL: "y_games"})

    paired = a.merge(b, on="player_id", how="inner")
    paired["pair"] = f"{year_from}->{year_to}"

    paired["y_ppg"] = paired["y_pts"] / paired["y_games"].replace(0, np.nan)

    # Filter out invalid/tiny samples
    paired = paired[paired["y_games"].notna() & (paired["y_games"] >= MIN_GAMES_FOR_PPG)]
    paired = paired[paired["y_ppg"].notna()]
    paired = paired[paired["y_ppg"] >= MIN_TARGET_PPG]

    # Remove unstable QB source rows (backup / spot-start seasons)
    if {"games_1y_x", "ppg_weighted_3y_x"}.issubset(paired.columns):
        paired = paired[
            ~(
                (paired["position"] == "QB") &
                (paired["games_1y_x"] < 8) &
                (paired["ppg_weighted_3y_x"] < 14)
            )
        ]

    return paired


def main():
    # ----------------------------
    # Pull data
    # ----------------------------
    season_stats = read_sql_df("""
        SELECT *
        FROM player_season_stats
        WHERE season BETWEEN 2021 AND 2025;
    """)

    players = read_sql_df("""
        SELECT player_id, full_name, position, status
        FROM players;
    """)

    df = season_stats.merge(players, on="player_id", how="left")

    # ----------------------------
    # Engineered fantasy-history features
    # ----------------------------
    df["ppg_1y"] = df["fantasy_points_ppr"] / df["games_played"].replace(0, np.nan)

    df = df.sort_values(["player_id", "season"]).copy()

    df["ppg_2y"] = df.groupby("player_id")["ppg_1y"].shift(1)
    df["ppg_3y"] = df.groupby("player_id")["ppg_1y"].shift(2)

    df["ppg_1y"] = df["ppg_1y"].fillna(0)
    df["ppg_2y"] = df["ppg_2y"].fillna(df["ppg_1y"])
    df["ppg_3y"] = df["ppg_3y"].fillna(df["ppg_2y"])

    df["ppg_weighted_3y"] = (
        0.6 * df["ppg_1y"] +
        0.3 * df["ppg_2y"] +
        0.1 * df["ppg_3y"]
    )

    df["ppg_delta_1y"] = (df["ppg_1y"] - df["ppg_2y"]).fillna(0)

    # ----------------------------
    # Engineered games-history features
    # ----------------------------
    df["games_1y"] = df["games_played"].fillna(0)

    df["games_2y"] = df.groupby("player_id")["games_1y"].shift(1)
    df["games_3y"] = df.groupby("player_id")["games_1y"].shift(2)

    df["games_2y"] = df["games_2y"].fillna(df["games_1y"])
    df["games_3y"] = df["games_3y"].fillna(df["games_2y"])

    df["games_weighted_3y"] = (
        0.6 * df["games_1y"] +
        0.3 * df["games_2y"] +
        0.1 * df["games_3y"]
    )

    # ----------------------------
    # Nonlinear age
    # ----------------------------
    if "age" in df.columns:
        df["age_sq"] = df["age"] ** 2
    else:
        raise ValueError("Column 'age' not found in merged dataframe. Make sure player_season_stats.age is populated.")

    # ----------------------------
    # Required columns check
    # ----------------------------
    for col in [TARGET_PTS, GAMES_COL, "season", "player_id", "position", "age"]:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in merged dataframe.")

    # ----------------------------
    # Auto-detect features
    # ----------------------------
    exclude = {
        "player_id",
        "season",
        "fantasy_points_ppr",
        "fantasy_points_half",
        "fantasy_points_std",
    }

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    features = [c for c in numeric_cols if c not in exclude]

    # Keep role-history features, but exclude raw games_played
    if GAMES_COL in features:
        features.remove(GAMES_COL)

    print(f"[INFO] Features count = {len(features)}")
    print(features)
    print("[INFO] age in features:", "age" in features)
    print("[INFO] age_sq in features:", "age_sq" in features)
    print("[INFO] ppg_weighted_3y in features:", "ppg_weighted_3y" in features)
    print("[INFO] games_weighted_3y in features:", "games_weighted_3y" in features)

    # ----------------------------
    # Holdout setup
    # ----------------------------
    train_pairs = [(2021, 2022), (2022, 2023), (2023, 2024)]
    holdout_pair = (2024, 2025)

    train_df = pd.concat(
        [build_pairs(df, features, a, b) for (a, b) in train_pairs],
        ignore_index=True
    )

    holdout_df = build_pairs(df, features, holdout_pair[0], holdout_pair[1])

    print(f"[INFO] Train rows (stacked pairs): {len(train_df)}")
    print(f"[INFO] Holdout rows (2024->2025): {len(holdout_df)}")

    # ----------------------------
    # Train + holdout eval per position
    # ----------------------------
    holdout_models = {}

    for pos in POSITIONS:
        tr = train_df[train_df["position"] == pos].copy()
        ho = holdout_df[holdout_df["position"] == pos].copy()

        if len(tr) < 60 or len(ho) < 20:
            print(f"[SKIP] {pos}: not enough data (train={len(tr)}, holdout={len(ho)})")
            continue

        X_train = tr[[f"{c}_x" for c in features]].fillna(0)
        y_train = tr["y_ppg"].fillna(0)

        X_hold = ho[[f"{c}_x" for c in features]].fillna(0)
        y_hold = ho["y_ppg"].fillna(0)

        model = RandomForestRegressor(
            n_estimators=900,
            random_state=42,
            min_samples_leaf=2,
            n_jobs=-1
        )

        model.fit(X_train, y_train)

        importances = pd.Series(
            model.feature_importances_,
            index=X_train.columns
        ).sort_values(ascending=False)

        print(f"\n[IMPORTANCE] Top 15 features for {pos}:")
        print(importances.head(15))

        pred_ppg = model.predict(X_hold)
        mae_ppg = mean_absolute_error(y_hold, pred_ppg)
        mae_pts_17 = mae_ppg * 17

        print(
            f"[HOLDOUT VAL] {pos}  "
            f"MAE_PPG={mae_ppg:.2f}  MAE_17G_PTS={mae_pts_17:.2f}  "
            f"train={len(tr)} holdout={len(ho)}"
        )

        holdout_models[pos] = model

    if not holdout_models:
        print("[ERROR] No models trained. Check data coverage.")
        return

    # ----------------------------
    # Final fit on all pairs through 2025
    # ----------------------------
    final_pairs = [(2021, 2022), (2022, 2023), (2023, 2024), (2024, 2025)]
    final_train = pd.concat(
        [build_pairs(df, features, a, b) for (a, b) in final_pairs],
        ignore_index=True
    )

    final_models = {}
    for pos in POSITIONS:
        tr = final_train[final_train["position"] == pos].copy()

        if len(tr) < 90:
            print(f"[SKIP FINAL] {pos}: not enough rows ({len(tr)})")
            continue

        X = tr[[f"{c}_x" for c in features]].fillna(0)
        y = tr["y_ppg"].fillna(0)

        model = RandomForestRegressor(
            n_estimators=900,
            random_state=42,
            min_samples_leaf=2,
            n_jobs=-1
        )

        model.fit(X, y)
        final_models[pos] = model

        print(f"[FINAL FIT] {pos}: trained_rows={len(tr)}")

    if not final_models:
        print("[ERROR] No final models trained. Check data coverage.")
        return

    # ----------------------------
    # Build 2026 projection candidate pool from 2025 rows
    # ----------------------------
    df_2025 = df[df["season"] == 2025].copy()
    df_2025 = df_2025[df_2025["position"].isin(POSITIONS)].copy()

    # General fantasy relevance / role-history filter
    df_2025 = df_2025[
        (
            (df_2025["ppg_1y"] >= 5) |
            (df_2025["ppg_weighted_3y"] >= 7) |
            (df_2025["games_weighted_3y"] >= 10)
        )
    ].copy()

    # Remove obvious backup / spot-start QB profiles
    df_2025 = df_2025[
        ~(
            (df_2025["position"] == "QB") &
            (df_2025["games_1y"] < 8) &
            (df_2025["ppg_weighted_3y"] < 14)
        )
    ].copy()

    # Restrict QB projections to projected starters
    qb_mask = (df_2025["position"] != "QB") | (df_2025["full_name"].isin(PROJECTED_STARTING_QBS))
    df_2025 = df_2025[qb_mask].copy()

    print(f"[INFO] Projection candidate rows after filters: {len(df_2025)}")

    # ----------------------------
    # Project 2026 from 2025 features
    # ----------------------------
    proj_parts = []

    for pos, model in final_models.items():
        part = df_2025[df_2025["position"] == pos].copy()
        if part.empty:
            continue

        X_2025 = part[features].fillna(0)
        X_2025.columns = [f"{c}_x" for c in features]

        proj_ppg_2026 = model.predict(X_2025)
        proj_pts_2026 = proj_ppg_2026 * ASSUMED_GAMES_2026
        proj_pts_2026 *= CALIBRATION_FACTOR

        proj_parts.append(pd.DataFrame({
            "player_id": part["player_id"].values,
            "full_name": part["full_name"].values,
            "position": part["position"].values,
            "season": 2026,
            "proj_fantasy_points_ppr": proj_pts_2026
        }))

    if not proj_parts:
        print("[ERROR] No projections generated.")
        return

    proj_2026 = pd.concat(proj_parts, ignore_index=True)

    print("\n[TOP 50] Projected 2026 PPR fantasy points:")
    print(
        proj_2026.sort_values("proj_fantasy_points_ppr", ascending=False)
        .head(50)
        .to_string(index=False)
    )

    upsert_projections_ppr(proj_2026)


if __name__ == "__main__":
    main()