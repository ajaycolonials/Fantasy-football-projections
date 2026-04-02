# rf_train_and_project_2026_holdout_ppg17_teamcontext_step1.py

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

MIN_GAMES_FOR_PPG = 8
ASSUMED_GAMES_2026 = 17

POSITIONS = ["QB", "RB", "WR", "TE"]

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
    "Jacoby Brissett",
    "Matthew Stafford",
    "Geno Smith",
    "Sam Darnold",
    "Aaron Rodgers",
    "Malik Willis",
    "Baker Mayfield",
    "Jaxson Dart",
    "Tyler Shough",
    "Shedeur Sanders",
    "Cam Ward",
    "Daniel Jones",
    "Kenny Pickett"
}

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "fantasy_app"),
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
            "teamcontext_step1"
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
    a = df[df["season"] == year_from].copy()
    b = df[df["season"] == year_to].copy()

    a = a[["player_id", "position", GAMES_COL] + features].copy()
    a = a.rename(columns={c: f"{c}_x" for c in features})
    a = a.rename(columns={GAMES_COL: "x_games"})

    b = b[["player_id", TARGET_PTS, GAMES_COL]].copy()
    b = b.rename(columns={TARGET_PTS: "y_pts", GAMES_COL: "y_games"})

    paired = a.merge(b, on="player_id", how="inner")
    paired["pair"] = f"{year_from}->{year_to}"

    paired["y_ppg"] = paired["y_pts"] / paired["y_games"].replace(0, np.nan)

    paired = paired[
        paired["x_games"].notna() & (paired["x_games"] >= MIN_GAMES_FOR_PPG) &
        paired["y_games"].notna() & (paired["y_games"] >= MIN_GAMES_FOR_PPG)
    ]
    paired = paired[paired["y_ppg"].notna()]

    return paired


def main():
    # ----------------------------
    # Pull player + team-context data
    # ----------------------------
    df = read_sql_df("""
        SELECT
            pss.*,
            p.full_name,
            p.position,
            p.status,

            tss.average_scoring_margin,
            tss.offensive_tds_per_game,
            tss.points_per_game,
            tss.points_per_play,
            tss.plays_per_game,
            tss.seconds_per_play,
            tss.neutral_situation_pace,
            tss.no_huddle_rate,
            tss.yards_per_play,
            tss.yards_per_game,
            tss.first_downs_per_game,
            tss.first_downs_per_play,
            tss.third_down_conversion_pct,
            tss.fourth_down_conversion_pct,
            tss.pass_attempts_per_game,
            tss.passing_play_pct,
            tss.passing_tds_per_game,
            tss.completions_per_game,
            tss.yards_per_completion,
            tss.pass_rate_over_expectation,
            tss.pass_rate_neutral_script,
            tss.pass_rate_positive_script,
            tss.pass_rate_negative_script,
            tss.pass_rate_red_zone,
            tss.rushing_attempts_per_game,
            tss.rushing_first_down_pct,
            tss.rushing_first_downs_per_game,
            tss.rushing_play_pct,
            tss.rushing_tds_per_game,
            tss.rushing_yards_per_game,
            tss.rush_yards_per_attempt,
            tss.rush_rate_neutral_script,
            tss.rush_rate_positive_script,
            tss.rush_rate_negative_script,
            tss.explosive_play_rate,
            tss.epa_per_play_pass,
            tss.epa_per_play_rush,
            tss.inside_zone_rate,
            tss.inside_zone_ypa,
            tss.outside_zone_rate,
            tss.outside_zone_ypa,
            tss.power_rate,
            tss.power_ypa,
            tss.stuff_rate,
            tss.avoided_tackle_rate,
            tss.yac_per_attempt,
            tss.ybc_per_attempt

        FROM player_season_stats pss
        JOIN players p
            ON pss.player_id = p.player_id
        LEFT JOIN team_season_stats tss
            ON pss.team_id = tss.team_id
           AND pss.season = tss.season
        WHERE pss.season BETWEEN 2021 AND 2025
          AND p.position IN ('QB', 'RB', 'WR', 'TE');
    """)

    # ----------------------------
    # Required columns check
    # ----------------------------
    required_cols = [TARGET_PTS, GAMES_COL, "season", "player_id", "position", "age"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in merged dataframe.")

    # ----------------------------
    # Create PPG history features
    # ----------------------------
    df = df.sort_values(["player_id", "season"]).copy()

    df["ppg_1y"] = df[TARGET_PTS] / df[GAMES_COL].replace(0, np.nan)
    df["ppg_2y"] = df.groupby("player_id")["ppg_1y"].shift(1)
    df["ppg_3y"] = df.groupby("player_id")["ppg_1y"].shift(2)

    pos_ppg_median = df.groupby("position")["ppg_1y"].transform("median")
    overall_ppg_median = df["ppg_1y"].median()
    pos_ppg_median = pos_ppg_median.fillna(overall_ppg_median)

    df["ppg_1y"] = df["ppg_1y"].fillna(pos_ppg_median)
    df["ppg_2y"] = df["ppg_2y"].fillna(df["ppg_1y"])
    df["ppg_3y"] = df["ppg_3y"].fillna(df["ppg_2y"])
    df["ppg_max_3y"] = df[["ppg_1y", "ppg_2y", "ppg_3y"]].max(axis=1)
    df["ppg_mean_3y"] = df[["ppg_1y", "ppg_2y", "ppg_3y"]].mean(axis=1)

    df["high_end_ppg_flag"] = 0
    df.loc[(df["position"] == "QB") & (df["ppg_max_3y"] >= 22), "high_end_ppg_flag"] = 1
    df.loc[(df["position"] == "RB") & (df["ppg_max_3y"] >= 19), "high_end_ppg_flag"] = 1
    df.loc[(df["position"] == "WR") & (df["ppg_max_3y"] >= 18), "high_end_ppg_flag"] = 1
    df.loc[(df["position"] == "TE") & (df["ppg_max_3y"] >= 14), "high_end_ppg_flag"] = 1

    df["ppg_weighted_3y"] = (
        0.6 * df["ppg_1y"] +
        0.3 * df["ppg_2y"] +
        0.1 * df["ppg_3y"]
    )

    # ----------------------------
    # Convert core total stats to per-game features
    # ----------------------------
    PER_GAME_BASE_STATS = [
        "pass_attempts",
        "pass_completions",
        "pass_yards",
        "pass_tds",
        "interceptions",
        "rush_attempts",
        "rush_yards",
        "rush_tds",
        "targets",
        "receptions",
        "rec_yards",
        "rec_tds"
    ]

    for col in PER_GAME_BASE_STATS:
        if col in df.columns:
            df[f"{col}_pg"] = df[col] / df["games_played"].replace(0, np.nan)
            df[f"{col}_pg"] = df[f"{col}_pg"].fillna(0)

    REDZONE_COUNT_STATS = [
        "rz_rush_attempts",
        "carries_inside_5",
        "red_zone_targets",
        "red_zone_rec_tds",
        "red_zone_rush_tds",
    ]

    for col in REDZONE_COUNT_STATS:
        if col in df.columns:
            df[f"{col}_pg"] = df[col] / df["games_played"].replace(0, np.nan)
            df[f"{col}_pg"] = df[f"{col}_pg"].fillna(0)

    usage_delta_cols = [
        "pass_attempts_pg",
        "pass_completions_pg",
        "pass_yards_pg",
        "pass_tds_pg",
        "rush_attempts_pg",
        "rush_yards_pg",
        "rush_tds_pg",
        "targets_pg",
        "receptions_pg",
        "rec_yards_pg",
        "rec_tds_pg",
        "rz_rush_attempts_pg",
        "carries_inside_5_pg",
        "red_zone_targets_pg",
        "red_zone_rec_tds_pg",
        "red_zone_rush_tds_pg",
    ]

    for col in usage_delta_cols:
        if col in df.columns:
            prev = df.groupby("player_id")[col].shift(1)
            df[f"{col}_delta_1y"] = (df[col] - prev).fillna(0)

    advanced_delta_cols = [
        "snap_share_pct",
        "true_target_share_pct",
        "first_read_target_share_pct",
        "rb_opportunity_share_pct",
        "first_read_pct",
        "deep_throw_rate_pct",
        "qbr_when_clean",
        "time_to_throw_sec",
        "rz_rush_share",
        "inside5_share",
        "td_rate_inside_5",
        "touch_share_pct",
        "red_zone_target_share",
        "red_zone_rec_conversion_rate"
    ]

    for col in advanced_delta_cols:
        if col in df.columns:
            prev = df.groupby("player_id")[col].shift(1)
            df[f"{col}_delta_1y"] = (df[col] - prev).fillna(0)

    # ----------------------------
    # Add team-relative opportunity features
    # ----------------------------
    if "targets_pg" in df.columns and "pass_attempts_per_game" in df.columns:
        df["target_share_team"] = (
            df["targets_pg"] / df["pass_attempts_per_game"].replace(0, np.nan)
        ).fillna(0)

    if "rush_attempts_pg" in df.columns and "rushing_attempts_per_game" in df.columns:
        df["rush_share_team"] = (
            df["rush_attempts_pg"] / df["rushing_attempts_per_game"].replace(0, np.nan)
        ).fillna(0)

    # ----------------------------
    # Add nonlinear age feature
    # ----------------------------
    df["age_sq"] = df["age"] ** 2

    # ----------------------------
    # Auto-detect FEATURES
    # ----------------------------
    exclude = {
        "player_id",
        "season",
        "team_id",
        "fantasy_points_ppr",     # remove direct fantasy-point leakage
        "fantasy_points_half",
        "fantasy_points_std",
        "pass_attempts",
        "pass_completions",
        "pass_yards",
        "pass_tds",
        "interceptions",
        "rush_attempts",
        "rush_yards",
        "rush_tds",
        "targets",
        "receptions",
        "rec_yards",
        "rec_tds",
        "ppg_1y",
        "ppg_2y",
        "ppg_3y",
        "ppg_max_3y",             # remove max historical fantasy anchor
        "ppg_min_3y",
        "ppg_mean_3y",
        "high_end_ppg_flag",      # remove ceiling-flag fantasy anchor
        "ppg_recent_ceiling_blend",
    }

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    features = [c for c in numeric_cols if c not in exclude]

    if GAMES_COL in features:
        features.remove(GAMES_COL)

    print(f"\n[INFO] Features count = {len(features)}")
    print(features)
    print("[INFO] fantasy_points_ppr in features:", "fantasy_points_ppr" in features)
    print("[INFO] ppg_weighted_3y in features:", "ppg_weighted_3y" in features)
    print("[INFO] ppg_max_3y in features:", "ppg_max_3y" in features)
    print("[INFO] high_end_ppg_flag in features:", "high_end_ppg_flag" in features)
    print("[INFO] target_share_team in features:", "target_share_team" in features)
    print("[INFO] rush_share_team in features:", "rush_share_team" in features)

    # ----------------------------
    # ROLLING VALIDATION SETUP
    # ----------------------------
    rolling_folds = [
        ([(2021, 2022), (2022, 2023)], (2023, 2024)),
        ([(2021, 2022), (2022, 2023), (2023, 2024)], (2024, 2025)),
    ]

    all_results = []
    eval_ok = {}

    # ----------------------------
    # Rolling validation per position
    # ----------------------------
    for pos in POSITIONS:
        print(f"\n================ {pos} ROLLING VALIDATION ================")

        pos_results = []

        for fold_i, (train_pairs, test_pair) in enumerate(rolling_folds):
            print(f"\n[FOLD {fold_i + 1}] Train={train_pairs} Test={test_pair}")

            train_df = pd.concat(
                [build_pairs(df, features, a, b) for (a, b) in train_pairs],
                ignore_index=True
            )

            test_df = build_pairs(df, features, test_pair[0], test_pair[1])

            tr = train_df[train_df["position"] == pos].copy()
            te = test_df[test_df["position"] == pos].copy()

            if len(tr) < 50 or len(te) < 15:
                print(f"[SKIP] not enough data (train={len(tr)}, test={len(te)})")
                continue

            X_train = tr[[f"{c}_x" for c in features]].copy()
            X_test = te[[f"{c}_x" for c in features]].copy()

            train_medians = X_train.median()
            X_train = X_train.fillna(train_medians)
            X_test = X_test.fillna(train_medians)

            y_train = tr["y_ppg"].copy()
            y_test = te["y_ppg"].copy()

            model = RandomForestRegressor(
                n_estimators=1200,
                random_state=42,
                max_depth=None,
                min_samples_leaf=1,
                n_jobs=-1
            )

            model.fit(X_train, y_train)

            importances = pd.Series(
                model.feature_importances_,
                index=X_train.columns
            ).sort_values(ascending=False)

            print(f"\n[IMPORTANCE] Top 15 features for {pos}, fold {fold_i + 1}:")
            print(importances.head(15))

            pred_ppg = model.predict(X_test)

            fold_df = pd.DataFrame({
                "player_id": te["player_id"].values,
                "position": pos,
                "actual_ppg": y_test.values,
                "pred_ppg": pred_ppg
            })

            fold_df["error"] = fold_df["pred_ppg"] - fold_df["actual_ppg"]
            fold_df["fold"] = fold_i + 1

            pos_results.append(fold_df)

        if not pos_results:
            print(f"[SKIP] {pos}: no valid rolling folds")
            continue

        pos_all = pd.concat(pos_results, ignore_index=True)

        mae = mean_absolute_error(pos_all["actual_ppg"], pos_all["pred_ppg"])
        bias = pos_all["error"].mean()
        median_error = pos_all["error"].median()
        pred_std = pos_all["pred_ppg"].std()
        actual_std = pos_all["actual_ppg"].std()

        print(f"\n[ROLLING RESULTS] {pos}")
        print(f"MAE_PPG={mae:.2f}")
        print(f"BIAS={bias:.2f} median_error={median_error:.2f}")
        print(f"SPREAD pred_std={pred_std:.2f} actual_std={actual_std:.2f}")

        if pos == "QB":
            top_n = 12
        elif pos in ["RB", "WR"]:
            top_n = 24
        else:
            top_n = 12

        top_actual = pos_all.sort_values("actual_ppg", ascending=False).head(top_n).copy()
        top_actual_bias = top_actual["error"].mean()

        print(f"[TOP-END BIAS] {pos} top_{top_n}_actual mean_error={top_actual_bias:.2f}")

        print(f"\n[TOP UNDERPROJECTIONS] {pos}")
        print(pos_all.sort_values("error").head(10).to_string(index=False))

        print(f"\n[TOP OVERPROJECTIONS] {pos}")
        print(pos_all.sort_values("error", ascending=False).head(10).to_string(index=False))

        mae_pts_17 = mae * 17
        print(f"[ROLLING VAL] {pos}  MAE_PPG={mae:.2f}  MAE_17G_PTS={mae_pts_17:.2f}")

        all_results.append(pos_all)
        eval_ok[pos] = True

    if not eval_ok:
        print("[ERROR] No models trained. Check data coverage.")
        return

    # ----------------------------
    # Fit FINAL models on ALL pairs up to 2025
    # ----------------------------
    final_pairs = [(2021, 2022), (2022, 2023), (2023, 2024), (2024, 2025)]
    final_train = pd.concat(
        [build_pairs(df, features, a, b) for (a, b) in final_pairs],
        ignore_index=True
    )

    final_models = {}
    final_feature_medians = {}

    for pos in POSITIONS:
        tr = final_train[final_train["position"] == pos].copy()

        if len(tr) < 90:
            print(f"[SKIP FINAL] {pos}: not enough rows ({len(tr)})")
            continue

        X = tr[[f"{c}_x" for c in features]].copy()
        feature_medians = X.median()
        X = X.fillna(feature_medians)

        y = tr["y_ppg"].copy()

        model = RandomForestRegressor(
            n_estimators=1200,
            random_state=42,
            min_samples_leaf=1,
            max_depth=None,
            n_jobs=-1
        )

        model.fit(X, y)

        final_models[pos] = model
        final_feature_medians[pos] = feature_medians

        print(f"[FINAL FIT] {pos}: trained_rows={len(tr)}")

    if not final_models:
        print("[ERROR] No final models trained. Check data coverage.")
        return

    # ----------------------------
    # Project 2026 from 2025 features
    # ----------------------------
    df_2025 = df[df["season"] == 2025].copy()
    df_2025 = df_2025[df_2025["position"].isin(POSITIONS)].copy()

    qb_mask = (df_2025["position"] != "QB") | (df_2025["full_name"].isin(PROJECTED_STARTING_QBS))
    df_2025 = df_2025[qb_mask].copy()

    print("\n[INFO] QBs included in 2026 projection pool:")
    qb_pool = df_2025[df_2025["position"] == "QB"][["full_name", "position"]].sort_values("full_name")
    if qb_pool.empty:
        print("(none)")
    else:
        print(qb_pool.to_string(index=False))

    proj_parts = []

    for pos, model in final_models.items():
        part = df_2025[df_2025["position"] == pos].copy()
        if part.empty:
            continue

        X_2025 = part[features].copy()
        X_2025.columns = [f"{c}_x" for c in features]

        X_2025 = X_2025.fillna(final_feature_medians[pos])

        proj_ppg_2026 = model.predict(X_2025)
        proj_pts_2026 = proj_ppg_2026 * ASSUMED_GAMES_2026

        proj_parts.append(pd.DataFrame({
            "player_id": part["player_id"].values,
            "full_name": part["full_name"].values,
            "position": part["position"].values,
            "season": 2026,
            "proj_fantasy_points_ppr": proj_pts_2026
        }))

    if not proj_parts:
        print("[ERROR] No projection rows were generated.")
        return

    proj_2026 = pd.concat(proj_parts, ignore_index=True)

    print("\n[TOP 50] Projected 2026 PPR fantasy points (PPG * 17):")
    print(
        proj_2026.sort_values("proj_fantasy_points_ppr", ascending=False)
                 .head(50)
                 .to_string(index=False)
    )

    upsert_projections_ppr(proj_2026)


if __name__ == "__main__":
    main()