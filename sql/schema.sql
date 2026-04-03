-- Simplified database schema for fantasy football projection system

-- Players table
CREATE TABLE players (
    player_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    position VARCHAR(10),
    team_id INT,
    active BOOLEAN
);

-- Teams table
CREATE TABLE teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(50)
);

-- Player season stats
CREATE TABLE player_season_stats (
    player_id INT,
    season INT,
    games_played INT,

    pass_attempts INT,
    pass_completions INT,
    pass_yards INT,
    pass_tds INT,
    interceptions INT,

    rush_attempts INT,
    rush_yards INT,
    rush_tds INT,

    targets INT,
    receptions INT,
    rec_yards INT,
    rec_tds INT,

    fantasy_points_ppr FLOAT,
    fantasy_points_half FLOAT,
    fantasy_points_std FLOAT,

    PRIMARY KEY (player_id, season)
);

-- Team season stats
CREATE TABLE team_season_stats (
    team_id INT,
    season INT,
    pass_attempts INT,
    rush_attempts INT,
    points_scored INT,
    offensive_plays INT,

    PRIMARY KEY (team_id, season)
);

-- Projections table
CREATE TABLE projections (
    player_id INT,
    season INT,
    scoring_type VARCHAR(10),
    projected_points FLOAT,

    PRIMARY KEY (player_id, season, scoring_type)
);

-- Note:
-- Additional advanced metrics (usage rates, efficiency stats, team share features)
-- are included in the full dataset but omitted here for clarity.