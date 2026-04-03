# Fantasy Football Projection System

Machine learning pipeline to project NFL fantasy football performance using multi-season player statistics, advanced metrics, and team-level context.

## Overview

This project forecasts next-season fantasy football production by combining:

- player box-score statistics
- advanced usage metrics
- team offensive context
- time-based validation

The model predicts fantasy points per game and converts them into full-season projections.

## Tech Stack

- Python
- MySQL
- pandas, numpy
- scikit-learn (Random Forest)

## Pipeline

1. Collect player and team data via API
2. Store and organize data in MySQL
3. Engineer predictive features:
   - per-game stats
   - rolling averages
   - red-zone usage
   - team share metrics (target share, rush share)
4. Train position-specific models (QB, RB, WR, TE)
5. Evaluate using rolling time-based validation
6. Generate future-season projections

## Key Features

- Team-share features:
  - target_share_team
  - rush_share_team
- Rolling validation (simulates real forecasting)
- Position-specific modeling
- Integration of player + team data

## Results (Rolling Validation)

- QB: MAE ≈ 2.4 PPG
- RB: MAE ≈ 3.0 PPG
- WR: MAE ≈ 2.6 PPG
- TE: MAE ≈ 2.1 PPG

## Notes

- Adding team-share features improved RB and WR predictions
- Model still slightly underprojects elite players due to regression-to-mean behavior
- Rolling validation used instead of random splits for realism

## Example Output

Sample projections are available in:

outputs/sample_top50_2026.csv

This file contains the top 50 projected players for the 2026 season based on the model.

## Database Schema

See `sql/schema.sql` for the structure of player, team, and projection data used in the model.

## How to Run

1. Install dependencies:
   pip install -r requirements.txt

2. Set up a MySQL database and update connection credentials in the script

3. Run the model:
   python models/rf_train_and_project_2026.py

## What I Learned

- How to build an end-to-end machine learning pipeline using real-world sports data  
- How feature selection and data leakage impact model performance  
- Why time-based validation is critical for forecasting problems  
- How team context and usage share improve player-level predictions  

## Future Improvements

- Two-stage modeling (predict usage → fantasy points)
- Better handling of top-end player ceilings
- Frontend for rankings display