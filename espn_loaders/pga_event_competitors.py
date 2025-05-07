import io
import pandas as pd
import requests
import json
from datetime import datetime
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def get_season_events(*args, **kwargs):
    
    # event_id
    event_id = "401703504"
    # competition_id
    competition_id = "401703504"
    
    url = f"http://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{competition_id}/competitors?lang=en&region=us&limit=300"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        competitors = data['items']
        
        # Flatten json
        df = pd.json_normalize(competitors)

        expected_columns = [
            "$ref", "id", "uid", "type", "order", "athlete.$ref", "status.$ref", 
            "score.$ref", "linescores.$ref", "statistics.$ref", "movement", "amateur"
        ]

        # Adding missing columns with NaN values if not present in the response
        for col in expected_columns:
            if col not in df.columns:
                df[col] = pd.NA

        # Reorder the columns to match the expected schema
        df = df[expected_columns]

        # Cast columns to the correct data types
        df["$ref"] = df['$ref'].astype('string')
        df["id"] = df['id'].astype('string')
        df["uid"] = df['uid'].astype('string')
        df["type"] = df['type'].astype('string')
        df["order"] = df['order'].astype('string')
        df["athlete.$ref"] = df['athlete.$ref'].astype('string')
        df["status.$ref"] = df['status.$ref'].astype('string')
        df["score.$ref"] = df['score.$ref'].astype('string')
        df["linescores.$ref"] = df['linescores.$ref'].astype('string')
        df["statistics.$ref"] = df['statistics.$ref'].astype('string')
        df["movement"] = df['movement'].astype('string')
        df["amateur"] = df['amateur'].astype(bool)

        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tournament leaderboard: {e}")
        return