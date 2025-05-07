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
def get_event_competitor_linescores(*args, **kwargs):
    
    # event_id
    event_id = "401703504"
    # competition_id
    competition_id = "401703504"
    # competitor_id
    competitor_id = "3470"
    
    url = f"http://sports.core.api.espn.com/v2/sports/golf/leagues/pga/events/{event_id}/competitions/{competition_id}/competitors/{competitor_id}/linescores?lang=en&region=us"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        # Flatten linescores nested field
        linescores_df = pd.json_normalize(
            data["items"],
            record_path='linescores',
            meta=[
                'value',
                'displayValue',
                'period',
                'inScore',
                'outScore',
                'courseId',
                'hasStream',
                'currentPosition',
                'startTee',
                'groupNumber',
                'teeTime'
            ],
            record_prefix='linescore_',
            sep='_',
            errors="ignore"
        )

        # Reorder columns: round and hole info first, then linescore details
        meta_cols = ['value', 'displayValue', 'period', 'inScore', 'outScore', 'courseId',
                    'hasStream', 'currentPosition', 'startTee', 'groupNumber', 'teeTime']
        linescore_cols = [col for col in linescores_df.columns if col.startswith('linescore_')]
        ordered_cols = meta_cols + linescore_cols

        # Final DataFrame
        df = linescores_df[ordered_cols]

        # Write to test.json
        df.to_json('test.json', orient='records', indent=2)

        # expected_columns = [
        #     "$ref", "id", "uid", "type", "order", "athlete.$ref", "status.$ref", 
        #     "score.$ref", "linescores.$ref", "statistics.$ref", "movement", "amateur"
        # ]

        # # Adding missing columns with NaN values if not present in the response
        # for col in expected_columns:
        #     if col not in df.columns:
        #         df[col] = pd.NA

        # # Reorder the columns to match the expected schema
        # df = df[expected_columns]

        # # Cast columns to the correct data types
        # df["$ref"] = df['$ref'].astype('string')
        # df["id"] = df['id'].astype('string')
        # df["uid"] = df['uid'].astype('string')
        # df["type"] = df['type'].astype('string')
        # df["order"] = df['order'].astype('string')
        # df["athlete.$ref"] = df['athlete.$ref'].astype('string')
        # df["status.$ref"] = df['status.$ref'].astype('string')
        # df["score.$ref"] = df['score.$ref'].astype('string')
        # df["linescores.$ref"] = df['linescores.$ref'].astype('string')
        # df["statistics.$ref"] = df['statistics.$ref'].astype('string')
        # df["movement"] = df['movement'].astype('string')
        # df["amateur"] = df['amateur'].astype(bool)

        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tournament leaderboard: {e}")
        return