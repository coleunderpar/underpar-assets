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
    
    # backfill = False # Use this later for backfill option at runtime
    year = datetime.now().year
    
    url = f"http://site.api.espn.com/apis/site/v2/sports/golf/pga/tourschedule?season={year}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        seasons = data['seasons']
        events = []

        # All seasons are returned, but only provided season has events populated
        for season in seasons:
            if season["year"] == year:
                events = season['events']
        
        # Flatten json
        df = pd.json_normalize(events)

        expected_columns = [
            "id", "label", "detail", "startDate", "endDate", 
            "link", "isMajor", "locations", "status", "fullStatus.type.id", 
            "fullStatus.type.name", "fullStatus.type.state", "fullStatus.type.completed", 
            "fullStatus.type.description", "winner.competitors.id", "winner.competitors.uid", 
            "winner.competitors.guid", "winner.competitors.firstName", "winner.competitors.lastName", 
            "winner.competitors.fullName", "winner.competitors.displayName", "winner.competitors.shortName", 
            "winner.competitors.citizenship", "winner.competitors.link", "winner.competitors.birthPlace.city", 
            "winner.competitors.birthPlace.country", "winner.competitors.birthPlace.countryAbbreviation", 
            "winner.competitors.flag.href", "winner.competitors.flag.alt", "winner.competitors.flag.rel", 
            "winner.competitors.earnings", "winner.competitors.score.$ref", "winner.competitors.score.value", 
            "winner.competitors.score.displayValue", "winner.competitors.score.completedRoundsValue", 
            "winner.competitors.score.completedRoundsDisplayValue", "winner.competitors.birthPlace.state", 
            "winner.competitors.birthPlace.stateAbbreviation", "winner.displayName", "winner.roster", 
            "defendingChampion.id", "defendingChampion.uid", "defendingChampion.guid", "defendingChampion.firstName", 
            "defendingChampion.lastName", "defendingChampion.fullName", "defendingChampion.displayName", 
            "defendingChampion.shortName", "defendingChampion.citizenship", "defendingChampion.link", 
            "defendingChampion.birthPlace.city", "defendingChampion.birthPlace.state", 
            "defendingChampion.birthPlace.stateAbbreviation", "defendingChampion.birthPlace.country", 
            "defendingChampion.birthPlace.countryAbbreviation", "defendingChampion.flag.href", 
            "defendingChampion.flag.alt", "defendingChampion.flag.rel", "purse.value", "purse.displayValue" 
        ]

        # Adding missing columns with NaN values if not present in the response
        for col in expected_columns:
            if col not in df.columns:
                df[col] = pd.NA

        # Reorder the columns to match the expected schema
        df = df[expected_columns]

        # Cast columns to the correct data types
        df["id"] = df['id'].astype('string')
        df["label"] = df['label'].astype('string')
        df["detail"] = df['detail'].astype('string')
        df["startDate"] = df['startDate'].astype('string')
        df["endDate"] = df['endDate'].astype('string')
        df["link"] = df['link'].astype('string')
        df["isMajor"] = df['isMajor'].astype(bool)
        df["locations"] = df["locations"] # List of strings, some tournaments have multiple courses
        df["status"] = df['status'].astype('string')
        df["fullStatus.type.id"] = df['fullStatus.type.id'].astype('string')
        df["fullStatus.type.name"] = df['fullStatus.type.name'].astype('string')
        df["fullStatus.type.state"] = df['fullStatus.type.state'].astype('string')
        df["fullStatus.type.completed"] = df['fullStatus.type.completed'].astype(bool)
        df["fullStatus.type.description"] = df['fullStatus.type.description'].astype('string')
        df["winner.competitors.id"] = df['winner.competitors.id'].astype('string')
        df["winner.competitors.uid"] = df['winner.competitors.uid'].astype('string')
        df["winner.competitors.guid"] = df['winner.competitors.guid'].astype('string')
        df["winner.competitors.firstName"] = df['winner.competitors.firstName'].astype('string')
        df["winner.competitors.lastName"] = df['winner.competitors.lastName'].astype('string')
        df["winner.competitors.fullName"] = df['winner.competitors.fullName'].astype('string')
        df["winner.competitors.displayName"] = df['winner.competitors.displayName'].astype('string')
        df["winner.competitors.shortName"] = df['winner.competitors.shortName'].astype('string')
        df["winner.competitors.citizenship"] = df['winner.competitors.citizenship'].astype('string')
        df["winner.competitors.link"] = df['winner.competitors.link'].astype('string')
        df["winner.competitors.birthPlace.city"] = df['winner.competitors.birthPlace.city'].astype('string')
        df["winner.competitors.birthPlace.country"] = df['winner.competitors.birthPlace.country'].astype('string')
        df["winner.competitors.birthPlace.countryAbbreviation"] = df['winner.competitors.birthPlace.countryAbbreviation'].astype('string')
        df["winner.competitors.flag.href"] = df['winner.competitors.flag.href'].astype('string')
        df["winner.competitors.flag.alt"] = df['winner.competitors.flag.alt'].astype('string')
        df["winner.competitors.flag.rel"] = df["winner.competitors.flag.rel"] # List of strings
        df["winner.competitors.earnings"] = pd.to_numeric(df['winner.competitors.earnings'], errors='coerce')
        df["winner.competitors.score.$ref"] =df['winner.competitors.score.$ref'].astype('string')
        df["winner.competitors.score.value"] = pd.to_numeric(df['winner.competitors.score.value'], errors='coerce')
        df["winner.competitors.score.displayValue"] = df['winner.competitors.score.displayValue'].astype('string')
        df["winner.competitors.score.completedRoundsValue"] = df['winner.competitors.score.completedRoundsValue'].astype('string')
        df["winner.competitors.score.completedRoundsDisplayValue"] = pd.to_numeric(df['winner.competitors.score.completedRoundsDisplayValue'], errors='coerce')
        df["winner.competitors.birthPlace.state"] = df['winner.competitors.birthPlace.state'].astype('string')
        df["winner.competitors.birthPlace.stateAbbreviation"] = df['winner.competitors.birthPlace.stateAbbreviation'].astype('string')
        df["winner.displayName"] = df['winner.displayName'].astype('string')
        df["winner.roster"] = df['winner.roster'].apply(json.dumps)
        df["defendingChampion.id"] = df['defendingChampion.id'].astype('string')
        df["defendingChampion.uid"] = df['defendingChampion.uid'].astype('string')
        df["defendingChampion.guid"] = df['defendingChampion.guid'].astype('string')
        df["defendingChampion.firstName"] = df['defendingChampion.firstName'].astype('string')
        df["defendingChampion.lastName"] = df['defendingChampion.lastName'].astype('string')
        df["defendingChampion.fullName"] = df['defendingChampion.fullName'].astype('string')
        df["defendingChampion.displayName"] = df['defendingChampion.displayName'].astype('string')
        df["defendingChampion.shortName"] = df['defendingChampion.shortName'].astype('string')
        df["defendingChampion.citizenship"] = df['defendingChampion.citizenship'].astype('string')
        df["defendingChampion.link"] = df['defendingChampion.link'].astype('string')
        df["defendingChampion.birthPlace.city"] = df['defendingChampion.birthPlace.city'].astype('string')
        df["defendingChampion.birthPlace.state"] = df['defendingChampion.birthPlace.state'].astype('string')
        df["defendingChampion.birthPlace.stateAbbreviation"] = df['defendingChampion.birthPlace.stateAbbreviation'].astype('string')
        df["defendingChampion.birthPlace.country"] = df['defendingChampion.birthPlace.country'].astype('string')
        df["defendingChampion.birthPlace.countryAbbreviation"] = df['defendingChampion.birthPlace.countryAbbreviation'].astype('string')
        df["defendingChampion.flag.href"] = df['defendingChampion.flag.href'].astype('string')
        df["defendingChampion.flag.alt"] = df['defendingChampion.flag.alt'].astype('string')
        df["defendingChampion.flag.rel"] = df["defendingChampion.flag.rel"] # List of strings
        df["purse.value"] = pd.to_numeric(df['purse.value'], errors='coerce')
        df["purse.displayValue"]  = df['purse.displayValue'].astype('string')

        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching tournament leaderboard: {e}")
        return