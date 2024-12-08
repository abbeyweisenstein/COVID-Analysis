import sqlite3
import requests
import pandas as pd
from io import StringIO

DB_NAME = "ClimateAndCovid.db"

URL = "https://api.covidactnow.org/v2/counties.csv?apiKey=1871617b6c71446aa9234ab7a88697a0"

def fetch_covid_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = pd.read_csv(StringIO(response.text))
       
        # Filter for Michigan and Ohio counties
        filtered_data = data[(data['state'] == 'MI') | (data['state'] == 'OH')]
        filtered_data = filtered_data[['county', 'state', 'actuals.cases']]
        filtered_data.rename(columns={'actuals.cases': 'cases'}, inplace=True)
        return filtered_data
    
    else:
        raise Exception(f"Error fetching data: {response.status_code}, {response.text}")

def save_to_database(dataframe, db_name):
    with sqlite3.connect(db_name) as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS covid_data (
            county TEXT,
            state TEXT,
            cases INTEGER
        )
        ''')
        dataframe.to_sql('covid_data', conn, if_exists='replace', index=False)
        print("Data successfully inserted into the database.")

def query_database(db_name):
    """Query the SQLite database and display the data."""
    with sqlite3.connect(db_name) as conn:
        query = "SELECT * FROM covid_data"
        df = pd.read_sql(query, conn)
        print("Queried Data:")
        print(df.to_string(index=False))

def main():
    try:
        covid_data = fetch_covid_data(URL)
        print("Filtered Data:")
        print(covid_data.head())
        save_to_database(covid_data, DB_NAME)
        query_database(DB_NAME)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
