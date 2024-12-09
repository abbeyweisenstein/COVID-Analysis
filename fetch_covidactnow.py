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
        filtered_data = filtered_data[['county', 'actuals.cases']]
        filtered_data.rename(columns={'actuals.cases': 'cases'}, inplace=True)
        return filtered_data
    
    else:
        raise Exception(f"Error fetching data: {response.status_code}, {response.text}")

def save_to_database(dataframe, db_name, batch_size=25):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table with two columns: county and cases
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS covid_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        county TEXT,
        cases INTEGER
    )
    ''')

    cursor.execute("SELECT COUNT(*) FROM covid_data")
    current_count = cursor.fetchone()[0]
    start_index = current_count
    end_index = min(start_index + batch_size, len(dataframe))

    if start_index >= len(dataframe):
        print("All data has already been inserted.")
        conn.close()
        return

    data_batch = dataframe.iloc[start_index:end_index]
    for _, row in data_batch.iterrows():
        cursor.execute('''
        INSERT INTO covid_data (county, cases)
        VALUES (?, ?)
        ''', (row['county'], row['cases']))

    conn.commit()
    conn.close()

    print(f"Inserted {end_index - start_index} records into the covid_data database.")

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
