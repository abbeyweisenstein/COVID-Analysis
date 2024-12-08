import sqlite3
import requests

DB_NAME = "ClimateAndCovid.db"

import pandas as pd
from io import StringIO

url = "https://api.covidactnow.org/v2/counties.csv?apiKey=1871617b6c71446aa9234ab7a88697a0"

response = requests.get(url)

if response.status_code == 200:
    
    data = pd.read_csv(pd.compat.StringIO(response.text))
    
    # Get MI and OH counties
    filtered_data = data[(data['state'] == 'MI') | (data['state'] == 'OH')]
    
    filtered_data = filtered_data[['county', 'state', 'actuals.cases']]
    filtered_data.cases(columns={'actuals.cases': 'cases'}, inplace=True)

    print("Filtered Data:")
    print(filtered_data.head())
else:
    print(f"Error fetching data: {response.status_code}, {response.text}")


conn = sqlite3.connect("ClimateAndCovid.db")
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS covid_data (
    county TEXT,
    state TEXT,
    cases INTEGER
)
''')

filtered_data.to_sql('covid_data', conn, if_exists='replace', index=False)

conn.commit()
conn.close()

print("Data insert success.")



def main():
    pass

if __name__ == "__main__":
    main()