import sqlite3
import requests
import csv

DB_NAME = "ClimateAndCovid.db"

def getClimateData():
    api_key = 'saffroncrane12'
    email = 'becklau@umich.edu'
    start_date = '20200101'
    end_date = '20201231'
    parameter_code = '88101'

    state_code_mi = '26'
    state_code_oh = '39'

    county_codes_mi = []
    with open("Michigan Cases - Sheet1.csv", 'r') as file:
        reader = csv.reader(file)
        next(reader)
            
        for row in reader:
            county_codes_mi.append((row[0], row[1]))
        
    county_codes_oh = []
    with open("Ohio Cases - Sheet1.csv", 'r') as file:
        reader = csv.reader(file)
        next(reader)
            
        for row in reader:
            county_codes_oh.append((row[0], row[1]))

    all_data = []

    for county_name, county_code in county_codes_mi:

        url = f"https://aqs.epa.gov/data/api/annualData/byCounty?email={email}&key={api_key}&param={parameter_code}&bdate={start_date}&edate={end_date}&state={state_code_mi}&county={county_code}"        

        response = requests.get(url)

        if response.status_code == 200:
                data = response.json()
                if data['Header'][0]['status'] == "No data matched your selection":
                     all_data.append((county_name, 0))
                else:
                     all_data.append(data)
        else:
            print(f"Error fetching data for MI, county {county_code}: {response.status_code}")
            print(response.text)

    for county_name, county_code in county_codes_oh:

        url = f"https://aqs.epa.gov/data/api/annualData/byCounty?email={email}&key={api_key}&param={parameter_code}&bdate={start_date}&edate={end_date}&state={state_code_oh}&county={county_code}"        

        response = requests.get(url)

        if response.status_code == 200:
                data = response.json()
                if data['Header'][0]['status'] == "No data matched your selection":
                     all_data.append((county_name, 0))
                else:
                     all_data.append(data)
        else:
            print(f"Error fetching data for OH, county {county_code}: {response.status_code}")
            print(response.text)

    return all_data


def prettyData(data):
     
    pretty_data = []
    for county in data:
          
        if type(county) == tuple:
            pretty_data.append(county)
        else:
            recent_standards_dict = county["Data"][-1]
            county_name = recent_standards_dict["county"]
            pm25 = recent_standards_dict["observation_count"] * recent_standards_dict["arithmetic_mean"]
            pretty_data.append((county_name, pm25))

    pretty_data = sorted(pretty_data, key=lambda x: x[0])

    return pretty_data


def setupClimateDatabase():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ClimateData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pm25 FLOAT
        )
    ''')
    conn.commit()
    conn.close()


def insertClimateDataBatch(data, batch_size=25):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ClimateData")
    current_count = cursor.fetchone()[0]
    start_index = current_count
    end_index = min(start_index + batch_size, len(data))
    for county in data[start_index:end_index]:
        pm25_value = county[1] 
        cursor.execute('''
            INSERT INTO ClimateData (pm25)
            VALUES (?)
        ''', (pm25_value,))

    conn.commit()
    conn.close()

    print(f"Inserted {end_index - start_index} records into the climate database.")
    

def main():
   data = getClimateData() 
   pretty = prettyData(data)
   setupClimateDatabase()
   insertClimateDataBatch(pretty, batch_size=25)

if __name__ == "__main__":
    main()

