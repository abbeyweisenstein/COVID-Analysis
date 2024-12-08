import sqlite3
import requests
import os

DB_NAME = "ClimateAndCovid.db"
pdDB = "PopulationDensity.db"
povertyDB = "Poverty.db"

apiKey = '93b284ef6a8d2b4b3287dd91193707b35548454d'

def getCensusData():
    # Fetch population data
    population_url_MI = ("https://api.census.gov/data/2021/acs/acs5"
                      "?get=NAME,B01003_001E&for=county:*&in=state:26&key=" + apiKey)
    population_response_MI = requests.get(population_url_MI)
    population_url_OH = ("https://api.census.gov/data/2021/acs/acs5"
                      "?get=NAME,B01003_001E&for=county:*&in=state:39&key=" + apiKey)
    population_response_OH = requests.get(population_url_OH)

    if population_response_MI.status_code == 200 and population_response_OH.status_code == 200:
        population_data_MI = population_response_MI.json()
        population_data_OH = population_response_OH.json()
        return population_data_MI, population_data_OH
    else:
        print("Error fetching data:")
        print("Population API MI:", population_response_MI.status_code, population_response_MI.text)
        print("Population API OH:", population_response_OH.status_code, population_response_OH.text)
        return [], []

def readCSV(filename):
    current_directory = os.getcwd()
    file_path = os.path.join(current_directory, filename)
    with open(file_path) as inFile:
        data = inFile.readlines()
    countyAndArea = []
    for line in data:
        countyAndArea.append(line.strip().split(','))
    cleanAreaData = []
    for pair in countyAndArea:
        newPair = pair[0].split()[:-1], int(pair[1])
        #print(newPair)
        try:
            newPair = ' '.join(newPair[0])
        except:
            continue
        cleanAreaData.append([newPair, int(pair[1])])
    return cleanAreaData

def cleanPopulationData(census):
    cleanPopData = []
    for state in census:
        #print(state[1:])
        for county in state[1:]:
            name = county[0].split()[:-2]
            #print(name)
            try:
                name = ' '.join(name)
            except:
                continue
            cleanPopData.append([name, int(county[1])])
    return cleanPopData

def popDensity(pop, area):
    countyData = []
    for index in range(len(pop)):
        #print(pop[index])
        #print(area[index])
        allDataCounty = [pop[index][0], pop[index][1], area[index][1], round(float(pop[index][1]/area[index][1]),2)]
        countyData.append(allDataCounty)
    return countyData

def getPovertyData():
    poverty_url_MI = ("https://api.census.gov/data/2021/acs/acs5"
                      "?get=NAME,B01003_001E,B17001_002E&for=county:*&in=state:26&key=" + apiKey)
    poverty_response_MI = requests.get(poverty_url_MI)

    poverty_url_OH = ("https://api.census.gov/data/2021/acs/acs5"
                      "?get=NAME,B01003_001E,B17001_002E&for=county:*&in=state:39&key=" + apiKey)
    poverty_response_OH = requests.get(poverty_url_OH)

    if poverty_response_MI.status_code == 200 and poverty_response_OH.status_code == 200:
        poverty_data_MI = poverty_response_MI.json()
        poverty_data_OH = poverty_response_OH.json()
        return poverty_data_MI, poverty_data_OH
    else:
        print("Error fetching poverty data:")
        print("Poverty API MI:", poverty_response_MI.status_code, poverty_response_MI.text)
        print("Poverty API OH:", poverty_response_OH.status_code, poverty_response_OH.text)
        return [], []
    
def cleanPovertyData(census_data):
    cleanData = []
    for state_data in census_data:
        for county in state_data[1:]:
            try:
                name = ' '.join(county[0].split()[:-2])  # Extract county name
                total_population = int(county[1])
                poverty_population = int(county[2])
                poverty_rate = round((poverty_population / total_population) * 100, 2)
                cleanData.append([name, total_population, poverty_population, poverty_rate])
            except:
                continue
    return cleanData


def setupPopulationDatabase():
    conn = sqlite3.connect(pdDB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CountyData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            population INTEGER,
            area INTEGER,
            density FLOAT
        )
    ''')
    conn.commit()
    conn.close()

def setupPovertyDatabase():
    conn = sqlite3.connect(povertyDB)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PovertyData (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            total_population INTEGER,
            poverty_population INTEGER,
            poverty_rate FLOAT
        )
    ''')
    conn.commit()
    conn.close()

def insertPopulationDataBatch(data, batch_size=25):
    conn = sqlite3.connect(pdDB)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM CountyData")
    current_count = cursor.fetchone()[0]
    start_index = current_count
    end_index = min(start_index + batch_size, len(data))
    for county in data[start_index:end_index]:
        cursor.execute('''
            INSERT INTO CountyData (name, population, area, density)
            VALUES (?, ?, ?, ?)
        ''', county)

    conn.commit()
    conn.close()

    print(f"Inserted {end_index - start_index} records into the population density database.")

def insertPovertyDataInBatch(data, batch_size=25):
    conn = sqlite3.connect(povertyDB)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM PovertyData")
    current_count = cursor.fetchone()[0]
    start_index = current_count
    end_index = min(start_index + batch_size, len(data))
    for county in data[start_index:end_index]:
        cursor.execute('''
            INSERT INTO PovertyData (name, total_population, poverty_population, poverty_rate)
            VALUES (?, ?, ?, ?)
        ''', county)

    conn.commit()
    conn.close()

    print(f"Inserted {end_index - start_index} records into the poverty database.")

def main():
    setupPopulationDatabase()
    setupPovertyDatabase()
    miAreaData = readCSV('MIArea.csv')
    ohAreaData = readCSV('OHArea.csv')
    areaData = miAreaData + ohAreaData
    popData = cleanPopulationData(getCensusData())
    countyData = popDensity(popData, areaData)
    povertyData = getPovertyData()
    povertyData = cleanPovertyData(povertyData)
    insertPopulationDataBatch(countyData, batch_size=25)
    insertPovertyDataInBatch(povertyData, batch_size=25)

if __name__ == "__main__":
    main()