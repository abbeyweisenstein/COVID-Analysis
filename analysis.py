import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import json

DB_NAME = "ClimateAndCovid.db"

def fetch_county_names():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id, name FROM CountyNameData')
    county_names = dict(cursor.fetchall())
    conn.close()
    return county_names

def fetchPopulationDensity():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id, density FROM CountyData')
    data = cursor.fetchall()
    conn.close()
    return data

def fetchPovertyRates():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT id, poverty_rate FROM PovertyData')
    data = cursor.fetchall()
    conn.close()
    return data

def plotPopulationDensity():
    county_names = fetch_county_names()
    data = fetchPopulationDensity()
    data = sorted(data, key=lambda x: x[1], reverse=True)
    counties = [county_names[row[0]] for row in data]
    densities = [row[1] for row in data]
    
    plt.figure(figsize=(15, 10))
    plt.bar(counties, densities, color='skyblue')
    plt.xticks(rotation=90, fontsize=8)
    plt.title('Population Density by County')
    plt.xlabel('County')
    plt.ylabel('Density (people per sq. mile)')
    plt.tight_layout()
    plt.show()

def plotPovertyRates():
    county_names = fetch_county_names()
    data = fetchPovertyRates()
    data = sorted(data, key=lambda x: x[1], reverse=True)
    counties = [county_names[row[0]] for row in data]
    poverty_rates = [row[1] for row in data]
    
    plt.figure(figsize=(15, 10))
    plt.bar(counties, poverty_rates, color='green')
    plt.xticks(rotation=90, fontsize=8)
    plt.title('Poverty Rates by County')
    plt.xlabel('County')
    plt.ylabel('Poverty Rate (%)')
    plt.tight_layout()
    plt.show()

def fetchCombinedData():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    query = '''
    SELECT cd.id, cd.density, pd.poverty_rate
    FROM CountyData cd
    JOIN PovertyData pd ON cd.id = pd.id
    '''
    
    cursor.execute(query)
    combined_data = cursor.fetchall()
    conn.close()
    
    county_names = fetch_county_names()
    return [(county_names[row[0]], row[1], row[2]) for row in combined_data]

def plotDensityVsPoverty():
    data = fetchCombinedData()
    densities = [row[1] for row in data]
    poverty_rates = [row[2] for row in data]
    plt.figure(figsize=(10, 6))
    plt.scatter(densities, poverty_rates, color='purple', alpha = 0.5)
    plt.title('Population Density vs. Poverty Rate')
    plt.xlabel('Population Density (people per sq. mile)')
    plt.ylabel('Poverty Rate (%)')
    plt.tight_layout()
    plt.show()

def plotCovidandDensity():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT
        CountyData.id,
        CountyData.density,
        Covid_data.cases
    FROM
        CountyData
    JOIN
        Covid_data
    ON
        CountyData.id = Covid_data.id;
    """
    merged_data = pd.read_sql_query(query, conn)
    conn.close()
    
    county_names = fetch_county_names()
    merged_data['county_name'] = merged_data['id'].map(county_names)
    
    plt.figure(figsize=(10, 6))
    plt.scatter(merged_data['density'], merged_data['cases'], alpha=0.7, edgecolors='w', linewidth=0.5)
    plt.title('Comparison of Population Density and COVID-19 Cases by County', fontsize=14)
    plt.xlabel('Population Density (people per square mile)', fontsize=12)
    plt.ylabel('COVID-19 Cases', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()


def plotCovidandPoverty():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT 
        PovertyData.id,  
        PovertyData.poverty_rate, 
        Covid_data.cases
    FROM 
        PovertyData
    JOIN 
        Covid_data
    ON 
        PovertyData.id = Covid_data.id;
    """
    merged_data = pd.read_sql_query(query, conn)
    conn.close()

    plt.figure(figsize=(10, 6))
    plt.scatter(merged_data['poverty_rate'], merged_data['cases'], alpha=0.7, edgecolors='w', linewidth=0.5)

    plt.title('Comparison of Poverty Rates and COVID-19 Cases by County', fontsize=14)
    plt.xlabel('Poverty Rate (in %)', fontsize=12)
    plt.ylabel('COVID-19 Cases', fontsize=12)

    plt.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()

def plotCovidandEnvironmentalImpact():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT 
        ClimateData.id, 
        ClimateData.pm25, 
        Covid_data.cases
    FROM 
        ClimateData
    JOIN 
        Covid_data
    ON 
        ClimateData.id = Covid_data.id;
    """
    merged_data = pd.read_sql_query(query, conn)
    conn.close()

    plt.figure(figsize=(10, 6))
    plt.scatter(merged_data['pm25'], merged_data['cases'], alpha=0.7, edgecolors='w', linewidth=0.5)

    plt.title('Comparison of Environmental Impact and COVID-19 Cases by County', fontsize=14)
    plt.xlabel('Particulate Matter under 2.5µm (in %)', fontsize=12)
    plt.ylabel('COVID-19 Cases', fontsize=12)

    plt.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()

def plotDensityandEnvironmentalImpact():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT 
        CountyData.id, 
        ClimateData.pm25, 
        CountyData.density
    FROM 
        CountyData
    JOIN 
        ClimateData
    ON 
        CountyData.id = ClimateData.id;
    """
    merged_data = pd.read_sql_query(query, conn)
    conn.close()

    plt.figure(figsize=(10, 6))
    plt.scatter(merged_data['pm25'], merged_data['density'], alpha=0.7, edgecolors='w', linewidth=0.5)

    plt.title('Comparison of Environmental Impact and Population Density by County', fontsize=14)
    plt.xlabel('Particulate Matter under 2.5µm (in %)', fontsize=12)
    plt.ylabel('Population Density', fontsize=12)

    plt.grid(True, linestyle='--', alpha=0.6)

    plt.tight_layout()
    plt.show()

def calculate():
    """
    Processes the database to calculate average population density and poverty rates.
    Writes the results to a JSON file.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    query = """
    SELECT 
        AVG(CountyData.density) AS avg_density,
        AVG(PovertyData.poverty_rate) AS avg_poverty_rate
    FROM 
        CountyData
    JOIN 
        PovertyData
    ON 
        CountyData.id = PovertyData.id;
    """
    cursor.execute(query)
    result = cursor.fetchone()
    conn.close()

    data_to_write = {
        "Average Population Density": result[0],
        "Average Poverty Rate": result[1]
    }

    with open("processed_data.json", "w") as json_file:
        json.dump(data_to_write, json_file, indent=4)

    print("Processed data written to 'processed_data.json'")


def main():
    plotPopulationDensity()
    plotPovertyRates()
    plotDensityVsPoverty()
    plotCovidandDensity()
    plotCovidandPoverty()
    plotCovidandEnvironmentalImpact()
    plotDensityandEnvironmentalImpact()
    calculate()
    pass

if __name__ == "__main__":
    main()