import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

DB_NAME = "ClimateAndCovid.db"

def fetchPopulationDensity():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT name, density FROM CountyData')
    data = cursor.fetchall()
    conn.close()
    return data

def fetchPovertyRates():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT name, poverty_rate FROM PovertyData')
    data = cursor.fetchall()
    conn.close()
    return data

def plotPopulationDensity():
    data = fetchPopulationDensity()
    data = sorted(data, key=lambda x: x[1], reverse=True)
    counties = [row[0] for row in data]
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
    data = fetchPovertyRates()
    data = sorted(data, key=lambda x: x[1], reverse=True)
    counties = [row[0] for row in data]
    poverty_rates = [row[1] for row in data]
    plt.figure(figsize=(15, 10))  # Adjust figure size
    plt.bar(counties, poverty_rates, color='green')
    plt.xticks(rotation=90, fontsize=8)  # Rotate labels to fit all counties
    plt.title('Poverty Rates by County')
    plt.xlabel('County')
    plt.ylabel('Poverty Rate (%)')
    plt.tight_layout()
    plt.show()

def fetchCombinedData():
    conn_pd = sqlite3.connect(DB_NAME)
    conn_poverty = sqlite3.connect(DB_NAME)
    query_pd = 'SELECT name, density FROM CountyData'
    query_poverty = 'SELECT name, poverty_rate FROM PovertyData'
    cursor_pd = conn_pd.cursor()
    cursor_poverty = conn_poverty.cursor()
    cursor_pd.execute(query_pd)
    population_density_data = cursor_pd.fetchall()
    cursor_poverty.execute(query_poverty)
    poverty_rate_data = cursor_poverty.fetchall()
    combined_data = []
    for pd_row in population_density_data:
        for poverty_row in poverty_rate_data:
            if pd_row[0] == poverty_row[0]:
                combined_data.append((pd_row[0], pd_row[1], poverty_row[1]))
    conn_pd.close()
    conn_poverty.close()
    return combined_data

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

def main():
    plotPopulationDensity()
    plotPovertyRates()
    plotDensityVsPoverty()
    pass

if __name__ == "__main__":
    main()