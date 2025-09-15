# ai_test_enhanced.py

from backend_chatbot import query_database, ALL_LOCATIONS
import sqlite3
from geopy.distance import geodesic

DB_FILE = "synthetic_argo_1M_upsampled_cleaned.db"
TABLE_NAME = "synthetic_argo_final"

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# ------------------- Natural language parser -------------------
def parse_input(user_input):
    """
    Extract param, year, and city from user input
    """
    user_input = user_input.lower()
    param_list = ["temperature", "salinity", "oxygen"]

    param = next((p for p in param_list if p in user_input), None)
    year = next((word for word in user_input.split() if word.isdigit() and len(word) == 4), None)
    
    # Extract city from known city list
    city_list = ["chennai", "mumbai", "kolkata", "bangalore"]
    city = next((c for c in city_list if c in user_input), None)

    return param, year, city

# ------------------- Dynamic city lookup -------------------
PREDEFINED_CITIES = {
    "chennai": (13.0827, 80.2707),
    "mumbai": (19.0760, 72.8777),
    "kolkata": (22.5726, 88.3639),
    "bangalore": (12.9716, 77.5946)
}

def get_city_coords(city_name):
    """
    Returns coordinates for a city
    """
    if not city_name:
        return None
    city_name = city_name.lower()
    return PREDEFINED_CITIES.get(city_name)

# ------------------- Find closest year if data missing -------------------
def get_closest_year(param, year):
    """
    Returns the closest year in the dataset for a given param
    """
    cursor.execute(f"SELECT DISTINCT strftime('%Y', date) FROM {TABLE_NAME} WHERE {param} IS NOT NULL")
    years = [int(y[0]) for y in cursor.fetchall()]
    if not years:
        return None
    year = int(year)
    closest = min(years, key=lambda y: abs(y - year))
    return closest

# ------------------- Main AI loop -------------------
if __name__ == "__main__":
    while True:
        user_input = input("Ask your AI: ")
        if user_input.lower() == "exit":
            break

        param, year, city = parse_input(user_input)
        if not param:
            print("Sorry, I could not understand the parameter.")
            continue

        city_coords = get_city_coords(city)
        if not city_coords:
            print(f"City '{city}' not recognized. Using closest ARGO location instead.")
            # Pick the first location from ALL_LOCATIONS as fallback
            city_coords = ALL_LOCATIONS[0]

        # Try fetching data for requested year
        result = query_database(param, year, city_coords)

        # If no data, find closest year
        if "No data found" in result:
            closest_year = get_closest_year(param, year)
            if closest_year:
                print(f"No data for {param} in {year}. Using closest available year: {closest_year}")
                result = query_database(param, closest_year, city_coords)
            else:
                print(f"No data available at all for {param}.")
                continue

        print(result)
