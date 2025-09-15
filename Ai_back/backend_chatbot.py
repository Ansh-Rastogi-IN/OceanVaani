# backend_chatbot.py

import sqlite3
from geopy.distance import geodesic

# ------------------- Database connection -------------------
DB_FILE = "synthetic_argo_1M_upsampled_cleaned.db"  # Path to your SQLite DB
TABLE_NAME = "synthetic_argo_final"  # Use your actual table name

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# ------------------- Load all unique locations -------------------
def get_all_locations():
    """
    Returns a list of tuples: (latitude, longitude)
    """
    try:
        cursor.execute(f"SELECT DISTINCT latitude, longitude FROM {TABLE_NAME}")
        results = cursor.fetchall()
        return results
    except sqlite3.OperationalError as e:
        print(f"Database error: {str(e)}")
        return []

ALL_LOCATIONS = get_all_locations()
print(f"Loaded {len(ALL_LOCATIONS)} unique locations from the database.")

# ------------------- Query function -------------------
def query_database(param, year, target_coords):
    """
    param: string, column to query (e.g., 'temperature', 'salinity')
    year: int or string, e.g., 2010
    target_coords: tuple, (latitude, longitude)
    """
    try:
        sql = f"""
        SELECT latitude, longitude, {param}, date
        FROM {TABLE_NAME}
        WHERE strftime('%Y', date) = ?
        """
        cursor.execute(sql, (str(year),))
        results = cursor.fetchall()
        print(f"[DEBUG] Fetched {len(results)} rows for {param} in {year}")

        if not results:
            return f"No data found for {param} in {year}."

        # Find closest point to given coordinates
        closest = min(
            results,
            key=lambda x: geodesic((x[0], x[1]), target_coords).km
        )

        lat, lon, value, date = closest
        distance = geodesic((lat, lon), target_coords).km

        return (
            f"{param.capitalize()} near ({target_coords[0]}, {target_coords[1]}) in {year}:\n"
            f"Value: {value}\n"
            f"Location: ({lat}, {lon})\n"
            f"Date: {date}\n"
            f"Distance from target: {distance:.2f} km"
        )

    except sqlite3.OperationalError as e:
        return f"Database error: {str(e)}"

# ------------------- City name lookup -------------------
def get_city_coords(city_name):
    """
    Attempts to find the closest match for a city name
    by querying your dataset's latitude/longitude.
    """
    city_name = city_name.lower()
    # Predefined city coordinates (can expand later)
    predefined = {
        "chennai": (13.0827, 80.2707),
        "mumbai": (19.0760, 72.8777),
        "kolkata": (22.5726, 88.3639),
        "bangalore": (12.9716, 77.5946)
    }
    return predefined.get(city_name)

# ------------------- CLI for testing -------------------
if __name__ == "__main__":
    while True:
        user_input = input("Enter query (param, year, city) or 'exit': ")
        if user_input.lower() == "exit":
            break

        try:
            param, year, city_name = [x.strip() for x in user_input.split(",")]
            city_coords = get_city_coords(city_name)

            if not city_coords:
                print(f"City '{city_name}' not recognized. Try: Chennai, Mumbai, Kolkata, Bangalore")
                continue

            print("[DEBUG] Query:", user_input)
            print("[DEBUG] Parsed ->", f"param={param}, year={year}, city={city_name}, coords={city_coords}")

            result = query_database(param, year, city_coords)
            print(result)

        except ValueError:
            print("Invalid input format. Use: param, year, city")
