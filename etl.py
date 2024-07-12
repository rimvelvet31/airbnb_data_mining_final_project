import sqlite3
import pandas as pd
from utils import parse_checkout, parse_checkin


def etl_pipeline():
    # EXTRACT RAW DATA FROM CSV
    airbnb = pd.read_csv("data/airbnb.csv", index_col=0)

    # TRANSFORM AND CLEAN
    # Drop columns with missing values
    airbnb = airbnb.dropna()

    # Drop irrelevant columns
    drop_cols = ['features', 'amenities',
                 'img_links', 'hourse_rules', 'safety_rules']
    airbnb = airbnb.drop(columns=drop_cols)

    # Correct spelling: "toiles" to "toilets"
    airbnb.rename(columns={'toiles': 'toilets'}, inplace=True)

    # Impute rating values of "New" in with mean rating (rounded to 2 decimal places)
    airbnb['rating'] = pd.to_numeric(airbnb['rating'], errors='coerce')
    mean_rating = round(airbnb['rating'].mean(), 2)
    airbnb['rating'] = airbnb['rating'].fillna(mean_rating)

    # Ensure that these columns have the correct data type
    airbnb['reviews'] = airbnb['reviews'].str.replace(',', '').astype(int)
    airbnb['host_id'] = airbnb['host_id'].astype(int)
    airbnb['price'] = airbnb['price'].astype(float)

    # Rename price to price_local
    airbnb.rename(columns={'price': 'price_local'}, inplace=True)

    # Convert checkout to time
    airbnb['checkout'] = airbnb['checkout'].apply(lambda x: parse_checkout(x))

    # Convert checkin to datetime
    airbnb['checkin'] = airbnb['checkin'].apply(lambda x: parse_checkin(x))

    # LOAD TO SQLITE DB
    conn = sqlite3.connect("data/airbnb.db")
    cur = conn.cursor()

    # Create tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hosts (
            host_id INTEGER PRIMARY KEY,
            host_name TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            country_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_name TEXT NOT NULL UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            rating REAL,
            reviews INTEGER,
            host_id INTEGER,
            address TEXT,
            price_local REAL,
            country_id INTEGER,
            bathrooms INTEGER,
            beds INTEGER,
            guests INTEGER,
            toilets INTEGER,
            bedrooms INTEGER,
            studios INTEGER,
            checkin TEXT,
            checkout TEXT,
            FOREIGN KEY (host_id) REFERENCES hosts(host_id),
            FOREIGN KEY (country_id) REFERENCES countries(country_id)
        );
    """)

    # Insert data into hosts table
    if not data_exists_in_table('hosts'):
        for index, row in airbnb[['host_id', 'host_name']].drop_duplicates().iterrows():
            cur.execute("INSERT INTO hosts (host_id, host_name) VALUES (?, ?)",
                        (row['host_id'], row['host_name']))

    # Insert data into countries table
    if not data_exists_in_table('countries'):
        for index, row in airbnb[['country']].drop_duplicates().iterrows():
            cur.execute(
                "INSERT INTO countries (country_name) VALUES (?)", (row['country'],))

    # Insert data into properties table
    if not data_exists_in_table('properties'):
        for index, row in airbnb.iterrows():
            cur.execute("""
                INSERT INTO properties (
                    id, name, rating, reviews, host_id, address, price_local, country_id,
                    bathrooms, beds, guests, toilets, bedrooms, studios, checkin, checkout
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            row['id'], row['name'], row['rating'], row['reviews'], row['host_id'],
                            row['address'], row['price_local'], row['country'], row['bathrooms'],
                            row['beds'], row['guests'], row['toilets'], row['bedrooms'], row['studios'],
                            str(row['checkin']), str(row['checkout'])
                        )
                        )

    # Commit changes and close connection
    conn.commit()
    conn.close()


def data_exists_in_table(table_name):
    with sqlite3.connect("data/airbnb.db") as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        return count > 0
