import streamlit as st
import pandas as pd
import math
from pathlib import Path
import psycopg2

class RealEstateListing:
    def __init__(self, booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad, price_text, url):
        self.booli_price = booli_price
        self.boarea = boarea
        self.rum = rum
        self.biarea = biarea
        self.tomtstorlek = tomtstorlek
        self.byggar = byggar
        self.utgangspris = utgangspris
        self.bostadstyp = bostadstyp
        self.omrade = omrade
        self.stad = stad
        self.price_text = price_text
        self.url = url

    def __repr__(self):
        return (f"RealEstateListing(booli_price={self.booli_price}, boarea={self.boarea}, rum={self.rum}, "
                f"biarea={self.biarea}, tomtstorlek={self.tomtstorlek}, byggar={self.byggar}, "
                f"utgangspris={self.utgangspris}, bostadstyp={self.bostadstyp}, omrade={self.omrade}, "
                f"stad={self.stad}, price_text={self.price_text}, url={self.url})")

    def store_in_db(self, connection):
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO real_estate_listings (booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad, price_text, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (self.booli_price, self.boarea, self.rum, self.biarea, self.tomtstorlek, self.byggar, self.utgangspris, self.bostadstyp, self.omrade, self.stad, self.price_text, self.url))
        connection.commit()

def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS real_estate_listings (
                id SERIAL PRIMARY KEY,
                booli_price FLOAT,
                boarea FLOAT,
                rum INTEGER,
                biarea FLOAT,
                tomtstorlek FLOAT,
                byggar INTEGER,
                utgangspris FLOAT,
                bostadstyp VARCHAR(100),
                omrade VARCHAR(100),
                stad VARCHAR(100),
                price_text VARCHAR(255),
                url TEXT
            )
        """)
    connection.commit()

def connect_to_db():
    try:
        connection = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="MjoQCReJhNxvGZQ7",
            host="caustically-usable-dinosaur.data-1.use1.tembo.io",
            port="5432"
        )
        return connection
    except Exception as error:
        print(f"Error connecting to database: {error}")
        return None

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='GDP dashboard',
    page_icon=':earth_americas:', # This is an emoji shortcode. Could be a URL too.
)

# -----------------------------------------------------------------------------
# Declare some useful functions.

@st.cache_data
def get_gdp_data():
    """Connect to DB and create the table, as well as a dummy-row
    """

    # Instead of a CSV on disk, you could read from an HTTP endpoint here too.
    connection = connect_to_db()
    create_table(connection)

    # Create dummy listing
    listing = RealEstateListing(1000000, 120, 4, 20, 500, 1990, 950000, "Villa", "Norrmalm", "Stockholm", "1,000,000 SEK", "http://example.com")
    listing.store_in_db(connection)
    return listing
 

# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.
'''
# :earth_americas: GDP dashboard

Browse GDP data from the [World Bank Open Data](https://data.worldbank.org/) website. As you'll
notice, the data only goes to 2022 right now, and datapoints for certain years are often missing.
But it's otherwise a great (and did I mention _free_?) source of data.
'''

# Add some spacing
''

st.header('GDP over time', divider='gray')

''
''
# Add a button to the page that runs the save_csv method
if st.button('Save CSV'):
    get_gdp_data()
''
''

st.header(f'GDP in 2022', divider='gray')

''

cols = st.columns(4)
