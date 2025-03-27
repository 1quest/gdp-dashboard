import streamlit as st
import pandas as pd
import math
from pathlib import Path
import psycopg2
import requests
from bs4 import BeautifulSoup
import re

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

#------------------------------------------------------------------------------
# Declare some useful parameters
url_booli_uppsala_kommun = 'https://www.booli.se/sok/till-salu?areaIds=1116&objectType=Villa&maxListPrice=7000000&minRooms=3.5'
url_booli_home = 'https://www.booli.se'

#------------------------------------------------------------------------------
# Declare some useful methods for scraping

def booli_find_number_of_pages_data(url):
    request = requests.get(url)
    soup = BeautifulSoup(request.text, 'lxml')
    data = soup.find_all('p', class_='m-2')
    # Regular expression to match the last number inside <!-- -->
    pattern = r'<!-- -->(\d+)<\/p>]'

    # Find all matches
    matches = re.findall(pattern, str(data))

    if matches:
        # Extract the last match and get the number
        last_number = matches[-1]
    else:
        print("No matches found")
        last_number = 0
    return int(last_number)

def booli_scrape_links(url, pages):
    hrefs = []
    for i in range(1, pages + 1):
        url_loop = f"{url}&page={i}"
        try:
            # Send a GET request to the URL
            response = requests.get(url_loop)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

            # Parse the response content with BeautifulSoup
            soup = BeautifulSoup(response.text, 'lxml')

            # Select all links with the specific class and href containing '/annons/'
            links = soup.select("a.expanded-link.no-underline.hover\\:underline[href*='/']")

            # Extract the href values from the link elements and append to the list
            hrefs.extend([link['href'] for link in links])

        except requests.RequestException as e:
            print(f"An error occurred on page {i}: {e}")
            continue  # Continue to the next page even if there's an error on the current page

    return hrefs

def booli_scrape_objects(links):
    listings = []
    for j, row in enumerate(links):
        # Compile the listing-url
        url_loop = url_booli_home + links[j]
        # Send a GET request to the URL
        response = requests.get(url_loop)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        soup = BeautifulSoup(response.text, 'lxml')
        print("URL: " + url_loop)

        # Find the span element with class 'heading-2'
        price_span = soup.find('span', class_='heading-2')

        if price_span:
            # Extract the text content and remove the 'kr' part
            price_text = price_span.get_text(strip=True).replace(u'\xa0', u'').replace('kr', '')
        try:
            int(price_text)
        except:
            price_text = '-999999'

        # Find the p element with the specific class containing the desired price
        booli_price = soup.find('p', class_='heading-5 whitespace-nowrap first-letter:uppercase tabular-nums lining-nums')

        if booli_price:
            # Extract the text content and remove the ' kr' part
            booli_price = booli_price.get_text(strip=True).split(' ')[0].replace(u'\xa0', u'').replace('kr', '')
        else:
            booli_price = '-999999'

        # Find the ul element with the housing details
        details_soup = soup.find('ul', class_='flex flex-wrap gap-y-4 gap-x-8 sm:gap-x-12 flex flex-wrap mt-6')

        # Find all <li> elements within the <ul>
        li_elements = details_soup.select('ul.flex > li')

        # Extract the desired values safely
        boarea = safe_extract(li_elements, 0, 'm²')
        rum = safe_extract(li_elements, 1)
        biarea = safe_extract(li_elements, 2, 'm²')
        tomtstorlek = safe_extract(li_elements, 3, 'm²')
        byggar = safe_extract(li_elements, 4)

        # Find the p element with the specific class containing the desired price
        utgangspris = soup.find('span', class_='text-sm text-content-secondary mt-2')

        # Regex pattern to extract text between > and <, excluding the brackets
        pattern = r'>([^<]+)<'

        # Find all matches
        bostadstyp, omrade, stad = re.findall(pattern, str(utgangspris))[0].split(' · ')

        listing = RealEstateListing(booli_price, boarea, rum, biarea, tomtstorlek, byggar, price_text, bostadstyp, omrade, stad, price_text, url_loop)
        listings.append(listing)

    return listings


# -----------------------------------------------------------------------------
# Declare some useful functions for database connection.

@st.cache_data
def db_save_dummy_row():
    """Connect to DB and create the table, as well as a dummy-row
    """

    # Instead of a CSV on disk, you could read from an HTTP endpoint here too.
    connection = connect_to_db()
    create_table(connection)

    # Create dummy listing
    listing = RealEstateListing(1000000, 120, 4, 20, 500, 1990, 950000, "Villa", "Norrmalm", "Stockholm", "1,000,000 SEK", "http://example.com")
    listing.store_in_db(connection)
    return listing

def fetch_all_rows(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM real_estate_listings")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)

# -----------------------------------------------------------------------------
# Declare some useful functions for the app.

def scrape_booli():
    pages = booli_find_number_of_pages_data(url_booli_uppsala_kommun)
    return pages

# -----------------------------------------------------------------------------
# Draw the actual page

# Set the title that appears at the top of the page.
'''
# :earth_americas: GDP dashboard 
'''

# Add some spacing
''

st.header('GDP over time', divider='gray')

''
''
# Add a button to the page that runs the save_csv method
if st.button('Save CSV'):
    db_save_dummy_row()

# Add a button to the page that runs the scraping-method
if st.button('Scrape again'):
    pages = scrape_booli()
    st.write("Returned " + str(pages) + " results.")

''
''

st.header(f'Current listings in Database', divider='gray')

''

connection = connect_to_db()

if connection:
    if st.button("Fetch All Listings"):
        df = fetch_all_rows(connection)
        if not df.empty:
            st.write("Top 5 Listings:")
            st.dataframe(df.head(5))
        else:
            st.write("No listings found.")
    connection.close()

cols = st.columns(4)
