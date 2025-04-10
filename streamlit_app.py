import streamlit as st
import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
import re


class RealEstateListing:
    def __init__(self, booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad,
                 price_text, url, rating=None, already_seen=False):
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
        self.already_seen = already_seen
        self.url = url
        self.rating = rating

    def __repr__(self):
        return (f"RealEstateListing(booli_price={self.booli_price}, boarea={self.boarea}, rum={self.rum}, "
                f"biarea={self.biarea}, tomtstorlek={self.tomtstorlek}, byggar={self.byggar}, "
                f"utgangspris={self.utgangspris}, bostadstyp={self.bostadstyp}, omrade={self.omrade}, "
                f"stad={self.stad}, price_text={self.price_text}, url={self.url}, rating={self.rating}, already_seen={self.already_seen})")

    @staticmethod
    def try_convert_to_float(value):
        if isinstance(value, str):
            try:
                return float(value.replace(',', '.'))
            except ValueError:
                return None
        return value

    def store_in_db(self, connection):
        booli_price = self.try_convert_to_float(self.booli_price)
        boarea = self.try_convert_to_float(self.boarea)
        biarea = self.try_convert_to_float(self.biarea)
        tomtstorlek = self.try_convert_to_float(self.tomtstorlek)
        utgangspris = self.try_convert_to_float(self.utgangspris)

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO real_estate_listings (booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad, price_text, url, rating, already_seen)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            """, (
                booli_price, boarea, self.rum, biarea, tomtstorlek, self.byggar, utgangspris, self.bostadstyp,
                self.omrade, self.stad, self.price_text, self.url, self.rating, self.already_seen))
        connection.commit()

    def update_in_db(self, connection):
        booli_price = self.try_convert_to_float(self.booli_price)
        boarea = self.try_convert_to_float(self.boarea)
        biarea = self.try_convert_to_float(self.biarea)
        tomtstorlek = self.try_convert_to_float(self.tomtstorlek)
        utgangspris = self.try_convert_to_float(self.utgangspris)

        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE real_estate_listings
                SET booli_price = %s, boarea = %s, rum = %s, biarea = %s, tomtstorlek = %s, byggar = %s, utgangspris = %s, bostadstyp = %s, omrade = %s, stad = %s, price_text = %s, rating = %s, already_seen = %s
                WHERE url = %s
            """, (
                booli_price, boarea, self.rum, biarea, tomtstorlek, self.byggar, utgangspris, self.bostadstyp,
                self.omrade, self.stad, self.price_text, self.rating, self.already_seen, self.url))
        connection.commit()


def create_table(connection):
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS real_estate_listings")
        connection.commit()

        cursor.execute("""
            CREATE TABLE real_estate_listings (
                id SERIAL,
                booli_price DOUBLE PRECISION,
                boarea DOUBLE PRECISION,
                rum DOUBLE PRECISION,
                biarea DOUBLE PRECISION,
                tomtstorlek DOUBLE PRECISION,
                byggar INTEGER,
                utgangspris DOUBLE PRECISION,
                bostadstyp VARCHAR(100),
                omrade VARCHAR(100),
                stad VARCHAR(100),
                price_text VARCHAR(255),
                url TEXT PRIMARY KEY,
                rating DOUBLE PRECISION,
                already_seen BOOLEAN
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
        st.error(f"Error connecting to database: {error}")
        return None


def fetch_all_rows(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT already_seen, rating, utgangspris, booli_price, omrade, bostadstyp, url FROM real_estate_listings")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)


st.set_page_config(
    layout="wide",
    page_title='Booli dashboard',
    page_icon=':earth_americas:',
)

url_booli_uppsala_kommun = 'https://www.booli.se/sok/till-salu?areaIds=1116&objectType=Villa&maxListPrice=7000000&minRooms=3.5'
url_booli_home = 'https://www.booli.se'


def booli_find_number_of_pages_data(url):
    request = requests.get(url)
    soup = BeautifulSoup(request.text, 'lxml')
    data = soup.find_all('p', class_='m-2')
    pattern = r'<!-- -->(\d+)<\/p>'

    matches = re.findall(pattern, str(data))

    if matches:
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
            response = requests.get(url_loop)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'lxml')
            links = soup.select("a.expanded-link.no-underline.hover\\:underline[href*='/']")
            hrefs.extend([link['href'] for link in links])

        except requests.RequestException as e:
            print(f"An error occurred on page {i}: {e}")
            continue

    return hrefs


def booli_scrape_objects(links):
    listings = []
    for j, row in enumerate(links):
        url_loop = url_booli_home + links[j]
        response = requests.get(url_loop)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        print("URL: " + url_loop)

        price_span = soup.find('span', class_='heading-2')

        if price_span:
            price_text = price_span.get_text(strip=True).replace(u'\xa0', u'').replace('kr', '')
        else:
            price_text = None

        booli_price = soup.find('p', class_='heading-5 whitespace-nowrap first-letter:uppercase tabular-nums lining-nums')

        if booli_price:
            booli_price = booli_price.get_text(strip=True).split(' ')[0].replace(u'\xa0', u'').replace('kr', '')
        else:
            booli_price = None

        details_soup = soup.find('ul', class_='flex flex-wrap gap-y-4 gap-x-8 sm:gap-x-12 flex flex-wrap mt-6')
        li_elements = details_soup.select('ul.flex > li')

        boarea = safe_extract(li_elements, 0, 'm²')
        rum = safe_extract(li_elements, 1)
        biarea = safe_extract(li_elements, 2, 'm²')
        tomtstorlek = safe_extract(li_elements, 3, 'm²')
        byggar = safe_extract(li_elements, 4)

        utgangspris = soup.find('span', class_='text-sm text-content-secondary mt-2')
        pattern = r'>([^<]+)<'
        bostadstyp, omrade, stad = re.findall(pattern, str(utgangspris))[0].split(' · ')

        listing = RealEstateListing(booli_price, boarea, rum, biarea, tomtstorlek, byggar, price_text, bostadstyp,
                                    omrade, stad, price_text, url_loop)
        listings.append(listing)

    return listings


@st.cache_data
def db_recreate_table():
    connection = connect_to_db()
    if connection:
        create_table(connection)
        connection.close()
    return True


def update_already_seen_in_db(connection, url, already_seen):
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE real_estate_listings
            SET already_seen = %s
            WHERE url = %s
        """, (already_seen, url))
        connection.commit()


def update_all_rows_in_db(connection, df):
    with connection.cursor() as cursor:
        for index, row in df.iterrows():
            cursor.execute("""
                UPDATE real_estate_listings
                SET already_seen = %s, rating = %s
                WHERE url = %s
            """, (row['already_seen'], row['rating'], row['url']))
        connection.commit()


def safe_extract(li_elements, index, suffix=''):
    try:
        return li_elements[index].find('p').get_text(strip=True).replace(suffix, '').replace(u'\xa0', u'').replace(
            'rum', '').strip().replace(',', '.')
    except IndexError:
        return None


def scrape_booli():
    connection = connect_to_db()
    if connection:
        pages = booli_find_number_of_pages_data(url_booli_uppsala_kommun)
        links = booli_scrape_links(url_booli_uppsala_kommun, pages)
        listings = booli_scrape_objects(links)
        for listing in listings:
            listing.store_in_db(connection)
        connection.close()
        return pages
    return 0


def validate_and_clean_df(df):
    for index, row in df.iterrows():
        rating = row['rating']
        if pd.notnull(rating):
            try:
                rating = int(rating)
                if not (0 <= rating <= 10):
                    return False, f"Rating for row {index} is out of bounds. It should be between 0 and 10."
                df.at[index, 'rating'] = rating
            except ValueError:
                return False, f"Rating for row {index} is not a valid number."
        df.at[index, 'rating'] = rating  # Ensure it's cleaned
    return True, None


st.title(':earth_americas: Booli Analysis for Uppsala')

st.header('Booli listings in Uppsala below 7M SEK', divider='gray')

# if st.button('Drop and Create table'):
#    if db_recreate_table():
#        st.success("Table recreated successfully.")
#    else:
#        st.error("Failed to recreate the table.")

if st.button('Scrape again'):
    pages = scrape_booli()
    st.write(f"Returned {pages} results.")

st.header('Current listings in Database', divider='gray')

if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = pd.DataFrame()
    st.session_state.filter_columns = []

if st.button("Fetch All Listings"):
    connection = connect_to_db()
    if connection:
        df = fetch_all_rows(connection)
        connection.close()
        if df is not None and not df.empty:
            st.session_state.df = df
            st.session_state.data_loaded = True
            st.session_state.filter_columns = df.columns.tolist()

if st.session_state.data_loaded:
    df = st.session_state.df
    st.session_state.filter_columns = df.columns.tolist()

    if st.session_state.filter_columns:
        filtered_columns = st.session_state.filter_columns.copy()
        if 'already_seen' in filtered_columns:
            filtered_columns.remove('already_seen')
            filtered_columns.remove('rating')
            edited_df = st.data_editor(df[st.session_state.filter_columns],
                                       column_config={
                                           "url": st.column_config.LinkColumn()
                                       },
                                       disabled=filtered_columns
                                       )

            if st.button('Update Database'):
                valid, error_message = validate_and_clean_df(edited_df)
                if valid:
                    with st.spinner('Saving...'):
                        connection = connect_to_db()
                        if connection:
                            update_all_rows_in_db(connection, edited_df)
                            connection.close()
                            st.success("Database updated successfully.")
                        else:
                            st.error("Failed to update the database.")
                else:
                    st.error(error_message)
        else:
            st.write("No listings found.")