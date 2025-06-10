import streamlit as st
import pandas as pd
import psycopg2
import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
import os

# -------------------- Default parameters --------------------
url_booli_uppsala_kommun = 'https://www.booli.se/sok/till-salu?areaIds=1116,116744,116741,885508,116747,9338,116704&maxListPrice=7000000&minRooms=3.5&objectType=Villa'
url_booli_home = 'https://www.booli.se'

# -------------------- Define RealEstateClass ----------------
class RealEstateListing:
    def __init__(self, booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad,
                 price_text, url, scrape_date=None, rating_aleks=None, rating_bae=None, already_seen=False):
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
        self.scrape_date = scrape_date if scrape_date else datetime.now().strftime('%Y-%m-%d')
        self.rating_aleks = rating_aleks
        self.rating_bae = rating_bae

    def __repr__(self):
        return (f"RealEstateListing(booli_price={self.booli_price}, boarea={self.boarea}, rum={self.rum}, "
                f"biarea={self.biarea}, tomtstorlek={self.tomtstorlek}, byggar={self.byggar}, "
                f"utgangspris={self.utgangspris}, bostadstyp={self.bostadstyp}, omrade={self.omrade}, "
                f"stad={self.stad}, price_text={self.price_text}, url={self.url}, scrape_date={self.scrape_date}, "
                f"rating_aleks={self.rating_aleks}, rating_bae={self.rating_bae}, already_seen={self.already_seen})")

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
            try:
                cursor.execute("""
                    INSERT INTO real_estate_listings (booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris, bostadstyp, omrade, stad, price_text, url, scrape_date, rating_aleks, rating_bae, already_seen)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                """, (
                    booli_price, boarea, self.rum, biarea, tomtstorlek, self.byggar, utgangspris, self.bostadstyp,
                    self.omrade, self.stad, self.price_text, self.url, self.scrape_date, self.rating_aleks, self.rating_bae,
                    self.already_seen))
                connection.commit()
            except Exception as e:
                st.error(f"Error storing the listing in the database: {e}")
                raise

    def update_in_db(self, connection):
        booli_price = self.try_convert_to_float(self.booli_price)
        boarea = self.try_convert_to_float(self.boarea)
        biarea = self.try_convert_to_float(self.biarea)
        tomtstorlek = self.try_convert_to_float(self.tomtstorlek)
        utgangspris = self.try_convert_to_float(self.utgangspris)

        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                    UPDATE real_estate_listings
                    SET booli_price = %s, boarea = %s, rum = %s, biarea = %s, tomtstorlek = %s, byggar = %s, utgangspris = %s, bostadstyp = %s, omrade = %s, stad = %s, price_text = %s, scrape_date = %s, rating_aleks = %s, rating_bae = %s, already_seen = %s
                    WHERE url = %s
                """, (
                    booli_price, boarea, self.rum, biarea, tomtstorlek, self.byggar, utgangspris, self.bostadstyp,
                    self.omrade, self.stad, self.price_text, self.scrape_date, self.rating_aleks, self.rating_bae,
                    self.already_seen, self.url))
                connection.commit()
            except Exception as e:
                st.error(f"Error updating the listing in the database: {e}")
                raise

# -------------------- Supporting Functions --------------------
def format_price(value):
    try:
        return f"{int(float(value)):,}".replace(",", " ") + " kr"
    except (ValueError, TypeError):
        return value

def safe_extract(li_elements, index, suffix=''):
    try:
        return li_elements[index].find('p').get_text(strip=True).replace(suffix, '').replace(u'\xa0', u'').replace(
            'rum', '').strip().replace(',', '.')
    except IndexError:
        return None

# -------------------- Database Functions --------------------
def connect_to_db():
    try:
        return psycopg2.connect(
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"]
        )
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

def fetch_unrated_listings(connection, user):
    rating_column = "rating_bae" if user == "Cecilia" else "rating_aleks"
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris,
                   bostadstyp, omrade, stad, price_text, url
            FROM real_estate_listings
            WHERE {rating_column} IS NULL
            ORDER BY scrape_date DESC
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)

def fetch_seen_rated(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT SUM(CASE WHEN rating_bae is null THEN 1 ELSE 0 END) as unrated_bae,
            SUM(CASE WHEN rating_aleks is null THEN 1 ELSE 0 END) as unrated_aleks,
            COUNT(*) as total
            FROM real_estate_listings 
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)

def mark_seen(connection, url, liked, user):
    rating = 10 if liked else 0
    column = "rating_bae" if user == "Cecilia" else "rating_aleks"
    with connection.cursor() as cursor:
        cursor.execute(f"""
            UPDATE real_estate_listings
            SET already_seen = true, {column} = %s
            WHERE url = %s
        """, (rating, url))
        connection.commit()

# -------------------- Booli Scraper --------------------
def booli_find_number_of_pages_data(url):
    request = requests.get(url)
    soup = BeautifulSoup(request.text, 'lxml')
    data = soup.find_all('p', class_='m-2')
    pattern = r'<!-- -->(\d+)<\/p>'
    matches = re.findall(pattern, str(data))
    return int(matches[-1]) if matches else 0

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
        try:
            url_loop = url_booli_home + links[j]
            response = requests.get(url_loop)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'lxml')
            print("URL: " + url_loop)

            price_span = soup.find('span', class_='heading-2')
            price_text = price_span.get_text(strip=True).replace(u'\xa0', u'').replace('kr', '') if price_span else None

            booli_price = soup.find('p', class_='heading-5 whitespace-nowrap first-letter:uppercase tabular-nums lining-nums')
            booli_price = booli_price.get_text(strip=True).split(' ')[0].replace(u'\xa0', u'').replace('kr', '') if booli_price else None

            details_soup = soup.find('ul', class_='flex flex-wrap gap-y-4 gap-x-8 sm:gap-x-12 flex flex-wrap mt-6')
            li_elements = details_soup.select('ul.flex > li')

            boarea = safe_extract(li_elements, 0, 'm²')
            rum = safe_extract(li_elements, 1)
            biarea = safe_extract(li_elements, 2, 'm²')
            tomtstorlek = safe_extract(li_elements, 3, 'm²')
            byggar = safe_extract(li_elements, 4)

            utgangspris = soup.find('span', class_='text-sm text-content-secondary mt-2')
            pattern = r'>([^<]+)<'
            parts = re.findall(pattern, str(utgangspris))[0].split(' · ')
            bostadstyp, omrade, stad = (parts + [''] * 3)[:3]

            listing = RealEstateListing(booli_price, boarea, rum, biarea, tomtstorlek, byggar, price_text, bostadstyp,
                                        omrade, stad, price_text, url_loop)
            listings.append(listing)
        except Exception as e:
            st.write(str(links[j]))


    return listings

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

# -------------------- Image Extraction --------------------
def extract_gallery_images(listing_url):
    try:
        response = requests.get(listing_url)
        response.raise_for_status()
        image_ids = re.findall(r'"Image:(\d+)"', response.text)
        image_urls = [f"https://bcdn.se/cache/{img_id}_1200x900.jpg" for img_id in image_ids]
        return list(dict.fromkeys(image_urls))
    except Exception as e:
        st.warning(f"Could not load images: {e}")
        return []

# -------------------- UI State Init --------------------
st.set_page_config(
    page_title="Swipe Your Next Home",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("\U0001F3E0 Swipe Your Next Home")

if "user_name" not in st.session_state:
    st.session_state.user_name = None
if "listing_index" not in st.session_state:
    st.session_state.listing_index = 0
    st.session_state.listings = pd.DataFrame()
if "image_index" not in st.session_state:
    st.session_state.image_index = 0
if "image_cache" not in st.session_state:
    st.session_state.image_cache = {}
if "show_top_matches" not in st.session_state:
    st.session_state.show_top_matches = False
if "show_swiping" not in st.session_state:
    st.session_state.show_swiping = True
if "confirm_scrape" not in st.session_state:
    st.session_state.confirm_scrape = False

# -------------------- User Selection Page --------------------
if not st.session_state.user_name:
    connection = connect_to_db()
    rated_df = fetch_seen_rated(connection)
    st.write("Who are you?")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Cecilia"):
            st.session_state.user_name = "Cecilia"
            st.rerun()
        st.info(f"Remaining listings: {rated_df.unrated_bae.iloc[0]}")
    with col2:
        if st.button("Aleksandar"):
            st.session_state.user_name = "Aleksandar"
            st.rerun()
        st.info(f"Remaining listings: {rated_df.unrated_aleks.iloc[0]}")
    st.stop()

# -------------------- Streamlit menu --------------------
st.sidebar.title("Menu")
if st.sidebar.button("Scrape new listings"):
    st.session_state.confirm_scrape = True
if st.sidebar.button("Top Matches ❤️🔥"):
    st.session_state.show_top_matches = True
    st.session_state.show_swiping = False
    st.rerun()

# -------------------- Confirmation UI --------------------
if st.session_state.confirm_scrape:
    st.sidebar.warning("Are you sure you want to scrape new listings?")
    col1, col2 = st.sidebar.columns(2)

    with col1:
        if st.button("Yes", key="confirm_yes"):
            pages = scrape_booli()
            st.sidebar.success(f"Scraped listings from {pages} pages.")
            st.session_state.confirm_scrape = False

    with col2:
        if st.button("No", key="confirm_no"):
            st.session_state.confirm_scrape = False
            st.rerun()

# -------------------- Swiping page --------------------
if st.session_state.show_swiping:
    # -------------------- Load Listings Button --------------------
    if st.button("\U0001F504 Load Listings"):
        conn = connect_to_db()
        if conn:
            df = fetch_unrated_listings(conn, st.session_state.user_name)
            conn.close()
            if not df.empty:
                st.session_state.listings = df
                st.session_state.listing_index = 0
                st.session_state.image_index = 0
                st.session_state.image_cache = {}
            else:
                st.warning("No new listings to rate.")

    # -------------------- Show One Listing --------------------
    if not st.session_state.listings.empty:
        index = st.session_state.listing_index
        listings = st.session_state.listings

        if index < len(listings):
            listing = listings.iloc[index]
            listing_url = listing["url"]
            st.subheader(f"{listing['bostadstyp']} in {listing['omrade']}, {listing['stad']}")

            if index + 1 < len(listings):
                next_url = listings.iloc[index + 1]["url"]
                if next_url not in st.session_state.image_cache:
                    st.session_state.image_cache[next_url] = extract_gallery_images(next_url)

            if listing_url not in st.session_state.image_cache:
                image_urls = extract_gallery_images(listing_url)
                st.session_state.image_cache[listing_url] = image_urls
            else:
                image_urls = st.session_state.image_cache[listing_url]

            if image_urls:
                if st.session_state.image_index >= len(image_urls):
                    st.session_state.image_index = 0

                current = st.session_state.image_index
                total = len(image_urls)

                st.image(image_urls[current], use_container_width=True)
                st.caption(f"\U0001F4F8 Image {current + 1} / {total}")

                col_prev, col_next = st.columns([1, 1])
                with col_prev:
                    if st.button("\u2B05\uFE0F Previous Image"):
                        st.session_state.image_index = max(0, current - 1)
                        st.rerun()
                with col_next:
                    if st.button("\u27A1\uFE0F Next Image"):
                        st.session_state.image_index = min(total - 1, current + 1)
                        st.rerun()
            else:
                st.write("No image found.")

            # Show listing info...
            st.write(f"**Price:** {format_price(listing['price_text'])}")
            st.write(f"**Booli estimate:** {format_price(listing['booli_price'])}")
            st.write(f"**Living Area:** {listing['boarea']} m²")
            st.write(f"**Rooms:** {listing['rum']}")
            st.write(f"**Plot Size:** {listing['tomtstorlek']} m²")
            st.write(f"**Year Built:** {listing['byggar']}")
            st.write(f"[\U0001F517 View Listing]({listing['url']})")

            # Buttons inside columns
            col1, col2 = st.columns(2)
            with col1:
                dislike_pressed = st.button("👎 Dislike", key="dislike")
            with col2:
                like_pressed = st.button("❤️ Like", key="like")

            # Handle button presses outside layout
            if dislike_pressed:
                conn = connect_to_db()
                if conn:
                    mark_seen(conn, listing['url'], liked=False, user=st.session_state.user_name)
                    conn.close()
                    st.session_state.listing_index += 1
                    st.session_state.image_index = 0
                    st.rerun()

            if like_pressed:
                conn = connect_to_db()
                if conn:
                    mark_seen(conn, listing['url'], liked=True, user=st.session_state.user_name)
                    conn.close()
                    st.session_state.listing_index += 1
                    st.session_state.image_index = 0
                    st.rerun()
            else:
                st.success("You've rated all listings! ✅")
    else:
        st.info("Click 'Load Listings' to begin swiping.")
# -------------------- Top Matches Page --------------------
if st.session_state.show_top_matches:
    if st.button("🔙 Back to Listings"):
        st.session_state.show_top_matches = False
        st.session_state.show_swiping = True
        st.rerun()

    conn = connect_to_db()
    if conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris,
                       bostadstyp, omrade, stad, price_text, url, scrape_date
                FROM real_estate_listings
                WHERE rating_aleks = '10' AND rating_bae = '10'
                ORDER BY scrape_date DESC
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
        conn.close()

        st.subheader("🏆 Listings liked by both Cecilia and Aleksandar (10/10)")

        if df.empty:
            st.info("No shared top-rated listings found yet.")
        else:
            df_display = df.copy()
            df_display["price_text"] = df_display["price_text"].apply(format_price)
            df_display["booli_price"] = df_display["booli_price"].apply(format_price)
            df_display = df_display.sort_values("scrape_date", ascending=False)

            st.data_editor(
                df_display[[
                    "scrape_date", "omrade", "price_text", "booli_price", "url"
                ]],
                column_config={
                    "url": st.column_config.LinkColumn("Listing", disabled=True)
                },
                use_container_width=True,
                hide_index=True,
            )