import streamlit as st
import pandas as pd
import psycopg2
import requests
import re

# -------------------- Supporting Functions --------------------
def format_price(value):
    try:
        return f"{int(value):,}".replace(",", " ") + " kr"
    except (ValueError, TypeError):
        return value


# -------------------- Database Functions --------------------

def connect_to_db():
    try:
        return psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="MjoQCReJhNxvGZQ7",
            host="caustically-usable-dinosaur.data-1.use1.tembo.io",
            port="5432"
        )
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

def fetch_unrated_listings(connection):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT booli_price, boarea, rum, biarea, tomtstorlek, byggar, utgangspris,
                   bostadstyp, omrade, stad, price_text, url
            FROM real_estate_listings
            WHERE already_seen = false OR already_seen IS NULL
            ORDER BY scrape_date DESC
        """)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(rows, columns=columns)

def mark_seen(connection, url, liked):
    rating = 10 if liked else 0
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE real_estate_listings
            SET already_seen = true, rating_aleks = %s
            WHERE url = %s
        """, (rating, url))
        connection.commit()

# -------------------- Image Extraction --------------------

def extract_gallery_images(listing_url):
    try:
        response = requests.get(listing_url)
        response.raise_for_status()
        image_ids = re.findall(r'"Image:(\d+)"', response.text)
        image_urls = [f"https://bcdn.se/cache/{img_id}_1200x900.jpg" for img_id in image_ids]
        return list(dict.fromkeys(image_urls))  # Remove duplicates
    except Exception as e:
        st.warning(f"Could not load images: {e}")
        return []

# -------------------- UI State Init --------------------

st.set_page_config(page_title="Swipe Your Next Home", layout="wide")
st.title("🏠 Swipe Your Next Home")

if "listing_index" not in st.session_state:
    st.session_state.listing_index = 0
    st.session_state.listings = pd.DataFrame()

if "image_index" not in st.session_state:
    st.session_state.image_index = 0

if "image_cache" not in st.session_state:
    st.session_state.image_cache = {}

# -------------------- Load Listings Button --------------------

if st.button("🔄 Load Listings"):
    conn = connect_to_db()
    if conn:
        df = fetch_unrated_listings(conn)
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
        listing_url = listing['url']
        st.subheader(f"{listing['bostadstyp']} in {listing['omrade']}, {listing['stad']}")

        # Cache and load images
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
            st.caption(f"📸 Image {current + 1} / {total}")

            col_prev, col_next = st.columns([1, 1])
            with col_prev:
                if st.button("⬅️ Previous Image"):
                    st.session_state.image_index = max(0, current - 1)
            with col_next:
                if st.button("➡️ Next Image"):
                    st.session_state.image_index = min(total - 1, current + 1)
        else:
            st.write("No images found.")

        # Show listing metadata
        st.write(f"**Price:** {format_price(listing['price_text'])}")
        st.write(f"**Booli estimate:** {format_price(listing['booli_price'])}")
        st.write(f"**Living Area:** {listing['boarea']} m²")
        st.write(f"**Rooms:** {listing['rum']}")
        st.write(f"**Plot Size:** {listing['tomtstorlek']} m²")
        st.write(f"**Year Built:** {listing['byggar']}")
        st.write(f"[🔗 View Listing]({listing['url']})")

        # Like/Dislike buttons
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("👎 Dislike", key="dislike"):
                conn = connect_to_db()
                if conn:
                    mark_seen(conn, listing['url'], liked=False)
                    conn.close()
                    st.session_state.listing_index += 1
                    st.session_state.image_index = 0
                    st.rerun()

        with col2:
            if st.button("❤️ Like", key="like"):
                conn = connect_to_db()
                if conn:
                    mark_seen(conn, listing['url'], liked=True)
                    conn.close()
                    st.session_state.listing_index += 1
                    st.session_state.image_index = 0
                    st.rerun()
    else:
        st.success("You've rated all listings! ✅")
else:
    st.info("Click 'Load Listings' to begin swiping.")
