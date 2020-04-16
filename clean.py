
import re
import sqlite3
import time


DB = sqlite3.connect("craigslist_apts.db")
C = DB.cursor()

C.execute("""
SELECT
    post_id,
    link,
    price,
    description,
    n_images,
    post_time,
    title,
    placename,
    latlon,
    location,
    housing,
    attrs
FROM cl_apts_tmp
WHERE link NOT IN (SELECT link FROM cl_apts);""")

def add_to_db(post):
    c = DB.cursor()
    try:
        c.execute("""
        INSERT INTO cl_apts (
            post_id,
            link,
            price,
            description,
            n_images,
            post_time,
            title,
            placename,
            lat,
            lon,
            location,
            housing,
            attrs,
            sqft,
            beds,
            dogs_ok,
            cats_ok
        ) VALUES (
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?
        );""", 
            post
        )
    except:
        DB.rollback()
        raise
    else:
        DB.commit()
    pass


for post in C.fetchall():
    ##### Extract the values #####
    (
        post_id,
        link,
        price,
        description,
        n_images,
        post_time,
        title,
        placename,
        latlon,
        location,
        housing,
        attrs
    ) = post
    ##### Now convert them #####
    # post_id
    if post_id is not None:
        post_id = re.sub(r"post id: ","",post_id)
    else:
        try:
            post_id = re.search(r"/(\d+).html$",link)[1]
        except:
            pass
    # price
    if price is not None:
        try:
            price = float(re.search(r"(\d+)",price)[1])
        except:
            price = None
    # description
    if description is not None:
        # remove this line at the beginning of each description
        description = re.sub(r"QR Code Link to This Post"," ",description)
        # remove extra punctuation
        description = re.sub(r"[!?]",". ",description)
        # get rid of any extra characters
        description = re.sub(r"[^a-z0-9\s.]"," ",description,flags=re.I)
        # normalize the spaces
        description = re.sub(r"\s+"," ",description).strip().lower()
    # n_images
    if n_images is not None:
        n_images = int(n_images)
    # title
    if title is not None:
        # remove extra punctuation
        title = re.sub(r"[!?]",".",title)
        # get rid of any extra characters
        title = re.sub(r"[^a-z0-9\s.]"," ",title,flags=re.I)
        # normalize the spaces
        title = re.sub(r"\s+"," ",title).strip().lower()
    # placename
    if placename is not None:
        placename = re.sub(r"\s+"," ",placename).strip().lower()
    # lat and lon
    if latlon is not None:
        try:
            lat, lon = latlon.split(';')
            lat = float(lat)
            lon = float(lon)
        except:
            lat, lon = None, None    
    else:
        lat, lon = None, None
    # location
    if location is not None:
        location = re.sub(r"[()]","",location)
    # attrs
    if attrs is not None:
        attrs = re.sub(r"\(google map\)"," ",attrs).lower()
        attrs = re.sub(r"\s+"," ",attrs).strip()
        if "dogs are ok - wooof" in attrs:
            dogs_ok = 1
            attrs = re.sub(r"dogs are ok - wooof"," ",attrs)
            attrs = re.sub(r"\s+"," ",attrs).strip()
        else:
            dogs_ok = 0
        if "cats are ok - purrr" in attrs:
            cats_ok = 1
            attrs = re.sub(r"cats are ok - purrr"," ",attrs)
            attrs = re.sub(r"\s+"," ",attrs).strip()
        else:
            cats_ok = 0
    else:
        dogs_ok, cats_ok = None, None
    # housing
    if housing is not None:
        housing = re.sub(r"[/-]"," ",housing)
        housing = re.sub(r"\s+"," ",housing).strip().lower()
        # beds
        try:
            beds = int(re.search(r"(\d+)\s?br",housing)[1])
        except:
            beds = None
        # sqft
        try:
            sqft = int(re.search(r"(\d+)\s?ft",housing)[1])
        except:
            sqft = None
    if housing is None or beds is None:
        try:
            beds = int(re.search(r"(\d+)\s?br",attrs)[1])
        except:
            beds = None
    if housing is None or sqft is None:
        try:
            sqft = int(re.search(r"(\d+)\s?ft",attrs)[1])
        except:
            sqft = None
    


    

    ##### Add the new values #####
    cleaned_post = (
        post_id,
        link,
        price,
        description,
        n_images,
        post_time,
        title,
        placename,
        lat,
        lon,
        location,
        housing,
        attrs,
        sqft,
        beds,
        dogs_ok,
        cats_ok
    )
    add_to_db(cleaned_post)

