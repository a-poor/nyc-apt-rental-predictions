
import re
import sqlite3
import time
import json

import requests
from bs4 import BeautifulSoup


page_url = lambda n=-1: f"https://newyork.craigslist.org/search/apa?s={n}"
make_url = lambda i=-1, min=0, max=1000: f"https://newyork.craigslist.org/search/apa?s={i}&bundleDuplicates=1&max_price={max}&min_price={min}"
make_url2 = lambda i, minprice, maxprice, minsqft, maxsqft: (
    f"https://newyork.craigslist.org/search/apa?s={i}&availabilityMode=0&bundleDuplicates=1&maxSqft={maxsqft}&max_price={maxprice}&minSqft={minsqft}&min_price={minprice}"
    )


price_start_ends = tuple((i*500, (i+1)*500) for i in range(12))
sqft_start_ends = tuple((i*200, (i+1)*200) for i in range(8))

links = []
for start_s, end_s in sqft_start_ends:
    for start_p, end_p in price_start_ends:
        for i in range(30):
            links.append(
                make_url2(
                    i*120,
                    start_p,
                    end_p,
                    start_s,
                    end_s
                )
            )



DB = sqlite3.connect("craigslist_apts.db")
C = DB.cursor()
C.execute("""
CREATE TABLE IF NOT EXISTS "cl_links" (
    link TEXT PRIMARY KEY,
    visited INT DEFAULT 0
);
""")
C.execute("""
CREATE TABLE IF NOT EXISTS "cl_apts_tmp" (
    post_id TEXT,
    link TEXT,
    price TEXT,
    description TEXT,
    n_images TEXT,
    post_time TEXT,
    title TEXT,
    placename TEXT,
    latlon TEXT,
    location TEXT,
    housing TEXT,
    attrs TEXT
);
""")
del C

def get_page_urls(n=5,sleep_time=0.1,links=None):
    links_added = {
            'success':0,
            'error':0
        }
    if links is None:
        link_iter = [page_url(i*120) for i in range(n)]
    else:
        link_iter = links

    for url in link_iter:
        resp = requests.get(url)
        soup = BeautifulSoup(resp.content,'lxml')
        rows = soup.find('ul',attrs={'class':'rows'}).find_all("li")
        link_tags = [r.find('a',attrs={'class':'result-title'}) for r in rows]
        links = [a.attrs['href'] for a in link_tags]
        c = DB.cursor()
        for l in links:
            c.execute("SELECT COUNT(*) FROM cl_links WHERE link = ?;",(l,))
            if c.fetchone()[0] > 0: continue
            try:
                c.execute(
                    "INSERT INTO cl_links (link,visited) VALUES (?,0)",
                    (l,)
                    )
            except:
                links_added['error'] += 1
                DB.rollback()
                raise
            else:
                DB.commit()
                links_added['success'] += 1
        time.sleep(sleep_time)
    print("Link scraping completed.")
    print("# of successes:",links_added['success'])
    print("# of errors:   ",links_added['error'])
    pass

def parse_apt_page(url):
    ### Get the page ###
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content,'lxml')
    #### Extract the values ###
    # post_id
    post_id = soup.find(text=re.compile(r"post id:"))
    post_id = str(post_id) if post_id is not None else None
    # post_time
    try:
        post_time = soup.find('time',attrs={'class':'timeago'})
        post_time = post_time.text.strip()
    except:
        post_time = None
    # post_description
    try:
        post_description = soup.find(attrs={'id':'postingbody'}).text.replace("\n"," ").strip()
    except:
        try:
            post_description = soup.find(attrs={'id':'postingbody'})
            if post_description is not None:
                post_description = str(post_description)
        except: 
            post_description = None
    # placename
    placename = soup.find('meta',attrs={'name':'geo.placename'})
    try:
        placename = placename.attrs['content']
    except:
        placename = None
    # latlon
    latlon = soup.find('meta',attrs={'name':'geo.position'})
    try:
        latlon = latlon.attrs['content']
    except:
        latlon = None
    # title
    title = soup.find('meta',attrs={'property':'og:title'})
    try:
        title = title.attrs['content']
    except:
        title = None
    # n_photos
    n_photos = len(soup.find_all(attrs={'class':'slide'}))
    # price
    price = soup.find('span',attrs={'class':'price'})
    try:
        price = price.text
    except:
        if price is not None:
            price = str(price)
    # location
    try:
        location = soup.find(attrs={'class':'postingtitle'}).find('small')
    except:
        location = None
    else:
        try:
            location = location.text
        except:
            if location is not None:
                location = str(location)
    # housing
    housing = soup.find(attrs={'class':'housing'})
    try:
        housing = housing.text
    except:
        if housing is not None:
            housing = str(housing)
    # attrs
    try:
        attrs = soup.find(attrs={'class':'mapAndAttrs'}).find_all('p')
    except:
        attrs = None
    else:
        try:
            attrs = re.sub(r"\s+"," "," ".join(t.text for t in attrs)).strip()
        except:
            if attrs is not None:
                attrs = str(attrs)

    ### Add to the database ###
    c = DB.cursor()
    try:
        c.execute("""
            INSERT INTO cl_apts_tmp (
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
            ) VALUES (
                ?,?,?,?,
                ?,?,?,?,
                ?,?,?,?
            );""",(
                post_id,
                url,
                price,
                post_description,
                n_photos,
                post_time,
                title,
                placename,
                latlon,
                location,
                housing,
                attrs
        ))
    except:
        DB.rollback()
        return False
    else:
        DB.commit()
        return True


def run(n_pages=10,sleep_time=0.5,scrape_links=True,scrape_apts=True,links=None):
    if scrape_links:
        print("Adding urls...")
        try:
            get_page_urls(n_pages, sleep_time=sleep_time, links=links)
        except Exception as e:
            print(e)
        print("Link scraping complete.")
    else:
        print("Skipping link scraping.")
    if scrape_apts:
        c = DB.cursor()
        c.execute("SELECT link FROM cl_links WHERE visited = 0 AND link NOT IN (SELECT link FROM cl_apts_tmp);")
        links = c.fetchall()
        for i, l in enumerate([link[0] for link in links]):
            if i % 10 == 0: print(f"Scraping page {i} of {len(links)}")
            parse_apt_page(l)
            c.execute("UPDATE cl_links SET visited = 1 WHERE link = ?;",(l,))
            DB.commit()
            time.sleep(sleep_time)
        print("Done scraping apartment pages.")
    else:
        print("Skipping apt scraping.")
    pass


if __name__ == '__main__':
    run(links=links)
