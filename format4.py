import pandas as pd
from bs4 import BeautifulSoup
from requests import get, Session
import wikipedia as wiki
import pyarrow.feather as feather
import os
import sys
from env import user
from time import time
import time
import concurrent.futures
import threading
import re
from acquire import HEADERS

def is_body(element):
    c1 = element.name == 'font'
    c2 = element.get('color') == '#666666'
    c3 = element.get('size') == '2'
    c4 = element.get('face') == 'Arial'
    c5 = 'Colour: ' in element.text

    return c1 and c2 and c3 and c4 and c5

def rating(soup):
    rating_soup = soup.find_next('strong').find_next('strong')
    if rating_soup:
        return rating_soup.text
    else:
        return None

def data(soup):
    body_soups = soup.find_all(lambda x: is_body(x))
    data = []
    for body_soup in body_soups:
        data.append({'content': body_soup.text,
                     'title': body_soup.find_next('font', color='#333333').text,
                     'date': body_soup.find_previous('font', color='#660000', face='Arial').text,
                     'rating': rating(body_soup)})
    return data

def scrape_page(archive_url):
    print(f'scraping {archive_url}')
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(archive_url).content, 'html.parser')

    page = pd.DataFrame(data(soup))

    page['author'] = 'Serge'

    feather.write_feather(page, f'data/{archive_url[25:-5]}.feather')