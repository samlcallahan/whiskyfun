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

HEADERS = {'User-Agent': user}

URL = "http://www.whiskyfun.com/"

thread_local = threading.local()

def get_session():
    '''
    if the current thread doesn't have a requests Session object, creates one and updates the headers
    '''

    if not hasattr(thread_local, "session"):
        thread_local.session = Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session

def make_full_link(link_url):
    '''
    Checks if url is full url or url stub. converts to full url if not already.
    '''
    if link_url[:4] != 'http':
        link_url = URL + link_url
    return link_url

def archive_list(main_url):
    '''
    Gets a list of each archive page from the main site
    '''
    session = get_session()

    soup = BeautifulSoup(session.get(URL).content)
    
    right_fonts = soup.find_all('font', color='#666666', size='2', face='Arial')

    links = []

    for html_blob in right_fonts:
        if html_blob.find('a'):
            links.append(html_blob.find('a')['href'])

    archives = []

    for link in links:
        if "archive" in link.lower() or 'feisile' in link.lower() or link.lower() == 'special.html':
            archives.append(link)

    archives = pd.Series(archives)

    archives = archives.apply(make_full_link)

    return archives

def is_date(soup_element):
    if soup_element.name == 'font' and soup_element.get('color') == '#660000' and soup_element.get('size') == '3':
        return True
    else:
        return False

def is_title(soup_element):
    if soup_element.name == 'span' and soup_element.get('class') == ['textegrandfoncegras'] and '(' in soup_element.text:
        return True
    else:
        return False

def titles_and_dates(page_soup):
    titles = []
    date_soups = page_soup.find_all('font', color='#660000', size='3')

    dates = [date_soup.text for date_soup in date_soups]

    mess = page_soup.find_all(['font', 'span'])

    cleaned = [i for i in mess if is_title(i) or is_date(i)]
    for element in cleaned:
        if element.name == 'font':
            date = element.text
        else:
            titles.append({'title': element.text, 'date': date})
    return titles

def angus_list(page_soup):
    angus_sections = page_soup.find_all('table', width='498')

    angus = []

    for section in angus_sections:
        angus_soups = section.find_all('span', class_='textegrandfoncegras')
        angus.extend([title.text for title in angus_soups if '(' in title.text])

    return angus

def author_by_title(title, angus_list):
    if title in angus_list:
        return 'Angus'
    
    else:
        return 'Serge'
    
def ratings_list(page_soup):
    rating_soups = page_soup.find_all('span', class_='textenormalgras')

    ratings = [rating.text.strip() for rating in rating_soups if ' points.' in rating.text]
    return ratings

def contents_list(page_soup):
    content_soups = page_soup.find_all('span', class_='TextenormalNEW')

    contents = [content.text for content in content_soups if 'Colour' in content.text]

    return contents

def scrape_page(archive_url):
    print(f'scraping {archive_url}')
    session = get_session()

    soup = BeautifulSoup(session.get(archive_url).content)

    page = pd.DataFrame(titles_and_dates(soup))

    angus_titles = angus_list(soup)

    authors = []

    for title in page['title']:
        if title in angus_titles:
            authors.append('Angus')
        else:
            authors.append('Serge')
    
    page['author'] = page['title'].apply(lambda x: author_by_title(x, angus_titles))

    page['rating'] = ratings_list(soup)

    page['content'] = contents_list(soup)

    feather.write_feather(df, f'data/{archive_url[25:-5]}.feather')

def all_pages(archives):
    for archive in archives:
        scrape_page(archive)

def combine_feathers():
    df = pd.DataFrame()
    for feather in os.listdir('data'):
        to_append = pd.read_feather(f'data/{feather}')
        df = df.append(to_append, ignore_index=True)
    return df

def whisky_df():
    archives = archive_list(URL)
    all_pages(archives)
    whisky = combine_feathers()
    whisky.to_csv('whiskyfun.csv')
    return whisky