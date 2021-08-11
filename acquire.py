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

    archives = [URL]

    for link in links:
        if "archive" in link.lower() or 'feisile' in link.lower() or link.lower() == 'special.html':
            archives.append(link)

    archives = pd.Series(archives)

    archives = archives.apply(make_full_link)

    return archives

def scrape_page(archive_url):
    session = get_session()

    soup = BeautifulSoup(session.get(URL).content)

    title_soups = soup.find_all('span', class_='textegrandfoncegras')

    titles = [title.text for title in title_soups if '(' in title.text]

    angus_sections = soup.find_all('table', width='498')

    angus_titles = []
    
    for section in angus_sections:
        angus_soups = section.find_all('span', class_='textegrandfoncegras')
        angus_titles.extend([title.text for title in angus_soups if '(' in title.text])

    authors = []

    for title in titles:
        if title in angus_titles:
            authors.append('Angus')
        else:
            authors.append('Serge')

    rating_soups = soup.find_all('span', class_='textenormalgras')

    ratings = [rating.text.strip() for rating in rating_soups if ' points.' in rating.text]

    content_soups = soup.find_all('span', class_='TextenormalNEW')

    contents = [content.text for content in content_soups if 'Colour' in content.text]

    date_soups = soup.find_all('font', color='#660000', size='3')

    dates = [date_soup.text for date_soup in date_soups]

    return 