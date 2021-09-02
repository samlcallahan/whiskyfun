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

# need a different scraper for each format of whiskyfun blog posts
import format1
import format2
import format3
import feisile
import format4

HEADERS = {'User-Agent': user}

URL = "http://www.whiskyfun.com/"

def make_full_link(link_url):
    '''
    Checks if url is full url or url stub. converts to full url if not already.
    This is necessary because Serge links his own pages in an inconsistent manner.
    '''

    # checks if the link already has the base url. if not it puts them together
    if link_url[:4] != 'http':
        link_url = URL + link_url
    return link_url

def is_archive(element):
    '''
    checks if an element could potentially be an archive.
    archive links are all contained in 'font' elements, with
    the #666666 color, Arial type face, and have an 'a' element
    with a link/href in them.
    '''

    c1 = element.name == 'font'
    c2 = element.get('color') == '#666666'
    c3 = element.get('face') == 'Arial'
    c4 = element.find('a')

    return c1 and c2 and c3 and c4

def feather_to_url(feather_file):
    '''
    converts feather file to corresponding url name
    '''
    feather_file = URL + feather_file
    site_url = feather_file[:-7] + 'html'
    return site_url

def archive_list(update):
    '''
    Gets a list of each archive page from the main site
    if update only includes archive pages currently missing from data directory and the homepage
    '''

    # create session object and get all html pieces with the correct archive font style
    # really wish there were an archive_link class or something... c'mon serge
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(URL).content)

    right_fonts = soup.find_all(lambda x: is_archive(x))

    # iterates through all those html chunks to pull out the links
    links = []

    for html_blob in right_fonts:
        links.append(html_blob.find('a')['href'])

    # goes through those links and makes sure it's either a normal archive or a feis ile archive (those are the ones that actually contain reviews)
    archives = [URL]

    for link in links:
        if "archive" in link.lower() or 'feisile' in link.lower() or link.lower() == 'special.html':
            archives.append(link)

    archives = pd.Series(archives)

    # normalizes those links. for more info read make_full_link documentation
    archives = archives.apply(make_full_link)

    archives = archives[:-2]

    if update:
        compare = pd.Series(os.listdir('data'))
        compare = set(compare.apply(feather_to_url))
        archives = set(archives)
        archives = pd.Series(list(archives.difference(compare)))

    return archives

def all_pages(archives):
    '''
    goes through the archive list and scrapes each page
    switches to the appropriate page scraper for each format
    '''

    # scrapes each archive page until it reaches the first page of format 2
    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/archivedecember17-1-Ardbeg-Chichibu-Ledaig.html':
            archives = archives.iloc[i:]
            break
        format1.scrape_page(archive)
        if i == len(archives) - 1:
            return

    # scrapes each archive page until it reaches the first page of format 3
    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/archivedecember09-1.html':
            archives = archives.iloc[i:]
            break
        format2.scrape_page(archive)
        if i == len(archives) - 1:
            return
    
    # scrapes each archive page until it reaches the first page of format 4
    # the one oddly formatted feis ile page will use the feis ile scraper
    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/ArchiveSeptember04.html':
            archives = archives.iloc[i:]
            break
        if archive == 'http://www.whiskyfun.com/special.html':
            feisile.scrape_page(archive)
            continue
        format3.scrape_page(archive)
        if i == len(archives) - 1:
            return
    
    # scrapes the remaining archive pages
    for archive in archives:
        format4.scrape_page(archive)
        if i == len(archives) - 1:
            return

def combine_feathers():
    '''
    finds all feather files (compressed representations of Python objects, in this case each
    is a dataframe representing one archive page) in the data folder. Puts them together into
    one larger dataframe and returns it.
    '''
    df = pd.DataFrame()
    for feather in os.listdir('data'):
        to_append = pd.read_feather(f'data/{feather}')
        df = df.append(to_append, ignore_index=True)
    return df

def whisky_df(update=True):
    '''
    Runs through the whole process - gets archive list,
    scrapes all the pages, combines the resulting feathers,
    stores it in a csv, and returns the resulting dataframe
    '''
    archives = archive_list(update)
    all_pages(archives)
    whisky = combine_feathers()
    whisky.to_csv('whiskyfun.csv')
    return whisky