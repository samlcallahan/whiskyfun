import pandas as pd
import sys
import re
import unicodedata
import json
import os

import nltk
from nltk.tokenize.toktok import ToktokTokenizer
from nltk.corpus import stopwords

'''
plan:
make columns:   distill year, bottle year, distillery, cask makeup, color, nose, palate, finish,
                abv, rating, sgp, region
maybe separate other types of liquors? rums, bourbons, yak, agave, etc
maybe separate angus reviews?
maybe regularize angus scale vs serge scale?
standard NLP prep on each NLP column
encode by distillery?
encode region
encode cask type?
break up sgp into s, g, and p?
'''

def normalize(string):
    return unicodedata.normalize('NFKD', string).encode('ascii', 'ignore').decode('utf-8', 'ignore')

def basic_clean(string, lower=False):
    """
    Lowercase the string
    Normalize unicode characters
    Replace anything that is not a letter, number, whitespace or a single quote.
    """
    string = str(string)
    if lower:
        string = string.lower()

    string = normalize(string)
    
    # remove anything not a space character, an apostrophe, letter, or number
    string = re.sub(r"[^\w\s\.\,\']", ' ', string)

    # drop weird words <=2 characters
    # string = re.sub(r'\b[a-z]{,2}\b', '', string)

    # convert newlines and tabs to a single space
    string = re.sub(r'\s+', ' ', string)
    string = string.strip()
    return string

def prep_whisky(df):
    '''
    sets date column to datetime type
    normalizes text columns
    extracts rating/SGP rating
    extracts distillery
    '''
    df['date'] = pd.to_datetime(df['date'])

    for col in df.columns.drop('date'):
        df[col] = df[col].apply(basic_clean)

    