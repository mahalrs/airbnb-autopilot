import pandas as pd
import numpy as np
import os


DATASET_AVERAGES_PATH = '../data/downloaded/airbnb-averages.csv'
DATASET_LISTINGS_PATH = '../data/downloaded/airbnb-listings.csv'
DATASET_VERSION = 'v1.0'

DATASET_OUT_DIR = '../data/raw'


def make_averages_dataset():
    df = pd.read_csv(DATASET_AVERAGES_PATH, encoding='utf-8', sep=';')

    # Drop columns
    columns_to_drop = [
        'filename',
        'Date',
        'Location',
        'geo_shape',
        'Geo Point',
    ]
    df.drop(columns_to_drop, axis=1, inplace=True)

    # Save dataset
    fpath = os.path.join(DATASET_OUT_DIR, 'airbnb_averages_' + DATASET_VERSION + '.parquet')
    df.to_parquet(fpath, compression='gzip')


def make_listings_dataset():
    df = pd.read_csv(DATASET_LISTINGS_PATH, encoding='utf-8', sep=';')

    # Casting data
    df['ID'] = pd.to_numeric(df['ID'], errors='coerce').astype('Int64')
    df['Host Since'] = pd.to_datetime(df['Host Since'], errors='coerce')
    df['Host Acceptance Rate']= df['Host Acceptance Rate'].apply(lambda x: int(x[:-1]) if (type(x) == str and x[-1] == '%') else np.nan)

    # Drop columns
    columns_to_drop = [
        'Listing Url',
        'Scrape ID',
        'Last Scraped',
        'Summary',
        'Space',
        'Description',
        'Experiences Offered',
        'Neighborhood Overview',
        'Notes',
        'Transit',
        'Access',
        'Interaction',
        'House Rules',
        'Thumbnail Url',
        'Medium Url',
        'Picture Url',
        'XL Picture Url',
        'Host URL',
        'Host Name',
        'Host Location',
        'Host About',
        'Host Thumbnail Url',
        'Host Picture Url',
        'Host Neighbourhood',
        'Host Verifications',
        'Neighbourhood Cleansed',
        'Neighbourhood Group Cleansed',
        'Street',
        'Market',
        'Smart Location',
        'Country Code',
        'Has Availability',
        'Calendar last Scraped',
        'Calendar Updated',
        'First Review',
        'Last Review',
        'License',
        'Calculated host listings count',
        'Jurisdiction Names',
        'Geolocation',
        'Latitude',
        'Longitude',
    ]
    df.drop(columns_to_drop, axis=1, inplace=True)

    # Save dataset
    fpath = os.path.join(DATASET_OUT_DIR, 'airbnb_listings_' + DATASET_VERSION + '.parquet')
    df.to_parquet(fpath, compression='gzip')


def make_descriptions_dataset():
    df = pd.read_csv(DATASET_LISTINGS_PATH, encoding='utf-8', sep=';')

    # Casting data
    df['ID'] = pd.to_numeric(df['ID'], errors='coerce').astype('Int64')

    # Filter columns
    df = df.filter(['ID', 'Description'], axis=1)

    # Split data into chunks
    chunk1_df = df.iloc[:len(df)//2]
    chunk2_df = df.iloc[len(df)//2:]

    # Save dataset
    fname_c1 = 'airbnb_descriptions_chunk1_' + DATASET_VERSION + '.parquet'
    fname_c2 = 'airbnb_descriptions_chunk2_' + DATASET_VERSION + '.parquet'
    chunk1_df.to_parquet(os.path.join(DATASET_OUT_DIR, fname_c1), compression='gzip')
    chunk2_df.to_parquet(os.path.join(DATASET_OUT_DIR, fname_c2), compression='gzip')


def make_datasets():
    make_averages_dataset()
    make_listings_dataset()
    make_descriptions_dataset()
