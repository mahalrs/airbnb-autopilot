import pandas as pd
import os


DATASET_AVERAGES_PATH = '../data/raw/airbnb-averages.csv'
DATASET_LISTINGS_PATH = '../data/raw/airbnb-listings.csv'
DATASET_VERSION = 'v1.0'

DATASET_OUT_DIR = '../data/raw'


def make_dataset():
    # Averages Dataset
    avg_df = pd.read_csv(DATASET_AVERAGES_PATH, encoding='utf-8', sep=';')
    
    # Drop columns
    avg_columns_to_drop = [
        'filename',
        'Date',
        'Location',
        'geo_shape',
        'Geo Point',
    ]

    avg_df.drop(avg_columns_to_drop, axis=1, inplace=True)
    avg_fpath = os.path.join(DATASET_OUT_DIR, 'airbnb_averages_' + DATASET_VERSION + '.parquet')
    avg_df.to_parquet(avg_fpath, compression='gzip')

    # Listings Dataset
    list_df = pd.read_csv(DATASET_LISTINGS_PATH, encoding='utf-8', sep=';')
    list_df['ID'] = pd.to_numeric(list_df['ID'], errors='coerce').astype('Int64')

    # Drop columns
    list_columns_to_drop = [
        'Listing Url',
        'Scrape ID',
        'Last Scraped',
        'Summary',
        'Space',
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

    list_df.drop(list_columns_to_drop, axis=1, inplace=True)
    desc_df = list_df.filter(['ID', 'Description'], axis=1)
    filtered_list_df = list_df.drop('Description', axis=1)
    
    desc_chunk1 = desc_df.iloc[:len(desc_df)//2]
    desc_chunk2 = desc_df.iloc[len(desc_df)//2:]

    listings_fname = 'airbnb_listings_' + DATASET_VERSION + '.parquet'
    desc_chunk1_fname = 'airbnb_descriptions_chunk1_' + DATASET_VERSION + '.parquet'
    desc_chunk2_fname = 'airbnb_descriptions_chunk2_' + DATASET_VERSION + '.parquet'

    filtered_list_df.to_parquet(os.path.join(DATASET_OUT_DIR, listings_fname), compression='gzip')
    desc_chunk1.to_parquet(os.path.join(DATASET_OUT_DIR, desc_chunk1_fname), compression='gzip')
    desc_chunk2.to_parquet(os.path.join(DATASET_OUT_DIR, desc_chunk2_fname), compression='gzip')
