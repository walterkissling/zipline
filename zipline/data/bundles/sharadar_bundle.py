# File to ingest an equities bundle for zipline

# Import libraries
import pandas as pd
import numpy as np
from zipline.utils.calendars import get_calendar
import pickle
from . import core as bundles

@bundles.register("sharadar")
def sharadar_bundle():

    # Define custom ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               start_session,
               end_session):

        # Read in data
        shar_folder = r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\QuantRocket\Sharadar\Prices/'

        # Save data
        file = shar_folder + 'all_shar_data.pkl'
        metadata_file = shar_folder + 'metadata.csv'
        with open(file, 'rb') as f:
            data_to_write = pickle.load(f)
        metadata = pd.read_csv(metadata_file)
        dividends = pd.read_csv(shar_folder + 'dividends.csv', parse_dates = ['declared_date','ex_date',
                                                                           'pay_date','record_date'])
        dividends = dividends.fillna(0)
        dividends = dividends[dividends.amount > 0]
        splits = pd.DataFrame(columns = ['sid', 'ratio','effective_date'])

        # Write metadata
        daily_bar_writer.write(data_to_write, show_progress = True)

        asset_db_writer.write(equities = metadata)

        adjustment_writer.write(splits = splits,
                                dividends = dividends)

    return ingest


