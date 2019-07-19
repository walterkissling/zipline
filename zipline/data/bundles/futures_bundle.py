# File to ingest an equities bundle for zipline

# Import libraries
import pandas as pd
import numpy as np
import os
from zipline.utils.calendars import get_calendar
import pickle

def futures_bundle():


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
        futures_folder = r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Norgate\Converted\Contracts/'
        
        # Get calendar
        fut_calendar = get_calendar('us_futures')
        all_sessions = fut_calendar.all_sessions

        # List futures available
        fut_names = os.listdir(futures_folder)
        #all_data = pd.DataFrame()
        #for fut in fut_names:
        #    contracts = os.listdir(futures_folder + fut)
        #    root = contracts[0].split('_')[0]
        #    for contract in contracts:
        #        data = pd.read_csv(futures_folder + fut + '/'+ contract, parse_dates = [0])
        #        data['root'] = root
        #        name = fut
        #        data['name'] = name
        #        # Reindex to have same dates as calendar
        #        first_dt = data.Date.min()
        #        last_dt = data.Date.max()
        #        calendar_first_session = all_sessions.get_loc(first_dt)
        #        calendar_last_session = all_sessions.get_loc(last_dt)
        #        calendar_sessions = all_sessions[calendar_first_session:calendar_last_session+1]
        #        data = data.set_index('Date').tz_localize('UTC')
        #        data = data.reindex(calendar_sessions).reset_index()
        #        data['Volume'] = data['Volume'].fillna(0)
        #        data = data.ffill()
        #        all_data = all_data.append(data)

        #all_data.loc[all_data['root'] == 'SPIM21983H.csv', 'root'] = 'SPIM2'
        #all_data.loc[all_data['root'] == 'SPIM31992H.csv', 'root'] = 'SPIM3'
        #all_data = all_data.rename(columns = {'index':'date', 'Open':'open', 'High':'high',
        #                                      'Low':'low', 'Close':'close', 'Volume':'volume',
        #                                      'Open Interest':'open_interest', 'Ticker':'ticker'})
        #all_data = all_data.set_index(['ticker','date'])
        #all_data[['volume','open_interest']] = all_data[['volume','open_interest']].astype(int)

        ## Save as CSV
        #all_data.to_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\all_futures.csv')
        all_data = pd.read_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\all_futures.csv', index_col = [0, 1], parse_dates = [1])
        all_data = all_data.reset_index().set_index('date').sort_index()
        all_data = all_data.loc[:'2018-02-02']
        all_data = all_data.reset_index().set_index(['ticker','date'])
        all_data[['volume','open_interest']] = all_data[['volume','open_interest']].astype(int)
        symbols = all_data.index.levels[0].tolist()
        roots = all_data.root.unique().tolist()

        # Create asset metadata
        dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('notice_date', 'datetime64[ns]'),
                 ('expiration_date', 'datetime64[ns]'),
                 ('tick_size', 'float'),
                 ('multiplier', 'float'),
                 ('symbol', 'object'),
                 ('root_symbol', 'object'),
                 ('asset_name', 'object'),
                 ('exchange', 'object')]
        metadata = pd.DataFrame(np.empty(len(symbols), dtype=dtype))

        # Create list to hold data
        data_to_write = []

        # Load in futures specs
        futures_specs = pd.read_excel(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Norgate\Ticker Mapping.xlsx')
        futures_specs = futures_specs.set_index('Ticker')

        # Create root symbol dataframe
        root_dtypes = [('root_symbol', 'object'),
                       ('root_symbol_id', 'int'),
                       ('sector', 'object'),
                       ('exchange', 'object')]
        root_symbols = pd.DataFrame(np.empty(len(roots), dtype=root_dtypes))

        for rid, root in enumerate(roots):
            sector = futures_specs.loc[root, 'Sector']
            exchange = futures_specs.loc[root, 'Exchange']
            root_symbols.iloc[rid] = root, rid, sector, exchange

        # Loop through symbols and prepare data
        #for sid, symbol in enumerate(symbols):
        #    data_ = all_data.loc[symbol].sort_index()
        #    start_dt = data_.index.min()
        #    end_dt = data_.index.max()
        #    expiration_date = end_dt
        #    notice_date = end_dt
        #    root_symbol = data_.root.unique()[0]
        #    name = data_.name.unique()[0]
        #    exchange = futures_specs.loc[root_symbol, 'Exchange']

        #    # Get future specs
        #    tick_size = futures_specs.loc[root_symbol, 'Tick Value']
        #    multiplier = futures_specs.loc[root_symbol, 'Point Value']

        #    # Set auto cloes to day after last trade
        #    ac_date = end_dt + pd.tseries.offsets.BDay()
        #    metadata.iloc[sid] = start_dt, end_dt, ac_date, notice_date, \
        #        expiration_date, tick_size, multiplier, symbol, root_symbol, name, exchange

        #    # Append data to list
        #    data_to_write.append((sid, data_[['open','high','low','close','volume']]))

        file = r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\data_to_write.pkl'
        metadata_file = r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\metadata.csv'
        #with open(file, 'wb') as f:
        #    pickle.dump(data_to_write, f)
        #metadata.to_csv(metadata_file, index = False)
        with open(file, 'rb') as f:
            data_to_write = pickle.load(f)
        metadata = pd.read_csv(metadata_file)
        for idx in range(len(data_to_write)):
            data_to_write[idx] = (idx, data_to_write[idx][1]['1970-01-02':])

        daily_bar_writer.write(data_to_write, show_progress = True)

        # Write metadata
        asset_db_writer.write(futures = metadata, root_symbols = root_symbols)

        adjustment_writer.write()

    return ingest
