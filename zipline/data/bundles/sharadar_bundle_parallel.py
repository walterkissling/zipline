# File to ingest an equities bundle for zipline

# Import libraries
import pandas as pd
import numpy as np
from joblib import Parallel, delayed
import multiprocessing

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
        px_fields = ['Open','High','Low','Volume','Dividends','Close']
        path_to_file = r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\QuantRocket\Sharadar\Prices/'
        #for field in px_fields[2:]:
        #    temp_data = pd.read_csv(path_to_file + field + '.csv', parse_dates = ['Date'],
        #                            index_col = ['Date'], infer_datetime_format = True)
        #    if 'Field' in temp_data.columns:
        #        temp_data = temp_data.drop('Field', axis = 1)
        #    temp_data = temp_data.stack()
        #    temp_data = temp_data.reset_index()
        #    temp_data = temp_data.rename(columns = {'level_1':'ConId',0:field})
        #    temp_data = temp_data.set_index(['ConId','Date'])
        #    if 'data' not in locals():
        #        data = temp_data.copy()
        #    else:
        #        data[field] = temp_data
        #data = data.rename(columns = {'Open':'open','High':'high','Low':'low',
        #                              'Close':'close','Volume':'volume','Dividends':'dividend'})
        #data.volume = data.volume.fillna(0)

        # Read in data
        #data = pd.read_csv(path_to_file + 'all_data.csv', index_col = [0, 1],
        #                   parse_dates = ['Date'], infer_datetime_format = True)
        data = pd.read_parquet(path_to_file + 'all_data.parquet')

        # Get symbols and sectors
        symbol_map = pd.read_csv(path_to_file + 'Symbols.csv').T.squeeze()
        symbol_map = symbol_map.drop('Date')
        symbol_map.name = 'Symbols'

        # Drop PHUN as it is duplicated
        #data = data.drop('126929', level = 1)
        symbol_map = symbol_map.drop('126929')
        symbol_map.iloc[10765] = 'TRUE'
#        data = data.rename(index = {'True':'TRUE'})
        data['open_interest'] = np.nan
        # Rename pricing to use symbol isntead of ConId
#        data = data.rename(index = symbol_map, level = 0)

        symbols = symbol_map.values.tolist()

        # Create asset metadata
        dtype = [('start_date', 'datetime64[ns]'),
                 ('end_date', 'datetime64[ns]'),
                 ('auto_close_date', 'datetime64[ns]'),
                 ('symbol', 'object')]
        metadata = pd.DataFrame(np.empty(len(symbols), dtype=dtype))

        # Create dividend and split dataframe
        dividends = pd.DataFrame(columns = ['sid', 'amount',
                                            'ex_date', 'record_date',
                                            'declared_date', 'pay_date'])
        splits = pd.DataFrame(columns = ['sid', 'ratio','effective_date'])

        # Create list to hold data
        data_to_write = []

        # Load calendar
        nyse_calendar = get_calendar('NYSE')
        all_sessions = nyse_calendar.all_sessions

        def process_sid_symbol(sid, symbol):
            data_ = data.loc[symbol].sort_index()
            start_dt = data_.index.min()
            end_dt = data_.index.max()

            # Reindex to sessions
            first_idx = all_sessions.get_loc(start_dt)
            last_idx = all_sessions.get_loc(end_dt)
            cal_sessions = all_sessions[first_idx:last_idx+1]

            data_ = data_.tz_localize('UTC').reindex(cal_sessions)

            # Set auto cloes to day after last trade
            ac_date = end_dt + pd.tseries.offsets.BDay()
            metadata = pd.Series([start_dt, end_dt, ac_date, symbol],
                                 index = ['start_date','end_date','auto_close_date','symbol'],
                                 name = sid)

            # Check for dividends
            if 'dividend' in data_.columns:
                # ex_date   amount  sid record_date declared_date pay_date
                tmp = data_[data_['dividend'] != 0.0]['dividend']
                div = pd.DataFrame(data = tmp.index.tolist(), columns = ['ex_date'])
                div['record_date'] = tmp.index
                div['declared_date'] = tmp.index
                div['pay_date'] = tmp.index
                div['amount'] = tmp.tolist()
                div['sid'] = sid

                ind = pd.Index(range(dividends.shape[0], dividends.shape[0] + div.shape[0]))
                div.set_index(ind, inplace=True)

            # Append data to list
            return ((sid, data_), div, metadata)

        num_cores = multiprocessing.cpu_count()
        results = Parallel(n_jobs=num_cores, verbose = 100)(delayed(process_sid_symbol)(sid, symbol) for sid, symbol in enumerate(symbols[:10]))

        for dta, divs, mta in results:
            data_to_write.append(dta)
            dividends.append(divs)
            metadata = metadata.append(mta)

        daily_bar_writer.write(data_to_write, show_progress = True)

        # Hardcode exchange data
        metadata['exchange'] = 'CSV'
            
        # Write metadata
        asset_db_writer.write(equities = metadata)

        # Write splits and dividents
        dividends['sid'] = dividends['sid'].astype(int)
        splits['sid'] = splits['sid'].astype(int)
        adjustment_writer.write(splits = splits,
                                dividends = dividends)

    return ingest
