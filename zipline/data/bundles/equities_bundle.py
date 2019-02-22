# File to ingest an equities bundle for zipline

# Import libraries
import pandas as pd
import numpy as np

def equities_bundle(path_to_file):

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
        data = pd.read_csv(path_to_file, index_col = [0, 1], parse_dates = [1], infer_datetime_format = True)
        data.volume = data.volume.astype(int)
        #data.loc[:, 'volume'] = 100000000
        symbols = data.index.levels[0].tolist()
        #start_dt = data.index.levels[1].min()
        #end_dt = data.index.levels[1].max()

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

        # Loop through symbols and prepare data
        for sid, symbol in enumerate(symbols):
            data_ = data.loc[symbol].sort_index()
            start_dt = data_.index.min()
            end_dt = data_.index.max()

            # Set auto cloes to day after last trade
            ac_date = end_dt + pd.tseries.offsets.BDay()
            metadata.iloc[sid] = start_dt, end_dt, ac_date, symbol

            # Check for splits and dividends
            if 'split' in data_.columns:
                tmp = 1. / data_[data_['split'] != 1.0]['split']
                split = pd.DataFrame(data = tmp.index.tolist(), columns = ['effective_date'])
                split['ratio'] = tmp.tolist()
                split['sid'] = sid

                index = pd.Index(range(splits.shape[0],
                                       splits.shape[0] + split.shape[0]))
                split.set_index(index, inplace=True)
                splits = splits.append(split)

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
                dividends = dividends.append(div)                

            # Append data to list
            data_to_write.append((sid, data_))

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