# File to ingest an equities bundle for zipline

# Import libraries
import pandas as pd
import numpy as np
import sys
data_folder = r'C:\Users\\walte\\OneDrive - K Squared Capital\\K Squared Capital\\Trading Models\\Code\\Live Trading\\Live Trading'
from zipline.utils.calendars import get_calendar

def eu_etfs_bundle():

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
        # Load in pricing data
        #prices, midpoint = get_data('GD')
        #prices.to_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\GD EU ETFs\gd_eu_etfs_prices.csv', index = False)
        #midpoint.to_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\GD EU ETFs\gd_eu_etfs_midpoint.csv', index = False)
        prices = pd.read_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\GD EU ETFs\gd_eu_etfs_prices.csv')
        midpoint = pd.read_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\GD EU ETFs\gd_eu_etfs_midpoint.csv')
        trades = pd.read_csv(r'C:\Users\walte\OneDrive - K Squared Capital\K Squared Capital\Trading Models\Data\Zipline\GD EU ETFs\gd_eu_etfs_trades.csv')
        prices.Date = pd.to_datetime(prices.Date) 
        prices = prices.set_index(['Date','ConId'])
        midpoint.Date = pd.to_datetime(midpoint.Date) 
        midpoint = midpoint.set_index(['Date','ConId'])
        trades.Date = pd.to_datetime(trades.Date) 
        trades = trades.set_index(['Date','ConId'])

        lse_calendar = get_calendar('LSE')
        all_sessions = lse_calendar.all_sessions

        #prices = prices.sort_index(level = 0)
        close_px = prices['Close'].unstack().loc['2016-01-01':]
        open_px = prices['Open'].unstack().loc['2016-01-01':]
        high_px = prices['High'].unstack().loc['2016-01-01':]
        low_px = prices['Low'].unstack().loc['2016-01-01':]
        volume = prices['Volume'].unstack().loc['2016-01-01':]

        first_idx = all_sessions.get_loc(close_px.index[0])
        last_idx = all_sessions.get_loc(close_px.index[-1])
        cal_sessions = all_sessions[first_idx:last_idx+1]

        # Load in midpoint data
        mid_close_px = midpoint['Close'].unstack().loc['2016-01-01':]
        mid_open_px = midpoint['Open'].unstack().loc['2016-01-01':]
        mid_high_px = midpoint['High'].unstack().loc['2016-01-01':]
        mid_low_px = midpoint['Low'].unstack().loc['2016-01-01':]

        # Load in trades data
        trd_close_px = trades['Close'].unstack().loc['2016-01-01':]
        trd_open_px = trades['Open'].unstack().loc['2016-01-01':]
        trd_high_px = trades['High'].unstack().loc['2016-01-01':]
        trd_low_px = trades['Low'].unstack().loc['2016-01-01':]

        # Reindex to calendar sessions
        close_px = close_px.tz_localize('UTC').reindex(cal_sessions)
        open_px = open_px.tz_localize('UTC').reindex(cal_sessions)
        high_px = high_px.tz_localize('UTC').reindex(cal_sessions)
        low_px = low_px.tz_localize('UTC').reindex(cal_sessions)
        volume = volume.tz_localize('UTC').reindex(cal_sessions)
        mid_close_px = mid_close_px.tz_localize('UTC').reindex(cal_sessions)
        mid_open_px = mid_open_px.tz_localize('UTC').reindex(cal_sessions)
        mid_low_px = mid_low_px.tz_localize('UTC').reindex(cal_sessions)
        mid_high_px = mid_high_px.tz_localize('UTC').reindex(cal_sessions)
        trd_close_px = trd_close_px.tz_localize('UTC').reindex(cal_sessions)
        trd_open_px = trd_open_px.tz_localize('UTC').reindex(cal_sessions)
        trd_low_px = trd_low_px.tz_localize('UTC').reindex(cal_sessions)
        trd_high_px = trd_high_px.tz_localize('UTC').reindex(cal_sessions)

        # Load in ETF info
        etf_info = pd.read_excel(data_folder + '\Global_Defensive_EU\EU ETFs.xlsx')
        ticker_map = etf_info[['Con ID','IB Ticker']].set_index('Con ID').dropna().squeeze()
        ticker_map.index = ticker_map.index.astype(int)

        # Rename pricing index
        close_px = close_px.rename(columns = ticker_map)
        open_px = open_px.rename(columns = ticker_map)
        high_px = high_px.rename(columns = ticker_map)
        low_px = low_px.rename(columns = ticker_map)
        volume = volume.rename(columns = ticker_map)
        mid_close_px = mid_close_px.rename(columns = ticker_map)
        mid_open_px = mid_open_px.rename(columns = ticker_map)
        mid_high_px = mid_high_px.rename(columns = ticker_map)
        mid_low_px = mid_low_px.rename(columns = ticker_map)
        trd_close_px = trd_close_px.rename(columns = ticker_map)
        trd_open_px = trd_open_px.rename(columns = ticker_map)
        trd_high_px = trd_high_px.rename(columns = ticker_map)
        trd_low_px = trd_low_px.rename(columns = ticker_map)
        mid_close_px = mid_close_px.ffill()
        mid_open_px = mid_open_px.ffill()
        mid_high_px = mid_high_px.ffill()
        mid_low_px = mid_low_px.ffill()
        volume = volume.fillna(1).astype(int)
        volume[:] = 1000000000

        # Fill in missing closing prices with midpoint data
        for etf in close_px.columns:
            first_idx = close_px[etf].first_valid_index()
            temp_data = close_px.loc[first_idx:, etf]
            midpoint_temp = mid_close_px.loc[first_idx:, etf]
            trades_temp = trd_close_px.loc[first_idx:, etf]
            adj_ratio = temp_data / trades_temp
            adj_ratio = adj_ratio.ffill()
            missing_idx = temp_data[temp_data.isna()].index
            temp_data[missing_idx] = midpoint_temp[missing_idx] * adj_ratio[missing_idx]
            close_px.loc[temp_data.index, etf] = temp_data
        for etf in open_px.columns:
            first_idx = open_px[etf].first_valid_index()
            temp_data = open_px.loc[first_idx:, etf]
            midpoint_temp = mid_open_px.loc[first_idx:, etf]
            trades_temp = trd_open_px.loc[first_idx:, etf]
            adj_ratio = temp_data / trades_temp
            adj_ratio = adj_ratio.ffill()
            missing_idx = temp_data[temp_data.isna()].index
            temp_data[missing_idx] = midpoint_temp[missing_idx] * adj_ratio[missing_idx]
            open_px.loc[temp_data.index, etf] = temp_data
        for etf in high_px.columns:
            first_idx = high_px[etf].first_valid_index()
            temp_data = high_px.loc[first_idx:, etf]
            midpoint_temp = mid_high_px.loc[first_idx:, etf]
            trades_temp = trd_high_px.loc[first_idx:, etf]
            adj_ratio = temp_data / trades_temp
            adj_ratio = adj_ratio.ffill()
            missing_idx = temp_data[temp_data.isna()].index
            temp_data[missing_idx] = midpoint_temp[missing_idx] * adj_ratio[missing_idx]
            high_px.loc[temp_data.index, etf] = temp_data
        for etf in low_px.columns:
            first_idx = low_px[etf].first_valid_index()
            temp_data = low_px.loc[first_idx:, etf]
            midpoint_temp = mid_low_px.loc[first_idx:, etf]
            trades_temp = trd_low_px.loc[first_idx:, etf]
            adj_ratio = temp_data / trades_temp
            adj_ratio = adj_ratio.ffill()
            missing_idx = temp_data[temp_data.isna()].index
            temp_data[missing_idx] = midpoint_temp[missing_idx] * adj_ratio[missing_idx]
            low_px.loc[temp_data.index, etf] = temp_data

        close_px = close_px.ffill()
        open_px = open_px.ffill()
        high_px = high_px.ffill()
        low_px = low_px.ffill()

        symbols = close_px.columns.tolist()

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
            data_ = pd.DataFrame(columns = ['open','high','low','close','volume','open_interest'],
                                 index = close_px.index)
            data_['open'] = open_px[symbol]
            data_['high'] = high_px[symbol]
            data_['low'] = low_px[symbol]
            data_['close'] = close_px[symbol]
            data_['volume'] = volume[symbol]
            data_['open_interest'] = 0
            start_dt = data_.index.min()
            end_dt = data_.index.max()

            # Set auto cloes to day after last trade
            ac_date = end_dt + pd.tseries.offsets.BDay()
            metadata.iloc[sid] = start_dt, end_dt, ac_date, symbol
            # Append data to list
            data_to_write.append((sid, data_))

        daily_bar_writer.write(data_to_write, show_progress = True)

        # Hardcode exchange data
        exchanges = etf_info.set_index('IB Ticker').loc[metadata.symbol,'Exchange']
        metadata['exchange'] = exchanges.values
            
        # Write metadata
        asset_db_writer.write(equities = metadata)

        # Write splits and dividents
        adjustment_writer.write(splits = splits,
                                dividends = dividends)

    return ingest