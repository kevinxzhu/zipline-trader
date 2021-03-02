import collections
from datetime import timedelta, time as dtime
import numpy as np
import csv
from os import listdir, mkdir, remove, makedirs, walk
from os.path import exists, isfile, join, expandvars, basename, splitext
from pathlib import Path
import pandas as pd
import pytz
from alpaca_trade_api.common import URL
from dateutil import tz
from trading_calendars import TradingCalendar

import config
from zipline.data.bundles import core as bundles
from zipline.data.bundles.common import asset_to_sid_map
from zipline.data.bundles.universe import Universe, read_all_alpaca_assets, get_sp500, get_sp100, get_nasdaq100
from dateutil.parser import parse as date_parse
from zipline.errors import SymbolNotFound, SidsNotFound
from datetime import date


user_home = str(Path.home())
custom_data_path = join(user_home, '.zipline/custom_data')

# CLIENT: tradeapi.REST = None
NY = "America/New_York"


# def initialize_client():
    # global CLIENT
    # conf = config.bundle.CustomCsv()
    # key = conf.key
    # secret = conf.secret
    # base_url = conf.base_url
    # CLIENT = tradeapi.REST(key_id=key,
    #                        secret_key=secret,
    #                        base_url=URL(base_url))

ASSETS = None
def list_assets():
    global ASSETS
    if not ASSETS:
        conf = config.bundle.CustomCsvConfig()
        custom_asset_list = conf.custom_asset_list
        if custom_asset_list:
            custom_asset_list = custom_asset_list.strip().replace(" ", "").split(",")
            ASSETS = list(set(custom_asset_list))
        else:
            try:
                universe = Universe[conf.universe]
            except:
                universe = Universe.ALL
            if universe == Universe.ALL:
                filename = expandvars(conf.ccsv['input_asset_filename'])
                ASSETS = read_all_alpaca_assets(filename)
            elif universe == Universe.SP100:
                ASSETS = get_sp100()
            elif universe == Universe.SP500:
                ASSETS = get_sp500()
            elif universe == Universe.NASDAQ100:
                ASSETS = get_nasdaq100()
            ASSETS = list(set(ASSETS))
    return ASSETS

# def write_symbol_list(store_data_path):
#     import datetime

#     if not exists(store_data_path):
#         mkdir(store_data_path)
#     x = datetime.datetime.now()
#     store_data_filename = join(store_data_path, 'allsymbols_' + x.strftime("%Y%m%d") + '.txt')
#     with open(store_data_filename, 'w') as f:
#         for item in ASSETS:
#             f.write("%s\n" % item)

def iso_date(date_str):
    """
    this method will make sure that dates are formatted properly
    as with isoformat
    :param date_str:
    :return: YYYY-MM-DD date formatted
    """
    return date_parse(date_str).date().isoformat()

# MAX_PER_REQUEST_AMOUNT = 200  # Alpaca max symbols per 1 http request
def df_generator(interval, start, end, assets_to_sids):
    exchange = 'NYSE'
    asset_list = list_assets()
    base_sid = 0
    # some symbols from alpaca are duplicated, which causes an issue with zipline
    # ingest process. for now, we make sure we serve one of them (for now the first one)
    already_ingested = {}
    #
    # loop all symbol.csv files
    #   each symbol, read into DataFrame
    #   take partial within start,end
    #   convert index to UTC
    #
    conf = config.bundle.CustomCsvConfig()
    source = expandvars(conf.ccsv['input_csv_path'])
    for root,d_names,f_names in walk(source):
        # print(f'{root} {d_names} {f_names}')
        for d_name in d_names:
            folder = join(root, d_name)
            for root2,d_names2,f_names2 in walk(folder):
                # print(f'{root2} {d_names2} {f_names2}')
                for f_name in f_names2:
                    exist_file_name = join(folder, f_name)
                    print(f'read {exist_file_name}')
                    in_data = pd.read_csv(exist_file_name, parse_dates=[0],
                           infer_datetime_format=True, index_col=0)
                    # start_date = in_data.index[0]
                    # end_date = in_data.index[-1]
                    base = basename(f_name)
                    symbol = splitext(base)[0]
                    sid = assets_to_sids[symbol]
                    if symbol not in already_ingested:
                        first_traded = start
                        auto_close_date = end + pd.Timedelta(days=1)
                        out_data = in_data[start: end]
                        yield (sid, out_data.sort_index()), symbol, start, end, first_traded, auto_close_date, exchange
                        already_ingested[symbol] = True

                    # in_data.index = pd.to_datetime(in_data.index, utc=True)
                    # out_data = in_data.tz_convert(pytz.utc)
                    # print(out_data.index)
                    # out_data.index = out_data.index.normalize()
                    # out_data.rename_axis('date', inplace=True)
                    # out_data.to_csv(new_file_name)

def metadata_df():
    metadata_dtype = [
        ('symbol', 'object'),
        # ('asset_name', 'object'),
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('first_traded', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange', 'object'), ]
    metadata_df = pd.DataFrame(
        np.empty(len(list_assets()), dtype=metadata_dtype))

    return metadata_df


@bundles.register('custom_csv', calendar_name="NYSE", minutes_per_day=390)
def api_to_bundle(interval=['1m']):
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):

        assets_to_sids = asset_to_sid_map(asset_db_writer.asset_finder, list_assets())

        def minute_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1m',
                                                                                   start=start_session,
                                                                                   end=end_session,
                                                                                   assets_to_sids=assets_to_sids))

        def daily_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1d',
                                                                                   start=start_session,
                                                                                   end=end_session,
                                                                                   assets_to_sids=assets_to_sids))
        for _interval in interval:
            metadata = metadata_df()
            if _interval == '1d':
                daily_bar_writer.write(daily_data_generator(), assets=assets_to_sids.values(), show_progress=True)
            elif _interval == '1m':
                minute_bar_writer.write(
                    minute_data_generator(), assets=assets_to_sids.values(), show_progress=True)

            # Drop the ticker rows which have missing sessions in their data sets
            metadata.dropna(inplace=True)

            asset_db_writer.write(equities=metadata)
            print(metadata)
            adjustment_writer.write()

    return ingest


if __name__ == '__main__':
    from zipline.data.bundles import register
    from zipline.data import bundles as bundles_module
    import trading_calendars
    import os

    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    # end_date = pd.Timestamp('now', tz='utc').date() - timedelta(days=1)
    end_date = pd.Timestamp('2021-02-26', tz='utc').date() - timedelta(days=1)
    while not cal.is_session(end_date):
        end_date -= timedelta(days=1)
    end_date = pd.Timestamp(end_date, tz='utc')

    # start_date = pd.Timestamp('2020-10-03 0:00', tz='utc')
    # while not cal.is_session(start_date):
    #     start_date += timedelta(days=1)

    # start_date = end_date - timedelta(days=365)
    # start_date = end_date - timedelta(days=1824)
    # start_date = pd.Timestamp('2016-01-04', tz='utc')
    start_date = pd.Timestamp('2021-02-24', tz='utc')

    while not cal.is_session(start_date):
        start_date -= timedelta(days=1)

    print(f'Ingest between {start_date} and {end_date}')
    # initialize_client()

    import time

    start_time = time.time()

    register(
        'custom_csv',
        # api_to_bundle(interval=['1d', '1m']),
        # api_to_bundle(interval=['1m']),
        api_to_bundle(interval=['1d']),
        calendar_name='NYSE',
        start_session=start_date,
        end_session=end_date
    )

    assets_version = ((),)[0]  # just a weird way to create an empty tuple
    bundles_module.ingest(
        "custom_csv",
        os.environ,
        assets_versions=assets_version,
        show_progress=True,
    )

    print(f"--- It took {timedelta(seconds=time.time() - start_time)} ---")
