from FinanceDataRetriever import CONFIG_FILEPATH, MT5_LOGIN_ID, MT5_LOGIN_PASSWORD, MT5_TRADE_SERVER, \
    MT5_TERMINAL_PATH, MT5_TIMEFRAMES, FOREX_DATA_DIR, PACKAGE_TEMP_DIR, MT5_TIME_COL_NAME
from FinanceDataRetriever import utils
import MetaTrader5 as mt5
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import os

logger = utils.Logger.get_instance(__name__)


def init_mt5(config_filepath=None):
    if config_filepath is None:
        config_filepath = CONFIG_FILEPATH
    var_names = [MT5_TERMINAL_PATH, MT5_TRADE_SERVER, MT5_LOGIN_ID, MT5_LOGIN_PASSWORD]
    config_variables = utils.get_config_variables(var_names, config_filepath)
    for var in var_names:
        if var == MT5_TERMINAL_PATH:
            continue
        required_num_type = int if var == MT5_LOGIN_ID else None
        if var not in config_variables:
            config_variables[var] = utils.request_input_value(var, required_num_type=required_num_type)
        elif required_num_type:
            config_variables[var] = required_num_type(config_variables[var])
    success = False
    if MT5_TERMINAL_PATH in config_variables:
        success = mt5.initialize(config_variables[MT5_TERMINAL_PATH], login=config_variables[MT5_LOGIN_ID],
                                 password=config_variables[MT5_LOGIN_PASSWORD],
                                 server=config_variables[MT5_TRADE_SERVER])
    else:
        success = mt5.initialize(login=config_variables[MT5_LOGIN_ID], password=config_variables[MT5_LOGIN_PASSWORD],
                                 server=config_variables[MT5_TRADE_SERVER])
    if not success:
        logger.error(f'Failed to initialize MT5 terminal, error:\n{mt5.last_error()}\n')
    return success


def download_mt5_data(symbol, resolution, start_datetime=None, end_datetime=None, bar_start_pos=None, bar_count=None,
                      download_dir=None):
    if mt5.terminal_info() is None:
        if not init_mt5():
            return

    if (start_datetime is None and end_datetime is None) and (bar_start_pos is None and bar_count is None):
        logger.error(
            'Either start_datetime & end_datetime or bar_start_pos & bar_count must be specified to download mt5 data')
        return

    if (start_datetime is not None and end_datetime is None) or (start_datetime is None and end_datetime is not None):
        logger.error('Both start_datetime and end_datetime must be specified to download mt5 data')
        return

    if (bar_start_pos is not None and bar_count is None) or (bar_start_pos is None and bar_count is not None):
        logger.error('Both bar_start_pos and bar_count must be specified to download mt5 data')
        return

    resolution = resolution.lower()
    if resolution not in MT5_TIMEFRAMES:
        logger.error(f'"{resolution}" is not a valid chart time frame')
        return

    time_frame = MT5_TIMEFRAMES[resolution]

    if download_dir is None:
        download_dir = FOREX_DATA_DIR
    download_dir = Path(download_dir)
    if not Path(download_dir).is_dir():
        Path(download_dir).mkdir(exist_ok=True, parents=True)

    dt_save_form = '%Y-%m-%dT%H;%M%Z'
    server = mt5.account_info().server
    if start_datetime is not None:
        start_datetime = datetime.fromisoformat(start_datetime)
        start_datetime = datetime(start_datetime.year, start_datetime.month, start_datetime.day,
                                  hour=start_datetime.hour, minute=start_datetime.minute, second=start_datetime.second,
                                  tzinfo=timezone.utc)
        cur_start_time = start_datetime

        end_datetime = datetime.fromisoformat(end_datetime)
        end_datetime = datetime(end_datetime.year, end_datetime.month, end_datetime.day, hour=end_datetime.hour,
                                minute=end_datetime.minute, second=end_datetime.second, tzinfo=timezone.utc)

        filepath = download_dir / f'mt5_{server}_{symbol}_{resolution}_{start_datetime.strftime(dt_save_form)}' \
                                  f'_to_{end_datetime.strftime(dt_save_form)}.csv'
    elif bar_start_pos is not None:
        current_utc_dt = datetime.now(tz=timezone.utc)
        filepath = download_dir / f'mt5_{server}_{symbol}_{resolution}_{bar_count}_bars_from_{bar_start_pos}_bar' \
                                  f'_on_{current_utc_dt.strftime(dt_save_form)}.csv'

    if resolution == "tick":
        time_skip = timedelta(days=30)
    else:
        time_skip = timedelta(days=365)
    retries = 0
    files_to_merge = []
    ticks = None

    if start_datetime is not None:
        while cur_start_time < end_datetime:
            cur_end_time = cur_start_time + time_skip
            cur_end_time = \
                min((cur_end_time - cur_start_time, cur_end_time), (end_datetime - cur_start_time, end_datetime),
                    key=lambda tup: tup[0])[1]
            if resolution == "tick":
                cur_ticks_batch = mt5.copy_ticks_range(symbol, cur_start_time, cur_end_time, mt5.COPY_TICKS_ALL)
            else:
                cur_ticks_batch = mt5.copy_rates_range(symbol, time_frame, cur_start_time, cur_end_time)
            if cur_ticks_batch is not None:
                if len(cur_ticks_batch) == 0:
                    logger.warn(f'no tick data returned by mt5 terminal for time range of {cur_start_time} '
                                f'to {cur_end_time}')

            res = mt5.last_error()
            if res[0] == 1:
                print(f'successfully retrieved {len(cur_ticks_batch)} rows of tick data '
                      f'for time range of {cur_start_time} to {cur_end_time}')
            # "No IPC connection" error
            elif res[0] == -10004:
                logger.error(f'{res} lost connection to mt5 terminal')
                if retries < 3:
                    logger.error('retrying...')
                    if not init_mt5():
                        return
                    retries += 1
                    continue
            # any other mt5 error: https://www.mql5.com/en/docs/integration/python_metatrader5/mt5lasterror_py
            else:
                logger.error(f'failed to retrieve tick data from MT5 terminal for {symbol} {resolution} data for '
                             f'time range of {cur_start_time} to {cur_end_time}, mt5 error:\n{res}')
                if retries < 3:
                    retries += 1
                    logger.error('retrying...')
                    continue

            # temporarily save data from mt5 terminal because it can
            # sometimes run out of memory if RAM is close to max
            if not PACKAGE_TEMP_DIR.is_dir():
                PACKAGE_TEMP_DIR.mkdir(exist_ok=True, parents=True)
            if cur_ticks_batch is not None and len(cur_ticks_batch) > 0:
                temp_cache_path = PACKAGE_TEMP_DIR / f'temp_mt5_{symbol}_{resolution}_' \
                                                     f'{cur_start_time.strftime(dt_save_form)}_to_' \
                                                     f'{cur_end_time.strftime(dt_save_form)}.npy'
                np.save(temp_cache_path, cur_ticks_batch)
                files_to_merge.append(temp_cache_path)

            cur_start_time += time_skip
            retries = 0

        if len(files_to_merge) > 0:
            ticks = np.load(files_to_merge[0])
            if len(files_to_merge) > 1:
                print('starting to concatenate all downloaded data...')
                for i, file_path in enumerate(files_to_merge[1:]):
                    ticks_to_append = np.load(file_path)
                    ticks = np.append(ticks, ticks_to_append, axis=0)
                    print(f'concatenated {i + 2}/{len(files_to_merge)} downloaded datasets')

            for file in files_to_merge:
                os.remove(file)
        else:
            logger.warn('no tick data retrieved, done.')
            return
    elif bar_start_pos is not None:
        if resolution == "tick":
            ticks = mt5.copy_ticks_from(symbol, bar_start_pos, bar_count, mt5.COPY_TICKS_ALL)
        else:
            ticks = mt5.copy_rates_from_pos(symbol, time_frame, bar_start_pos, bar_count)

    col_names = ticks.dtype.names
    ticks = ticks.tolist()
    formatted_ticks = [(datetime.fromtimestamp(row[0], tz=timezone.utc), *row[1:]) for row in ticks]

    ticks_df = pd.DataFrame(formatted_ticks, columns=col_names)

    # remove any duplicate datetimes (most likely caused by using timedelta)
    ticks_df.drop_duplicates(subset=[MT5_TIME_COL_NAME], inplace=True)

    ticks_df.to_csv(filepath, index=False)
    print(f'saved {len(ticks_df)} rows of tick data to {filepath}')

    return str(filepath)
