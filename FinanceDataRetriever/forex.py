from FinanceDataRetriever import CONFIG_FILEPATH, MT5_LOGIN_ID, MT5_LOGIN_PASSWORD, MT5_TRADE_SERVER, \
    MT5_TERMINAL_PATH, MT5_TIMEFRAMES, FOREX_DATA_DIR, PACKAGE_TEMP_DIR, MT5_TIME_COL_NAME, MT5_TICK_TIMEFRAME_NAME, \
    MT5_OPEN_COL_NAME, MT5_TICK_VOLUME_COL_NAME, MT5_CLOSE_COL_NAME, MT5_LOW_COL_NAME, MT5_HIGH_COL_NAME, \
    MT5_SPREAD_COL_NAME, MT5_BID_COL_NAME, MT5_ASK_COL_NAME, LEAN_FOREX_DATA_DIR_PATH, LEAN_FMT_MT5_DATA_DIR, \
    MT5_TF_TO_DAYS_SKIPPED
from FinanceDataRetriever import utils
import MetaTrader5 as mt5
from datetime import datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import os
import shutil

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
            password = True if "password" in var else False
            config_variables[var] = utils.request_input_value(var, required_num_type=required_num_type,
                                                              password=password)
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
        logger.error(f"Failed to initialize MT5 terminal, error:\n{mt5.last_error()}\n")
    return success


def download_mt5_data(symbol, resolution, start_datetime=None, end_datetime=None, bar_start_pos=None, bar_count=None,
                      download_dir=None, save_in_lean_fmt=False):
    if mt5.terminal_info() is None:
        if not init_mt5():
            return

    if (start_datetime is None and end_datetime is None) and (bar_start_pos is None and bar_count is None):
        logger.error(
            "Either start_datetime & end_datetime or bar_start_pos & bar_count must be specified to download mt5 data")
        return

    if (start_datetime is not None and end_datetime is None) or (start_datetime is None and end_datetime is not None):
        logger.error("Both start_datetime and end_datetime must be specified to download mt5 data")
        return

    if (bar_start_pos is not None and bar_count is None) or (bar_start_pos is None and bar_count is not None):
        logger.error("Both bar_start_pos and bar_count must be specified to download mt5 data")
        return

    resolution = resolution.lower()
    if resolution not in MT5_TIMEFRAMES:
        logger.error(f"\"{resolution}\" is not a valid FX data time frame")
        return

    time_frame = MT5_TIMEFRAMES[resolution]

    if download_dir is None:
        download_dir = FOREX_DATA_DIR
    download_dir = Path(download_dir)
    if not Path(download_dir).is_dir():
        Path(download_dir).mkdir(exist_ok=True, parents=True)

    dt_save_form = "%Y-%m-%dT%H;%M%Z"
    server = mt5.account_info().server
    if start_datetime is not None:
        start_datetime = utils.from_iso_format(start_datetime)
        start_datetime = datetime(start_datetime.year, start_datetime.month, start_datetime.day,
                                  hour=start_datetime.hour, minute=start_datetime.minute, second=start_datetime.second,
                                  tzinfo=timezone.utc)
        cur_start_time = start_datetime

        end_datetime = utils.from_iso_format(end_datetime)
        end_datetime = datetime(end_datetime.year, end_datetime.month, end_datetime.day, hour=end_datetime.hour,
                                minute=end_datetime.minute, second=end_datetime.second, tzinfo=timezone.utc)

        filepath = download_dir / f"mt5_{server}_{symbol}_{resolution}_{start_datetime.strftime(dt_save_form)}" \
                                  f"_to_{end_datetime.strftime(dt_save_form)}.csv"
    elif bar_start_pos is not None:
        current_utc_dt = datetime.now(tz=timezone.utc)
        filepath = download_dir / f"mt5_{server}_{symbol}_{resolution}_{bar_count}_bars_from_{bar_start_pos}_bar" \
                                  f"_on_{current_utc_dt.strftime(dt_save_form)}.csv"

    days_to_skip = MT5_TF_TO_DAYS_SKIPPED["default"]
    if resolution in MT5_TF_TO_DAYS_SKIPPED:
        days_to_skip = MT5_TF_TO_DAYS_SKIPPED[resolution]
    time_skip = timedelta(days=days_to_skip)
    retries = 0
    max_retries = 3
    files_to_merge = []
    ticks = None

    if start_datetime is not None:
        while cur_start_time < end_datetime:
            cur_end_time = cur_start_time + time_skip
            cur_end_time = min((cur_end_time - cur_start_time, cur_end_time),
                               (end_datetime - cur_start_time, end_datetime), key=lambda tup: tup[0])[1]
            if resolution == MT5_TICK_TIMEFRAME_NAME:
                cur_ticks_batch = mt5.copy_ticks_range(symbol, cur_start_time, cur_end_time, mt5.COPY_TICKS_ALL)
            else:
                cur_ticks_batch = mt5.copy_rates_range(symbol, time_frame, cur_start_time, cur_end_time)
            if cur_ticks_batch is not None:
                if len(cur_ticks_batch) == 0:
                    logger.warn(f"no tick data returned by mt5 terminal for time range of {cur_start_time} "
                                f"to {cur_end_time}")

            res = mt5.last_error()
            if res[0] == 1:
                print(f"successfully retrieved {len(cur_ticks_batch)} rows of tick data "
                      f"for time range of {cur_start_time} to {cur_end_time}")
            # "No IPC connection" error
            elif res[0] == -10004:
                logger.error(f"{res} lost connection to mt5 terminal")
                if retries < 3:
                    logger.error("retrying...")
                    if not init_mt5():
                        return
                    retries += 1
                    continue
            # any other mt5 error: https://www.mql5.com/en/docs/integration/python_metatrader5/mt5lasterror_py
            else:
                logger.error(f"failed to retrieve tick data from MT5 terminal for {symbol} {resolution} data for "
                             f"time range of {cur_start_time} to {cur_end_time}, mt5 error:\n{res}")
                if retries < max_retries:
                    retries += 1
                    logger.error("retrying...")
                    continue

            # temporarily save data from mt5 terminal because it can
            # sometimes run out of memory if RAM is close to max
            if not PACKAGE_TEMP_DIR.is_dir():
                PACKAGE_TEMP_DIR.mkdir(exist_ok=True, parents=True)
            if cur_ticks_batch is not None and len(cur_ticks_batch) > 0:
                temp_cache_path = PACKAGE_TEMP_DIR / f"temp_mt5_{symbol}_{resolution}_" \
                                                     f"{cur_start_time.strftime(dt_save_form)}_to_" \
                                                     f"{cur_end_time.strftime(dt_save_form)}.npy"
                np.save(temp_cache_path, cur_ticks_batch)
                files_to_merge.append(temp_cache_path)

            cur_start_time += time_skip
            retries = 0

        if len(files_to_merge) > 0:
            ticks = np.load(files_to_merge[0])
            if len(files_to_merge) > 1:
                print("starting to concatenate all downloaded data...")
                for i, file_path in enumerate(files_to_merge[1:]):
                    ticks_to_append = np.load(file_path)
                    ticks = np.append(ticks, ticks_to_append, axis=0)
                    print(f"concatenated {i + 2}/{len(files_to_merge)} downloaded datasets")

            for file in files_to_merge:
                os.remove(file)
        else:
            logger.warn("no tick data retrieved, done.")
            return
    elif bar_start_pos is not None:
        if resolution == MT5_TICK_TIMEFRAME_NAME:
            ticks = mt5.copy_ticks_from(symbol, bar_start_pos, bar_count, mt5.COPY_TICKS_ALL)
        else:
            ticks = mt5.copy_rates_from_pos(symbol, time_frame, bar_start_pos, bar_count)

    col_names = ticks.dtype.names
    datetimes = [datetime.fromtimestamp(row[0], tz=timezone.utc) for row in ticks]
    ticks_df = pd.DataFrame(ticks, columns=col_names)
    ticks_df.insert(0, "datetime", datetimes)

    # remove any duplicate datetimes (most likely caused by using timedelta)
    ticks_df.drop_duplicates(subset=[MT5_TIME_COL_NAME], inplace=True)

    ticks_df.to_csv(filepath, index=False)
    print(f"saved {len(ticks_df)} rows of tick data to {filepath}")

    if save_in_lean_fmt:
        convert_mt5_data_to_lean_fmt(filepath, symbol, resolution, num_cur_pair_decimals=mt5.symbol_info(symbol).digits)

    return str(filepath)


def convert_mt5_data_to_lean_fmt(mt5_data_path, symbol, resolution, num_cur_pair_decimals="infer", lean_cli_dir=None,
                                 custom_save_dir=None):
    """Converts market data from mt5 to lean format specified here: https://github.com/QuantConnect/Lean/blob/master/Data/forex/readme.md"""
    resolution = resolution.lower()
    if resolution not in MT5_TIMEFRAMES:
        logger.error(f"\"{resolution}\" is not a valid FX data time frame")
        return
    if resolution[0] == "m":
        resolution = "minute"

    mt5_data_df = pd.read_csv(mt5_data_path)
    if num_cur_pair_decimals == "infer":
        price_col = MT5_OPEN_COL_NAME if MT5_OPEN_COL_NAME in mt5_data_df.columns else MT5_BID_COL_NAME
        decimal_counts = {}
        highest_count = 0
        for i, row in mt5_data_df.iterrows():
            price_str = str(row[price_col])
            decimal_point_idx = price_str.find(".")
            num_decimals = len(price_str) - decimal_point_idx - 1
            if num_decimals not in decimal_counts:
                decimal_counts[num_decimals] = 0
            decimal_counts[num_decimals] += 1
            if decimal_counts[num_decimals] > highest_count:
                highest_count = decimal_counts[num_decimals]
                num_cur_pair_decimals = num_decimals

    # minute format: [Time, Bid Open, Bid High, Bid Low, Bid Close, Last Bid Size, Ask Open, Ask High, Ask Low,
    #                 Ask Close, Last Ask Size]
    # tick format: [Time, Bid Price, Ask Price]
    spread_multiplier = (10 ** -(num_cur_pair_decimals - 1))
    dates = {}
    formatted_data = []
    for i, row in mt5_data_df.iterrows():
        half_ask_bid_diff = row[MT5_SPREAD_COL_NAME] * spread_multiplier / 2
        half_volume = row[MT5_TICK_VOLUME_COL_NAME] / 2  # bad estimate used for ask and bid volumes
        dt = datetime.fromtimestamp(row[MT5_TIME_COL_NAME], tz=timezone.utc)

        if resolution == MT5_TICK_TIMEFRAME_NAME or resolution[0] == "m":
            # make sure Time is an int value instead of floating point, otherwise period and tailing 0s will be counted
            # as additional zeros making time value larger once in lean engine.
            lean_time = int((dt.hour * 60 + dt.minute) * 60 * 1000)
        else:
            lean_time = dt.strftime("%Y%m%d %H:%M")

        if resolution == MT5_TICK_TIMEFRAME_NAME:
            lean_data_row = [lean_time, row[MT5_BID_COL_NAME], row[MT5_ASK_COL_NAME]]
        else:
            lean_data_row = [lean_time, row[MT5_OPEN_COL_NAME] - half_ask_bid_diff,
                             row[MT5_HIGH_COL_NAME] - half_ask_bid_diff, row[MT5_LOW_COL_NAME] - half_ask_bid_diff,
                             row[MT5_CLOSE_COL_NAME] - half_ask_bid_diff, half_volume,
                             row[MT5_OPEN_COL_NAME] + half_ask_bid_diff, row[MT5_HIGH_COL_NAME] + half_ask_bid_diff,
                             row[MT5_LOW_COL_NAME] + half_ask_bid_diff, row[MT5_CLOSE_COL_NAME] + half_ask_bid_diff,
                             half_volume]

        if resolution == MT5_TICK_TIMEFRAME_NAME or resolution[0] == "m":
            date = dt.strftime("%Y%m%d")
            if date not in dates:
                dates[date] = []
            dates[date].append(lean_data_row)
        else:
            formatted_data.append(lean_data_row)

    if lean_cli_dir is not None:
        save_dir = Path(lean_cli_dir) / LEAN_FOREX_DATA_DIR_PATH
        if resolution == MT5_TICK_TIMEFRAME_NAME:
            save_dir /= f"tick/{symbol.lower()}"
        elif resolution[0] == "m":
            save_dir /= f"minute/{symbol.lower()}"
        elif resolution[0] == "h":
            save_dir /= "hour"
        else:
            save_dir /= "daily"
    elif custom_save_dir is not None:
        save_dir = Path(custom_save_dir)
    else:
        save_dir = Path(LEAN_FMT_MT5_DATA_DIR)

    def create_zips(parent_dir, zip_filename, csv_filename, data_rows):
        temp_save_dir = PACKAGE_TEMP_DIR / zip_filename
        temp_save_dir.mkdir(parents=True, exist_ok=True)
        parent_dir.mkdir(parents=True, exist_ok=True)
        temp_save_file = temp_save_dir / csv_filename
        pd.DataFrame(data_rows).to_csv(temp_save_file, index=False, header=False)
        zip_filepath = parent_dir / zip_filename
        shutil.make_archive(str(zip_filepath), "zip", str(temp_save_dir))
        shutil.rmtree(temp_save_dir)

    if resolution == MT5_TICK_TIMEFRAME_NAME or resolution[0] == "m":
        for date in dates:
            create_zips(save_dir, f"{date}_quote", f"{date}_{symbol.lower()}_{resolution}_quote.csv", dates[date])
    else:
        create_zips(save_dir, symbol.lower(), f"{symbol.lower()}.csv", formatted_data)
