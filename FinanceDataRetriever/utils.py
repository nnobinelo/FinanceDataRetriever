import json
import os
from pathlib import Path
import logging
from datetime import datetime
from getpass import getpass


class Logger:
    __logger_instance = None

    @staticmethod
    def get_instance(logger_name):
        if Logger.__logger_instance is None:
            logging.basicConfig(format=("%(filename)s: "
                                        "%(levelname)s: "
                                        "%(funcName)s(): "
                                        "%(lineno)d:\t"
                                        "%(message)s"),
                                handlers=[logging.FileHandler(filename=f"./{logger_name}.log",
                                                              encoding='utf-8', mode='a+')])
            Logger.__logger_instance = logging.getLogger(logger_name)
        return Logger.__logger_instance


def get_config_variables(var_names, config_filepath):
    env_variables = os.environ
    config_file_variables = {}
    if Path(config_filepath).is_file():
        with open(config_filepath) as config:
            config_file_variables = json.load(config)
    variables = {}
    for var in var_names:
        if var in config_file_variables:
            variables[var] = config_file_variables[var]
        elif var in env_variables:
            variables[var] = env_variables[var]
    return variables


def request_input_value(name, hint="", required_num_type=None, password=False):
    if hint:
        hint = f" ({hint})"
    print(f"Please enter {name}{hint}:")
    if not password:
        value = input()
    else:
        value = getpass(prompt="")
    while required_num_type and type(value) != required_num_type:
        try:
            value = required_num_type(value)
            break
        except ValueError:
            print(f"Invalid input for {name}, must be of type {required_num_type}.\nPlease enter {name}{hint}:")
        if not password:
            value = input()
        else:
            value = getpass(prompt="")
    return value


def from_iso_format(iso_str):
    """Expected iso format (or subset): YYYY-MM-DDThh:mm:ss.s+hhmm"""
    iso_format = "%Y-%m-%dT%H:%M:%S.%f"
    iso_str_len_to_datetime_fmt_len = {4: 2, 7: 5, 10: 8, 13: 11, 16: 14, 19: 17}
    tz_present = True if len(iso_str) > 4 and (iso_str[-5] == "+" or iso_str[-5] == "-") else False
    iso_str_len = len(iso_str) if not tz_present else len(iso_str) - 5
    fmt_len = len(iso_format)  # everything
    if iso_str_len in iso_str_len_to_datetime_fmt_len:
        fmt_len = iso_str_len_to_datetime_fmt_len[iso_str_len]  # no decimal seconds
    iso_format = iso_format[:fmt_len]
    if tz_present:
        iso_format = iso_format + "%z"  # add timezone
    return datetime.strptime(iso_str, iso_format)
