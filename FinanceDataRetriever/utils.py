import json
import os
from pathlib import Path
import logging


class Logger:
    __logger_instance = None

    @staticmethod
    def get_instance(logger_name):
        if Logger.__logger_instance is None:
            logging.basicConfig(format=('%(filename)s: '
                                        '%(levelname)s: '
                                        '%(funcName)s(): '
                                        '%(lineno)d:\t'
                                        '%(message)s'),
                                encoding="utf-8")
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


def request_input_value(name, hint="", required_num_type=None):
    if hint:
        hint = f" ({hint})"
    print(f"Please enter {name}{hint}:")
    value = input()
    while required_num_type and type(value) != required_num_type:
        try:
            value = required_num_type(value)
            break
        except ValueError:
            print(f"Invalid input for {name}, must be of type {required_num_type}.\nPlease enter {name}{hint}:")
        value = input()
    return value
