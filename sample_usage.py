from FinanceDataRetriever import forex

symbol = "EURUSD"
resolution = "m1"
start_iso_time = "2021-12-11"
end_iso_time = "2022-02-11"

mt5_data_path = forex.download_mt5_data(symbol, resolution, start_iso_time, end_iso_time)

forex.convert_mt5_data_to_lean_fmt(mt5_data_path, symbol, resolution, lean_cli_dir="C:\\quantconnect_lean_cli")
