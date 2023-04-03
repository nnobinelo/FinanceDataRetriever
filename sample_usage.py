from FinanceDataRetriever import forex

symbol = "EURUSD"
resolution = "h1"
start_iso_time = "2010-03-26"
end_iso_time = "2023-03-31"

mt5_data_path = forex.download_mt5_data(symbol, resolution, start_iso_time, end_iso_time)

forex.convert_mt5_data_to_lean_fmt(mt5_data_path, symbol, resolution, lean_cli_dir="C:\\quantconnect_lean\\lean_py38")
