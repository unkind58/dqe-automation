import os
import pandas as pd


class ParquetReader:
    def __init__(self):
        pass

    def process(self, path: str, include_subfolders: bool = False) -> pd.DataFrame:

        parquet_files = []

        if include_subfolders:
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith('.parquet'):
                        parquet_files.append(os.path.join(root, file))
        else:
            if os.path.isdir(path):
                for file in os.listdir(path):
                    if file.endswith('.parquet'):
                        parquet_files.append(os.path.join(path, file))
            elif path.endswith('.parquet') and os.path.isfile(path):
                parquet_files.append(path)

        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in {path}")

        # Read all found parquet files and concatenate them
        dataframes = [pd.read_parquet(f) for f in parquet_files]
        return pd.concat(dataframes, ignore_index=True)

