import pandas as pd

df = pd.read_parquet(
    "data/raw/2024/01/yellow_tripdata_2024-01.parquet"
)

print(df.head())
print(df.columns)