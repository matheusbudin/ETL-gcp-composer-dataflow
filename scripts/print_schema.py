import pandas as pd
import os

# Replace 'your_file.parquet' with the actual path to your Parquet file
parquet_file = os.environ.get("PARQUET_PATH")

# Read the Parquet file into a DataFrame
df = pd.read_parquet(parquet_file)

# Print the first three rows
print(df.head(3))
