import os
import sys
import requests
import pyarrow as pa
import pyarrow.parquet as pq


url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
    "X-RapidAPI-Key": os.environ.get("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "cricbuzz-cricket.p.rapidapi.com"
}
params = {
    'formatType': 'test'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])
    parquet_filename = 'batsmen_rankings.parquet'

    if data:
        field_names = ['rank', 'name', 'country']

        # Create a PyArrow table from the data
        table = pa.Table.from_pydict({field: [entry.get(field) for entry in data] for field in field_names})

        # Write the table to a Parquet file
        pq.write_table(table, parquet_filename)

        print(f"Data fetched successfully and written to '{parquet_filename}'")
        print(f"data frame schema: "{})
    else:
        print("No data available from the API.")

else:
    print("Failed to fetch data:", response.status_code)
