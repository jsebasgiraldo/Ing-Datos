"""@bruin

name: ingestion.trips
type: python
images: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.

import json
import os
from datetime import datetime
from typing import List, Tuple
import io
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data/"

def _iter_month_starts(start_date, end_date):
    """Yield the first day of each month from start_date to end_date (inclusive).

    Both inputs are date objects.
    """
    cur = start_date.replace(day=1)
    last = end_date.replace(day=1)
    while cur <= last:
        yield cur
        cur = (cur + relativedelta(months=1)).replace(day=1)

def materialize():
  """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
  # Read window and variables from environment
  start_date_s = os.environ.get("BRUIN_START_DATE")
  end_date_s = os.environ.get("BRUIN_END_DATE")
  vars_json = os.environ.get("BRUIN_VARS")

  if not start_date_s or not end_date_s:
      raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be provided")

  start_date = datetime.strptime(start_date_s, "%Y-%m-%d").date()
  end_date = datetime.strptime(end_date_s, "%Y-%m-%d").date()

  taxi_types = ["yellow"]
  if vars_json:
      try:
          parsed = json.loads(vars_json)
          if isinstance(parsed, dict) and "taxi_types" in parsed:
              taxi_types = parsed["taxi_types"]
          elif isinstance(parsed, list):
              taxi_types = parsed
      except Exception:
          # fallback: try to read as simple JSON list
          try:
              parsed = json.loads(vars_json)
              if isinstance(parsed, list):
                  taxi_types = parsed
          except Exception:
              pass

  dfs = []
  for taxi in taxi_types:
      for month_start in _iter_month_starts(start_date, end_date):
          year = month_start.year
          month = month_start.month
          file_name = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
          url = BASE_URL + file_name
          try:
              resp = requests.get(url, timeout=30)
              if resp.status_code != 200:
                  print(f"[ingestion.trips] WARNING: {url} returned {resp.status_code}")
                  continue
              buf = io.BytesIO(resp.content)
              # Use pandas with pyarrow engine to read parquet bytes
              df = pd.read_parquet(buf, engine="pyarrow")
              df["taxi_type"] = taxi
              df["extracted_at"] = datetime.utcnow().isoformat()
              dfs.append(df)
              print(f"[ingestion.trips] fetched {file_name} rows={len(df)}")
          except Exception as e:
              print(f"[ingestion.trips] ERROR fetching {url}: {e}")
              continue

  if not dfs:
      # return empty dataframe with no rows
      return pd.DataFrame()

  result = pd.concat(dfs, ignore_index=True)
  return result



def generate_endpoints(start_date: str, end_date: str, taxi_types: List[str]) -> List[Tuple[str, str]]:
    """
    Generate a list of (taxi_type, endpoint_url) tuples for the given date range and taxi types.
    """
    endpoints = []
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    current_dt = start_dt
    while current_dt <= end_dt:
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{current_dt.strftime('%Y-%m')}.parquet"
            url = f"{BASE_URL}{filename}"
            endpoints.append((taxi_type, url))
        current_dt += relativedelta(months=1)

    return endpoints

def fetch_and_parse_data(endpoints: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Fetch data from each endpoint, parse into DataFrames, and concatenate.
    """
    dataframes = []
    for taxi_type, url in endpoints:
        try:
            df = pd.read_parquet(url)
            df['taxi_type'] = taxi_type  # Add taxi type column for lineage
            df['extracted_at'] = datetime.utcnow()  # Add extraction timestamp
            dataframes.append(df)
        except Exception as e:
            print(f"Error fetching/parsing {url}: {e}")

    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        return pd.DataFrame()