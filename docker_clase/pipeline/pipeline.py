"""First simple pipeline to run docker"""

import sys
print("arguments", sys.argv)

#Especificar siempre un argumento al invocar la función en bash como python pipeline.py argumento para que se pase en el argv
day = int(sys.argv[1])
print(f"Running pipeline for day {day}")

import pandas as pd

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")