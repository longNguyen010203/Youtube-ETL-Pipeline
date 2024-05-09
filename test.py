import pandas as pd

path = "/home/longnguyen/Documents/Coding/FDE-Course-2024/Project-Day/Youtube-ETL-Pipeline/linkVideos_trending_data.pq"
df = pd.read_parquet(path)

print(df.shape)
print(df)