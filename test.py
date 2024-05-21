import pandas as pd

path = "/home/longnguyen/Documents/Coding/FDE-Course-2024/Project-Day/Youtube-ETL-Pipeline/dataset/youTube_trending_video/RU_youtube_trending_data.csv"

df = pd.read_csv(path)

df = df.drop("description", axis=1)

df.to_csv(path, index=False)