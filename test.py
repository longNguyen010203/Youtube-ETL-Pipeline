import pandas as pd


df = pd.read_parquet("202010.pq")
print()
print(df[["categoryId", "view_count", "likes", "dislikes","comment_count"]].head(10))