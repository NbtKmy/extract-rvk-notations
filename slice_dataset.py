import pandas as pd


df = pd.read_excel("./dev_data/isbn_only.xlsx")

# Shuffle the dataframe
df_shuffled = df.sample(frac=1, random_state=42)

# Chunk the dataframe into pieces of 5000 rows each
chunk_size = 5000
chunks = [df_shuffled.iloc[i : i + chunk_size] for i in range(0, len(df_shuffled), chunk_size)]


for i, chunk in enumerate(chunks):
    print(f"--- Chank Num {i+1} ---")
    print(f"Num of Rows: {len(chunk)}")
    # Save each chunk to a separate CSV file with the original index
    chunk.to_csv(f"./dev_data/chunk_{i}.csv", index=True)




