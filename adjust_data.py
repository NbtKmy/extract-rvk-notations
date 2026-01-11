import pandas as pd

def adjust_data():
    # Load the dataset
    df = pd.read_csv("./dev_data/chunk_0.csv")
    df_original_cols = df[["MMS Id", "Publisher", "Publication Date", "Künftiger Standort"]].copy()

    df_consolidated = pd.read_csv("./dev_data/rvk_chunk_0.csv")
    df_rvk = pd.concat([
        df_original_cols.reset_index(drop=True),
        df_consolidated.reset_index(drop=True)
    ], axis=1)
    
    df_rvk.to_csv("./dev_data/rvk_chunk_0_v2", index=False)

if __name__ == "__main__":
    adjust_data()

