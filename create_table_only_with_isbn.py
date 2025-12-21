import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    args = parser.parse_args()
    filename = args.file
    df = pd.read_excel(filename, header=0)
    df_isbn = df[
        df["ISBN"].notna() &
        (df["ISBN"].astype(str).str.strip() != "")
    ]

    print(df_isbn.shape[0])
    df_isbn.to_excel("dev_data/isbn_only.xlsx", index=False)

if __name__ == "__main__":
    main()