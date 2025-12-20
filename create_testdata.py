import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    args = parser.parse_args()
    filename = args.file
    df = pd.read_excel(filename, header=0)
    testdata_df = df[0:3000]
    print("Erstelle Testdatensatz mit 3000 Einträgen...")
    testdata_df.to_excel("dev_data/testdata_3000.xlsx", index=False)

if __name__ == "__main__":
    main()
