import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    parser.add_argument("-n", "--number", type=int, default=50)
    args = parser.parse_args()
    filename = args.file
    number_of_entries = args.number
    df = pd.read_excel(filename, header=0)
    testdata_df = df[0:number_of_entries]
    str_number = str(number_of_entries)
    print(f"Erstelle Testdatensatz mit {str_number} Einträgen...")
    testdata_df.to_excel(f"dev_data/testdata_isbn_{str_number}.xlsx", index=False)

if __name__ == "__main__":
    main()
