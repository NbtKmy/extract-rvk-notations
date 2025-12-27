import requests
import requests_cache
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import time
import re
import os
import logging
from datetime import datetime
from tqdm import tqdm
from dotenv import load_dotenv
import pytz

load_dotenv()

# DNB SRU
DNB_ENDPOINT = "https://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=marcxml.isbn="
DNB_SUFFIX = "&recordSchema=MARC21-xml&maximumRecords=1"

# B3Kat SRU
B3KAT_ENDPOINT = "http://bvbr.bib-bvb.de:5661/bvb01sru?version=1.1&recordSchema=marcxml&operation=searchRetrieve&query=marcxml.isbn="
B3KAT_SUFFIX = "&maximumRecords=1"

# Swisscovery
SLSP_ENDPOINT = "https://swisscovery.slsp.ch/view/sru/41SLSP_NETWORK?version=1.2&operation=searchRetrieve&recordSchema=marcxml&query=alma.isbn="
SLSP_SUFFIX = "&maximumRecords=1"

# Header für API-Endpunkte
my_contact = os.environ.get("MY_CONTACT")
HEADER = {
    "User-Agent": "Automatische Signatur-Erstellung mit RVK/1.0 (contact: " + my_contact + ")"
}

endpoint_dic = {
    "DNB": (DNB_ENDPOINT, DNB_SUFFIX),
    "B3KAT": (B3KAT_ENDPOINT, B3KAT_SUFFIX),
    "SLSP": (SLSP_ENDPOINT, SLSP_SUFFIX)
} 

# RVK API
# Notation => SU+680
RVK_API = "https://rvk.uni-regensburg.de/api_neu/json/node/"
RVK_SUFFIX = "?json"

# RVK response in json-Format
# {
#   "host": "rvk.uni-regensburg.de",
#   "request": "/api_neu/json/node/SU+680?json",
#   "node": {
#     "notation": "SU 680",
#     "benennung": "IBM",
#     "bemerkung": "",
#     "verweis": "",
#     "has_children": "no",
#     "register": [
#         "Goethe Utilities",
#         "Master Key"
#       ]
#     },
#   "root_name": "UB Regensburg"
# }


def extract_rvk_name (string, max_retries=3, timeout=30):
    rvk_notation = string.replace(" ", "+")
    rvk_query = RVK_API + rvk_notation + RVK_SUFFIX

    for attempt in range(max_retries):
        try:
            r = requests.get(rvk_query, headers=HEADER, timeout=timeout)
            r.raise_for_status()
            json_res = r.json()
            time.sleep(1) # RVK API nicht überlasten
            # print(json_res)
            if "node" in json_res:
                return json_res["node"]["benennung"]
            else:
                return "Unknown RVK Notation"

        except requests.exceptions.Timeout as err:
            logging.warning(f"Timeout error for RVK notation {string} (attempt {attempt+1}/{max_retries}): {err}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Failed after {max_retries} attempts for RVK notation {string}")
                return "Unknown RVK Notation (Timeout)"

        except requests.exceptions.RequestException as err:
            logging.error(f"Request error for RVK notation {string}: {err}")
            return "Unknown RVK Notation (Error)"


# RVK-Notation herausfiltern (es gibt mehrfach RVK-Notation)
#
# <datafield tag="245" ind1="1" ind2="0">
#   <subfield code="a">100 Jahre Institut für Ethnologie der Universität Leipzig</subfield>
#   <subfield code="b">eine Anthologie seiner Vertreter</subfield>
#   <subfield code="c">Katja Geisenhainer ; Lothar Bohrmann ; Berhard Streck (Hg.)</subfield>
# </datafield>
# <datafield tag="084" ind1=" " ind2=" ">
#   <subfield code="a">LB 15080</subfield>
#   <subfield code="0">(DE-625)90509:797</subfield>
#   <subfield code="2">rvk</subfield>
# </datafield>

def extract_rvk (xml):
    soup = BeautifulSoup(xml, "xml")

    title = None
    author = None
    title_field = soup.find("datafield", tag="245")
    if title_field:
        subfields = title_field.find_all("subfield")
        title_parts = []
        author_parts = []
        for sf in subfields:
            if sf.get("code") in ["a", "b"]:
                title_parts.append(sf.text.strip())
            if sf.get("code") == "c":
                author_parts.append(sf.text.strip())
        title = " : ".join(title_parts).strip()
        author = "; ".join(author_parts).strip()

    rvk_notations = []
    rvk_fields = soup.find_all("datafield", tag="084")
    for rvk_field in rvk_fields:
        for subfield_2 in rvk_field.find_all("subfield", code="2"):
            if subfield_2.text == "rvk":
                rvk_notation_subfield = rvk_field.find("subfield", code="a")
                if rvk_notation_subfield:
                    rvk_notation = rvk_notation_subfield.text.strip()
                    rvk_benennung = extract_rvk_name(rvk_notation)
                    rvk_notations.append((rvk_notation, rvk_benennung))
    return title, author, rvk_notations

def metadata_query (bib_verbund, isbn, max_retries=3, timeout=30):
    url_prefix = endpoint_dic[bib_verbund][0]
    url_suffix = endpoint_dic[bib_verbund][1]
    url_query = url_prefix + str(isbn) + url_suffix

    for attempt in range(max_retries):
        try:
            metadata_res = requests.get(url_query, headers=HEADER, timeout=timeout)
            metadata_res.raise_for_status()
            title, author, rvk_notations = extract_rvk(metadata_res.text)
            return title, author, rvk_notations

        except requests.exceptions.Timeout as err:
            logging.warning(f"Timeout error for {bib_verbund} ISBN {isbn} (attempt {attempt+1}/{max_retries}): {err}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Failed after {max_retries} attempts for {bib_verbund} ISBN {isbn}")
                raise SystemExit(err)

        except requests.exceptions.RequestException as err:
            logging.error(f"Request error for {bib_verbund} ISBN {isbn}: {err}")
            raise SystemExit(err)

def extract_metadata (isbn):
    isbn_entry = {
        "isbn": isbn,
        "dnb_title": None,
        "dnb_author": None,
        "dnb_rvk_notations": [],
        "b3kat_title": None,
        "b3kat_author": None,
        "b3kat_rvk_notations": [],
        "slsp_title": None,
        "slsp_author": None,
        "slsp_rvk_notations": []
    }

    # Query DNB
    """
    try:
        t_DNB, rvk_ns_DNB = metadata_query("DNB", isbn)
        isbn_entry["dnb_title"] = t_DNB
        isbn_entry["dnb_rvk_notations"] = rvk_ns_DNB
    except SystemExit as e:
        print(f"Error querying DNB for ISBN {isbn}: {e}")
    """
    
    # Query B3KAT
    try:
        t_B3KAT, a_B3KAT, rvk_ns_B3KAT = metadata_query("B3KAT", isbn)
        isbn_entry["b3kat_title"] = t_B3KAT
        isbn_entry["b3kat_author"] = a_B3KAT
        isbn_entry["b3kat_rvk_notations"] = rvk_ns_B3KAT
    except SystemExit as e:
        print(f"Error querying B3KAT for ISBN {isbn}: {e}")

    # Query SLSP
    try:
        t_SLSP, a_SLSP, rvk_ns_SLSP = metadata_query("SLSP", isbn)
        isbn_entry["slsp_title"] = t_SLSP
        isbn_entry["slsp_author"] = a_SLSP
        isbn_entry["slsp_rvk_notations"] = rvk_ns_SLSP
    except SystemExit as e:
        print(f"Error querying SLSP for ISBN {isbn}: {e}")

    time.sleep(3) # Zwischen den ISBN-Abfragen eine Pause einlegen
    
    return isbn_entry

def extract_year(text):
    if not text:
        return None

    m = re.search(r"(18|19|20)\d{2}", text)
    return m.group(0) if m else None

def check_b3kat_maintenance_window():
    """
    B3kat Wartungszeit CET 5:00-5:30
    Von CET 4:59 bis 5:31 prüfen und ggf. warten
    """
    cet = pytz.timezone('Europe/Berlin')
    current_time_cet = datetime.now(cet)
    current_hour = current_time_cet.hour
    current_minute = current_time_cet.minute

    # CET 4:59～5:30 für B3kat Wartungsfenster prüfen
    if (current_hour == 4 and current_minute >= 59) or (current_hour == 5 and current_minute < 31):
        logging.warning(f"B3Kat maintenance window detected. Current time: {current_time_cet.strftime('%H:%M:%S CET')}")

        # By 5:31 warten
        target_hour = 5
        target_minute = 31
        wait_minutes = (target_hour * 60 + target_minute) - (current_hour * 60 + current_minute)

        if wait_minutes > 0:
            logging.info(f"Sleeping for {wait_minutes} minutes until CET 5:31...")
            time.sleep(wait_minutes * 60)
            logging.info("Resuming after maintenance window")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-l", "--logfile", default="rvk_extraction.log", help="Log file path")
    args = parser.parse_args()
    filename = args.file
    outputfile = args.output
    logfile = args.logfile

    # Caching all API responses (RVK, B3Kat, SLSP)
    requests_cache.install_cache(
        'api_cache',
        backend='sqlite',
        expire_after=2592000  # Cache behalten für 30 Tage
    )

    # Logging konfigurieren
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logfile, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    # Startzeit erfassen
    start_time = datetime.now()
    logging.info(f"=== Starting RVK extraction ===")
    logging.info(f"Input file: {filename}")
    logging.info(f"Output file: {outputfile}")
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    file_extension = os.path.splitext(filename)[1].lower()
    basename_without_ext = os.path.splitext(os.path.basename(filename))[0]
    if file_extension == ".csv":
        df = pd.read_csv(filename, header=0)
    elif file_extension in [".xls", ".xlsx"]:
        df = pd.read_excel(filename, header=0)
    

    total_records = len(df)
    logging.info(f"Number of records: {total_records}")

    cautions = []
    all_isbn_data = []


    for row in tqdm(df.itertuples(), total=total_records, desc="Getting metadata for ISBNs"):
        # Check B3Kat maintenance window
        check_b3kat_maintenance_window()

        if pd.isna(row.ISBN):
            all_isbn_data.append(None)
            cautions.append("Keine ISBN vorhanden")
            logging.debug(f"Record {row.Index}: ISBN unknown, skipping")
            continue

        isbn_list = str(row.ISBN).split(";")
        cleaned_isbn_list = [isbn.strip() for isbn in isbn_list if isbn.strip()]
        isbn = cleaned_isbn_list[0]
        logging.debug(f"Record {row.Index}: ISBN {isbn} processing")
        isbn_data = extract_metadata(isbn)
        all_isbn_data.append(isbn_data)
        cautions.append("")

    logging.info("Data collected!")
    collected_data_df = pd.DataFrame(all_isbn_data)
    collected_cautions = pd.DataFrame(cautions, columns=["caution"])
    collected_data_df = pd.concat([df, collected_data_df, collected_cautions], axis=1)
    collected_data_df.to_csv("./dev_data/collected_data_raw.csv", index=False)

    consolidated_isbn_data = []
    for isbn_entry in  all_isbn_data:
        consolidated_entry = {
            "consolidated_title": None,
            "unique_rvk_notations": None,
            "author": None,
        }
        if isbn_entry is None:
            consolidated_isbn_data.append(consolidated_entry)
            continue

        consolidated_title = None
        if isbn_entry["b3kat_title"] is not None:
            consolidated_title = isbn_entry["b3kat_title"]
        elif isbn_entry["slsp_title"] is not None:
            consolidated_title = isbn_entry["slsp_title"]
        elif isbn_entry["dnb_title"] is not None:
            consolidated_title = isbn_entry["dnb_title"]

        unique_rvk_notations_set = set()
        for rvk in isbn_entry["dnb_rvk_notations"]:
            unique_rvk_notations_set.add(rvk)
        for rvk in isbn_entry["b3kat_rvk_notations"]:
            unique_rvk_notations_set.add(rvk)
        for rvk in isbn_entry["slsp_rvk_notations"]:
            unique_rvk_notations_set.add(rvk)
        unique_rvk_notations = list(unique_rvk_notations_set)

        consolidated_entry = {
            "consolidated_title": consolidated_title,
            "unique_rvk_notations": unique_rvk_notations,
            "author": isbn_entry["b3kat_author"] if isbn_entry["b3kat_author"] is not None else isbn_entry["slsp_author"]
        }
        consolidated_isbn_data.append(consolidated_entry)

    df_consolidated = pd.DataFrame(consolidated_isbn_data)
    df_rvk = df_consolidated[[ "consolidated_title", "unique_rvk_notations", "author"]]
    # df_rvk.to_csv("./data/extracted_rvk_data.csv", index=False)

    # Collumns aus den Originaldaten übernehmen
    df_original_cols = df[["MMS Id", "Publisher", "Publication Date", "Künftiger Standort"]].copy()

    # Concat der DataFrames
    df_rvk = pd.concat([
        df_original_cols.reset_index(drop=True),
        df_consolidated[["consolidated_title", "unique_rvk_notations", "author"]].reset_index(drop=True)
    ], axis=1)
    # df_rvk.to_csv(f"./dev_data/{basename_without_ext}_result.csv", index=False)

    rvk_callnums_part1 = []
    for row in df_rvk.itertuples():
        index = row.Index
        
        if row.unique_rvk_notations is None:
            cautions[index] += "; Keine RVK-Notationen gefunden"
            rvk_callnums_part1.append(None)
            continue

        if row.consolidated_title != df.at[index, "Title"]:
            cautions[index] += "; Titelabweichung festgestellt"
        
        # Signatur mit RVK-Notation generieren
        # Hier zum Test nur die erste RVK-Notation verwenden
        # Struktur der Signatur: RVK-Notation + Publikationsjahr YYYY + Nummerus currens
        # print(row.unique_rvk_notations)
        if row.unique_rvk_notations is None or len(row.unique_rvk_notations) == 0:
            rvk_callnums_part1.append(None)
            continue

        first_rvk = row.unique_rvk_notations[0]
        rvk_notation = first_rvk[0].replace(" ", "")
        pub_year = str(df.at[index, "Publication Date"])
        # Die eckigen Klammern entfernen, falls vorhanden (z.B. [1999]. )
        pub_year = extract_year(pub_year)
        if pub_year is None:
            pub_year = "YYYY"
        rvk_sig_part1 = f"{rvk_notation} {pub_year}"
        rvk_callnums_part1.append(rvk_sig_part1)

    df_rvk["rvk_callnum_part1"] = rvk_callnums_part1
    df_rvk["caution"] = cautions

    df_rvk.to_csv(outputfile, index=False)

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)

    # Cache-Statistiken ausgeben
    cache_info = requests_cache.get_cache()
    logging.info(f"Cache stats: {len(cache_info.responses)} responses cached")

    logging.info(f"=== Task finished ===")
    logging.info(f"Endtime: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Duration: {int(hours)} H {int(minutes)} Min {int(seconds)} Sec")
    logging.info(f"Outputfile: {outputfile}")

    print(f"\n{'='*50}")
    print(f"Task finished!")
    print(f"Duration: {int(hours)} H, {int(minutes)} Min, {int(seconds)} Sec")
    print(f"Please see the logfile for further information: {logfile}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
