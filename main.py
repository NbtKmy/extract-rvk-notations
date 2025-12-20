import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import time
import re

# DNB SRU
DNB_ENDPOINT = "https://services.dnb.de/sru/dnb?version=1.1&operation=searchRetrieve&query=marcxml.isbn="
DNB_SUFFIX = "&recordSchema=MARC21-xml&maximumRecords=1"

# B3Kat SRU
B3KAT_ENDPOINT = "http://bvbr.bib-bvb.de:5661/bvb01sru?version=1.1&recordSchema=marcxml&operation=searchRetrieve&query=marcxml.isbn="
B3KAT_SUFFIX = "&maximumRecords=1"

# Swisscovery
SLSP_ENDPOINT = "https://swisscovery.slsp.ch/view/sru/41SLSP_NETWORK?version=1.2&operation=searchRetrieve&recordSchema=marcxml&query=alma.isbn="
SLSP_SUFFIX = "&maximumRecords=1"

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


def extract_rvk_name (string):
    rvk_notation = string.replace(" ", "+")
    rvk_query = RVK_API + rvk_notation + RVK_SUFFIX
    try:
        r = requests.get(rvk_query)
        r.raise_for_status()
        json_res = r.json()
        time.sleep(1) # RVK API nicht überlasten
        # print(json_res)
        if "node" in json_res:
            return json_res["node"]["benennung"]
        else:
            return "Unknown RVK Notation"

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


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
    title_field = soup.find("datafield", tag="245")
    if title_field:
        subfields = title_field.find_all("subfield")
        title_parts = []
        for sf in subfields:
            if sf.get("code") in ["a", "b"]:
                title_parts.append(sf.text.strip())
        title = " : ".join(title_parts).strip()

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
    return title, rvk_notations

def metadata_query (bib_verbund, isbn):
    url_prefix = endpoint_dic[bib_verbund][0]
    url_suffix = endpoint_dic[bib_verbund][1]
    url_query = url_prefix + str(isbn) + url_suffix

    try:
        metadata_res = requests.get(url_query)
        metadata_res.raise_for_status()
        title, rvk_notations = extract_rvk(metadata_res.text)
        return title, rvk_notations

    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

def extract_metadata (isbn):
    isbn_entry = {
        "isbn": isbn,
        "dnb_title": None,
        "dnb_rvk_notations": [],
        "b3kat_title": None,
        "b3kat_rvk_notations": [],
        "slsp_title": None,
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
        t_B3KAT, rvk_ns_B3KAT = metadata_query("B3KAT", isbn)
        isbn_entry["b3kat_title"] = t_B3KAT
        isbn_entry["b3kat_rvk_notations"] = rvk_ns_B3KAT
    except SystemExit as e:
        print(f"Error querying B3KAT for ISBN {isbn}: {e}")

    # Query SLSP
    try:
        t_SLSP, rvk_ns_SLSP = metadata_query("SLSP", isbn)
        isbn_entry["slsp_title"] = t_SLSP
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()
    filename = args.file
    outputfile = args.output

    df = pd.read_excel(filename, header=0)

    cautions = []
    all_isbn_data = []
    for row in df.itertuples():
        if row.Index % 10 == 0:
            print(f"Processing record {row.Index}...")
        
        if pd.isna(row.ISBN):
            all_isbn_data.append(None)
            cautions.append("Keine ISBN vorhanden")
            continue

        isbn_list = str(row.ISBN).split(";")
        cleaned_isbn_list = [isbn.strip() for isbn in isbn_list if isbn.strip()]
        isbn = cleaned_isbn_list[0]
        isbn_data = extract_metadata(isbn)
        all_isbn_data.append(isbn_data)
        cautions.append("")

    print("Data Collected!")
    collected_data_df = pd.DataFrame(all_isbn_data)
    collected_cautions = pd.DataFrame(cautions, columns=["caution"])
    collected_data_df = pd.concat([df, collected_data_df, collected_cautions], axis=1)
    collected_data_df.to_csv("./dev_data/collected_data_raw.csv", index=False)

    consolidated_isbn_data = []
    for isbn_entry in  all_isbn_data:
        consolidated_entry = {
            "consolidated_title": None,
            "unique_rvk_notations": None,
        }
        if isbn_entry is None:
            consolidated_isbn_data.append(consolidated_entry)
            continue

        consolidated_title = None
        if isbn_entry["dnb_title"] is not None:
            consolidated_title = isbn_entry["dnb_title"]
        elif isbn_entry["b3kat_title"] is not None:
            consolidated_title = isbn_entry["b3kat_title"]
        elif isbn_entry["slsp_title"] is not None:
            consolidated_title = isbn_entry["slsp_title"]

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
        }
        consolidated_isbn_data.append(consolidated_entry)

    df_consolidated = pd.DataFrame(consolidated_isbn_data)
    df_rvk = df_consolidated[[ "consolidated_title", "unique_rvk_notations"]]
    # df_rvk.to_csv("./data/extracted_rvk_data.csv", index=False)

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
        print(row.unique_rvk_notations)
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


if __name__ == "__main__":
    main()
