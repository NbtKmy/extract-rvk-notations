import requests
from bs4 import BeautifulSoup
import pandas as pd
import argparse
import time

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
        time.sleep(0.5) # RVK API nicht überlasten
        return json_res["node"]["benennung"]

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    args = parser.parse_args()
    filename = args.file
    df = pd.read_excel(filename)
    isbn_column = df["isbn"]

    # Datensammeln
    all_isbn_data = []
    for isbn in isbn_column:
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
        try:
            t_DNB, rvk_ns_DNB = metadata_query("DNB", isbn)
            isbn_entry["dnb_title"] = t_DNB
            isbn_entry["dnb_rvk_notations"] = rvk_ns_DNB
        except SystemExit as e:
            print(f"Error querying DNB for ISBN {isbn}: {e}")

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

        time.sleep(1) # Zwischen den ISBN-Abfragen eine Pause einlegen
        all_isbn_data.append(isbn_entry)
    
    print("Data Collected!")
    
    # Konsolidieren der Daten
    consolidated_isbn_data = []
    for isbn_entry in all_isbn_data:
        # Consolidate title
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
            "isbn": isbn_entry["isbn"],
            "consolidated_title": consolidated_title,
            "unique_rvk_notations": unique_rvk_notations,
            "dnb_title": isbn_entry["dnb_title"],
            "b3kat_title": isbn_entry["b3kat_title"],
            "slsp_title": isbn_entry["slsp_title"],
            "dnb_rvk_notations": isbn_entry["dnb_rvk_notations"],
            "b3kat_rvk_notations": isbn_entry["b3kat_rvk_notations"],
            "slsp_rvk_notations": isbn_entry["slsp_rvk_notations"]
        }
        consolidated_isbn_data.append(consolidated_entry)

    # DF vorbereiten und Ausgeben
    for entry in consolidated_isbn_data:
        entry["has_dnb_title"] = entry["dnb_title"] is not None
        entry["has_b3kat_title"] = entry["b3kat_title"] is not None
        entry["has_slsp_title"] = entry["slsp_title"] is not None
        entry["has_dnb_rvk"] = len(entry["dnb_rvk_notations"]) > 0
        entry["has_b3kat_rvk"] = len(entry["b3kat_rvk_notations"]) > 0
        entry["has_slsp_rvk"] = len(entry["slsp_rvk_notations"]) > 0

    df_consolidated = pd.DataFrame(consolidated_isbn_data)

    df_final = df_consolidated[[
        "isbn",
        "consolidated_title",
        "unique_rvk_notations",
        "has_dnb_title",
        "has_b3kat_title",
        "has_slsp_title",
        "has_dnb_rvk",
        "has_b3kat_rvk",
        "has_slsp_rvk"
    ]]

    df_final.to_csv("./data/result.csv")

if __name__ == "__main__":
    main()
