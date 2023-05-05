import os.path
from eutl_data_augmentation import (
    get_installation_coordinates_google,
    parse_leakage_lists,
    extract_nace_scheme,
    impute_orbis_identifiers,
)


def ingest_data(dir_parsed, dir_additional):
    """Ingest several types of data into the eutl data
    :param dir_in: <str> path to directory with incoming eutl data
    :param dir_additional: <str> path to directory with additional data
    """
    # add coordinates to installations
    fn_coordinates = dir_additional + "/installation_coordinates.csv"
    key_google = os.environ.get("GMAPKEY")
    if key_google:
        get_installation_coordinates_google(
            dir_parsed, key_google, fn_out=fn_coordinates, ignore_existing=False
        )
    else:
        print("---- No google api key provided")

    # extract nace classification
    fn_nace_scheme = dir_additional + "nace_scheme.csv"
    fn_nace_scheme_html = dir_additional + "leakage_list/NACE_REV2_20200427_154248.htm"
    extract_nace_scheme(fn_nace_scheme_html, fn_nace_scheme)

    # get nace classification of installations
    fn_leakage_2015 = dir_additional + "leakage_list/leakage_2015.xls"
    fn_leakage_2020 = dir_additional + "leakage_list/leakage_2020.xlsx"
    fn_nace_installations = dir_additional + "nace_leakage.csv"
    parse_leakage_lists(fn_leakage_2015, fn_leakage_2020, fn_out=fn_nace_installations)

    # impute orbis matching from JRC
    fn_jrc = dir_additional + "jrc_orbis_ids/JRC-EU ETS-FIRMS_V2_012022_public.xlsx"
    fn_acc = dir_parsed + "accounts.csv"
    fn_out = dir_parsed + "accounts_w_orbis.csv"
    impute_orbis_identifiers(fn_jrc, fn_acc, fn_out)


if __name__ == "__main__":
    dir_parsed = "./data/parsed/"  # directory with data as provided by scarper
    # directory for csv tables to be imported into the database
    dir_out = "./data/tables/"
    dir_final = "./data/final/"  # final directory for database export
    dir_additional = "./data/additional/"
    fn_nace_codes = "./data/additional/nace_all.csv"  # table with all nace codes
    # table with nace code of installations
    fn_nace = "./data/additional/nace_leakage.csv"
    # table with installation coordinates

    fn_nace_codes = "./data/additional/nace_all.csv"
    fn_ohaMatching = dir_final + "foha_matching.csv"

    ingest_data(dir_parsed, dir_additional)
