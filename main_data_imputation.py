import os.path
from eutl_data_augmentation import (
    get_installation_coordinates_google,
    parse_leakage_lists,
    extract_nace_scheme,
    impute_orbis_identifiers,
    create_csv_tables,
)
from eutl_data_augmentation.create_tables import create_table_account


def ingest_data(dir_parsed, dir_additional):
    """Ingest several types of data into the eutl data
    :param dir_in: <str> path to directory with incoming eutl data
    :param dir_additional: <str> path to directory with additional data
    :return: <dict> with filenames for newly created files
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
    parse_leakage_lists(
        fn_leakage_2015,
        fn_leakage_2020,
        fn_out=fn_nace_installations,
        fn_nace=fn_nace_scheme,
    )

    # impute orbis matching from JRC
    fn_jrc = dir_additional + "jrc_orbis_ids/JRC-EU ETS-FIRMS_V2_012022_public.xlsx"
    fn_acc = dir_parsed + "accounts.csv"
    fn_contacts = dir_parsed + "contacts.csv"
    fn_out = dir_parsed + "accounts_w_orbis.csv"
    impute_orbis_identifiers(fn_jrc, fn_acc, fn_contacts, fn_out)

    return {
        "accounts_w_orbis": fn_out,
        "nace_installations": fn_nace_installations,
        "nace_scheme": fn_nace_scheme,
        "location_installations": fn_coordinates,
    }


if __name__ == "__main__":
    # directories
    dir_parsed = "./data/parsed/"  # directory with data as provided by scarper
    # directory for csv tables to be imported into the database
    dir_tables = "./data/tables/"
    dir_additional = "./data/additional/"

    # get additional data and impute them into existing tables
    file_names = ingest_data(dir_parsed, dir_additional)

    # create cleaned and augmented csv tables
    create_csv_tables(
        dir_parsed,
        dir_tables,
        fn_coordinates=file_names["location_installations"],
        fn_nace=file_names["nace_installations"],
        fn_nace_codes=file_names["nace_scheme"],
        dir_additional=dir_additional,
    )
