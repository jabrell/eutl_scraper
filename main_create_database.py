from eutl_database import (
    create_csv_tables,
    create_database,
    export_database,
    link_foha_installations,
    get_installation_coordinates_google,
    DataAccessLayer,
)
import os.path


if __name__ == "__main__":
    dir_in = "./data/parsed/"  # directory with data as provided by scarper
    # directory for csv tables to be imported into the database
    dir_out = "./data/tables/"
    dir_final = "./data/final/"  # final directory for database export
    fn_nace_codes = "./data/additional/nace_all.csv"  # table with all nace codes
    # table with nace code of installations
    fn_nace = "./data/additional/nace_leakage.csv"
    # table with installation coordinates
    fn_coordinates = "./data/additional/installation_coordinates.csv"
    fn_nace_codes = "./data/additional/nace_all.csv"
    fn_ohaMatching = dir_final + "foha_matching.csv"

    # build the database
    localConnectionSettings = dict(
        user="JanAdmin", host="localhost", db="eutl2022", passw="1234"
    )
    dal = DataAccessLayer(**localConnectionSettings)
    dal.clear_database(askConfirmation=False)
    create_database(dal, dir_out)

    # try to establish the mapping between current and former operator holding accounts
    link_foha_installations(dal.Session(), fn_out=fn_ohaMatching)

    # export the database
    export_database(dal, dir_final)
