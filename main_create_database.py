from eutl_database import (create_csv_tables, create_database, 
                           export_database,  link_foha_installations,
                           get_installation_coordinates_google,
                           DataAccessLayer)
import os.path
import sys 


if __name__ == "__main__":
    dir_in = "./data/parsed/"     # directory with data as provided by scarper
    dir_out = "./data/tables/"    # directory for csv tables to be importated into the databse 
    dir_final = "./data/final/"  # final directory for database export
    fn_nace_codes = "./data/additional/nace_all.csv" # table with all nace codes
    fn_nace = "./data/additional/nace_leakage.csv"   # table with nace code of installations
    fn_coordinates = "./data/additional/installation_coordinates.csv" # table with installation coordinates
    fn_nace_codes= "./data/additional/nace_all.csv" 
    fn_ohaMatching = dir_final + "foha_matching.csv"
    
    # check if coordinates exist, if not get them
    if not os.path.isfile(fn_coordinates):
        print("##### Get coordinates for installations")
        # add you google key api key here
        key_google = os.environ.get("GMAPKEY")
        if key_google is not None:
            get_installation_coordinates_google(dir_in, key_google, fn_out=fn_coordinates)
        else: 
            print("##### No key for google maps api provided continue building database without coordinates")
            fn_coordinates = None
    else:
        print("##### Use already existing file for installation coordinates")

    # create csv tables for database creation
    create_csv_tables(dir_in, dir_out, fn_coordinates=fn_coordinates, fn_nace=fn_nace, fn_nace_codes=fn_nace_codes)
    
    # build the database
    localConnectionSettings = dict(
            user="JanAdmin", 
            host="localhost", 
            db="eutl2022", 
            passw="1234"
        )
    dal = DataAccessLayer(**localConnectionSettings)
    dal.empty_database(askConfirmation=False)
    create_database(dal, dir_out)

    # try to establish the mapping between current and former oerpator holding accounts
    link_foha_installations(dal.Session(), fn_out=fn_ohaMatching)
    
    # export the database
    export_database(dal, dir_final)