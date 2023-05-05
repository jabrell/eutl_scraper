import os.path
from eutl_data_augmentation import get_installation_coordinates_google

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

    key_google = os.environ.get("GMAPKEY")
    print(key_google)
    get_installation_coordinates_google(dir_in, key_google, fn_out=fn_coordinates)
