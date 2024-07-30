from eutl_database import (
    create_database,
    export_database,
    link_foha_installations,
    DataAccessLayer,
    restore_missing_transaction_accounts,
)

from zipfile import ZipFile
import pandas as pd
from tqdm import tqdm
from eutl_database.model import Account, Transaction


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    # directory for csv tables to be imported into the database
    dir_tables = "./data/tables/"
    dir_final = "./data/final/"  # final directory for database export
    # file with former operator holding mappings
    fn_ohaMatching = dir_final + "foha_matching.csv"

    # build the database
    localConnectionSettings = dict(
        user="JanAdmin", host="localhost", db="eutl2024_build", passw="1234"
    )
    dal = DataAccessLayer(**localConnectionSettings)

    # empty the database
    # dal.clear_database(askConfirmation=False)
    # create_database(dal, dir_tables)

    # this has only to be done once after database creation, afterwards run
    # data augmentation again
    df_map_21_to_23, not_matchable = restore_missing_transaction_accounts(
        dal, fn_2021="./data/additional/eutl_2021.zip", dir_out="./data/additional/"
    )
    print("here")
    # try to establish the mapping between current and former operator holding accounts
    link_foha_installations(dal.Session(), fn_out=fn_ohaMatching)

    # export the database
    export_database(dal, dir_final)
