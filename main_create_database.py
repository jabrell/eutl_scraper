from eutl_database import (
    create_database,
    export_database,
    link_foha_installations,
    DataAccessLayer,
)


if __name__ == "__main__":
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
    dal.clear_database(askConfirmation=False)
    create_database(dal, dir_tables)

    # try to establish the mapping between current and former operator holding accounts
    link_foha_installations(dal.Session(), fn_out=fn_ohaMatching)

    # export the database
    export_database(dal, dir_final)
