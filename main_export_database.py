from eutl_database import (export_database, 
                           DataAccessLayer)

if __name__ == "__main__":
    # build the database
    localConnectionSettings = dict(
            user="JanAdmin", 
            host="localhost", 
            db="eutl2022", 
            passw="1234"
        )
    dal = DataAccessLayer(**localConnectionSettings)
    dir_final = "./data/final/"  # final directory for database export
    # export the database
    export_database(dal, dir_final)