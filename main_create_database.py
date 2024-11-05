import pandas as pd
from eutl_database import (
    create_database,
    export_database,
    link_foha_installations,
    DataAccessLayer,
    restore_missing_transaction_accounts,
)


def get_missing_from_previous_version(dir_current: str, dir_previous: str):
    """
    Get missing transactions from the previous version of the database.

    Args:
        dir_current: <str> path to directory with tables of the current version
        dir_previous: <str> path to directory with tables of the previous version
    """
    df_trans = pd.read_csv(
        f"{dir_current}/transactionBlocks.csv", low_memory=False, parse_dates=["date"]
    )
    df_acc = pd.read_csv(
        f"{dir_current}/accounts.csv",
        low_memory=False,
        parse_dates=["openingDate", "closingDate"],
    )
    df_trans_prev = pd.read_csv(
        f"{dir_previous}/transaction.csv", low_memory=False, parse_dates=["date"]
    )
    df_acc_prev = pd.read_csv(
        f"{dir_previous}/account.csv",
        low_memory=False,
        parse_dates=["openingDate", "closingDate"],
    )

    # get missing transactions
    missing = list(
        set(df_trans_prev.transactionID.unique()).difference(
            set(df_trans.transactionID.unique())
        )
    )
    print(f"Transactions in previous version but not in current {len(missing)}")
    df_missing = df_trans_prev[df_trans_prev["transactionID"].isin(missing)]
    groupers = [i for i in df_missing.columns if i not in ["amount", "id"]]
    df_missing = df_missing.groupby(groupers, as_index=False, dropna=False)[
        "amount"
    ].sum()
    df_missing["id"] = df_trans.id.max() + 1 + df_missing.index
    print(f"Restored {len(df_missing.transactionID.unique())} missing transactions")
    df_missing.to_csv(f"{dir_current}/missing_transactions.csv", index=False)


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

    # get missing transactions from the previous version
    dir_previous = "data_may_2024/final/"
    get_missing_from_previous_version(dir_current=dir_tables, dir_previous=dir_previous)

    # empty the database
    dal.clear_database(askConfirmation=False)
    create_database(dal, dir_tables)

    # this has only to be done once after database creation, afterwards run
    # data augmentation again
    # df_map_21_to_23, not_matchable = restore_missing_transaction_accounts(
    #     dal, fn_2021="./data/additional/eutl_2021.zip", dir_out="./data/additional/"
    # )

    # try to establish the mapping between current and former operator holding accounts
    link_foha_installations(
        dal.Session(), fn_out=fn_ohaMatching, overwrite_exiting=False
    )

    # export the database
    export_database(dal, dir_final)
