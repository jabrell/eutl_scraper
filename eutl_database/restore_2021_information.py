import logging
from zipfile import ZipFile
import pandas as pd
import numpy as np
from tqdm import tqdm

from eutl_database import DataAccessLayer
from eutl_database.model import Account, Transaction


def match_2021_accounts(
    df_21: pd.DataFrame,
    df_24: pd.DataFrame,
    lst_acc: list[str],
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """Match accounts from the 2021 data with the 2024 accounts

    Args:
        df_21 (pd.DataFrame): 2021 account data
        df_24 (pd.DataFrame): 2024 account data
        cols (list[str]): columns to use for the matching
        lst_acc (list[str]): list of account ids to match
        drop_duplicates (bool): drop duplicates in the resulting DataFrame
    """

    def match_2021_accounts_(
        df_21: pd.DataFrame,
        df_24: pd.DataFrame,
        on: list[str],
        lst_acc: list[str],
        drop_duplicates: bool = True,
    ) -> pd.DataFrame:
        """Match accounts from the 2021 data with the 2024 accounts

        Args:
            df_21 (pd.DataFrame): 2021 account data
            df_24 (pd.DataFrame): 2024 account data
            cols (list[str]): columns to use for the matching
            lst_acc (list[str]): list of account ids to match
            drop_duplicates (bool): drop duplicates in the resulting DataFrame
        """
        cols_ = ["id"] + on
        df_merged = (
            df_21[df_21["id"].isin(lst_acc)][cols_]
            .assign(name=lambda x: x["name"].str.lower())
            .rename(columns={"id": "id_2021"})
        )
        df_ = df_24.assign(
            name_2024=lambda x: x["name"],
            name=lambda x: x["registry_id"].str.lower()
            + " "
            + x["name_2024"].str.lower(),
        ).loc[:, cols_]
        df_merged = df_merged.merge(df_, how="inner", on=on, suffixes=("_21", "_24"))
        if drop_duplicates:
            len_with_duplicates = len(df_merged)
            df_merged = df_merged.drop_duplicates(
                subset=["id_2021"], keep=False
            ).drop_duplicates(subset=["id"], keep=False)
            logging.info(f"Dropped {len_with_duplicates - len(df_merged)} duplicates")
        logging.info(f"Matched {len(df_merged)} of {len(lst_acc)} accounts")
        return df_merged

    matching_fields = [
        [
            "name",
            "registry_id",
            "accountType_id",
            "openingDate",
            "closingDate",
            "installation_id",
            "companyRegistrationNumber",
        ],
        [
            "name",
            "registry_id",
            "accountType_id",
            "openingDate",
            "closingDate",
            "installation_id",
        ],
        [
            "name",
            "registry_id",
            "accountType_id",
            "openingDate",
            "installation_id",
            "companyRegistrationNumber",
        ],
        [
            "name",
            "registry_id",
            "accountType_id",
            "openingDate",
            "closingDate",
            "companyRegistrationNumber",
        ],
        [
            "name",
            "registry_id",
            "accountType_id",
            "openingDate",
            "companyRegistrationNumber",
        ],
        ["name", "registry_id", "accountType_id", "openingDate", "closingDate"],
        ["name", "registry_id", "accountType_id", "openingDate"],
        ["name", "registry_id", "accountType_id"],
    ]

    lst_df = []
    to_match = [i for i in lst_acc]
    total_to_match = len(to_match)
    for i, fields in enumerate(matching_fields):
        logging.info(f"\nMatch on: {fields}")
        df = match_2021_accounts_(
            df_21, df_24, on=fields, lst_acc=to_match, drop_duplicates=drop_duplicates
        )
        # drop the matched accounts from the list
        to_match = np.setdiff1d(to_match, df["id_2021"].unique(), assume_unique=True)
        # [i for i in to_match if i not in df["id_2021"].unique()]
        lst_df.append(df)
        logging.info(f"Remaining accounts to match: {len(to_match)}")
    df_matched = pd.concat(lst_df)
    logging.info(f"\n------ Matching results ------")
    logging.info(f"Matched: {df_matched['id_2021'].nunique()} of {total_to_match}")
    logging.info(f"Unmatched: {len(to_match)} of {total_to_match}: {to_match}")
    return df_matched


def restore_missing_transaction_accounts(
    dal: DataAccessLayer, fn_2021: str, dir_out: str
) -> tuple[pd.DataFrame, list[int]]:
    """In the data from 2022 onwards some transactions miss on one of the trading
    accounts. We restore this information from the 2021 transactions where
    the information is available."""
    # get the 2021 transaction and account data from the zip file
    with ZipFile(fn_2021) as z:
        df_trans_21 = pd.read_csv(z.open("transaction.csv"))
        df_acc_21 = pd.read_csv(
            z.open("account.csv"), parse_dates=["openingDate", "closingDate"]
        )

    # get the current transactions that miss the account information
    with dal.session as s:
        df_missing_acq = pd.read_sql(
            s.query(Transaction)
            .filter(Transaction.acquiringAccount_id == None)
            .statement,
            s.bind,
        )
        df_missing_trans = pd.read_sql(
            s.query(Transaction)
            .filter(Transaction.transferringAccount_id == None)
            .statement,
            s.bind,
        )
        df_acc = pd.read_sql(s.query(Account).statement, s.bind)

    # get the corresponding 2021 transactions and extract the 2021 account id
    # of the missing accounts
    idx_acq = [i for i in df_missing_acq.transactionID.unique() if pd.notnull(i)]
    idx_trans = [i for i in df_missing_trans.transactionID.unique() if pd.notnull(i)]
    df_t_21_acq = df_trans_21.query(f"transactionID in {idx_acq}")
    df_t_21_trans = df_trans_21.query(f"transactionID in {idx_trans}")
    missings = list(
        set(
            [int(i) for i in df_t_21_acq.acquiringAccount_id.unique() if pd.notnull(i)]
            + [
                int(i)
                for i in df_t_21_trans.transferringAccount_id.unique()
                if pd.notnull(i)
            ]
        )
    )

    # For each 2021 account id:
    #   - find remaining 2021 transactions in which the account is involved
    #   - find corresponding 2024 transaction
    #   - assign the 2021 account id if it is not missing
    res = {}
    not_matchable = []
    with dal.session as s:
        for a21 in tqdm(missings):
            # first try on the acquiring accounts
            df_t = df_trans_21.query(f"acquiringAccount_id == {a21}")
            idx_trans = df_t.transactionID.unique()
            t = (
                s.query(Transaction)
                .filter(
                    Transaction.transactionID.in_(idx_trans),
                    Transaction.acquiringAccount_id != None,
                )
                .first()
            )
            if t is not None:
                res[a21] = t.acquiringAccount_id
                continue
            # nothing found, try on the transferring accounts
            else:
                df_t = df_trans_21.query(f"transferringAccount_id == {a21}")
                idx_trans = df_t.transactionID.unique()
                t = (
                    s.query(Transaction)
                    .filter(
                        Transaction.transactionID.in_(idx_trans),
                        Transaction.transferringAccount_id != None,
                    )
                    .first()
                )
                if t is not None:
                    res[a21] = t.transferringAccount_id
                else:
                    not_matchable.append(a21)
    df_matched = (
        pd.Series(res)
        .to_frame("id_2024")
        .reset_index()
        .rename(columns={"index": "id_2021"})
    )

    # for the unmatched accounts, try to match them by name
    df_matched_name = match_2021_accounts(df_acc_21, df_acc, lst_acc=not_matchable)[
        ["id_2021", "id"]
    ].rename(columns={"id": "id_2024"})

    # add one account by hand and combined all matches
    to_add = pd.DataFrame([{"id_2021": 37552, "id_2024": 48828}])
    df_matched_all = pd.concat([df_matched, df_matched_name, to_add])
    assert (
        df_matched_all.id_2021.is_unique
    ), "Duplicated 2021 account ids in final matching table"
    assert (
        df_matched_all.id_2024.is_unique
    ), "Duplicated 2024 account ids in final matching table"
    print(
        f"Matched {len(df_matched_all)} of {len(missings)} missing transaction accounts"
    )

    # store the updated transaction tables
    map_acc = df_matched_all.set_index("id_2021")["id_2024"].to_dict()
    df_t_21_acq = df_t_21_acq.assign(
        acquiringAccountID_2024=df_t_21_acq.acquiringAccount_id.map(map_acc)
    ).query("acquiringAccountID_2024.notnull()")
    df_t_21_trans = df_t_21_trans.assign(
        transferringAccountID_2024=df_t_21_trans.transferringAccount_id.map(map_acc)
    ).query("transferringAccountID_2024.notnull()")

    df_t_21_acq.to_csv(dir_out + "transaction_acquiring_missing.csv", index=False)
    df_t_21_trans.to_csv(dir_out + "transaction_transferring_missing.csv", index=False)

    return df_matched_all, not_matchable
