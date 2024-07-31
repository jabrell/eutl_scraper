import pandas as pd
from .model import *


def export_database(dal, dir_out):
    """Exports final database to csv files
    :param dal: <DataAccessLayer>
    :param dir_out: <string> path to final data directory
    """
    session = dal.Session()

    def get_query_df(query):
        """Returns pandas dataframe with query results"""
        df = pd.read_sql(query.statement, query.session.bind)
        return df

    get_query_df(session.query(Account)).drop(["accountIDEutl"], axis=1).to_csv(
        dir_out + "account.csv", index=False
    )
    get_query_df(session.query(AccountHolder)).to_csv(
        dir_out + "account_holder.csv", index=False
    )
    get_query_df(session.query(Installation)).rename(
        columns={"mainAddress": "addressMain", "secondaryAddress": "addressSecondary"}
    ).to_csv(dir_out + "installation.csv", index=False)
    get_query_df(session.query(Compliance)).to_csv(
        dir_out + "compliance.csv", index=False
    )
    # get_query_df(session.query(EsdCompliance)).to_csv(
    #     dir_out + "esd_compliance.csv", index=False
    # )
    get_query_df(session.query(Surrender)).to_csv(
        dir_out + "surrender.csv", index=False
    )
    get_query_df(session.query(OffsetProject)).to_csv(
        dir_out + "project.csv", index=False
    )
    get_query_df(session.query(TransactionTypeMain)).to_csv(
        dir_out + "transaction_type_main.csv", index=False
    )
    get_query_df(session.query(TransactionTypeSupplementary)).to_csv(
        dir_out + "transaction_type_supplementary.csv", index=False
    )
    get_query_df(session.query(AccountType)).to_csv(
        dir_out + "account_type.csv", index=False
    )
    get_query_df(session.query(ActivityType)).to_csv(
        dir_out + "activity_type.csv", index=False
    )
    get_query_df(session.query(UnitType)).to_csv(dir_out + "unit_type.csv", index=False)
    get_query_df(session.query(CountryCode)).to_csv(
        dir_out + "country_code.csv", index=False
    )
    get_query_df(session.query(TradingSystemCode)).to_csv(
        dir_out + "trading_system_code.csv", index=False
    )
    get_query_df(session.query(ComplianceCode)).to_csv(
        dir_out + "compliance_code.csv", index=False
    )
    get_query_df(session.query(NaceCode)).to_csv(dir_out + "nace_code.csv", index=False)
    get_query_df(session.query(Transaction)).groupby(
        [
            "id",
            "transactionID",
            "date",
            "transactionTypeMain_id",
            "transactionTypeSupplementary_id",
            "transferringAccount_id",
            "acquiringAccount_id",
            "unitType_id",
            "project_id",
            "tradingSystem_id",
            "acquiringYear",
            "transferringYear",
        ],
        as_index=False,
        dropna=False,
    ).amount.sum().to_csv(dir_out + "transaction.csv", index=False)


def create_database(dal, dir_in):
    """Create new database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    insert_lookup_tables(dal, dir_in)
    insert_installation_tables(dal, dir_in)
    insert_account_tables(dal, dir_in)
    insert_transaction_table(dal, dir_in)


def insert_transaction_table(dal, dir_in):
    """Insert account related tables to database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert Transaction table")
    df_trans = pd.read_csv(
        dir_in + "transactionBlocks.csv",
        low_memory=False,
        parse_dates=["date", "created_on", "updated_on"],
    )
    df_trans = df_trans.rename(columns={"tradingSystem": "tradingSystem_id"})
    int_cols = [
        "id",
        "transactionTypeSupplementary_id",
        "transactionTypeMain_id",
        "project_id",
        "amount",
        "transferringAccount_id",
        "acquiringAccount_id",
        "transactionBlock",
        "acquiringYear",
        "transferringYear",
    ]
    dal.insert_df_large(
        df_trans, "transaction", integerColumns=int_cols, if_exists="append"
    )


def insert_account_tables(dal, dir_in):
    """Insert account related tables to database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert AccountHolder table")
    df_accHold = pd.read_csv(
        dir_in + "accountHolders.csv", parse_dates=["updated_on", "created_on"]
    )
    df_accHold["tradingSystem_id"] = df_accHold["tradingSystem"]
    df_accHold = df_accHold.drop(["tradingSystem"], axis=1)
    dal.insert_df_large(df_accHold, "account_holder", if_exists="append")

    print("---- Insert Account table")
    df_acc = pd.read_csv(
        dir_in + "accounts.csv",
        low_memory=False,
        parse_dates=["openingDate", "closingDate", "created_on", "updated_on"],
    )

    # rename and drop some columns to comply with schema
    cols_rename = {
        "jrcRegistrationIdType": "companyRegistrationNumberType",
        "tradingSystem": "tradingSystem_id",
    }
    cols_drop = [
        "jrcLEI",
        "jrcRegistrationIDStandardized",
        "jrcOrbisName",
        "jrcOrbisPostalCode",
        "jrcOrbisCity",
        "accountIdentifierDB",
    ]
    df_acc = df_acc.rename(columns=cols_rename).drop(cols_drop, axis=1)
    # account id has already been modified to be unique and existing for
    # all accounts in the csv table export step
    df_acc["id"] = df_acc["accountIDEutl"]

    int_cols = ["id", "accountIDEutl", "accountHolder_id", "yearValid"]
    dal.insert_df_large(df_acc, "account", integerColumns=int_cols, if_exists="append")


def insert_installation_tables(dal, dir_in):
    """Insert installation related tables to database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert OffsetProject table")
    df_proj = pd.read_csv(dir_in + "offset_projects.csv")
    int_cols = ["id", "track"]
    dal.insert_df_large(
        df_proj, "offset_project", integerColumns=int_cols, if_exists="append"
    )

    print("---- Insert Installation table")
    df_inst = pd.read_csv(
        dir_in + "installations.csv",
        parse_dates=["created_on", "updated_on"],
        low_memory=False,
        dtype={"nace15_id": "str", "nace20_id": "str", "nace_id": "str"},
    )
    df_inst = df_inst.rename(columns={"tradingSystem": "tradingSystem_id"})
    # we might have duplicates here, Drop complete duplicates and report duplicates
    dupi = df_inst.duplicated().astype("int").sum()
    if dupi > 0:
        print("Drop %d duplicated installation rows" % dupi)
        df_inst = df_inst.drop_duplicates()

    dal.insert_df_large(
        df_inst,
        "installation",
        integerColumns=["euEntitlement", "chEntitlement"],
        if_exists="append",
    )

    print("---- Insert Compliance table")
    df_comp = pd.read_csv(dir_in + "compliance.csv", low_memory=False)
    df_comp["reportedInSystem_id"] = df_comp.reportedInSystem.str.lower()
    df_comp = df_comp.drop("reportedInSystem", axis=1)
    int_cols = [
        "allocatedFree",
        "allocatedNewEntrance",
        "allocatedTotal",
        "allocated10c",
        "verified",
        "verifiedCummulative",
        "verifiedUpdated",
        "surrendered",
        "surrenderedCummulative",
        "balance",
        "penalty",
    ]
    dal.insert_df_large(
        df_comp, "compliance", integerColumns=int_cols, if_exists="append"
    )

    print("---- Insert Surrender table")
    df_surr = pd.read_csv(dir_in + "surrender.csv")
    df_surr["reportedInSystem_id"] = df_surr.reportedInSystem.str.lower()
    df_surr = df_surr.drop(["reportedInSystem", "id"], axis=1)
    # create a unique id
    df_surr["id"] = df_surr.index
    int_cols = ["id", "amount", "project_id"]

    dal.insert_df_large(
        df_surr, "surrender", integerColumns=int_cols, if_exists="append"
    )
    pass


def insert_lookup_tables(dal, dir_in):
    """Create all basic lookup tables
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert TransactionTypeMain table")
    df = pd.read_csv(dir_in + "mainTransactionTypeCodes.csv")
    dal.insert_df(df, TransactionTypeMain)

    print("---- Insert TransactionTypeSupplementary table")
    df = pd.read_csv(dir_in + "supplementaryTransactionTypeCodes.csv")
    dal.insert_df(df, TransactionTypeSupplementary)

    print("---- Insert AccountType table")
    df = pd.read_csv(dir_in + "accountTypeCodes.csv")
    dal.insert_df(df, AccountType)

    print("---- Insert ActivityType table")
    df = pd.read_csv(dir_in + "activityCodes.csv")
    dal.insert_df(df, ActivityType)

    print("---- Insert UnitType table")
    df = pd.read_csv(dir_in + "unitTypeCodes.csv")
    dal.insert_df(df, UnitType)

    print("---- Insert CountryCode table")
    df = pd.read_csv(dir_in + "registryCodes.csv", keep_default_na=False)
    dal.insert_df(df, CountryCode)

    print("---- Insert ComplianceCode table")
    df = pd.read_csv(dir_in + "complianceCodes.csv")
    dal.insert_df(df, ComplianceCode)

    print("---- Insert NaceClassification table")
    df = pd.read_csv(dir_in + "nace_scheme.csv")
    dal.insert_df(df, NaceCode)

    print("---- Insert TransactionTypeMain table")
    df = pd.read_csv(dir_in + "tradingSystemCodes.csv")
    dal.insert_df(df, TradingSystemCode)
