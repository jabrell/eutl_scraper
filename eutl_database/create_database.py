import pandas as pd 
from .model import *


def export_database(dal, dir_out):
    """ Exports final database to csv files 
    :param dal: <DataAccessLayer> 
    :param dir_out: <string> path to final data directory
    """
    session = dal.Session()
    def get_query_df(query):
        """Returns pandas dataframe with query results """
        df = pd.read_sql(query.statement, query.session.bind)
        return df

    get_query_df(session.query(Account)).drop(["accountIDEutl", "accountIDtransactions"], axis=1)\
            .to_csv(dir_out + "account.csv", index=False)
    get_query_df(session.query(AccountHolder)).to_csv(dir_out + "account_holder.csv", index=False)
    get_query_df(session.query(Installation)).rename(columns={"mainAddress": "addressMain", "secondaryAddress": "addressSecondary"})\
            .to_csv(dir_out + "installation.csv", index=False)
    get_query_df(session.query(Compliance)).to_csv(dir_out + "compliance.csv", index=False)
    get_query_df(session.query(Surrender)).to_csv(dir_out + "surrender.csv", index=False)
    get_query_df(session.query(OffsetProject)).to_csv(dir_out + "project.csv", index=False)
    get_query_df(session.query(TransactionTypeMain)).to_csv(dir_out + "transaction_type_main.csv", index=False)
    get_query_df(session.query(TransactionTypeSupplementary)).to_csv(dir_out + "transaction_type_supplementary.csv", index=False)
    get_query_df(session.query(AccountType)).to_csv(dir_out + "account_type.csv", index=False)
    get_query_df(session.query(ActivityType)).to_csv(dir_out + "activity_type.csv", index=False)
    get_query_df(session.query(UnitType)).to_csv(dir_out + "unit_type.csv", index=False)
    get_query_df(session.query(CountryCode)).to_csv(dir_out + "country_code.csv", index=False)
    get_query_df(session.query(ComplianceCode)).to_csv(dir_out + "compliance_code.csv", index=False)
    get_query_df(session.query(NaceCode)).to_csv(dir_out + "nace_code.csv", index=False)
    get_query_df(session.query(Transaction))\
            .groupby(["id", "transactionID", "date", "transactionTypeMain_id", "transactionTypeSupplementary_id", 
                    "transferringAccount_id", "acquiringAccount_id", "unitType_id", "project_id"],
                    as_index=False, dropna=False).amount.sum()\
            .to_csv(dir_out + "transaction.csv", index=False)


def create_database(dal, dir_in):
    """ Create new database
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
    df_trans = pd.read_csv(dir_in + "transactionBlocks.csv", low_memory=False,
                           parse_dates=["date", "created_on", "updated_on"]) 
    int_cols = ["id", 
                "transactionTypeSupplementary_id", "transactionTypeMain_id",
                "project_id", "amount",
                "transferringAccount_id", "acquiringAccount_id"]   
    dal.insert_df_large(df_trans, "transaction", 
                        integerColumns=int_cols, if_exists="append") 


def insert_account_tables(dal, dir_in):
    """Insert account related tables to database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert AccountHolder table")
    df_accHold = (pd.read_csv(dir_in + "accountHolders.csv",
                             parse_dates=["updated_on", "created_on"])
    )
    dal.insert_df_large(df_accHold, "account_holder", if_exists="append")    
    
    print("---- Insert Account table")
    df_acc = ( pd.read_csv(dir_in + "accounts.csv", low_memory=False,
                         parse_dates=["openingDate", "closingDate", "created_on", "updated_on"]) 
            )
    df_acc["id"] = df_acc["accountIDEutl"]
    # bug fix: accountIDtransactions should by unique but is not for those accounts created
    # for transactions
    df_acc["accountIDtransactions"] = df_acc["accountIdentifierDB"]
    df_acc = df_acc.drop("accountIdentifierDB", axis=1)
    int_cols = ["id", "accountIDEutl", "accountHolder_id"]   
    dal.insert_df_large(df_acc, "account", integerColumns=int_cols, if_exists="append")


def insert_installation_tables(dal, dir_in):
    """Insert installation related tables to database
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert OffsetProject table")
    df_proj = pd.read_csv(dir_in + "offset_projects.csv")
    int_cols = ["id", "track"]
    dal.insert_df_large(df_proj, "offset_project", 
                        integerColumns=int_cols, if_exists="append")
    
    print("---- Insert Installation table")
    df_inst = pd.read_csv(dir_in + "installations.csv",
                      parse_dates=["created_on", "updated_on"],
                      low_memory=False, 
                      dtype={"nace15_id": "str", "nace20_id": "str",
                             "nace_id": "str"})
    # we might have duplicates here, Drop complete duplicates and report duplicates
    dupi = df_inst.duplicated().astype("int").sum()
    if dupi > 0:
        print("Drop %d duplicated installation rows" % dupi)
        df_inst = df_inst.drop_duplicates() 
    dal.insert_df_large(df_inst, "installation", integerColumns=["euEntitlement", "chEntitlement"],
                        if_exists="append")
    
    print("---- Insert Compliance table")
    df_comp = pd.read_csv(dir_in + "compliance.csv")
    int_cols = ['allocatedFree', 'allocatedNewEntrance', 'allocatedTotal', "allocated10c",
                'verified', 'verifiedCummulative', 'verifiedUpdated',
                'surrendered', 'surrenderedCummulative']
    dal.insert_df_large(df_comp, "compliance", integerColumns=int_cols,
                        if_exists="append")
    
    print("---- Insert Surrender table")
    df_surr = pd.read_csv(dir_in + "surrender.csv")
    int_cols = ["id", "amount", "project_id"]
    dal.insert_df_large(df_surr, "surrender", integerColumns=int_cols,
                        if_exists="append")    
    pass


def insert_lookup_tables(dal, dir_in):
    """Create all basic lookup tables
    :param dal: <DataAccessLayer>
    :param dir_in: <string> path to final data directory
    """
    print("---- Insert TransactionTypeMain table")
    df = pd.read_csv( dir_in + "mainTransactionTypeCodes.csv")
    dal.insert_df(df, TransactionTypeMain)
    
    print("---- Insert TransactionTypeSupplementary table")
    df = pd.read_csv( dir_in + "supplementaryTransactionTypeCodes.csv")
    dal.insert_df(df, TransactionTypeSupplementary)
    
    print("---- Insert AccountType table")
    df = pd.read_csv( dir_in + "accountTypeCodes.csv")
    dal.insert_df(df, AccountType)
    
    print("---- Insert ActivityType table")
    df = pd.read_csv( dir_in + "activityCodes.csv")
    dal.insert_df(df, ActivityType)
    
    print("---- Insert UnitType table")
    df = pd.read_csv( dir_in + "unitTypeCodes.csv")
    dal.insert_df(df, UnitType)
    
    print("---- Insert CountryCode table")
    df = pd.read_csv( dir_in + "registryCodes.csv", keep_default_na=False)
    dal.insert_df(df, CountryCode)
    
    print("---- Insert ComplianceCode table")
    df = pd.read_csv( dir_in + "complianceCodes.csv")
    dal.insert_df(df, ComplianceCode)
    
    print("---- Insert NaceClassification table")
    df = pd.read_csv(dir_in + "nace_all.csv")
    dal.insert_df(df, NaceCode)