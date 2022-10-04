import pandas as pd 
import numpy as np
from datetime import datetime
from .mappings import map_registryCode_inv, map_account_type_inv, map_unitType_inv, export_mappings
import os
import glob


# account information added by hand
NEW_ACC = [{"accountIDEutl": 111264,
            "name": "EU Credit Exchange Account - Aviation",
            'registry_id': "EU",
            "openingDate": pd.to_datetime("2014-01-29"),
            "isOpen": True,
            "accountType_id": "100-23"},
           {"accountIDEutl": 111265,
            "name": "EU Credit Exchange Account - Aviation",
            'registry_id': "EU",
            "openingDate": pd.to_datetime("2014-01-29"),
            "isOpen": True,
            "accountType_id": "100-23"},
           {"accountIDEutl": 111267,
            "name": "EU Credit Exchange Account",
            'registry_id': "EU",
            "openingDate": pd.to_datetime("2014-01-29"),
            "isOpen": True,
            "accountType_id": "100-23"},
           {"accountIDEutl": 111266,
            "name": "EU Credit Exchange Account",
            'registry_id': "EU",
            "openingDate": pd.to_datetime("2014-01-29"),
            "isOpen": True,
            "accountType_id": "100-23"}]


def create_csv_tables(dir_in, dir_out, fn_coordinates=None,
                  fn_nace=None, fn_nace_codes=None):
    """Create all tables
    :param dir_in: <string> directory with parsed data
    :param dir_out: <string> output directory
    :param fn_coordinates: <string> path to file with installation coordinates
    :param fn_nace: <string> name of file with nace codes for installations
                if None, NACE codes are not processed
    :param fn_nace_codes: <string> name of file with nace classification scheme
                If None, calssification lookup not exported                
    """
    print("####### Create lookup tables")
    create_tables_lookup(dir_in, dir_out, fn_nace_codes=fn_nace_codes)
    
    print("####### Create installation tables")
    create_table_installation(dir_in, dir_out, 
                            fn_coordinates=fn_coordinates,
                            fn_nace=fn_nace)
    create_table_compliance(dir_in, dir_out)
    create_table_surrender(dir_in, dir_out)
    
    print("####### Create account tables")
    create_table_accountHolder(dir_in, dir_out)
    create_table_account(dir_in, dir_out)

    print("####### Create transcation tables")
    create_table_transaction(dir_in, dir_out)  

    
def create_table_installation(dir_in, dir_out, fn_coordinates=None,
                              fn_nace=None):
    """Create installation table
    :param dir_in: <string> directory with parsed data
    :param dir_out: <string> output directory
    :param fn_coordinates: <string> name of file with coordinates
    :param fn_nace: <string> name of file with nace codes
                if None, NACE codes are not processed
    """
    # get data: installation data together with addresses with updated coordinates
    #           and entitlements
    df_inst = pd.read_csv(dir_in + "installations.csv",)
    df_enti = pd.read_csv(dir_in + "entitlements.csv", na_values=["Not Applicable", "Not Set"])
    df_enti["installationID_new"] = df_enti.registry.map(lambda x: map_registryCode_inv.get(x))
    df_enti["installationID"] = df_enti["installationID_new"] + "_" + df_enti["installationID"].map(str)
    df_enti = df_enti[["installationID", "euEntitlement", "chEntitlement"]].copy()    
    df_inst = df_inst.merge(df_enti, on="installationID", how="left")

    # transform dataframe to be consistent with Installation object
    cols_inst =    {'installationID': 'id',
                    "name": "name",
                    'registryCode': 'registry_id',
                    'activity': 'activity_id',
                    'eprtrID': 'eprtrID',
                    'parent': 'parentCompany',
                    'subsidiary': 'subsidiaryCompany',
                    'permitID': 'permitID',
                    'icaoID': 'designatorICAO',
                    'monitoringPlanId': 'monitoringID',
                    'monitoringPlanExpiry': 'monitoringExpiry',
                    'monitoringPlanFirstYear': 'monitoringFirstYear',
                    'permitExpiry': 'permitDateExpiry',
                    'isAircraftOperator': 'isAircraftOperator',
                    'ec7482009ID': 'ec748_2009Code',
                    'permitEntryDate': 'permitDateEntry',
                    'mainAddress': 'mainAddress',
                    'secondaryAddress': 'secondaryAddress',
                    'postalCode': 'postalCode',
                    'city': 'city',
                    'country': 'country_id',
                    'latitude': 'latitudeEutl',
                    'longitude': 'longitudeEutl',
                    "euEntitlement": "euEntitlement",
                    "chEntitlement": "chEntitlement",
                    }
    df_inst_to_tbl = df_inst[[c for c in cols_inst.keys() if c in df_inst.columns]].copy()
    df_inst_to_tbl = df_inst_to_tbl.rename(columns=cols_inst)
    # convert activity id to id only (without description)
    df_inst_to_tbl.activity_id = df_inst_to_tbl.activity_id.map(lambda x: int(x.split("-")[0]))
    
    if fn_coordinates is not None:    
        df_ = pd.read_csv(fn_coordinates,
                names=["id", "latitudeGoogle", "longitudeGoogle"],
                usecols=["id", "latitudeGoogle", "longitudeGoogle"],
                header=0)
        df_inst_to_tbl = df_inst_to_tbl.merge(df_, on="id", how="left")
               
    # add nace codes
    if fn_nace:
        # primarily use 2020 leakage list but fill with 15
        df_ = pd.read_csv(fn_nace, usecols=["id", "nace15", "nace20"],
                          dtype={"nace15": "str", "nace20": "str"}).drop_duplicates()
        df_["nace_id"] = df_.nace20.fillna(df_.nace15)
        df_ = df_.rename(columns={"nace15": "nace15_id", "nace20": "nace20_id"})
        df_inst_to_tbl = df_inst_to_tbl.merge(df_, on="id", how="left") 
        # for aircraft add the nace code 51 (Air transport)
        df_inst_to_tbl.loc[df_inst_to_tbl.isAircraftOperator, "nace_id"] = df_inst_to_tbl.loc[df_inst_to_tbl.isAircraftOperator, "nace_id"].fillna(51)
        
    # add created timestamp
    df_inst_to_tbl["created_on"] = datetime.now()
    df_inst_to_tbl["updated_on"] = datetime.now()
    
    # export to csv
    df_inst_to_tbl.to_csv(dir_out + "installations.csv", index=False, encoding="utf-8")
    
    return


def create_table_compliance(dir_in, dir_out):
    """Create table with compliance data
    :param dir_in: <string> directory with parsed data
    :param dir_out: <string> output directory
    """
    # get data
    df_comp = pd.read_csv(dir_in + "compliance.csv")

    # transform dataframe to be consistent with Installation object
    cols_comp = {'installationID': 'installation_id',
                'year': 'year',
                'phase': 'euetsPhase',
                'complianceCode': 'compliance_id',
                'allocationFree': 'allocatedFree',
                'allocationNewEntrance': 'allocatedNewEntrance',
                'allocationTotal': 'allocatedTotal',
                'allocation10c': 'allocated10c',
                'verified': 'verified',
                'verifiedCumulative': 'verifiedCummulative',
                'complianceCodeUpdated': 'verifiedUpdated',
                'surrendered': 'surrendered',
                'surrenderedCumulative': 'surrenderedCummulative',
                "reportedInSystem": "reportedInSystem"}

    # calculate total allocation 
    df_comp["allocationTotal"] = (df_comp["allocationNewEntrance"].fillna(0) +
            df_comp["allocationFree"].fillna(0) + df_comp["allocation10c"].fillna(0))
    df_comp_to_tbl = df_comp[cols_comp.keys()].copy()
    df_comp_to_tbl = df_comp_to_tbl.rename(columns=cols_comp)
    # verified emission might have status "Excluded" which we set to missing (to have an int column)
    df_comp_to_tbl.verified = df_comp_to_tbl.verified.replace(["Excluded", "Not Reported"], np.nan)   
    df_comp_to_tbl.verifiedCummulative = df_comp_to_tbl.verifiedCummulative.replace("Not Calculated", np.nan)   
    # add created timestamp
    df_comp_to_tbl["created_on"] = datetime.now()
    df_comp_to_tbl["updated_on"] = datetime.now()
    
    # save table 
    df_comp_to_tbl.to_csv(dir_out + "compliance.csv", index=False, encoding="utf-8")
    
    return


def create_table_surrender(dir_in, dir_out):
    """Create table with surrendering details as well as offset projects
    :param dir_in: <string> directory with parsed data
    :param dir_out: <string> output directory
    """
    # get data
    df_surr = pd.read_csv(dir_in + "surrendering.csv")

    # create offset project table
    df_proj = df_surr[["projectID", "track", "originatingRegistry"]].dropna(subset=["projectID"]).drop_duplicates()
    df_proj.columns = ["id", "track", "country_id"]
    # convert country names to country ids
    df_proj.country_id = df_proj.country_id.map(map_registryCode_inv)
    df_proj["created_on"] = datetime.now()
    df_proj["updated_on"] = datetime.now()

    # choose and rename columns in the surrendering table an insert them into the database
    cols_surr = {'installationID': 'installation_id',
                'year': 'year',
                'unitType': 'unitType_id',
                'amount': 'amount',
                'originatingRegistry': 'originatingRegistry_id',
                # 'accountID': 'account_id',
                'projectID': 'project_id',
                # 'expiryDate': 'expiryDate',
                "reportedInSystem": "reportedInSystem"
                }                 
    df_surr_to_tbl = df_surr[cols_surr.keys()].copy()
    df_surr_to_tbl = df_surr_to_tbl.rename(columns=cols_surr)
    # impose lookup codes
    df_surr_to_tbl.unitType_id = df_surr_to_tbl.unitType_id.map(map_unitType_inv)
    df_surr_to_tbl.originatingRegistry_id = df_surr_to_tbl.originatingRegistry_id.map(map_registryCode_inv)

    # need to add an primary key for surrendendering rows
    # here we simply use the index 
    df_surr_to_tbl["id"] = df_surr_to_tbl.index

    # add created timestamp
    df_surr_to_tbl["created_on"] = datetime.now()
    df_surr_to_tbl["updated_on"] = datetime.now()

    # save data
    df_surr_to_tbl.to_csv(dir_out + "surrender.csv", index=False, encoding="utf-8")
    df_proj.to_csv(dir_out + "offset_projects.csv", index=False, encoding="utf-8")


def create_table_accountHolder(dir_in, dir_out):
    """ Create account holder table dropping duplicated account holders
    :param dir_in: <string> directory with parsed data
    :param dir_out: <string> output directory
    """
    df = pd.read_csv(dir_in + "/contacts.csv", na_values=["-", "na", ".", "0", "XXX"])

    # Create a unique account holder ID that identifies duplicates
    def get_duplicate_matching(df, cols_duplication, col_id):
        """Mapping of duplicated rows to ID of first occurance of the duplicated row
        :param df: <pd.DataFrame> with data
        :param cols_duplication: <list: string> name of columns checked for duplicates
        :param col_id: <string> name of column with identifier
        :return: <dict: col_id> to id in first occurance row
        """
        df_d = df[df.duplicated(subset=cols_duplication, keep=False)
                ].drop_duplicates(cols_duplication)
        df_d["__newID__"] = df_d[col_id]
        df_f = df.merge(df_d, on=cols_duplication, suffixes=('', '_y'))
        df_f = df_f[df_f["__newID__"].notnull()].copy()
        m = pd.Series(df_f["__newID__"].values, index=df_f[col_id])#.to_dict()
        return m

    # require a minimum of information to identify duplicates
    cols_nonNull = ["name", "mainAddress", "city", "country"]
    df_ = df[df[cols_nonNull].notnull().all(axis=1)]

    # get duplicates by all columns (except associated accountID)
    cols_duplication = [c for c in df.columns if c not in ["accountID", "accountURL"]]
    match_all = get_duplicate_matching(df_, cols_duplication, col_id="accountID")    

    # insert map on accountHolderID into original frame
    # if not duplicate simply assign the original account ID
    df["accountHolderID"] = df.accountID.map(lambda x: match_all.get(x, x))

    # get a mapping from account holder to accountID 
    df_map_accountHolders = df[["accountHolderID", "accountID"]].copy()

    # drop duplicates and map country column to codes
    df = df.drop_duplicates("accountHolderID")

    # create country lookups instead of full country names
    df.country = df.country.map(map_registryCode_inv)

    # rename columns
    cols_accountHolder = {'accountHolderID': 'id',
                        'name': 'name',
                        'mainAddress': 'addressMain',
                        'secondaryAddress': 'addressSecondary',
                        'postalCode': 'postalCode',
                        'city': 'city',
                        'country': 'country_id',
                        "telephone1": "telephone1",
                        "telephone2": "telephone2",
                        "eMail": "eMail", 
                        "legalEntityIdentifier": "legalEntityIdentifier", 
                        # "accountID": "account_id"
                        }
    df = df.rename(columns=cols_accountHolder)[cols_accountHolder.values()].copy()   
    
    # add created timestamp
    df["created_on"] = datetime.now()
    df["updated_on"] = datetime.now()
    
    # save table 
    df.to_csv(dir_out + "accountHolders.csv", index=False, encoding="utf-8")
    # also save the mapping from account holder to accounts 
    df_map_accountHolders.to_csv(dir_in + "accountHolder_mapping.csv", index=False)
    return 


def create_table_account(dir_in, dir_out):
    """Create account table. 
    AccountHolder table needs to be created first
    :param dir_data: <string> directory with parsed data
    :param dir_out: <string> output directory"""    
    # get account data and mapping for account types
    df_acc = pd.read_csv(dir_in + "accounts.csv",
                    parse_dates=["closingDate", "openingDate"])
    
    # renamce columns
    cols_account = {'accountID': 'accountIDEutl',
                    'accountName': 'name',
                    'registryCode': 'registry_id',
                    'accountType': 'accountType_id',
                    'openingDate': 'openingDate',
                    'closingDate': 'closingDate',
                    'status': 'isOpen',
                    "commitmentPeriod": "commitmentPeriod",
                    'companyRegistrationNumber': 'companyRegistrationNumber',
                    'installationID': 'installation_id'}                    
    # mark accounts with status "closing pending" as closed
    # note that accounts with missin status are accounts of MT and CY in first periods. Thus, closed
    df_acc["status"] = df_acc.status.replace({"closed": False, 
                                              "open": True, 
                                              "Closure Pending": False}).fillna(False).astype("boolean")
    df_acc = df_acc.rename(columns=cols_account)[cols_account.values()].copy()

    # impose accountTypes_ids
    df_acc.accountType_id = df_acc.accountType_id.map(map_account_type_inv)

    # make installation id unique
    def form_id(row):
        if pd.notnull(row["installation_id"]):
            return f'{row["registry_id"]}_{int(row["installation_id"])}'
        return
    df_acc.installation_id = df_acc.apply(form_id, axis=1)

    # Clean account names:
    df_acc["name"] = df_acc["name"].map(lambda x: "-".join(x.split("-")[1:])[4:])
    
    # add EU offset accounts by hand
    # NOTE: We could also identify the missing accounts my non-matches and download the information
    res = []
    for i in NEW_ACC:
        print("Added missing account:", i)
        if i["accountIDEutl"] in df_acc.accountIDEutl:
            continue
        
        res.append(i)
    df_new = pd.DataFrame(res)
    if len(df_new) > 0:
        df_acc = pd.concat([df_acc, df_new])
     
    # add account identifiers used in transactions
    map_accId = pd.read_csv(dir_in + "account_mapping.csv")
    map_accId = map_accId.set_index("accountID")["accountIdentifierDB"].to_dict() 
    df_acc["accountIDtransactions"] = df_acc.accountIDEutl.map(lambda x: map_accId.get(x))
    df_acc["isRegisteredEutl"] = df_acc["accountIDtransactions"].notnull()

    # add the corresponding account holder ID

    mapper = (pd.read_csv(dir_in + "accountHolder_mapping.csv")
                .set_index("accountID")
                .accountHolderID.to_dict()
            )
    df_acc["accountHolder_id"] = df_acc["accountIDEutl"].map(lambda x: mapper.get(x))
    
    # add created timestamp
    df_acc["created_on"] = datetime.now()
    df_acc["updated_on"] = datetime.now()    

    # save to csv 
    df_acc.to_csv(dir_out + "accounts.csv", index=False, encoding="utf-8")
    return


def create_table_transaction(dir_in, dir_out):
    """Create transaction table. This has to be run after all 
       all other tables have been created.
    :param dir_data: <string> directory with parsed data
    :param dir_out: <string> output directory"""
    # load data: we need original transaction data as well as 
    # as the account table with new account ID. Also load already 
    # created project table to (eventually) add further projects.
    # Finally, unit type mappings to map to unitType_id
    # merge information from main transaction table to blocks
    df = pd.read_csv(dir_in + "transactionBlocks.csv", low_memory=False, 
                           parse_dates=["transactionDate"])
    
    # extract cdm projects included in transaction data
    # in version 05/2021 that does not seem to be necessary anymore
    # NOTE: Here we drop one entry for project 5342 which as origin entry GH and NG.
    df_proj_trans = df[["projectID", "projectTrack", "originatingRegistry"]
                    ].dropna(subset=["projectID"]).drop_duplicates(subset=["projectID"])
    df_proj_trans.columns = ["id", "track", "country_id"]

    df_proj_trans["created_on"] = datetime.now()
    df_proj_trans["updated_on"] = datetime.now()
    df_proj_trans["source"] = "transactions"
    df_proj_trans
    df_proj = pd.read_csv(dir_out + "offset_projects.csv")
    df_proj["source"] = "surrendering_details"
    # only include those additional projects
    df_proj_trans = df_proj_trans[~df_proj_trans["id"].isin(df_proj["id"])]
    df_proj_new = pd.concat([df_proj, df_proj_trans])
    df_proj_new.to_csv(dir_out + "offset_projects.csv", index=False, encoding="utf-8")
    

    # create accounts which do not exist in the account table
    # get accounts with accountID in transaction data but
    # account missing in account table (all MT0 and CY0)
    # we create accounts out of the data provided in the 
    # transaction data
    res = []
    for pf in ["acquiring", "transferring"]: 
        df_miss = df[df[pf + "AccountID"].isnull()].drop_duplicates()
        df_miss = df_miss[[pf + "AccountIdentifierDB", pf + "AccountIdentifier", pf + "AccountName", pf + "RegistryCode"]]
        df_miss.columns = ["accountIdentifierDB", "accountIDtransactions", "accountName", "registryCode"]
        res.append(df_miss)
    df_miss = pd.concat(res).drop_duplicates()

    # for those accounts without an accountIdentierDB we
    # create an account "unknwon" which is unique by country
    if df_miss[df_miss.accountIdentifierDB.isnull()].registryCode.is_unique:
        df_miss.accountIdentifierDB = df_miss.accountIdentifierDB.fillna(df_miss.registryCode + "_unknown")
        df_miss.accountIDtransactions = df_miss.accountIDtransactions.fillna("unknown")

    # these are accounts that are missing in the account database
    # to easily identify and to get in conflict with newly emerging
    # account IDs provided by the EUTL, we assign negative integers
    # as account ids
    df_miss = df_miss.reset_index(drop=True)
    df_miss["accountIDEutl"] = -df_miss.index - 1
    df_miss["created_on"] = datetime.now()
    df_miss["updated_on"] = datetime.now()
    
    # also insert the corresponding account id into the
    # transaction block data
    map_acc_new = df_miss[["accountIdentifierDB", "accountIDEutl"]
                        ].set_index("accountIdentifierDB")["accountIDEutl"].to_dict()
    for pf in ["acquiring", "transferring"]:
        df[pf + "AccountIdentifierDB"] = (df[pf + "AccountIdentifierDB"]
                                        .fillna(df[pf + "RegistryCode"] + "_unkown"))
        df[pf + "AccountID"] = (df[pf + "AccountID"]
                                .fillna(df[pf + "AccountIdentifierDB"]
                                        .map(lambda x: map_acc_new.get(x))
                                    )
                            )
        
    # Update account list as well as account mapping list
    df_map_acc = pd.read_csv(dir_in + "account_mapping.csv")
    df_map_acc = pd.concat([df_map_acc, 
                            pd.DataFrame([[k, v] for k,v in map_acc_new.items()],
                                        columns=["accountIdentifierDB", "accountID"])
                        ])
    df_acc = pd.read_csv(dir_out + "accounts.csv")
    df_acc = pd.concat([df_acc, 
                        df_miss.rename(columns={
                            "accountName": "name",
                            "registryCode": "registry_id",})
                    ])
    mapper = df_map_acc.set_index("accountID")["accountIdentifierDB"].to_dict() 
    df_acc.accountIdentifierDB = df_acc.accountIDEutl.map(lambda x: mapper.get(x))
    df_acc.to_csv(dir_out + "accounts.csv", index=False) 
    df_map_acc.to_csv(dir_in + "account_mapping.csv", index=False)
    
    # select and rename transaction columns and save to csv 
    cols_trans = {'transactionID': 'transactionID',
                'transactionDate': 'date',
                'transactionTypeMain': 'transactionTypeMain_id',
                'transactionTypeSupplementary': 'transactionTypeSupplementary_id',
                'transferringAccountID': 'transferringAccount_id',
                'acquiringAccountID': 'acquiringAccount_id',
                'unitTypeCode': 'unitType_id',
                'projectID': 'project_id',
                'amount': 'amount'}    
    df = df.rename(columns=cols_trans)
    df = df[cols_trans.values()]
    df["id"] = df.reset_index().index  

    # add created timestamp
    df["created_on"] = datetime.now()
    df["updated_on"] = datetime.now()   
         
    # save to csv 
    df.to_csv(dir_out + "transactionBlocks.csv", index=False, encoding="utf-8")    
    return


def create_tables_lookup(dir_in, dir_out,
                         fn_nace_codes=None):
    """Create transaction table.
    We only alter the header of tables from "code" to "id"
    :param dir_data: <string> directory with parsed data
    :param dir_out: <string> output directory
    :param fn_nace_codes: <string> name of file with nace classification scheme
                If None, calssification lookup not exported"""
    export_mappings(dir_in)
    for fn in glob.iglob(dir_in + "*Codes.csv"):
        fn_out = dir_out + "/" + os.path.basename(fn)
        df = pd.read_csv(fn, keep_default_na=False)
        df.columns = ["id", "description"]
        df.to_csv(fn_out, index=False, encoding="utf-8")
    if fn_nace_codes is not None: 
        fn_out = dir_out + "/" + os.path.basename(fn_nace_codes)
        df = pd.read_csv(fn_nace_codes)
        df.to_csv(fn_out, index=False, encoding="utf-8")
    return