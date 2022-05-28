import requests
import time
import pandas as pd
import re
from zipfile import ZipFile
from .mappings import map_unitType_inv, map_registryCode_inv


def parse_ec_transactions(fn, fn_file="transactions_EUTL_PUBLIC_NOTESD_20220502.csv",
                          dir_out=None):
    """Parse transaction file as provided by the European Commission
    see: https://ec.europa.eu/clima/eu-action/eu-emissions-trading-system-eu-ets/union-registry_en#tab-0-1
    :param fn: <string> path to zip file with transaction as provided by commissions
    :param fn_file: <string> name of csv file inside the zip file
    :dir_out: <string> path to output directory (normally the one with all parsed data)    
    """
    # get data as provided by commission
    with ZipFile(fn) as zf:
        with zf.open(fn_file, 'r') as infile:
            df = pd.read_csv(infile, low_memory=False)
            
    cols = ['TRANSACTION_ID', 'TRANSACTION_TYPE', 'TRANSACTION_DATE', 'UNIT_TYPE_DESCRIPTION', 
            'SUPP_UNIT_TYPE_DESCRIPTION', 'ORIGINATING_REGISTRY', 
            'PROJECT_IDENTIFIER', 'TRACK', 'AMOUNT',
            "TRANSFERRING_REGISTRY_NAME", "ACQUIRING_REGISTRY_NAME",
            "TRANSFERRING_ACCOUNT_NAME", "ACQUIRING_ACCOUNT_NAME",
            "TRANSFERRING_ACCOUNT_IDENTIFIER", "ACQUIRING_ACCOUNT_IDENTIFIER"]

    df = df[cols].copy()
    # get transaction types
    df["transactionTypeMain_id"] = df["TRANSACTION_TYPE"].map(lambda x: x.split("-")[0])
    df["transactionTypeSupplementary_id"] = df["TRANSACTION_TYPE"].map(lambda x: x.split("-")[1])

    # get unit types
    nans = pd.to_numeric("A", errors='coerce')
    replacer = {
        "ERU - Emission Reduction Unit": "ERU - Emission Reduction Unit (Converted from an AAU)",
        "CER - Certified Emission Reduction Unit converted from an AAU": 'CER - Certified Emission Reduction Unit',
        "ERU - Converted from an RMU": "ERU - Emission Reduction Unit (Converted from an RMU)", 
        "AAU - Assigned Amount Unit": nans,
        "Non-Kyoto Unit": nans
    }
    df["unitType"] = df["UNIT_TYPE_DESCRIPTION"].map(lambda x: replacer.get(x, x))
    replacer = {
        'No supplementary unit type': nans,
        'Allowance issued for the 2008 to 2012 and subsequent five-year periods by a Member State that does not have AAUs': 'AAU - Assigned Amount Unit - Allowance issued for the 2008-2012 period and subsequent 5-year periods and is converted from an AAU',
        'Allowance issued for the 2008-2012 period and subsequent 5-year periods and is converted from an AAU': 'AAU - Assigned Amount Unit - Allowance issued for the 2008-2012 period and subsequent 5-year periods and is converted from an AAU',
    }
    df["unitType"] = (df["unitType"]
                     .fillna(df["SUPP_UNIT_TYPE_DESCRIPTION"].map(lambda x: replacer.get(x, x)))
                     .fillna('AAU - Assigned Amount Unit - Allowance issued for the 2008-2012 period and subsequent 5-year periods and is converted from an AAU')
                     )
    df["unitType_id"] = df["unitType"].map(map_unitType_inv)

    def create_account_id(row, prefix):
        if pd.notnull(row[prefix.upper() + "_ACCOUNT_IDENTIFIER"]):
            return row[prefix + "RegistryCode"] + "_%d" % row[prefix.upper() + "_ACCOUNT_IDENTIFIER"]
    # convert registries to codes
    for pf in ["acquiring", "transferring"]:
        # convert registries to codes
        df[pf + "RegistryCode"] = df[pf.upper() + "_REGISTRY_NAME"].map(lambda x: map_registryCode_inv.get(x.strip()))
        # Create an account identifier unique in the transaction database
        df[pf + "AccountIdentifierDB"] = df.apply(lambda x: create_account_id(x, pf), axis=1)

    # rename remaining columns
    new_names = {
        "TRANSACTION_ID": "transactionID",
        "TRANSACTION_DATE": "transactionDate",
        "ORIGINATING_REGISTRY": "originatingRegistry",
        "PROJECT_IDENTIFIER": "projectID",
        "TRACK": "projectTrack",
        "AMOUNT": "amount",
        "TRANSFERRING_ACCOUNT_IDENTIFIER": "transferringAccountIdentifier", 
        "ACQUIRING_ACCOUNT_IDENTIFIER": "acquiringAccountIdentifier",
        "TRANSFERRING_ACCOUNT_NAME": "transferringAccountName",
        "ACQUIRING_ACCOUNT_NAME": "acquiringAccountName",
    }
    df = df.rename(columns=new_names)

    # add a unique transaction id and clean up columns
    df.index.name = "id"
    df = df.drop(["TRANSACTION_TYPE", "unitType", 
                  "UNIT_TYPE_DESCRIPTION", "SUPP_UNIT_TYPE_DESCRIPTION",
                  "TRANSFERRING_REGISTRY_NAME", "ACQUIRING_REGISTRY_NAME",
                 ], axis=1)      
    
    # save file:
    if dir_out is not None:
        df.to_csv(dir_out + "transactionBlocks.csv")
    return df


def link_accounts(dir_in, df=None, tries=3, wait=5):
    """Linking of account identifiers used in the transaction data and
       those used in the account database
    :param dir_in: <string> path to input data directory with data scraped
    :param df: <pd.DataFrame> with transaction data. If provided, data will not
                be loaded from file
    :param tries: <int> number of retries for downloads
    :param wait: <int> number of seconds to wait between retires"""
    # get transaction block data
    if df is None:
        df = pd.read_csv(dir_in + "transactionBlocks.csv", low_memory=False)

    # get a list of unique account identifies in the transaction database
    acc_id_db = list(set(list(df.transferringAccountIdentifierDB.unique()) +
                             list(df.acquiringAccountIdentifierDB.unique())))
    acc_id_db = [a for a in acc_id_db if pd.notnull(a)]  

    # loop over all account identifiers in the transaction database and get the accountID
    def get_linked_accountID(content):
        """ Establish mapping from accountIdentifier to accountID
        :param content: <string> content of downloaded account page
        """
        if content is None:
            return
        expr = b'<input type="hidden" name="accountID" value="\d*">'
        found = re.findall(expr, content)
        if len(found) > 0:
            found2 = re.findall(b"\d+", found[0])
            if len(found2) > 0:
                return int(found2[0])
        return

    # basic settings for donwload
    base_url = "https://ec.europa.eu/environment/ets/singleAccount.do?"
    base_qry = {"accountID": "",
                "action": "transaction",
                "registryCode": None,
                "accountIdentifier": None}

    # function to catch commissions errors string 
    def is_success(x):
            return b"An error occurred during execution of the request. Please try again later or redefine the request." not in x

    # loop over all accountIdentifier and extract associated accountID
    res = {}
    
    for i, accIdentifierDB in enumerate(acc_id_db):  
        if i % 100 == 0:
            print("Link accountIdentifier %s (%d/%d)" % (accIdentifierDB, i, len(acc_id_db)))
        # prepare query getting data from accountIdentifier
        query = dict(base_qry)
        query["registryCode"] = accIdentifierDB.split("_")[0]
        query["accountIdentifier"] = str(accIdentifierDB.split("_")[-1])    
         # download page
        content = download_file(base_url, parms=query, tries=tries, wait=wait, test_success=is_success)    

        accountID = get_linked_accountID(content)
        if accountID is not None:
            res[accIdentifierDB] = accountID
        else: 
            print("\tFailed to link transaction account: %s" % accIdentifierDB)
            
    # for security save the account mapping
    df_map = pd.DataFrame([[k,v] for k, v in res.items()], columns=["accountIdentifierDB", "accountID"])   
    df_map.to_csv(dir_in + "account_mapping.csv", index=False)
    
    # map account identified into transaction data
    for pf in ["acquiring", "transferring"]:
        df[pf + "AccountID"] = df[pf + "AccountIdentifierDB"].map(lambda x: res.get(x))
        
    # save transaction data
    df.to_csv(dir_in + "transactionBlocks.csv", index=False) 
    return

def download_file(url, parms=None, outfile=None, tries=1, wait=0, test_success=None):
    """Downloads file and checks for reponse status
    :param url: <string> basic url for the query
    :param parms: <dict> parameters for query
    :param outfile: <string> file name of output file
                    if None not saved to disk
    :param tries: <int> number of retries for download
    :param wait: <int> seconds to wait between retries
    :param test_success: <function(content) -> Boolean> function to test sucess of download based on content.
                          Output has to be boolean with True for sucess
    :return: <string> content of downloaded pages
             If download fails None is returned
    """
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36",
               "Accept-Encoding": "gzip, deflate, sdch",
               "Accept-Language": "de-DE,de;q=0.8,en-US;q=0.6,en;q=0.4,es;q=0.2",
               "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
               "Upgrade-Insecure-Requests": "1",
               "Cache-Control": "max-age=0",
               "Connection": "keep-alive"}

    for _ in range(0, tries):
        try:
            r = requests.get(url, params=parms, headers=headers)
            # test success of download
            success = False
            if r.status_code == 200:
                if test_success is not None:
                    success = test_success(r.content)
                else:
                    success = True
            # save download
            if success:
                if outfile is not None:
                    with open(outfile, "wb") as f:
                        f.write(r.content)
                return r.content
            else:
                time.sleep(wait)
        except Exception:
            continue
    return None
