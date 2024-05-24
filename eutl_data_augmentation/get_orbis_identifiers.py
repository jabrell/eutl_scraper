import pandas as pd
import numpy as np

from .mappings import map_registryCode_inv

def impute_orbis_identifiers(fn_jrc, fn_acc, fn_contacts, fn_out):
    """Impute ORBIS identifiers from JRC matching into account table
    :param fn_jrc: <str> path to excel file with JRC Orbis matching
    :param fn_acc: <str> path to account file as downloaded by scrapy
    :param fn_out: <str> output file name
    :return: <pd.DataFrame> with account data including orbis id
    """
    # get the data
    df_jrc = pd.read_excel(
        fn_jrc,
        sheet_name="JRC-EU ETS-FIRMS",
        dtype={"EUTL_REGID": str}
        )
    df_acc = pd.read_csv(
        fn_acc,
        dtype={"companyRegistrationNumber": str}
        )
    df_contacts = pd.read_csv(
        fn_contacts,
        usecols=["accountID", "country"],
        )
    df_contacts["countryCode"] = df_contacts.country.map(map_registryCode_inv)
    df_acc = df_acc.merge(
            df_contacts[["accountID", "countryCode"]],
            on="accountID",
            how='left',
            validate='one_to_one'
        )
    

    # intial overview
    accRegNum = df_acc.companyRegistrationNumber.astype("str").unique()
    jrcRegNum = df_jrc.EUTL_REGID.astype("str").unique()
    print(
        f"""EUTL data: {len(df_acc[df_acc.companyRegistrationNumber.notnull()])} accounts with company registration number of which {len(accRegNum)} are unique.
    JRC data: {len(df_jrc[df_jrc.EUTL_REGID.notnull()])} accounts with company registration number of which {len(jrcRegNum)} are unique. {len(df_jrc[df_jrc.ORBIS_BVD_ID.notnull()].ORBIS_BVD_ID.unique())} unique ORBIS ids are provided
    {len(np.setdiff1d(jrcRegNum, accRegNum))} company registration numbers are in the JRC list but not in the EUTL accounts.
    {len(np.setdiff1d(accRegNum, jrcRegNum))} companyregistration numbers are in the EUTL accounts but not in the JRC list.
    {len(set(accRegNum).intersection(set(jrcRegNum)))} company registration numbers can be matched"""
    )
    
    # some national identifiers miss leading 0s
    # e.g., for Umicore: 401574852 (JRC) versus 0401574852 (EUTL)
    # repeat merge on national identifiers with leading zeroes stripped
    df_acc['companyRegistrationNumberStripped'] = df_acc.companyRegistrationNumber.str.lstrip('0')
    df_jrc['EUTL_REGID_STRIPPED'] = df_jrc.EUTL_REGID.str.lstrip('0')
    
    # first merge of JRC and EUTL
    on_first = {
        "EUTL_AH_COUNTRY_ID": "countryCode",
        "EUTL_REGID": "companyRegistrationNumber"
        }
    df_merged = do_merge(df_jrc=df_jrc, df_acc=df_acc, on=on_first)
    
    # second merge of JRC and EUTL
    on_second = {
        "EUTL_AH_COUNTRY_ID": "countryCode",
        "EUTL_REGID_STRIPPED": "companyRegistrationNumberStripped"
        }
    df_merged = do_merge(df_jrc=df_jrc, df_acc=df_merged, on=on_second)

    # short summary
    print(
        f"""After ingesting ORBIS identifiers provided by the JRC matching:
    {len(df_merged)} accounts 
    {len(df_merged[df_merged.jrcBvdId.notnull()])} with Orbis identifier"""
    )

    # save data
    if fn_out:
        df_merged.to_csv(fn_out, index=False)

    return df_merged


def do_merge(df_jrc, df_acc, on):
    # drop accounts with missing company registration number from JRC list
    # also drop duplicates by prioritizing matches that are valid by location
    # and afterwards choose those with higher name matching score
    df_jrc_ = df_jrc.sort_values(
        list(on) + ["JRC_IS_VALID_BY_LOCATION", "JRC_NAME_SIMILARITY_RATIO"],
        ascending=[True, True, False, False],
    )
    df_jrc_.drop_duplicates(
        subset=list(on),
        keep="first",
        inplace=True
        )

    # rename columns in the JRC dataframe
    col_rename = {
        "EUTL_AH_NAME": "jrcAccountHolderName",
        "JRC_EUTL_LEI_STD": "jrcLEI",
        "ORBIS_BVD_ID": "jrcBvdId",
        "JRC_REGID_TYPE": "jrcRegistrationIdType",
        "JRC_REGID_STD": "jrcRegistrationIDStandardized",
        "JRC_ORBIS_NAME_STD": "jrcOrbisName",
        "JRC_ORBIS_POSTCODE_STD": "jrcOrbisPostalCode",
        "JRC_ORBIS_CITY_STD": "jrcOrbisCity",
    }
    col_rename.update(on)
    df_jrc_ = df_jrc_[list(col_rename.keys())].rename(columns=col_rename)

    # merge the frames
    df_merged = df_acc.merge(
        df_jrc_,
        on=list(on.values()),
        how="left",
        validate='many_to_one',  # a single company can have multiple accounts
        suffixes=(None, '_merged')
        )
    
    # fill values missing from previous merge and drop duplicate columns
    for column in df_merged.columns[df_merged.columns.str.endswith("_merged")]:
        df_merged[column.replace("_merged", "")].fillna(
            df_merged[column],
            inplace=True
            )
        df_merged.drop(column, axis=1, inplace=True)
        
    # check that we do not lost any EUTL accounts or created duplicates
    assert len(df_merged) == len(
        df_acc
    ), "Mismatch in account data after merging ORBIS matching"
    
    return df_merged
    
