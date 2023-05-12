import pandas as pd


def parse_leakage_lists(fn_leakage_2015, fn_leakage_2020, fn_out):
    """Parse leakage lists
    :param fn_leakage_2015: <str> path to 2015 leakage list
    :param fn_leakage_2020: <str> path to 2020 leakage list
    :param fn_out: <str> output file name
    :return: <pd.DataFrame> with nace classification by installation
    """
    # get the two leakage lists and merge them
    df_l15 = pd.read_excel(fn_leakage_2015, na_values="-")
    df_l15["id"] = (
        df_l15.COUNTRY_CODE + "_" + df_l15.INSTALLATION_IDENTIFIER.astype("str")
    )
    df_l15 = df_l15[["id", "NACE Rev2"]].copy()
    df_l15.columns = ["id", "nace15"]
    df_l15 = df_l15[df_l15.nace15.notnull()].copy()

    df_l20 = pd.read_excel(fn_leakage_2020, skiprows=2)
    df_l20["id"] = (
        df_l20.COUNTRY_CODE + "_" + df_l20.INSTALLATION_IDENTIFIER.astype("str")
    )
    df_l20 = df_l20[["id", "NACE Rev2"]].copy()
    df_l20.columns = ["id", "nace20"]
    df_l20 = df_l20[df_l20.nace20.notnull()].copy()

    # merge the lists
    # For the overall nace value use the more recent 2020 value if available
    # else fallback to 2015
    df_nace = df_l15.merge(df_l20, on="id", how="outer")
    df_nace["nace"] = df_nace.nace20.fillna(df_nace.nace15)

    # Modify codes to have always have two digits before the dot
    to_adjust = df_nace.nace.map(lambda x: len(str(x).split(".")[0]) < 2)
    df_nace.loc[to_adjust, "nace"] = df_nace[to_adjust].nace.map(lambda x: "0" + str(x))

    to_adjust = df_nace.nace15.map(lambda x: len(str(x).split(".")[0]) < 2)
    df_nace.loc[to_adjust, "nace15"] = df_nace[to_adjust].nace15.map(
        lambda x: "0" + str(x)
    )

    to_adjust = df_nace.nace20.map(lambda x: len(str(x).split(".")[0]) < 2)
    df_nace.loc[to_adjust, "nace20"] = df_nace[to_adjust].nace20.map(
        lambda x: "0" + str(x)
    )

    # save data
    df_nace.to_csv(fn_out, index=False)

    return df_nace


def extract_nace_scheme(fn_in, fn_out):
    """Extract NACE codes with sub-classification from html
    file provided by Eurostat RAMON
    :param fn_in: <str> path to input html file from Eurostat RAMON
    :param fn_out: <str> output file path
    :return: <pd.DataFrame> with nace classification scheme
    """
    df_in = pd.read_html(fn_in)[0]

    # bring all levels into one dataframe and structure them
    col_rename = {
        "Code": "id",
        "Level": "level",
        "Parent": "parent_id",
        "Description": "description",
        "This item includes": "includes",
        "This item also includes": "includesAlso",
        "Rulings": "ruling",
        "This item excludes": "excludes",
        "Reference to ISIC Rev. 4": "isic4_id",
    }
    df_all = df_in.rename(columns=col_rename)[col_rename.values()].copy()

    # Need to add the level 3 codes ending with .0 as they are sometime used
    new_rows = []
    for i, row in df_all[df_all.level == 2].iterrows():
        if row["id"] + ".0" not in df_all.id.values:
            r = {k: v for k, v in row.items()}
            r["id"] = r["id"] + ".0"
            r["level"] = 3
            r["parent_id"] = row["id"]
            r["isic4_id"] = r["isic4_id"] + "0"
            new_rows.append(r)
    df_ = pd.DataFrame(new_rows)

    # save to disk
    df_out = pd.concat([df_all, df_])
    df_out.to_csv(fn_out, index=False)
    df_out.id.is_unique

    return df_out
