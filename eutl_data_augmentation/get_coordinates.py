import pandas as pd
import googlemaps
from .mappings import map_registryCodes


def form_address(row):
    """Forms address based on Series with address data
    :param row: <pd.Series> with address data"""
    address = ""
    for a in ["mainAddress", "secondaryAddress", "postalCode", "city", "country"]:
        if pd.notnull(row[a]):
            if a == "country":
                address += f"{map_registryCodes.get(row[a])}, "
            else:
                address += f"{row[a]}, "
    if len(address) > 0:
        address = address[:-2]
    return address, row["country"]


def get_gmaps_coordinates(gmaps, address, countryCode=None):
    """Get latitude and longitude from google maps
    :param gmaps: <googlemaps.client.Client> google maps client
    :param address: <string> address
    :param countryCode: <string> two digit iso code"""
    # get locations
    # for countries with oversea teritores exclude the country identifier
    if countryCode in ["FR", "GB", "NL", "DK", "NO"]:
        loc = gmaps.geocode(address=address)
    else:
        loc = gmaps.geocode(address=address, components={"country": countryCode})
    # if get no results, try without countryCode to include overseas territories
    """ if len(loc) == 0:
         loc = gmaps.geocode(address=address)"""

    # in case of multiple matches, we take the first one
    if len(loc) > 0:
        try:
            return tuple(loc[0]["geometry"]["location"].values())
        except KeyError:
            pass
    return None, None


def get_installation_coordinates_google(
    dir_in, key_google, fn_out=None, ignore_existing=False
):
    """Gets installation coordinates using googlemaps api
    :param dir_in: <string> path to directory with downloaded installation
        data file
    :param key_google: <string> google api key
    :param fn_out: <string> name output file. If None, output not saved
    :param ignore_existing: <boolean> to ignore already existing entries
    """
    # get installation data, excluding aircrafts
    df_in = pd.read_csv(dir_in + "installations.csv").query(
        "activity != '10-Aircraft operator activities'"
    )

    # if a file is provided and if we do not ignore it, get already existing
    # installation coordinates
    if fn_out and (not ignore_existing):
        df_existing = pd.read_csv(fn_out)
        df_to_process = df_in[
            ~df_in.installationID.isin(df_existing.installation_id)
        ].copy()
    else:
        df_existing = pd.DataFrame()
        df_to_process = df_in

    # google client
    gmaps = googlemaps.Client(key=key_google)

    # loop over installations, get address and coordinates
    lst_res = []
    print(f"Fetch locations for {len(df_to_process)} installations")
    for i, (_, row) in enumerate(df_to_process.iterrows()):
        if ((i + 1) % 500) == 0:
            print(
                "Get GoogleMaps coordinates for installation %s (%d/%d)"
                % (row["installationID"], (i + 1), len(df_in))
            )
        address, countryCode = form_address(row)
        lat, lng = get_gmaps_coordinates(gmaps, address, countryCode=countryCode)
        if lat:
            res = {}
            res["installation_id"] = row["installationID"]
            res["latitude"] = lat
            res["longitude"] = lng
            lst_res.append(res)
    df_new = pd.DataFrame(lst_res)
    df = pd.concat([df_existing, df_new])
    print(
        f"""Retrieved new locations for {len(df)} installations. 
        In total {len(df)} of {len(df_in)} installations have locations"""
    )

    # save file
    if fn_out:
        df.to_csv(fn_out, index=False)
    return df
