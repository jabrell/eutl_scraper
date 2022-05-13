import requests
import time
import pandas as pd
import re

def link_accounts(dir_in, tries=3, wait=5):
    """Linking of account identifiers used in the transaction data and
       those used in the account database
    :param dir_in: <string> path to input data directory with data scraped
    :param trie: <int> number of retries for downloads
    :param wait: <int> number of seconds to wait between retires"""
    # get transaction block data
    df = pd.read_csv(dir_in + "transactionBlocks.csv", low_memory=False)
    
    # impose country codes and account identifiers
    def form_accountIdentifier(x, prefix):
        try:
            return f'{x[prefix + "RegistryCode"]}_{int(x[prefix + "AccountIdentifier"])}'
        except:
            return 
    for pf in ["acquiring", "transferring"]:
        df[pf + "RegistryCode"] = df[pf + "Registry"].map(lambda x: map_registryCode_inv.get(x))
        df[pf + "AccountIdentifierDB"] = df.apply(lambda x: form_accountIdentifier(x, pf), axis=1)
        
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

map_registryCodes = {"AT": "Austria",
                     "BE": "Belgium",
                     "BG": "Bulgaria",
                     "HR": "Croatia",
                     "CY": "Cyprus",
                     "CZ": "Czech Republic",
                     "DK": "Denmark",
                     "ED": "ESD",
                     "EE": "Estonia",
                     "EU": "European Commission",
                     "FI": "Finland",
                     "FR": "France",
                     "DE": "Germany",
                     "GR": "Greece",
                     "HU": "Hungary",
                     "IS": "Iceland",
                     "IE": "Ireland",
                     "IT": "Italy",
                     "LV": "Latvia",
                     "LI": "Liechtenstein",
                     "LT": "Lithuania",
                     "LU": "Luxembourg",
                     "MT": "Malta",
                     "NL": "Netherlands",
                     "NO": "Norway",
                     "PL": "Poland",
                     "PT": "Portugal",
                     "RO": "Romania",
                     "SK": "Slovakia",
                     "SI": "Slovenia",
                     "ES": "Spain",
                     "SE": "Sweden",
                     "GB": "United Kingdom",
                     "AF": "Afghanistan",
                     "AX": "Aland Islands",
                     "AL": "Albania",
                     "DZ": "Algeria",
                     "AS": "American Samoa",
                     "AD": "Andorra",
                     "AO": "Angola",
                     "AI": "Anguilla",
                     "AQ": "Antarctica",
                     "AG": "Antigua And Barbuda",
                     "AR": "Argentina",
                     "AM": "Armenia",
                     "AW": "Aruba",
                     "AU": "Australia",
                     "AZ": "Azerbaijan",
                     "BS": "Bahamas",
                     "BH": "Bahrain",
                     "BD": "Bangladesh",
                     "BB": "Barbados",
                     "BY": "Belarus",
                     "BZ": "Belize",
                     "BJ": "Benin",
                     "BM": "Bermuda",
                     "BT": "Bhutan",
                     "BO": "Bolivia",
                     "BQ": "Bonaire, Sint Eustatius and Saba",
                     "BA": "Bosnia And Herzegovina",
                     "BW": "Botswana",
                     "BV": "Bouvet Island",
                     "BR": "Brazil",
                     "IO": "British Indian Ocean Territory",
                     "BN": "Brunei Darussalam",
                     "BF": "Burkina Faso",
                     "BI": "Burundi",
                     "CDM": "CDM",
                     "KH": "Cambodia",
                     "CM": "Cameroon",
                     "CA": "Canada",
                     "CV": "Cape Verde",
                     "KY": "Cayman Islands",
                     "CF": "Central African Republic",
                     "TD": "Chad",
                     "CL": "Chile",
                     "CN": "China",
                     "CX": "Christmas Island",
                     "CC": "Cocos (Keeling) Islands",
                     "CO": "Colombia",
                     "KM": "Comoros",
                     "CG": "Congo",
                     "CD": "Congo, The Democratic Republic Of The",
                     "CK": "Cook Islands",
                     "CR": "Costa Rica",
                     "CI": "Cote D&#39;Ivoire",
                     "CU": "Cuba",
                     "CW": "Curacao",
                     "CY0": "Cyprus CP0",
                     "DJ": "Djibouti",
                     "DM": "Dominica",
                     "DO": "Dominican Republic",
                     "EC": "Ecuador",
                     "EG": "Egypt",
                     "SV": "El Salvador",
                     "GQ": "Equatorial Guinea",
                     "ER": "Eritrea",
                     "ET": "Ethiopia",
                     "FK": "Falkland Islands (Malvinas)",
                     "FO": "Faroe Islands",
                     "FJ": "Fiji",
                     "GF": "French Guiana",
                     "PF": "French Polynesia",
                     "TF": "French Southern Territories",
                     "GA": "Gabon",
                     "GM": "Gambia",
                     "GE": "Georgia",
                     "GH": "Ghana",
                     "GI": "Gibraltar",
                     "GL": "Greenland",
                     "GD": "Grenada",
                     "GP": "Guadeloupe",
                     "GU": "Guam",
                     "GT": "Guatemala",
                     "GN": "Guinea",
                     "GW": "Guinea-Bissau",
                     "GY": "Guyana",
                     "HT": "Haiti",
                     "HM": "Heard Island And Mcdonald Islands",
                     "VA": "Holy See (Vatican City State)",
                     "HN": "Honduras",
                     "HK": "Hong Kong",
                     "ITL": "ITL",
                     "IN": "India",
                     "ID": "Indonesia",
                     "IR": "Iran, Islamic Republic Of",
                     "IQ": "Iraq",
                     "IL": "Israel",
                     "JM": "Jamaica",
                     "JP": "Japan",
                     "JE": "Jersey",
                     "JO": "Jordan",
                     "KZ": "Kazakhstan",
                     "KE": "Kenya",
                     "KI": "Kiribati",
                     "KP": "Korea, Democratic People&#39;S Republic Of",
                     "KR": "Korea, Republic Of",
                     "KW": "Kuwait",
                     "KG": "Kyrgyzstan",
                     "LA": "Lao People&#39;S Democratic Republic",
                     "LB": "Lebanon",
                     "LS": "Lesotho",
                     "LR": "Liberia",
                     "LY": "Libyan Arab Jamahiriya",
                     "MO": "Macao",
                     "MK": "Macedonia, The Former Yugoslav Republic Of",
                     "MG": "Madagascar",
                     "MW": "Malawi",
                     "MY": "Malaysia",
                     "MV": "Maldives",
                     "ML": "Mali",
                     "MT0": "Malta CP0",
                     "MH": "Marshall Islands",
                     "MQ": "Martinique",
                     "MR": "Mauritania",
                     "MU": "Mauritius",
                     "YT": "Mayotte",
                     "MX": "Mexico",
                     "FM": "Micronesia, Federated States Of",
                     "MD": "Moldova, Republic Of",
                     "MC": "Monaco",
                     "MN": "Mongolia",
                     "MS": "Montserrat",
                     "MA": "Morocco",
                     "MZ": "Mozambique",
                     "MM": "Myanmar",
                     "NA": "Namibia",
                     "NR": "Nauru",
                     "NP": "Nepal",
                     "NC": "New Caledonia",
                     "NZ": "New Zealand",
                     "NI": "Nicaragua",
                     "NE": "Niger",
                     "NG": "Nigeria",
                     "NU": "Niue",
                     "NF": "Norfolk Island",
                     "MP": "Northern Mariana Islands",
                     "OM": "Oman",
                     "PK": "Pakistan",
                     "PW": "Palau",
                     "PS": "Palestinian Territory, Occupied",
                     "PA": "Panama",
                     "PG": "Papua New Guinea",
                     "PY": "Paraguay",
                     "PE": "Peru",
                     "PH": "Philippines",
                     "PN": "Pitcairn",
                     "PR": "Puerto Rico",
                     "QA": "Qatar",
                     "RE": "Reunion",
                     "RU": "Russian Federation",
                     "RW": "Rwanda",
                     "BL": "Saint Barthelemy",
                     "SH": "Saint Helena",
                     "KN": "Saint Kitts And Nevis",
                     "LC": "Saint Lucia",
                     "MF": "Saint Martin (French part)",
                     "PM": "Saint Pierre And Miquelon",
                     "VC": "Saint Vincent And The Grenadines",
                     "WS": "Samoa",
                     "SM": "San Marino",
                     "ST": "Sao Tome And Principe",
                     "SA": "Saudi Arabia",
                     "SN": "Senegal",
                     "CS": "Serbia And Montenegro",
                     "SC": "Seychelles",
                     "SL": "Sierra Leone",
                     "SG": "Singapore",
                     "SX": "Sint Maarten (Dutch part)",
                     "SB": "Solomon Islands",
                     "SO": "Somalia",
                     "ZA": "South Africa",
                     "GS": "South Georgia And The South Sandwich Islands",
                     "SS": "South Sudan",
                     "LK": "Sri Lanka",
                     "SD": "Sudan",
                     "SR": "Suriname",
                     "SJ": "Svalbard And Jan Mayen",
                     "SZ": "Swaziland",
                     "CH": "Switzerland",
                     "SY": "Syrian Arab Republic",
                     "TW": "Taiwan, Province Of China",
                     "TJ": "Tajikistan",
                     "TZ": "Tanzania, United Republic Of",
                     "TH": "Thailand",
                     "TL": "Timor-Leste",
                     "TG": "Togo",
                     "TK": "Tokelau",
                     "TO": "Tonga",
                     "TT": "Trinidad And Tobago",
                     "TN": "Tunisia",
                     "TR": "Turkey",
                     "TM": "Turkmenistan",
                     "TC": "Turks And Caicos Islands",
                     "TV": "Tuvalu",
                     "UG": "Uganda",
                     "UA": "Ukraine",
                     "AE": "United Arab Emirates",
                     "US": "United States",
                     "UM": "United States Minor Outlying Islands",
                     "UY": "Uruguay",
                     "UZ": "Uzbekistan",
                     "VU": "Vanuatu",
                     "VE": "Venezuela",
                     "VN": "Viet Nam",
                     "VG": "Virgin Islands, British",
                     "VI": "Virgin Islands, U.S.",
                     "WF": "Wallis And Futuna",
                     "EH": "Western Sahara",
                     "YE": "Yemen",
                     "ZM": "Zambia",
                     "ZW": "Zimbabwe",
                     "XI": "Northern Ireland",
                     "ESD": "ESD"}

map_registryCode_inv = {v: k for k, v in map_registryCodes.items()}