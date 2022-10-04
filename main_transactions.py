from eutl_scraper.transactions import link_accounts, parse_ec_transactions

if __name__ == "__main__":

    # path to file downloaded from European Commission
    # https://ec.europa.eu/clima/eu-action/eu-emissions-trading-system-eu-ets/union-registry_en#tab-0-1
    fn_trans_ec = "./data/additional/transactions_commission/transactions_eutl_2022.zip" 
    
    print("###### Parse transaction data provided by European Commission")
    df = parse_ec_transactions(fn_trans_ec, dir_out="./data/parsed/")
    print("###### Link transaction to account data")
    link_accounts("./data/parsed/", df=df)