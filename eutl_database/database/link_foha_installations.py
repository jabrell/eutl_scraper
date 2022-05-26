from .DataAccessLayer import DataAccessLayer
from .model import Account, Compliance, Installation
from sqlalchemy import and_
import pandas as pd
from collections import Counter


def link_foha_installations(session, fn_out, min_score=0.75,
                            acceptDuplicates=False):
    """ Linking between former operator holding accounts and 
    installations. 
    :param session: <sqlalchemy.orm.Session>    
    :param fn_out: <string> to save matching details      
    :param min_score: <float> minimum score to accept matches. Score is provided
                              as percentage of transactions over years matched by installation
                              Default: 0.75
    """
    print("------- Establish link between former operator holding accounts and installations")
    df = get_link_foha_installation(session=session, 
                                    fn_out=fn_out, min_score=min_score)
    print("------- Insert linking to database")
    insert_link_foha_installations(df, session, acceptDuplicates=acceptDuplicates)


def insert_link_foha_installations(df, session, acceptDuplicates=False):
    """Insert ids of installations for former operator accounts
    :param df: <pd.DataFrame> with mapping from former opertor account id to installation id
    :param session: <sqlalchemy.orm.Session>
    :param  acceptDuplicates: <boolean> Accept installations mapped to more than one account
    """
    if acceptDuplicates:
        df_ = df.copy()
    else:
        df_ = df.drop_duplicates(subset=["installation_id"])
        print(f"Dropped {len(df)-len(df_)} matches due to duplicated installation id")
    for i, row in df_.iterrows():
        if (i + 1) % 1000 == 0:
            print("Insert installation_id for former operator holding account %s (%d/%d)" 
                  % (row["name"], (i + 1), len(df_)))
        session.query(Account).filter(Account.id == row["id"]
                                     ).update({Account.installation_id: row["installation_id"]})
    session.commit()


def get_link_foha_installation(session, fn_out, min_score=0.75):
    """ Linking of former operator holding accounts to installation id
    :param session: <sqlalchemy.orm.Session>
    :param min_score: <float> minimum score to accept matches. Score is provided
                              as percentage of transactions over years matched by installation
    :param fn_out: <string> to save matching details
    :return: <pd.DataFrame> with linking, linking score and method
    """
    # get former operator holding accounts
    foha = session.query(Account).filter(and_(Account.accountType_id == "120-0",
                                             Account.installation_id == None)).all()
    print(f"{len(foha)} former operator holding accounts to match")
    
    # Step 1: Match by name of installation and account holder via current operator holding account
    print("------ Try matching of former operating accounts based on accountHolder")
    lst_res = []
    for i, fo in enumerate(foha):
        if i % 1000 == 0:
            print(f"Try to match account number {i} of {len(foha)} by name of account holder")
        q = session.query(Account).filter_by(name=fo.name, 
                                             accountHolder_id=fo.accountHolder_id, 
                                             accountType_id="100-7").all()
        # only accept unique matches
        if len(q) == 1: 
            # store matches
            res = {}
            res["id"] = fo.id
            res["name"] = fo.name
            res["installation_id"] = q[0].installation_id
            lst_res.append(res)
    df_matched_name = pd.DataFrame(lst_res)
    df_matched_name["matched_by"] = "name"
    # exclude matches that match on the same installation
    df_matched_name = df_matched_name[~df_matched_name.installation_id.duplicated(keep=False)].copy()
    print("Uniquely matched %d former operator holding accounts (of %d) to installations using account name and holder"
          % (len(df_matched_name), len(foha)))    

    # Step 2: match by surrendering transfer
    print("------ Try matching of former operating accounts based on surrendering transfers")
    lst_matched = [int(i) for i in list(df_matched_name.id.unique())]
    foha_to_match = session.query(Account
                                 ).filter(and_(
                                    Account.accountType_id == "120-0",
                                    Account.installation_id == None,
                                    ~Account.id.in_(lst_matched)
                                    )).all()
    print("Try to match remaining %d former operator holding accounts by surrendering transfers"
          % len(foha_to_match))
    lst_res = []
    for i, fo in enumerate(foha_to_match):
        if i % 100 == 0:
            print("Try to match %s by surrendering transfers (%d/%d)" % (fo.name, i, len(foha_to_match)))
        matched = get_surrendering_matches(fo, session)
        if len(matched) > 0:
            matched = sorted(matched.items(), key=lambda x: x[1], reverse=True)
            if matched[0][1] >= min_score:
                res = {}
                res["id"] = fo.id
                res["name"] = fo.name
                res["installation_id"] = matched[0][0]
                res["score"] = matched[0][1]
                lst_res.append(res)
    df_matched_surrender = pd.DataFrame(lst_res)
    df_matched_surrender["matched_by"] = "surrender"
    df_matched_surrender = df_matched_surrender.drop_duplicates(subset=["installation_id"], keep=False)
    print("Uniquely matched %d former operator holding accounts (of %d) to installations using surrendering transfers" 
          % (len(df_matched_surrender), len(foha_to_match)))
    
    # Step 3: Match by allocation transfers
    print("------ Try matching of former operating accounts based on allocation transfers")
    foha_to_match_alloc = [f for f in foha_to_match if f.id not in list(df_matched_surrender.id.unique())]
    print("Try to match remaining %d former operator holding accounts by allocation transfers" % len(foha_to_match_alloc))
    lst_res = []
    for i, fo in enumerate(foha_to_match_alloc):
        if i % 100 == 0:
            print("Try to match %s by allocating transfers (%d/%d)" % (fo.name, i, len(foha_to_match_alloc)))
        matched = get_allocation_matches(fo, session)
        if len(matched) > 0:
            matched = sorted(matched.items(), key=lambda x: x[1], reverse=True)
            if matched[0][1] >= min_score:
                res = {}
                res["id"] = fo.id
                res["name"] = fo.name
                res["installation_id"] = matched[0][0]
                res["score"] = matched[0][1]
                lst_res.append(res)
    df_matched_allocation = pd.DataFrame(lst_res)
    df_matched_allocation["matched_by"] = "allocation"
    df_matched_allocation = df_matched_allocation.drop_duplicates(subset=["installation_id"], keep=False)
    print("Uniquely matched %d former operator holding accounts (of %d) to installations using allocation transfers" 
          % (len(df_matched_allocation), len(foha_to_match_alloc)))  
    
    # Step 4: Combine all matches
    df_matched_all = pd.concat([df_matched_name, df_matched_surrender, df_matched_allocation], sort=True)
    print("Matched %d of %d former operator holding accounts. " 
        % (len(df_matched_all), len(foha)))
    print("%d installations are matched twice or more to former operator holding account " 
        % (len(df_matched_all[df_matched_all.installation_id.duplicated(keep=False)])))
    df_matched_all.to_csv(fn_out, index=False, encoding="utf-8")
    return df_matched_all
    

def get_surrendering_matches(fo, session):
    """Get a list of possible installation matches by surrendering transfer
    :parm fo: <Account> former operator holding account
    :param session: <sqlalchemy.orm.Session>    
    """
    # get a list transferring transaction classified as surrendering and aggreate by year
    lst_trans = [trans for trans in fo.transferringTransactions if
                 (trans is not None) and
                 (trans.acquiringAccount is not None) and
                 (trans.acquiringAccount.accountType_id == "100-0") and
                 (trans.transactionTypeSupplementary_id == 2)]
    lst_res = [{"date": t.date, "amount": t.amount, "transactionID": t.transactionID} for t in lst_trans]

    if len(lst_res) == 0:
        return []
    s_trans = pd.DataFrame(lst_res).set_index("date").groupby([lambda x: x.year]).amount.sum()

    # get list of possible matches
    # we use surrendering for the years 2008 and beyond to avoid trial period problems
    lst_res = []
    for year, v in s_trans.iteritems():
        if year > 2008:
            q = session.query(Compliance.installation_id
                              ).join(Installation
                                     ).filter(and_(Compliance.year == (year - 1),  # need to take into account the one year lag of surrendering
                                                   Compliance.surrendered == v,
                                                   Installation.registry_id == fo.registry_id)
                                              ).all()
            lst_res.append([i[0] for i in q])
    # count list occurance, convert to relative number of matches, and sort by matchig score
    counted = dict(Counter(x for sublist in lst_res for x in sublist))
    counted = {k: v / len(lst_res) for k, v in sorted(counted.items(), key=lambda item: item[1])}
    return counted


def get_allocation_matches(fo, session):
    """Get a list of possible installation matches by allocation transfer
    :parm fo: <Account> former operator holding account
    :param session: <sqlalchemy.orm.Session>        
    """
    # get a list transferring transaction classified as surrendering and aggreate by year
    lst_trans = [trans for trans in fo.acquiringTransactions if
                 (trans is not None) and
                 (trans.transferringAccount is not None) and
                 (trans.transferringAccount.accountType_id == "100-0") and
                 (trans.transactionTypeSupplementary_id == 53)]
    lst_res = [{"date": t.date, "amount": t.amount, "registry_id": fo.registry_id} for t in lst_trans]
    if len(lst_res) == 0:
        return []
    s_trans = pd.DataFrame(lst_res).set_index("date").groupby([lambda x: x.year]).amount.sum()

    # get list of possible matches
    # we use allocating for the years 2008 and beyond to avoid trial period problems
    lst_res = []
    for year, v in s_trans.iteritems():
        if year > 2007:
            q = session.query(Compliance.installation_id
                              ).join(Installation
                                     ).filter(and_(Compliance.year == year,   # need to take into account the one year lag of surrendering
                                                   Compliance.allocatedTotal == v,
                                                   Installation.registry_id == fo.registry_id
                                                   )
                                              ).all()
            lst_res.append([i[0] for i in q])
    # count list occurance, convert to relative number of matches, and sort by matchig score
    counted = dict(Counter(x for sublist in lst_res for x in sublist))
    counted = {k: v / len(lst_res) for k, v in sorted(counted.items(), key=lambda item: item[1])}
    return counted
