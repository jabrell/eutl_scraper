import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import TransactionItem, TransactionBlockItem, AccountIDMapItem
import urllib.parse
import csv, os
import sys


class TransactionSpider(scrapy.Spider):
    name = "transactions"
    next_page_number = 1  # zero indexed
    max_pages = None
    accountIdentifiersMapped = []
    # 30000
    start_urls = []
    url_transaction_overview = "https://ec.europa.eu/clima/ets/transaction.do"
    params_transaction_search = {}

    def __init__(
        self, start_date="", end_date="", fn_accountIdentifiers=None, **kwargs
    ):
        """
        :param start_date: <str> start date for transaction search
                            has to be string of format "dd/mm/yyyy"
        :param end_date: <str> end date for transaction search
                            has to be string of format "dd/mm/yyyy"
        :param fn_accountIdentifiers: <str> file name of already existing account identifiers
        """
        self.params_transaction_search = {
            "languageCode": "en",
            "startDate": "%s" % start_date,
            "endDate": "%s" % end_date,
            "transactionStatus": "4",
            "fromCompletionDate": "",
            "toCompletionDate": "",
            "transactionID": "",
            "transactionType": -1,
            "suppTransactionType": -1,
            "originatingRegistry": -1,
            "destinationRegistry": -1,
            "originatingAccountType": -1,
            "destinationAccountType": -1,
            "originatingAccountIdentifier": "",
            "destinationAccountIdentifier": "",
            "originatingAccountHolder": "",
            "destinationAccountHolder": "",
            "currentSortSettings": "",
            "resultList.currentPageNumber": "0",
            "nextList": "Next>",
        }
        self.start_urls = [
            f"{self.url_transaction_overview}?{urllib.parse.urlencode(self.params_transaction_search)}"
        ]

        # update the list of already existing account identifiers
        # no longer maintained but left for illustrations
        if False:
            if fn_accountIdentifiers is not None and os.path.isfile(
                fn_accountIdentifiers
            ):
                with open(fn_accountIdentifiers, mode="r") as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        self.accountIdentifiersMapped.append(row["accountIdentifier"])
        super().__init__(**kwargs)  # python3

    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = (
                int(
                    response.css("input[name='resultList.lastPageNumber']").attrib[
                        "value"
                    ]
                )
                - 1
            )
            # self.max_pages = 0

        # get table with transaction overview and process each row
        rows = response.css("table#tblTransactionSearchResult>tr")
        cols = [
            "transactionID",
            "transactionType",
            "transactionDate",
            "transactionStatus",
            "transferringRegistry",
            "transferringAccountType",
            "transferringAccountName",
            "transferringAccountIdentifier",
            "transferringAccountHolderName",
            "acquiringRegistry",
            "acquiringAccountType",
            "acquiringAccountName",
            "acquiringAccountIdentifier",
            "acquiringAccountHolderName",
            "amount",
        ]
        for row in rows[2:]:
            l = ItemLoader(item=TransactionItem(), selector=row, response=response)
            for i, c in enumerate(cols):
                transactionID = (
                    row.css("td:nth-child(1)>span.classictext::text").get().strip()
                )
                transactionDate = (
                    row.css("td:nth-child(3)>span.classictext::text").get().strip()
                )
                transactionType = (
                    row.css("td:nth-child(2)>span.classictext::text").get().strip()
                )
                transferringRegistry = (
                    row.css("td:nth-child(5)>span.classictext::text").get().strip()
                )
                acquiringRegistry = (
                    row.css("td:nth-child(10)>span.classictext::text").get().strip()
                )
                l.add_css(c, f"td:nth-child({i+1})>span.classictext::text")
                # extraction of link to transaction blocks and parse page
                url = response.urljoin(row.css("a.listlink::attr(href)").get())
                yield response.follow(
                    url,
                    callback=self.parse_transaction_blocks,
                    meta={
                        "transactionID": transactionID,
                        "transactionDate": transactionDate,
                        "transactionType": transactionType,
                        "transferringRegistry": transferringRegistry,
                        "acquiringRegistry": acquiringRegistry,
                    },
                )
            yield l.load_item()

        # navigate to next page of transaction overview
        self.params_transaction_search["resultList.currentPageNumber"] = (
            self.next_page_number
        )
        next_page = f"{self.url_transaction_overview}?{urllib.parse.urlencode(self.params_transaction_search)}"
        # "https://ec.europa.eu/clima/ets/transaction.do?languageCode=en&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.next_page_number
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if (self.next_page_number - 1) % 100 == 0:
                print(
                    "Process transaction overview %d of %d"
                    % (self.next_page_number, self.max_pages)
                )
            yield response.follow(next_page, callback=self.parse)

    def parse_transaction_blocks(self, response):
        block_number = response.meta.get("block_number", 0)
        isFirstPage = response.meta.get("isFirstPage", True)

        rows = response.css("table#tblTransactionBlocksInformation>tr")
        for row in rows[2:]:
            l = ItemLoader(item=TransactionBlockItem(), selector=row, response=response)
            # get meta information from header table
            # create identifiers
            l.add_value("transactionID", response.meta["transactionID"])
            l.add_value("transactionDate", response.meta["transactionDate"])
            l.add_value("transactionType", response.meta["transactionType"])
            l.add_value("transactionBlock", str(block_number))
            block_number += 1

            # extract table content
            l.add_css("originatingRegistry", "td:nth-child(1)>span.classictext::text")
            l.add_css("unitType", "td:nth-child(2)>span.classictext::text")
            l.add_css("amount", "td:nth-child(3)>span.classictext::text")
            l.add_css(
                "originalCommitmentPeriod", "td:nth-child(4)>span.classictext::text"
            )
            # link and name to transferring account
            # we need to check whether it is a link or not
            link = row.css("td:nth-child(5)>a.resultlink::attr(href)").get()
            if link is not None:
                l.add_css(
                    "transferringAccountName",
                    "td:nth-child(5)>a.resultlink>span.resultlink::text",
                )
                l.add_value("transferringAccountURL", link.strip())
            else:
                l.add_css(
                    "transferringAccountName", "td:nth-child(5)>span.classictext::text"
                )
                # l.add_css("transferringAccountURL", "td:nth-child(5)>a.resultlink::attr(href)")
            l.add_css(
                "transferringAccountIdentifier",
                "td:nth-child(6)>span.classictext::text",
            )
            l.add_value("transferringRegistry", response.meta["transferringRegistry"])
            # for the moment deactivate creation of account mapping here.
            # will be added as routine after table creation
            if False:
                url = row.css("td:nth-child(5)>a.resultlink::attr(href)").get()
                if url:
                    url = response.urljoin(url)
                    idx = (
                        row.css("td:nth-child(6)>span.classictext::text").get().strip()
                    )
                    if idx not in self.accountIdentifiersMapped:
                        self.accountIdentifiersMapped.append(idx)
                        yield response.follow(
                            url,
                            callback=self.get_account_id_map,
                            meta={"accountIdentifier": idx},
                        )
            # link and name to acquiring account
            # link and name to transferring account
            # we need to check whether it is a link or not
            link = row.css("td:nth-child(7)>a.resultlink::attr(href)").get()
            if link is not None:
                l.add_css(
                    "acquiringAccountName",
                    "td:nth-child(7)>a.resultlink>span.resultlink::text",
                )
                l.add_value("acquiringAccountURL", link.strip())
                # l.add_css("acquiringAccountURL", "td:nth-child(7)>a.resultlink::attr(href)")
            else:
                l.add_css(
                    "acquiringAccountName", "td:nth-child(7)>span.classictext::text"
                )
            l.add_css(
                "acquiringAccountIdentifier", "td:nth-child(8)>span.classictext::text"
            )
            l.add_value("acquiringRegistry", response.meta["acquiringRegistry"])
            # for the moment deactivate creation of account mapping here.
            # will be added as routine after table creation
            if False:
                url = row.css("td:nth-child(7)>a.resultlink::attr(href)").get()
                if url:
                    url = response.urljoin(url)
                    idx = (
                        row.css("td:nth-child(8)>span.classictext::text").get().strip()
                    )
                    if idx not in self.accountIdentifiersMapped:
                        self.accountIdentifiersMapped.append(idx)
                        yield response.follow(
                            url,
                            callback=self.get_account_id_map,
                            meta={"accountIdentifier": idx},
                        )
            l.add_css("lulucfActivity", "td:nth-child(9)>span.classictext::text")
            l.add_css("projectID", "td:nth-child(10)>span.classictext::text")
            l.add_css("projectTrack", "td:nth-child(11)>span.classictext::text")
            l.add_css("expiryDate", "td:nth-child(12)>span.classictext::text")
            yield l.load_item()

        # print(response.meta["transactionID"])
        if isFirstPage:
            nextButton = response.css("input[name='resultList.lastPageNumber']")
            if nextButton:
                max_pages = int(nextButton.attrib["value"])
                for i in range(1, max_pages + 1):
                    url = (
                        response.url
                        + "&resultList.currentPageNumber=%d&nextList=Next>" % i
                    )
                    yield response.follow(
                        url,
                        callback=self.parse_transaction_blocks,
                        meta={
                            "transactionID": response.meta["transactionID"],
                            "isFirstPage": False,
                            "block_number": block_number,
                            "transactionDate": response.meta["transactionDate"],
                            "transactionType": response.meta["transactionType"],
                            "acquiringRegistry": response.meta["acquiringRegistry"],
                            "transferringRegistry": response.meta[
                                "transferringRegistry"
                            ],
                        },
                    )

    def get_account_id_map(self, response):
        l = ItemLoader(AccountIDMapItem(), response=response)
        l.add_value("accountIdentifier", response.meta.get("accountIdentifier"))
        accountID = response.css("input[name='accountID']::attr(value)").get()
        # if accountID create the map. if not remove from list and try again later
        if accountID:
            l.add_value("accountID", accountID.strip())
            yield l.load_item()
        else:
            self.accountIdentifiersMapped.remove(response.meta.get("accountIdentifier"))
