import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import EsdTransactionItem, EsdAllocationItem, EsdTransactionBlockItem
import urllib.parse
import sys

from eutl_scraper.items.esdItems import EsdComplianceItem, EsdEntitlementItem


class EsdTransactionSpider(scrapy.Spider):
    name = "esd_transactions"
    next_page_number = 1  # zero indexed
    max_pages = None
    accountIdentifiersMapped = []
    # 30000
    start_urls = []
    url_transaction_overview = "https://ec.europa.eu/clima/ets/esdTransactions.do"
    params_transaction_search = {}

    def __init__(self, start_date="", end_date="", **kwargs):
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
            "originatingAccountIdentifier": "",
            "destinationAccountIdentifier": "",
            "transferringEsdRegistryCode": -1,
            "acquiringEsdRegistryCode": -1,
            "transferringEsdYear": "",
            "acquiringEsdYear": "",
            "currentSortSettings": "",
            "resultList.currentPageNumber": "0",
            "nextList": "Next>",
        }
        self.start_urls = [
            f"{self.url_transaction_overview}?{urllib.parse.urlencode(self.params_transaction_search)}"]
        super().__init__(**kwargs)  # python3

    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css(
                "input[name='resultList.lastPageNumber']").attrib["value"]) - 1

        # get table with transaction overview and process each row
        rows = response.css("table#tblTransactionSearchResult>tr")
        cols = [
            "transactionID",
            "transactionType",
            "transactionDate",
            "transferringRegistry",
            "transferringMemberState",
            "transferringYear",
            "transferringAccountIdentifier",
            "acquiringRegistry",
            "acquiringMemberState",
            "acquiringYear",
            "acquiringAccountIdentifier",
            "amount"]

        for row in rows[2:]:
            l = ItemLoader(item=EsdTransactionItem(),
                           selector=row, response=response)
            for i, c in enumerate(cols):
                l.add_css(c, f"td:nth-child({i+1})>span.classictext::text")
                transactionID = row.css(
                    "td:nth-child(1)>span.classictext::text").get().strip()
                transactionType = row.css(
                    "td:nth-child(2)>span.classictext::text").get().strip()
                transactionDate = row.css(
                    "td:nth-child(3)>span.classictext::text").get().strip()
                # extraction of link to transaction blocks and parse page
                url = response.urljoin(row.css("a.listlink::attr(href)").get())
                yield response.follow(url, callback=self.parse_transaction_blocks,
                                      meta={"transactionID": transactionID,
                                            "transactionDate": transactionDate,
                                            "transactionType": transactionType,
                                            "transactionURL": url,
                                            "isFirstPage": True
                                            })
            yield l.load_item()

        # navigate to next page of transaction overview
        self.params_transaction_search["resultList.currentPageNumber"] = self.next_page_number
        next_page = f"{self.url_transaction_overview}?{urllib.parse.urlencode(self.params_transaction_search)}"
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if (self.next_page_number-1) % 100 == 0:
                print("Process transaction overview %d of %d" %
                      (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)

    def parse_transaction_blocks(self, response):
        isFirstPage = response.meta.get("isFirstPage", True)
        if response.meta["transactionID"] == "EU405464":
            print(response.url)
        cols = [
            "originatingRegistry",
            "unitType",
            "amount",
            "originalCommitmentPeriod",
            "transferringAccountIdentifier",
            "transferringAccountId",
            "acquiringAccountIdentifier",
            "acquiringAccountId",
            "lulucfActivity",
            "projectID",
            "projectTrack",
            "expiryDate"
        ]
        rows = response.css("table#tblTransactionBlocksInformation>tr")
        for row in rows[2:]:
            l = ItemLoader(item=EsdTransactionBlockItem(),
                           selector=row, response=response)
            # get meta information from header table
            # create identifiers
            l.add_value("transactionID", response.meta["transactionID"])
            l.add_value("transactionDate", response.meta["transactionDate"])
            l.add_value("transactionType", response.meta["transactionType"])
            l.add_value("transactionURL", response.meta["transactionURL"])
            for i, c in enumerate(cols):
                l.add_css(c, f"td:nth-child({i+1})>span.classictext::text")
            yield l.load_item()

        if isFirstPage:
            nextButton = response.css(
                "input[name='resultList.lastPageNumber']")
            if nextButton:
                max_pages = int(nextButton.attrib["value"])
                print("___________________________________________________")
                print(max_pages)
                print("___________________________________________________")
                for i in range(1, max_pages):
                    url = response.url + "&resultList.currentPageNumber=%d&nextList=Next>" % i
                    yield response.follow(url, callback=self.parse_transaction_blocks,
                                          meta={"transactionID": response.meta["transactionID"],
                                                "transactionDate": response.meta["transactionDate"],
                                                "transactionType": response.meta["transactionType"],
                                                "transactionURL": url,
                                                "isFirstPage": False
                                                })


class EsdAllocationSpider(scrapy.Spider):
    name = "esd_allocations"
    next_page_number = 1  # zero indexed
    max_pages = None
    base_url = "https://ec.europa.eu/clima/ets/esdAllocations.do"
    params_search = {
        "languageCode": "en",
        "esdRegistry": -1,
        "esdYear": "",
        "search": "Search",
        "currentSortSettings": "",
        "resultList.currentPageNumber": 0,
        "nextList": "Next>"
    }

    start_urls = [
        f"{base_url}?{urllib.parse.urlencode(params_search)}"]

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

    def parse(self, response):
        if not self.max_pages:
            self.max_pages = int(response.css(
                "input[name='resultList.lastPageNumber']").attrib["value"]) - 1

        cols = ["memberState", "year", "accountStatus",
                "accountIdentifier", "allocated"]

        rows = response.css("table#tblAllocations>tr")
        for row in rows[2:]:
            l = ItemLoader(item=EsdAllocationItem(),
                           selector=row, response=response)
            for i, c in enumerate(cols):
                l.add_css(c, f"td:nth-child({i+1})>font.classictext::text")
            yield l.load_item()

        # navigate to next page of transaction overview
        self.params_search.pop("search", None)
        self.params_search["resultList.currentPageNumber"] = self.next_page_number
        next_page = f"{self.base_url}?{urllib.parse.urlencode(self.params_search)}"
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if (self.next_page_number-1) % 100 == 0:
                print("Process transaction overview %d of %d" %
                      (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)


class EsdComplianceSpider(scrapy.Spider):
    name = "esd_compliance"
    next_page_number = 1  # zero indexed
    max_pages = None
    base_url = "https://ec.europa.eu/clima/ets/transactionsCompliance.do"
    params_search = {
        "languageCode": "en",
        "esdRegistry": -1,
        "esdYear": "",
        "search": "Search",
        "currentSortSettings": "",
        "resultList.currentPageNumber": 0,
        "nextList": "Next>"
    }

    start_urls = [
        f"{base_url}?{urllib.parse.urlencode(params_search)}"]

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

    def parse(self, response):
        if not self.max_pages:
            self.max_pages = int(response.css(
                "input[name='resultList.lastPageNumber']").attrib["value"]) - 1

        cols = ["memberState", "year", "accountStatus",
                "accountIdentifier", "allocated", "verified", "penalty",
                "surrenderedAea", "surrenderedCredits", "balance",
                "compliance"]

        rows = response.css("table#tblComplianceDashboard>tr")
        for row in rows[2:]:
            l = ItemLoader(item=EsdComplianceItem(),
                           selector=row, response=response)
            for i, c in enumerate(cols):
                l.add_css(c, f"td:nth-child({i+1})>font.classictext::text")
            yield l.load_item()

        # navigate to next page of transaction overview
        self.params_search.pop("search", None)
        self.params_search["resultList.currentPageNumber"] = self.next_page_number
        next_page = f"{self.base_url}?{urllib.parse.urlencode(self.params_search)}"
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if (self.next_page_number-1) % 100 == 0:
                print("Process transaction overview %d of %d" %
                      (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)


class EsdEntitlementSpider(scrapy.Spider):
    name = "esd_entitlement"
    next_page_number = 1  # zero indexed
    max_pages = None
    base_url = "https://ec.europa.eu/clima/ets/transactionsEntitlements.do"
    params_search = {
        "languageCode": "en",
        "startDate": "",
        "endDate": "",
        "transactionStatusCode": 4,
        "transactionId": "",
        "suppTransactionTypeCode": -1,
        "transferringEsdRegistryCode": -1,
        "acquiringEsdRegistryCode": -1,
        "transferringEsdYear": "",
        "acquiringEsdYear": "",
        "search": "Search",
        "currentSortSettings": "",
        "resultList.currentPageNumber": 0,
        "nextList": "Next>"
    }

    start_urls = [
        f"{base_url}?{urllib.parse.urlencode(params_search)}"]

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

    def parse(self, response):
        if not self.max_pages:
            self.max_pages = int(response.css(
                "input[name='resultList.lastPageNumber']").attrib["value"]) - 1

        cols = ["transactionID", "transactionType", "transactionDate",
                "transferringMemberState", "transferringYear",
                "acquiringMemberState", "acquiringYear",
                "transactionStatus", "amount"]

        rows = response.css("table#tblEsdTransactions>tr")
        for row in rows[2:]:
            l = ItemLoader(item=EsdEntitlementItem(),
                           selector=row, response=response)
            for i, c in enumerate(cols):
                l.add_css(c, f"td:nth-child({i+1})>font.classictext::text")
            yield l.load_item()

        # navigate to next page of transaction overview
        self.params_search.pop("search", None)
        self.params_search["resultList.currentPageNumber"] = self.next_page_number
        next_page = f"{self.base_url}?{urllib.parse.urlencode(self.params_search)}"
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if (self.next_page_number-1) % 100 == 0:
                print("Process transaction overview %d of %d" %
                      (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)
