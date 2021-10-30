import scrapy 
from scrapy.loader import ItemLoader
from eutl_scraper.items import TransactionItem, TransactionBlockItem, AccountIDMapItem

class TransactionSpider(scrapy.Spider):
    name = "transactions"
    next_page_number = 1  # zero indexed
    max_pages = None
    accountIdentifiersMapped = []
    # 30000
    start_urls  = ["https://ec.europa.eu/clima/ets/transaction.do?languageCode=en&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&resultList.currentPageNumber=0&nextList=Next>"]
    
    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css("input[name='resultList.lastPageNumber']").attrib["value"]) - 1
            #self.max_pages = 0
        
        # get table with transaction overview and process each row
        rows = response.css("table#tblTransactionSearchResult>tr") 
        cols = ["transactionID", "transactionType", "transactionDate", "transactionStatus", 
                "transferringRegistry", "transferringAccountType", "transferringAccountName", 
                "transferringAccountIdentifier", "transferringAccountHolderName", 
                "acquiringRegistry", "acquiringAccountType", "acquiringAccountName", 
                "acquiringAccountIdentifier", "acquiringAccountIdentifierquiringAccountHolderName", 
                "amount"]
        for row in rows[2:]:
            l = ItemLoader(item=TransactionItem(), selector=row, response=response)
            for i, c in enumerate(cols):
                transactionID = row.css("td:nth-child(1)>span.classictext::text").get().strip()
                l.add_css(c, "td:nth-child(%d)>span.classictext::text" % (i+1))
                # extraction of link to transaction blocks and parse page
                url = response.urljoin(row.css("a.listlink::attr(href)").get())
                yield response.follow(url, callback=self.parse_transaction_blocks, 
                                      meta={"transactionID": transactionID} )
            yield l.load_item()

        # navigate to next page of transaction overview 
        next_page = "https://ec.europa.eu/clima/ets/transaction.do?languageCode=en&startDate=&endDate=&transactionStatus=4&fromCompletionDate=&toCompletionDate=&transactionID=&transactionType=-1&suppTransactionType=-1&originatingRegistry=-1&destinationRegistry=-1&originatingAccountType=-1&destinationAccountType=-1&originatingAccountIdentifier=&destinationAccountIdentifier=&originatingAccountHolder=&destinationAccountHolder=&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.next_page_number
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            yield response.follow(next_page, callback=self.parse)
            
    def parse_transaction_blocks(self, response):
        block_number = response.meta.get("block_number", 0)
        isFirstPage = response.meta.get("isFirstPage", True)
        
        
        rows = response.css("table#tblTransactionBlocksInformation>tr")
        for row in rows[2:]:
            l = ItemLoader(item=TransactionBlockItem(), selector=row, response=response)
            # create identifiers
            l.add_value("transactionID", response.meta["transactionID"])
            l.add_value("transactionBlock", str(block_number))
            block_number += 1
            #l.add_value("transactionURL", response.url)
            
            # extract table content
            l.add_css("originatingRegistry", "td:nth-child(1)>span.classictext::text")
            l.add_css("unitType", "td:nth-child(2)>span.classictext::text")
            l.add_css("amount", "td:nth-child(3)>span.classictext::text")
            l.add_css("originalCommitmentPeriod", "td:nth-child(4)>span.classictext::text")
            # link and name to transferring account
            l.add_css("transferringAccountName", "td:nth-child(5)>span.resultlink::text")
            l.add_css("transferringAccountURL", "td:nth-child(5)>a.resultlink::attr(href)")
            l.add_css("transferringAccountIdentifier", "td:nth-child(6)>span.classictext::text")
            url = row.css("td:nth-child(5)>a.resultlink::attr(href)").get()
            if url:
                url = response.urljoin(url)
                idx = row.css("td:nth-child(6)>span.classictext::text").get().strip()
                if not idx in self.accountIdentifiersMapped:
                    self.accountIdentifiersMapped.append(idx)
                    yield response.follow(url, callback=self.get_account_id_map, 
                                meta={"accountIdentifier": idx })
            # link and name to acquiring account
            l.add_css("acquiringAccountName", "td:nth-child(7)>span.resultlink::text")
            l.add_css("acquiringAccountURL", "td:nth-child(7)>a.resultlink::attr(href)")
            url = row.css("td:nth-child(7)>a.resultlink::attr(href)").get()
            if url:
                url = response.urljoin(url)
                idx = row.css("td:nth-child(8)>span.classictext::text").get().strip()
                if not idx in self.accountIdentifiersMapped:
                    self.accountIdentifiersMapped.append(idx)
                    yield response.follow(url, callback=self.get_account_id_map, 
                                meta={"accountIdentifier": idx })          
            l.add_css("acquiringAccountIdentifier", "td:nth-child(8)>span.classictext::text")
            l.add_css("lulucfActivity", "td:nth-child(9)>span.classictext::text")
            l.add_css("projectID", "td:nth-child(10)>span.classictext::text")
            l.add_css("projectTrack", "td:nth-child(11)>span.classictext::text")
            l.add_css("expiryDate", "td:nth-child(12)>span.classictext::text")
            yield l.load_item()
        
        #print(response.meta["transactionID"])        
        if isFirstPage:
            nextButton = response.css("input[name='resultList.lastPageNumber']")
            if nextButton:
                max_pages = int(nextButton.attrib["value"])
                for i in range(1, max_pages + 1):
                    url = response.url + "&resultList.currentPageNumber=%d&nextList=Next>" % i
                    yield response.follow(url, callback=self.parse_transaction_blocks,
                                          meta={"transactionID": response.meta["transactionID"],
                                                "isFirstPage": False,
                                                "block_number": block_number})
        
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
        
            