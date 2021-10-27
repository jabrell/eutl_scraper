import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import AccountItem

class AccountSpider(scrapy.Spider):
    name = "account"
    page_number = 1  # zero indexed
    max_pages = None
    start_urls  = ["https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=0&nextList=Next>"]
    
    
    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css("input[name='resultList.lastPageNumber']").attrib["value"]) - 1
            self.max_pages = 1
            
        # extract table
        rows = response.css("table#tblAccountSearchResult>tr")#tableAccounts.css("tbody>tr")
        # get column titles from second row (first is table title)
        keys = rows[1].css("a::text").getall() + ["Details"]
        
        # get table content
        columns = ["nationalAdministrator", "accountType", "accountHolderName", 
            "installationID", "companyRegistrationNumber", "mainAddressLine", 
            "city", "legalEntityIdentifier", "telephone1", "telephone2", 
            "eMail"]
        for r in rows[2:]:
            l = ItemLoader(item = AccountItem(), selector=r, response=response)
            l.context['response'] = response
            for i, col in enumerate(columns):
                print(col, "td:nth-child(%i)>span.classictext::text" % i)
                l.add_css(col, "td:nth-child(%i)>span.classictext::text" % i)
            l.add_css("details", "a.listlink::attr(href)")
            #fields = list(map(lambda x: x.strip(), r.css("span.classictext::text").getall()))
            #fields.append(r.css("a.listlink").attrib["href"])
            yield l.load_item()
            
        next_page = "https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.page_number
        if self.page_number <= self.max_pages:
            self.page_number += 1
            yield response.follow(next_page, callback=self.parse)
            
    def parse_accountDetails(self, response):
        row  = response.css("table#tblAccountGeneralInfo>tr:nth-child(3)")   
        l = ItemLoader(item = AccountItem(), selector=row, response=response)