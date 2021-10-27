import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import AccountItem, ContactItem

class AccountSpider(scrapy.Spider):
    name = "accounts"
    next_page_number = 1  # zero indexed
    max_pages = None
    start_urls  = ["https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=0&nextList=Next>"]
    
    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css("input[name='resultList.lastPageNumber']").attrib["value"]) - 1
            #self.max_pages = 0
            
        # extract links to detail pages from table
        for r in response.css("table#tblAccountSearchResult>tr")[2:]:
            url = response.urljoin(r.css("a.listlink").attrib["href"])
            yield response.follow(url, callback=self.parse_accountDetails)
           
        # navigate to next page of account overview 
        next_page = "https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.next_page_number
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            yield response.follow(next_page, callback=self.parse)
            
    def parse_accountDetails(self, response):
        # parse account details
        l = ItemLoader(item = AccountItem(), response=response)
        l.add_css("accountID", "input[name='accountID']::attr(value)")
        l.add_css("registryCode", "input[name='registryCode']::attr(value)")
        l.add_css("accountType", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(1)>span.classictext::text")
        l.add_css("registry", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(2)>span.classictext::text")
        l.add_css("installationID", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a>span::text")
        l.add_css("installationURL", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a::attr(href)")
        l.add_css("accountHolderName", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(4)>span.classictext::text")
        l.add_css("status", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(5)>span.classictext::text")
        l.add_css("openingDate", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(6)>span.classictext::text")
        l.add_css("closingDate", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(7)>span.classictext::text")
        l.add_css("companyRegistrationNumber", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(8)>span.classictext::text")
        yield l.load_item()
        
        # parse contact details
        columns = ["accountID", "contactType", "name", "legalEntityIdentifier",
                   "mainAddress", "secondaryAddress", "postalCode", "country",
                   "telephone1", "telephone2", "eMail"]
        l = ItemLoader(item = ContactItem(), response=response) 
        l.add_css("accountID", "input[name='accountID']::attr(value)")      
        for i, c in enumerate(columns[1:]):
            l.add_css(c, "table#tblAccountContactInfo>tr:nth-child(3)>td:nth-child(%d)>span.classictext::text" % (i+1)) 
        yield l.load_item()    
        
        