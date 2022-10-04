import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import EntitlementItem


class EntitlementSpider(scrapy.Spider):
    name = "entitlements"
    next_page_number = 1  # zero indexed
    max_pages = None
    start_urls  = ["https://ec.europa.eu/clima/ets/ice.do?languageCode=en&registryCode=-1&accountFullTypeCode=-1&iceInstallationId=&search=Search&currentSortSettings="]

    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css("input[name='resultList.lastPageNumber']").attrib["value"]) - 1
        
        # get all rows 
        cols = ["registry", "entityType", "installationName", "installationID", "euEntitlement", "chEntitlement"]
        for row in response.css("table#tblEntitlements>tr")[2:]:
            l = ItemLoader(item = EntitlementItem(), selector=row, response=response)
            for i, c in enumerate(cols):
                #test = row.css("td:nth-child(%d)>font.classictext::text" % (i+1)).get().strip()
                l.add_css(c, "td:nth-child(%d)>font.classictext::text" % (i+1))
            yield l.load_item()
            #url = response.urljoin(r.css("a.listlink").attrib["href"])
            #yield response.follow(url, callback=self.parse_accountDetails)
        
        # navigate to next page 
        next_page = "https://ec.europa.eu/clima/ets/ice.do?languageCode=en&registryCode=-1&accountFullTypeCode=-1&iceInstallationId=&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.next_page_number
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if self.next_page_number % 100 == 0:
                print("Process entitlement page %d of %d" % (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)
        