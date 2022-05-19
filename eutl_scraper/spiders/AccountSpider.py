import scrapy
from scrapy.loader import ItemLoader
from eutl_scraper.items import AccountItem, ContactItem
from eutl_scraper.items import InstallationItem, ComplianceItem, SurrenderingDetailsItem

class AccountSpider(scrapy.Spider):
    name = "accounts"
    next_page_number = 1  # zero indexed
    max_pages = None
    # 30
    # 1416
    start_urls  = ["https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=0&nextList=Next>"]
    
    def parse(self, response):
        # update maximum number of pages (corrected for 0 indexing)
        if not self.max_pages:
            self.max_pages = int(response.css("input[name='resultList.lastPageNumber']").attrib["value"]) - 1

        # extract links to detail pages from table
        for r in response.css("table#tblAccountSearchResult>tr")[2:]:
            url = response.urljoin(r.css("a.listlink").attrib["href"])
            yield response.follow(url, callback=self.parse_accountDetails)
           
        # navigate to next page of account overview 
        next_page = "https://ec.europa.eu/clima/ets/account.do?languageCode=en&accountHolder=&searchType=account&currentSortSettings=&resultList.currentPageNumber=%d&nextList=Next>" % self.next_page_number
        if self.next_page_number <= self.max_pages:
            self.next_page_number += 1
            if self.next_page_number % 100 == 0:
                print("Process account overview page %d of %d" % (self.next_page_number, self.max_pages))
            yield response.follow(next_page, callback=self.parse)
            
    def parse_accountDetails(self, response):  
        # determine correct field for company registration and commitment period field
        td_commitmentPeriod = 8
        td_companyRegistration = 9
        for i, f in enumerate(response.css("table#tblAccountGeneralInfo>tr:nth-child(2)>td>span.titlelist::text").getall()):  
            if f.strip() == "Company Registration No":
                td_companyRegistration = i + 1
            elif f.strip() == "Commitment Period":
                td_commitmentPeriod = i + 1
        # parse account details
        l = ItemLoader(item = AccountItem(), response=response)
        l.add_value('accountURL', response.url)
        l.add_css("accountID", "input[name='accountID']::attr(value)")
        l.add_css("accountName", "font.bordertbheadfont::text")
        l.add_css("registryCode", "input[name='registryCode']::attr(value)")
        l.add_css("accountType", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(1)>span.classictext::text")
        l.add_css("registry", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(2)>span.classictext::text")
        l.add_css("installationID", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a>span::text")
        l.add_css("installationURL", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a::attr(href)")
        l.add_css("accountHolderName", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(4)>span.classictext::text")
        l.add_css("status", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(5)>span.classictext::text")
        l.add_css("openingDate", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(6)>span.classictext::text")
        l.add_css("closingDate", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(7)>span.classictext::text")
        l.add_css("commitmentPeriod", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(%d)>span.classictext::text" % td_commitmentPeriod)
        l.add_css("companyRegistrationNumber", "table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(%d)>span.classictext::text" % td_companyRegistration)
        yield l.load_item()
        
        # parse contact details
        columns = ["accountID", "contactType", "name", "legalEntityIdentifier",
                   "mainAddress", "secondaryAddress", "postalCode", "city", "country",
                   "telephone1", "telephone2", "eMail"]
        l = ItemLoader(item = ContactItem(), response=response) 
        l.add_value('accountURL', response.url)
        l.add_css("accountID", "input[name='accountID']::attr(value)")      
        for i, c in enumerate(columns[1:]):
            l.add_css(c, "table#tblAccountContactInfo>tr:nth-child(3)>td:nth-child(%d)>span.classictext::text" % (i+1)) 
        yield l.load_item()

        # in case of operator account, parse installation information
        installationURL= response.css("table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a::attr(href)").get()

        if installationURL:
            installationURL = response.urljoin(installationURL.strip())
            accountID = response.css("input[name='accountID']::attr(value)").get().strip()
            registryCode = response.css("input[name='registryCode']::attr(value)").get().strip()
            installationID = response.css("table#tblAccountGeneralInfo>tr:nth-child(3)>td:nth-child(3)>a>span::text").get().strip()
            installationID = registryCode + "_" + installationID
            yield response.follow(installationURL, callback=self.parse_installation, 
                                  meta={"accountID": accountID, "registryCode": registryCode,
                                        "installationID": installationID})

    def parse_installation(self, response):
        # determine whether it is an aicraft account
        isAircraft = "Aircraft" in response.css("span.bordertbheadfont::text").get().strip()

        # get tables with installation details
        tables = response.css("table#tblChildDetails")

        # Installation details: general
        l = ItemLoader(item = InstallationItem(), selector=tables[0], response=response)
        l.add_value('installationURL', response.url) 
        l.add_value('installationID', response.meta.get("installationID")) # already combined with registry code, i.e., unique in EUTL
        l.add_value('registryCode', response.meta.get("registryCode"))
        l.add_value("isAircraftOperator", str(isAircraft))

        # Installation details: installation details
        if isAircraft:
            cols = ["installationID", "ec7482009ID", "monitoringPlanId", "monitoringPlanFirstYear", 
                    "monitoringPlanExpiry", "subsidiary", "parent", "eprtrID", "icaoID","firstYearOfEmissions", 
                    "lastYearOfEmissions"]
        else:
            cols = ["installationID", "name", "permitID", "permitEntryDate", "permitExpiry", "subsidiary", "parent", 
                    "eprtrID", "firstYearOfEmissions", "lastYearOfEmissions"]
        for i, c in enumerate(cols[1:]):
            l.add_css(c, "tr>td>table:nth-child(1)>tr:nth-child(3)>td:nth-child(%i)>span.classictext::text" % (i+2))

        # Installation details: address
        cols = ["mainAddress", "secondaryAddress", "postalCode", "city", "country", "latitude", "longitude", "activity"] 
        for i, c in enumerate(cols):
            l.add_css(c, "tr>td>table:nth-child(2)>tr:nth-child(3)>td:nth-child(%i)>span.classictext::text" % (i+1))  
        yield l.load_item()

        # Compliance information
        # need to take care of possible compliance over CHETS
        # thus, there might be two compliance tables for aircrafts
        complianceTables = tables[1].css("tr>td>div>table")
        etsSystems = ["EUETS", "CHETS"]
        for i, table in enumerate(complianceTables): 
            rows = table.css("tr")
            for row in rows[2:]:
                try:
                    year = row.css("td:nth-child(2)>span.classictext::text").get().strip()
                    year = int(year)
                except:
                    continue
                l = ItemLoader(item = ComplianceItem(), selector=row, response=response)
                l.add_value("installationID", response.meta.get("installationID"))
                l.add_value("installationURL", response.url)
                l.add_value("reportedInSystem", etsSystems[i])
                l.add_css("phase", "td:nth-child(1)>span.classictext::text")
                l.add_css("year", "td:nth-child(2)>span.classictext::text")
                # extract the different allocation values
                for td in row.css("td:nth-child(3)>span.classictext"):
                    stars = td.css("sup::text").get()
                    if stars == "****":
                        l.add_value("allocation10c", td.css("::text").get().strip())
                    elif stars == "*****":
                        l.add_value("allocationNewEntrance", td.css("::text").get().strip())   
                    else:
                        l.add_value("allocationFree", td.css("::text").get())   
                l.add_css("verified", "td:nth-child(4)>span.classictext::text")
                l.add_css("surrendered", "td:nth-child(5)>span.classictext::text")
                l.add_css("surrenderedCumulative", "td:nth-child(6)>span.classictext::text")
                l.add_css("verifiedCumulative", "td:nth-child(7)>span.classictext::text")
                l.add_css("complianceCode", "td:nth-child(8)>span.classictext::text")
                l.add_css("complianceCodeUpdated", "td:nth-child(8)>span.classictext::text")
                yield l.load_item()

            # Extract surrendering details but not for the CH table (links to same information)
            if i > 0:
                continue
            links = table.css("a.listlink")
            for link in links:
                text = link.css("span::text").get().strip()
                if text.startswith("Details on Surrendered Units"):
                    url = response.urljoin(link.attrib["href"])
                    yield response.follow(url, callback=self.parse_surrendered_details, 
                                meta={"accountID": response.meta.get("accountID"), 
                                        "registryCode": response.meta.get("registryCode"),
                                        "installationID": response.meta.get("installationID"),
                                        "reportedInSystem": etsSystems[i],
                                        "installationURL": response.url})

    def parse_surrendered_details(self, response):
        rows= response.css("table#tblChildDetails>tr>td>table>tr>td>div>table>tr") 
        cols = ["originatingRegistry", "unitType", "amount", "originalCommitmentPeriod",
                "applicableCommitmentPeriod", "year", "lulucfActivity", "projectID",
                 "track", "expiryDate"]

        for row in rows[2:]:
            l = ItemLoader(SurrenderingDetailsItem(), selector=row, response=response)
            l.add_value("installationID", response.meta.get("installationID"))
            l.add_value("installationURL", response.meta.get("installationURL"))
            l.add_value("surrenderingURL", response.url)
            l.add_value("accountID", response.meta.get("accountID"))
            l.add_value("registryCode", response.meta.get("registryCode"))
            l.add_value("reportedInSystem", response.meta.get("reportedInSystem"))
            for i, c in enumerate(cols):
                l.add_css(c, "td:nth-child(%d)>span.classictext::text" % (i+1)) 
            yield l.load_item()
        return