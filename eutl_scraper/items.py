import scrapy
from itemloaders.processors import TakeFirst, MapCompose


def strip_values(x):
    return x.strip()

def convert_relative_url(url, loader_context):
    return loader_context['response'].urljoin(url)

class AccountItem(scrapy.Item):
    accountID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    accountType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst() )
    registryCode= scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    registry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationURL = scrapy.Field(input_processor=MapCompose(convert_relative_url), output_processor=TakeFirst())
    accountHolderName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    status = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    openingDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    closingDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    companyRegistrationNumber  = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    


class ContactItem(scrapy.Item):
    accountID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    contactType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    name = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    legalEntityIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    mainAddress = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    secondaryAddress = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    postalCode = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    country = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone1 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone2 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    eMail = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())    
