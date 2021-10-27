import scrapy
from itemloaders.processors import TakeFirst, MapCompose

def strip_values(x):
    return x.strip()

def convert_relative_url(url, loader_context):
    print("here", loader_context)
    return loader_context['response'].urljoin(url)

class AccountItem(scrapy.Item):
    nationalAdministrator = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    accountType = scrapy.Field(input_processor=MapCompose(strip_values), )
    accountHolderName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    companyRegistrationNumber  = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    mainAddressLine = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    city = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    legalEntityIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone1 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone2 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    eMail = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    details = scrapy.Field(input_processor=MapCompose(convert_relative_url), output_processor=TakeFirst())
    
    
class AccountHolder(scrapy.Item):
    nationalAdministrator = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    accountType = scrapy.Field(input_processor=MapCompose(strip_values), )
    accountHolderName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    companyRegistrationNumber  = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    mainAddressLine = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    city = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    legalEntityIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone1 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    telephone2 = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    eMail = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    details = scrapy.Field(input_processor=MapCompose(convert_relative_url), output_processor=TakeFirst())    
