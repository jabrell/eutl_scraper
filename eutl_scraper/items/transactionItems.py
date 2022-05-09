import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from ._utils import strip_values, convert_relative_url

class TransactionItem(scrapy.Item):
    transactionID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transactionType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst()) 
    transactionDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transactionStatus = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringRegistry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountHolderName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringRegistry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountHolderName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())    
    amount = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())

class TransactionBlockItem(scrapy.Item):
    transactionID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transactionDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transactionBlock = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transactionURL = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    originatingRegistry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    unitType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    amount = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    originalCommitmentPeriod = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountURL = scrapy.Field(input_processor=MapCompose(strip_values, convert_relative_url), output_processor=TakeFirst())
    acquiringAccountName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountIdentifier = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringAccountURL = scrapy.Field(input_processor=MapCompose(strip_values, convert_relative_url), output_processor=TakeFirst())
    lulucfActivity = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    projectID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    projectTrack = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    expiryDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst()) 
    
 
