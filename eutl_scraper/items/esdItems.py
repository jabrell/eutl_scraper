import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from ._utils import strip_values, convert_relative_url


class EsdTransactionItem(scrapy.Item):
    transactionID = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionType = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionDate = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transferringRegistry = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transferringMemberState = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringYear = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transferringAccountIdentifier = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    acquiringRegistry = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringMemberState = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringYear = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringAccountIdentifier = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    amount = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())


class EsdTransactionBlockItem(scrapy.Item):
    transactionID = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionDate = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionType = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionBlock = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionURL = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    originatingRegistry = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    unitType = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    amount = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    originalCommitmentPeriod = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountIdentifier = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    transferringAccountId = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringAccountIdentifier = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringAccountId = scrapy.Field(
        input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    lulucfActivity = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    projectID = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    projectTrack = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    expiryDate = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())


class EsdAllocationItem(scrapy.Item):
    memberState = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    year = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    accountStatus = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    accountIdentifier = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    allocated = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())


class EsdComplianceItem(scrapy.Item):
    memberState = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    year = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    accountStatus = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    accountIdentifier = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    allocated = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    verified = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    penalty = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    surrenderedAea = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    surrenderedCredits = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    balance = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    compliance = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())


class EsdEntitlementItem(scrapy.Item):
    transactionID = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionType = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionDate = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transferringMemberState = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transferringYear = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringMemberState = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    acquiringYear = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    transactionStatus = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
    amount = scrapy.Field(input_processor=MapCompose(
        strip_values), output_processor=TakeFirst())
