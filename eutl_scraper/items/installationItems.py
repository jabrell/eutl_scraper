import scrapy
from itemloaders.processors import TakeFirst, MapCompose

from ._utils import strip_values

def get_compliance_status(x):
    if x: 
        return x[:1]
    return x


def get_compliance_status_update(x):
    if "*" in x: 
        return True
    return False


class InstallationItem(scrapy.Item):
    registryCode =  scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    isAircraftOperator = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    name = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    permitID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    permitEntryDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    permitExpiry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    subsidiary = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    parent = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    eprtrID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    firstYearOfEmissions = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    lastYearOfEmissions = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    mainAddress = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    secondaryAddress = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    postalCode = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    city = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    country = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    latitude = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    longitude = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    activity = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationURL = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    # specific to aircrafts
    ec7482009ID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    monitoringPlanId = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    monitoringPlanFirstYear = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    monitoringPlanExpiry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    icaoID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())


class ComplianceItem(scrapy.Item):
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    phase = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    year = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    allocationFree = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    allocation10c = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    allocationNewEntrance = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    verified = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    surrendered = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    verifiedCumulative = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    surrenderedCumulative = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    complianceCode = scrapy.Field(input_processor=MapCompose(strip_values, get_compliance_status), output_processor=TakeFirst())
    complianceCodeUpdated = scrapy.Field(input_processor=MapCompose(strip_values, get_compliance_status_update), output_processor=TakeFirst())
    installationURL = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    reportedInSystem = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())


class SurrenderingDetailsItem(scrapy.Item):
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    accountID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    registryCode = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    reportedInSystem = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())

    originatingRegistry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    unitType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    amount = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    originalCommitmentPeriod = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    applicableCommitmentPeriod = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    year = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    lulucfActivity = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    projectID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    track = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    expiryDate = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationURL = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    surrenderingURL = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())


class EntitlementItem(scrapy.Item):
    registry = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    entityType = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationName = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    installationID = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    euEntitlement = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())
    chEntitlement = scrapy.Field(input_processor=MapCompose(strip_values), output_processor=TakeFirst())


