# eutl_scrapper

This projects provides routines to extract data from the EU Transaction Log ([EUTL](https://ec.europa.eu/clima/ets/)) that records compliance and transaction information for the EU Emissions Trading System (EU-ETS). The project is split into two major parts:

1. The project implements several [Scrapy](https://scrapy.org/) spiders crawling the EUTL pages. Downloaded and cleaned data are available for download under [EUETS.INFO](https://euets.info). These spiders are bundled in the module: euetl_scraper
2. Once the data are downloaded, the module eutl_database implements a relation data model of the EUTL and augments the data adding NACE codes and locations to installations as well as tryping to re-establish the relation between former operator holding accounts and installations.

The final database can be downloaded from [EUETS.INFO](https://euets.info/) which also provides the data documentation. The project [pyeutl](https://github.com/jabrell/pyeutl) provides routines to locally re-build the database and easily access the csv files provided. 

# Installation

Create a virtual environment, activate it, and install the dependencies:

```
python -m venv ./venv
./venv/Scripts/activate.bat
pip install -r requirements.txt
```

# Scraping the data

## Account data

To scrape the account data (including installation details, compliance informations, and surrendered units details) run the spider "accounts" from the command line ensuring that you are in the upper folder (the one with data and eutl_scraper sub-folder). Data will be available under \_data/parsed (you can change this in own_settings.py). The whole process takes about 4 hours:

```
scrapy crawl accounts -L INFO
```

## Transaction data
Transaction data are compiled from the transaction list as provided on the [EU Union Registry Page](https://ec.europa.eu/clima/eu-action/eu-emissions-trading-system-eu-ets/union-registry_en#tab-0-1). The data provided include the unit type transferred and account informations. However, they do not link the same account identifiers as provided by the account database. To compile the data and link them to downloaded account data run:

```
python main_transactions.py
```

### Transaction Spider

**Using the transaction spider is depreciated. For documentation we keep the description here**
Each transaction block is downloaded together with the main transaction information and the linking between account identifiers used in in the transaction and those used in the account data. A complete extraction takes about 12 hours:

```
scrapy crawl transactions -L INFO
```

To limit transaction download times and allow scrapping in chunks, it is possible to limit the time span for which transactions are downloaded using the _start_date_ and _end_date_ option.

```
scrapy crawl transactions -a start_date=01/01/2019 -a end_date=31/01/2019 -L INFO
```

**In running the transaction spider, be aware that occasionally data are downloaded twice leading to possible duplicates in the data which are, however, difficult to identify as they get an unique transaction block identifier.**

## Entitlement data
Data on offset entitlements are downloaded using 
```
scrapy crawl entitlements -L INFO
```

# Effort Sharing Data
Downloading the effort sharing data, follow the same principle. Available spiders are
```
scrapy crawl esd_transactions -L INFO
scrapy crawl esd_allocations -L INFO
scrapy crawl esd_compliance -L INFO
scrapy crawl esd_entitlement -L INFO
```

# Build the database

Database creation requires a complete set of data *data/parsed* folder. 

**If you do not want to run the scraper (which takes quite some time), we provide the raw data [EUETS.info](https://euets.info/download/)**

To create the database, one has to create an empty postgres database ([see. e.g. here](https://www.postgresqltutorial.com/postgresql-administration/postgresql-create-database/]). The default configuration uses used "eutlAdmin" with password "1234" for the database "eutl_orm".

```
python main_create_database.py
```

# Considerations for Unix systems

To run the eutl scraper on macOS or Linux, delete the entry for the package "twisted-iocpsupport" in *requirements.txt*
The package twisted-iocpsupport provides bindings to the Windows "I/O Completion Ports" APIs. These are Windows-only APIs.
