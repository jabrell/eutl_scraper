# eutl_scrapper

This projects provides routines to extract data from the EU Transaction Log ([EUTL](https://ec.europa.eu/clima/ets/)) that records compliance and transaction information for the EU Emissions Trading System (EU-ETS).
A detailed description of the processing steps, data sources, and final data model is provided at [EUETS.INFO](https://euets.info)

The project is split into three major parts:

1. *Scraping the data*: The project implements several [Scrapy](https://scrapy.org/) spiders crawling the EUTL pages. These spiders are bundled in the module: **euetl_scraper**.
2. *Data imputation*: We augment the data by inferring the installation location using the google map api, inserting NACE sector codes, and adding ORBIS identifiers. 
These routines are bundled in the module **eutl_data_augmentation**.
3.*Database creation*: Once the data are downloaded, cleaned, and additional data are imputed, we implement a relational data model on the data, create a SQL database based on that model, and export the final tables. In that steps we also infer the relation between former and current operator holding accounts. The model and routines to import and export the data to/from the SQL database are bundled in the module **eutl_database**.

The final database can be downloaded from [EUETS.INFO](https://euets.info/) which also provides the data documentation. The project [pyeutl](https://github.com/jabrell/pyeutl) provides routines to locally re-build the database and easily access the csv files provided. 

# Installation

Create a virtual environment, activate it, and install the dependencies:

```
python -m venv ./venv
./venv/Scripts/activate.bat
pip install -r requirements.txt
```

# Scraping the data

To obtain the data from the EUTL proceed with the following steps (in order):

1. To scrape the account data (including installation details, compliance informations, and surrendered units details) run the spider "accounts" from the command line ensuring that you are in the upper folder (the one with data and eutl_scraper sub-folder). Data will be available under \_data/parsed (you can change this in own_settings.py). The whole process takes about 4 hours:
```
scrapy crawl accounts -L INFO
```
2. Data on offset entitlements are downloaded using 
```
scrapy crawl entitlements -L INFO
```

3. Downloading the effort sharing data, follow the same principle. Available spiders are
```
scrapy crawl esd_transactions -L INFO
scrapy crawl esd_allocations -L INFO
scrapy crawl esd_compliance -L INFO
scrapy crawl esd_entitlement -L INFO
```

4. Transaction data are compiled from the transaction list as provided on the [EU Union Registry Page](https://ec.europa.eu/clima/eu-action/eu-emissions-trading-system-eu-ets/union-registry_en#tab-0-1). The data provided include the unit type transferred and account information. However, they do not link the same account identifiers as provided by the account database. To compile the data and link them to downloaded account data run:
```
python main_transactions.py
```
  
    
**Depreciated**    
#### Transaction Spider
*Using the transaction spider is depreciated. For documentation we keep the description here*
Each transaction block is downloaded together with the main transaction information and the linking between account identifiers used in in the transaction and those used in the account data. A complete extraction takes about 12 hours:

```
scrapy crawl transactions -L INFO
```

To limit transaction download times and allow scrapping in chunks, it is possible to limit the time span for which transactions are downloaded using the _start_date_ and _end_date_ option.

```
scrapy crawl transactions -a start_date=01/01/2020 -a end_date=31/12/2021 -L INFO
```

**In running the transaction spider, be aware that occasionally data are downloaded twice leading to possible duplicates in the data which are, however, difficult to identify as they get an unique transaction block identifier.**

# Data imputation
All additional data used together with a description of the source data are provided 
in the folder *data/additional/* To impute additional data run the main file for data imputation:
```
python main_data_imputation.py
```

# Database creation

Database creation requires a complete set of data *data/parsed* folder. 

**If you do not want to run the scraper (which takes quite some time), we provide the raw data [EUETS.info](https://euets.info/download/)**

Two major steps are necessary to create the database:
1. To create the database, one has to create an empty postgres database ([see. e.g. here](https://www.postgresqltutorial.com/postgresql-administration/postgresql-create-database/]). The default configuration uses used "eutlAdmin" with password "1234" for the database "eutl_orm".
2. Run the python file to create the database:
```
python main_create_database.py
```

# Considerations for Unix systems

To run the eutl scraper on macOS or Linux, delete the entry for the package "twisted-iocpsupport" in *requirements.txt*
The package twisted-iocpsupport provides bindings to the Windows "I/O Completion Ports" APIs. These are Windows-only APIs.
