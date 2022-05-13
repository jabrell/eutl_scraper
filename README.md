# eutl_scrapper

Routines to extract data from the EU Transaction Log ([EUTL](https://ec.europa.eu/clima/ets/)) that records compliance and transaction information for the EU Emissions Trading System (EU-ETS). The project implements several [Scrapy](https://scrapy.org/) spiders crawling the EUTL pages. Downloaded and cleaned data are available for download under [EUETS.INFO](https://euets.info).

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

Each transaction block is downloaded together with the main transaction information and the linking between account identifiers used in in the transaction and those used in the account data. A complete extraction takes about 12 hours:

```
scrapy crawl transactions -L INFO
```

To limit transaction download times and allow scrapping in chunks, it is possible to limit the time span for which transactions are downloaded using the _start_date_ and _end_date_ option.

```
scrapy crawl transactions -a start_date=01/01/2019 -a end_date=31/01/2019 -L INFO
```

By default, transaction data will append to existing output files (in contrast to account data). Therefore, be aware of possible duplicates (e.g., if the same time span has been scrapped twice). This can be changed modifying the _appendExisting_ option in the _itemsToProcess_ list in _pipelines.py_.

## Linking transaction and account data

The transaction data use identifiers for accounts different from those used by the account database. In final step, we construct the mapping from account to transaction data using the linked urls as provided in the transaction data. To establish this linking run:

```
python main_linking.py
```

# Considerations for Unix systems

To run the eutl scraper on macOS or Linux, delete the entry for the package "twisted-iocpsupport".
The package twisted-iocpsupport provides bindings to the Windows "I/O Completion Ports" APIs. These are Windows-only APIs.
