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

To scrape the account data (including installation details, compliance informations, and surrendered units details) run the spider "accounts" from the command line ensuring that you are in the upper folder (the one with data and eutl\_scraper sub-folder). Data will be available under \_data/parsed (you can change this in own_settings.py). The whole process takes about 2 hours:

```
scrapy crawl accounts -L INFO
```

Each transaction block is downloaded together with the main transaction information and the linking between account identifiers used in in the transaction and those used in the account data. A complete extraction takes about 12 hours:

```
scrapy crawl transactions -L INFO
```
# Considerations for Unix systems
To run the eutl scraper on macOS or Linux, delete the entry for the package "twisted-iocpsupport".
The package twisted-iocpsupport provides bindings to the Windows "I/O Completion Ports" APIs. These are Windows-only APIs.