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

To scrape the account data (including installation details, compliance informations, and surrendered units details) run the spider "accounts" from the command line ensuring that you are in the upper folder (the one with data and eutl*scraper sub-folder). Data will be available under \_data/parsed* (you can change this in own_settings.py):

```
scrapy crawl accounts -L INFO
```

The download transaction data run

```
scrapy crawl transactions -L INFO
```
