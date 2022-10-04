set years=2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019
set years=2014 2015 2016 2017 2018 2019 2009 2010 2011 2012 2013
set years=2009 2010 2011 2012 2013

for %%y in (%years%) do (
	scrapy crawl transactions -a start_date=01/01/%%y -a end_date=31/12/%%y -L INFO
	cd .\data\parsed
	rename transactions.csv transactions_%%y.csv
	rename transactionBlocks.csv transactionBlocks_%%y.csv
	cd .. 
	cd ..
)