import csv
from itemadapter import ItemAdapter
from eutl_scraper.items import AccountItem, ContactItem
from eutl_scraper.own_settings import DIR_PARSED

class EutlScraperPipeline:
    accounts = []
    accountKeys = []
    contacts = []
    contactKeys = []
    
    def process_item(self, item, spider):
        if isinstance(item, AccountItem):
            adapter = dict(ItemAdapter(item))
            self.accounts.append(adapter)
            if len(adapter) > len(self.accountKeys):
                self.accountKeys = list(adapter.keys())
        if isinstance(item, ContactItem):
            adapter = dict(ItemAdapter(item))
            self.contacts.append(adapter)
            if len(adapter) > len(self.contactKeys):
                self.contactKeys = list(adapter.keys())                
        return item
    
    def close_spider(self, spider):
        # export account data 
        with open(DIR_PARSED + 'accounts.csv', 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, self.accountKeys)
            dict_writer.writeheader()
            dict_writer.writerows(self.accounts)       
            
        # export contact data 
        with open(DIR_PARSED + 'contacts.csv', 'w', newline='')  as output_file:
            dict_writer = csv.DictWriter(output_file, self.contactKeys)
            dict_writer.writeheader()
            dict_writer.writerows(self.contacts)