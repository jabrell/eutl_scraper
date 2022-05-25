import csv
from itemadapter import ItemAdapter
from eutl_scraper.items import (AccountItem, ContactItem, 
                                InstallationItem, ComplianceItem, 
                                SurrenderingDetailsItem, TransactionItem,
                                TransactionBlockItem, AccountIDMapItem,
                                EntitlementItem)
from eutl_scraper.own_settings import DIR_PARSED
import os.path



class EutlScraperPipeline:
    # all items to process
    # if new items, add them to list for post-processing
    itemsToProcess = [
        {"item": AccountItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'accounts.csv', "appendExisting": False},
        {"item": ContactItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'contacts.csv', "appendExisting": False},
        {"item": InstallationItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'installations.csv', "appendExisting": False},
        {"item": ComplianceItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'compliance.csv', "appendExisting": False},
        {"item": SurrenderingDetailsItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'surrendering.csv', "appendExisting": False},
        {"item": TransactionItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'transactions.csv', "appendExisting": False},
        {"item": TransactionBlockItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'transactionBlocks.csv', "appendExisting": False},
        {"item": AccountIDMapItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'accountIdMap.csv', "appendExisting": False},
        {"item": EntitlementItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'entitlements.csv', "appendExisting": False},
    ]

    def process_item(self, item, spider):
        for it in self.itemsToProcess:
            if isinstance(item, it["item"]):
                adapter = dict(ItemAdapter(item))
                it["rows"].append(adapter)
                it["header"] = [k for k in item.fields.keys()]
                
    def close_spider(self, spider):
        for it in self.itemsToProcess:
            if len(it["rows"]) == 0:
                continue
            # check whether we allow to append to existing files
            if os.path.isfile(it["output_file_name"]) and it["appendExisting"]:              
                # append to existing file
                with open(it["output_file_name"], "a", newline="", encoding="utf-8") as output_file:
                    dict_writer = csv.DictWriter(output_file, it["header"])
                    dict_writer.writerows(it["rows"])
            else:
                with open(it["output_file_name"], "w", newline="", encoding="utf-8") as output_file:
                    dict_writer = csv.DictWriter(output_file, it["header"])
                    dict_writer.writeheader()
                    dict_writer.writerows(it["rows"])