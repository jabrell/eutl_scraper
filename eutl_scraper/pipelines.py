import csv
from itemadapter import ItemAdapter
from eutl_scraper.items import AccountItem, ContactItem
from eutl_scraper.items import InstallationItem, ComplianceItem, SurrenderingDetailsItem
from eutl_scraper.own_settings import DIR_PARSED


class EutlScraperPipeline:
    itemsToProcess = [
        {"item": AccountItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'accounts.csv'},
        {"item": ContactItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'contacts.csv'},
        {"item": InstallationItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'installations.csv'},
        {"item": ComplianceItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'compliance.csv'},
        {"item": SurrenderingDetailsItem, "rows": [], "header": [], "output_file_name": DIR_PARSED + 'surrendering.csv'}
    ]

    def process_item(self, item, spider):
        for it in self.itemsToProcess:
            if isinstance(item, it["item"]):
                adapter = dict(ItemAdapter(item))
                it["rows"].append(adapter)
                it["header"] = list(set(it["header"] + list(adapter.keys())))

    def close_spider(self, spider):
        for it in self.itemsToProcess:
            with open(it["output_file_name"], "w", newline="", encoding="utf-8") as output_file:
                dict_writer = csv.DictWriter(output_file, it["header"])
                dict_writer.writeheader()
                dict_writer.writerows(it["rows"])