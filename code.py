import pandas as pd
import pickle
from datetime import datetime


class DataExtractor:
    def __init__(self, invoice_file, expired_file):
        self.invoice_file = invoice_file
        self.expired_file = expired_file
        self.data = None
        self.expired_invoices = None

    def load_data(self):
        with open(self.invoice_file, 'rb') as f:
            self.data = pickle.load(f)
        
        with open(self.expired_file, 'r') as f:
            self.expired_invoices = set(map(int, f.read().split(',')))


    def transform_data(self):
        type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}
        transformed_data = []

        for invoice in self.data:
            total_invoice_price = 0

            try:
                invoice_id = int(invoice['id'])
            except ValueError:
                print(f"Error: Invalid invoice ID '{invoice['id']}' for invoice {invoice}. Skipping")
                continue

            if 'items' not in invoice:
                print(f"Error: 'items' key missing in invoice {invoice}. Skipping")
                continue

            for item in invoice['items']:
                try:
                    unit_price = int(item['item']['unit_price'])
                    quantity = int(item['quantity'])
                    
                except ValueError:
                    print(f"Error converting 'unit_price' or 'quantity' to int for item {item}. Skipping")
                    continue

                total_invoice_price += unit_price * quantity

                item_id = item['item']['id']
                item_name = item['item']['name']
                item_type = type_conversion.get(item['item']['type'], 'Other')
                is_expired = invoice['id'] in self.expired_invoices
                percentage_in_invoice = (unit_price*quantity / total_invoice_price)

                transformed_data.append({
                    'invoice_id': invoice_id,
                    'created_on' : datetime.now(),
                    'invoiceitem_id': item_id,
                    'invoiceitem_name': str(item_name),
                    'type': str(item_type),
                    'unit_price': unit_price,
                    'total_price': total_invoice_price,
                    'percentage_in_invoice' : percentage_in_invoice,
                    'is_expired': is_expired,
                })

        return transformed_data


extractor = DataExtractor("C:\Users\jorka\Downloads\data\invoices_new.pkl", "C:\Users\jorka\Downloads\data\expired_invoices.txt")
extractor.load_data()
extracted_list = extractor.transform_data()
df = pd.DataFrame(extracted_list)
print(df.info())
