import json
import requests

class GetAttributes:
    def __init__(self):
        self.api_url = "https://www.vinnauto.com/api/api/inventory/attributes"

    def index(self):
        try:
            res = requests.get(self.api_url).json()
            attr_data = {'body_types': res['body_types'], 'colours': res['colours'], 'fuel_types': res['fuel_types'], 'drive_types': res['drive_types'], 'transmissions': res['transmissions']}
            json.dump(attr_data, open("scrapers/vinn_auto/attributes.json", 'w'))
        except Exception as e:
            pass
            # print("ERROR:", e)

def main():
    attr = GetAttributes()
    attr.index()
