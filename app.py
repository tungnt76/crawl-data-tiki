from bs4 import BeautifulSoup
import requests
import json
import re
from pymongo import MongoClient

dict_product = {
    "laptop": "https://tiki.vn/laptop/c8095?src=c.1846.hamburger_menu_fly_out_banner&page={}",
    "mouse": "https://tiki.vn/chuot-van-phong/c1829?page={}",
    "hdd" : "https://tiki.vn/o-cung-hdd/c4051?src=c.4051.hamburger_menu_fly_out_banner&page={}"
}
product_url = "https://tiki.vn/api/v2/products/{}"

product_id_file = "product_ids.txt"
product_data_file = "products.txt"
product_file = r"products.csv"
data_import_file = "data_import.json"

digit = re.compile(r'\d+')
PAGE = 1

headers = {'user-agent': 'my-app/0.0.1', 'Content-Type': 'application/json'}


def crawl_product_id():
    product_list = []

    for page_index in range(PAGE):
        for type_product in dict_product:
            print('Product {}: '.format(type_product))
            try:
                response = requests.get(dict_product[type_product].format(page_index), headers=headers)
            except requests.exceptions.RequestException as ex:
                print(ex)
                continue
            parser = BeautifulSoup(response.text, 'html.parser')

            product_box = parser.find_all(class_="product-item")

            if (len(product_box) == 0):
                break
            for product in product_box:
                href = product.get('href')
                product_list.append(digit.findall(href)[-1])

    return product_list


def save_product_id(product_list: list):
    with open(product_id_file, 'w') as f:
        content = '\n'.join(product_list)
        f.write(content)
        f.close()
        print("Save file: ", product_id_file)


def crawl_product(product_list=[]):
    product_detail_list = []
    for product_id in product_list:
        response = requests.get(product_url.format(product_id), headers=headers)
        if (response.status_code == 200):
            response.encoding = 'utf-8-sig'
            content = response.text.replace('\/', '/')
            product_detail_list.append(str(content))
            with open(f'./data/{product_id}.json', encoding='utf-8', mode='w+') as file:
                file.write(str(content))
                file.close()
            print("Crawl product: ", product_id, ": ", response.status_code)
    return product_detail_list


schema_field = ["id", "sku", "name", "short_description", "price", "thumbnail_url", "productset_group_name"]


def adjust_product(product):
    e = json.loads(product)
    if not e.get("id", False):
        return None

    p = dict()

    for field in schema_field:
        if field in e:
            p[field] = e.get(field, False)

    return p


def save_raw_product(product_detail_list=[]):
    with open(product_data_file, 'w+') as f:
        content = "\n".join(product_detail_list)
        f.write(content)
        f.close()
        print("Save file: ", product_data_file)


def save_product_json():
    with open(data_import_file, mode='w') as f:
        f.write(json.dumps(product_json_list))
        f.close()


if __name__ == "__main__":
    product_list = crawl_product_id()
    save_product_id(product_list)
    
    product_list = crawl_product(product_list)
    save_raw_product(product_list)

    product_json_list = [adjust_product(p) for p in product_list]
    save_product_json()

    uri = "mongodb://nttung:mongodb@localhost:27017/crawl-data?authSource=admin&w=1"
    myclient = MongoClient(uri)

    # database
    db = myclient["crawl-data"]

    Collection = db["data"]

    with open(data_import_file) as file:
        file_data = json.load(file)
        if isinstance(file_data, list):
            Collection.insert_many(file_data)
        else:
            Collection.insert_one(file_data)
        file.close()
