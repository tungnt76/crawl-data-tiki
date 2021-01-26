import os
import json
import re

import aiohttp
import asyncio
from bs4 import BeautifulSoup
from pymongo import MongoClient

dict_product = {
    "laptop": "https://tiki.vn/laptop/c8095?src=c.1846.hamburger_menu_fly_out_banner&page={page}",
    "mouse": "https://tiki.vn/chuot-van-phong/c1829?page={page}",
    "hdd": "https://tiki.vn/o-cung-hdd/c4051?src=c.4051.hamburger_menu_fly_out_banner&page={page}",
    "network-equipment": "https://tiki.vn/thiet-bi-mang/c2663?page={page}"
}

# api
product_api_url = "https://tiki.vn/api/v2/products/{id}"
review_api_url = "https://tiki.vn/api/v2/reviews?product_id={id}"

# file
product_id_file = "product_ids.txt"
product_data_file = "products.txt"
product_file = r"products.csv"
product_data_import_file = "product_data_import.json"
product_id_file = "product_ids.txt"
review_data_file = "reviews.txt"
review_file = r"reviews.csv"
review_data_import_file = "review_data_import.json"
user_data_import_file = "user_data_import.json"

digit = re.compile(r'\d+')
PAGE = 1

headers = {'user-agent': 'my-app/0.0.1', 'Content-Type': 'application/json'}

schema_product_field = ["id", "name", "price", "description", "specifications", "productset_group_name"]
schema_review_field = ["id", "title", "content", "rating", "created_by", "product_id"]
schema_user_field = ["id", "name", "fullname","region", "avatar_url", "purchased", "purchased_at"]

uri_mongodb = "mongodb://admin:mongo@localhost:27017/crawl-data?authSource=admin&w=1"


async def crawl_product_id():
    product_list = []

    for page_index in range(PAGE):
        for type_product in dict_product:
            print('Product {}: '.format(type_product))

            async with aiohttp.ClientSession() as session:
                async with session.get(dict_product[type_product].format(page=page_index), headers=headers) as response:
                    parser = BeautifulSoup(await response.text(), 'html.parser')
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


async def crawl_product(list_products=[]):
    product_detail_list = []
    for product_id in list_products:
        async with aiohttp.ClientSession() as session:
            async with session.get(product_api_url.format(id=product_id), headers=headers) as response:
                if (response.status == 200):
                    # response.encoding = 'utf-8'
                    # raw = await response.text()
                    # content = raw.replace('\/', '/')
                    content = await response.text()
                    product_detail_list.append(str(content))
                    with open(f'./data/products/{product_id}.json', mode='w+') as file:
                        file.write(str(content))
                        file.close()
                    print("Crawl product: ", product_id, " --> ", response.status)
    return product_detail_list


async def crawl_review(list_products=list()):
    review_detail_list = []
    for product_id in list_products:
        async with aiohttp.ClientSession() as session:
            async with session.get(review_api_url.format(id=product_id), headers=headers) as response:
                if (response.status == 200):
                    # response.encoding = 'utf-8'
                    # raw = await response.text()
                    # content = raw.replace('\/', '/')
                    content = await response.text()
                    list_reviews = json.loads(content).get('data')
                    for review in list_reviews:
                        review_detail_list.append(json.dumps(review))
                        review_id = review.get('id')
                        with open(f'./data/reviews/{review_id}.json', mode='w+') as file:
                            file.write(str(review))
                            file.close()
                        print("Crawl review: ", review_id, " --> ", response.status)
    return review_detail_list


def field_filter_product(obj, schema_field):
    e = json.loads(obj)
    if not e.get("id", False):
        return None

    p = dict()

    for field in schema_field:
        if field in e:
            p[field] = e.get(field, False)

    return p
    

def field_filter_review(obj, schema_field):
    e = json.loads(obj)
    if not e.get("id", False):
        return None

    p = dict()

    for field in schema_field:
        if field in e:
            if field == 'created_by':
                p[field] = e['created_by']['id']
            else:
                p[field] = e.get(field, False)

    return p



def field_filter_user(obj, field, schema_field):
    e = json.loads(obj)[field]
    if not e.get("id", False):
        return None

    p = dict()

    for field in schema_field:
        if field in e:
            p[field] = e.get(field, False)

    return p


def save_raw(product_detail_list=[], file_path=''):
    with open(file_path, 'w+') as f:
        content = "\n".join(product_detail_list)
        f.write(content)
        f.close()
        print("Save file: ", file_path)


def save_json(item_json_list, file_path):
    with open(file_path, mode='w') as f:
        f.write(json.dumps(item_json_list))
        f.close()


async def main():
    os.makedirs("./data", exist_ok=True)
    os.makedirs("./data/products", exist_ok=True)
    os.makedirs("./data/reviews", exist_ok=True)
    os.makedirs("./data/users", exist_ok=True)
    
    
    product_id_list = await crawl_product_id()
    save_product_id(product_id_list)

    # crawl product and save to file
    product_list = await crawl_product(product_id_list)
    save_raw(product_list, product_data_file)

    product_json_list = [field_filter_product(p, schema_product_field) for p in product_list]
    save_json(product_json_list, product_data_import_file)

    # crawl review and save to file
    review_list = await crawl_review(product_id_list)
    save_raw(product_list, review_data_file)

    review_json_list = [field_filter_review(r, schema_review_field) for r in review_list]
    save_json(review_json_list, review_data_import_file)

    user_json_list = [field_filter_user(r, 'created_by', schema_user_field) for r in review_list]
    save_json(user_json_list, user_data_import_file)

def save_db():
    # database
    myclient = MongoClient(uri_mongodb)
    db = myclient["crawl-data"]

    Product = db["products"]
    Product.delete_many({})
    with open(product_data_import_file) as file:
        file_data = json.load(file)
        if isinstance(file_data, list):
            Product.insert_many(file_data)
        else:
            Product.insert_one(file_data)
        file.close()

    Review = db["reviews"]
    Review.delete_many({})
    with open(review_data_import_file) as file:
        file_data = json.load(file)
        if isinstance(file_data, list):
            Review.insert_many(file_data)
        else:
            Review.insert_one(file_data)
        file.close()

    User = db["users"]
    User.delete_many({})
    with open(user_data_import_file) as file:
        file_data = json.load(file)
        if isinstance(file_data, list):
            # error
            User.insert_many(file_data)
        else:
            User.insert_one(file_data)
        file.close()


# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
save_db()