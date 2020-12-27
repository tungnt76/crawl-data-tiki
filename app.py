from bs4 import BeautifulSoup
import aiohttp
import asyncio
import json
import re
from pymongo import MongoClient

dict_product = {
    "laptop": "https://tiki.vn/laptop/c8095?src=c.1846.hamburger_menu_fly_out_banner&page={}",
    "mouse": "https://tiki.vn/chuot-van-phong/c1829?page={}",
    "hdd" : "https://tiki.vn/o-cung-hdd/c4051?src=c.4051.hamburger_menu_fly_out_banner&page={}"
}
product_api_url = "https://tiki.vn/api/v2/products/{}"
review_api_url = "https://tiki.vn/api/v2/reviews?product_id={}"

product_id_file = "product_ids.txt"
product_data_file = "products.txt"
product_file = r"products.csv"
product_data_import_file = "product_data_import.json"
product_id_file = "product_ids.txt"
review_data_file = "reviews.txt"
review_file = r"reviews.csv"
review_data_import_file = "review_data_import.json"

digit = re.compile(r'\d+')
PAGE = 1

headers = {'user-agent': 'my-app/0.0.1', 'Content-Type': 'application/json'}

schema_product_field = ["id", "name", "price", "description", "specifications", "productset_group_name"]
schema_review_field = ["id", "title", "content", "rating", "created_by", "product_id"]

uri_mongodb = "mongodb://admin:mongo@localhost:27017/crawl-data?authSource=admin&w=1"

async def crawl_product_id():
    product_list = []

    for page_index in range(PAGE):
        for type_product in dict_product:
            print('Product {}: '.format(type_product))
          
            async with aiohttp.ClientSession() as session:
                async with session.get(dict_product[type_product].format(page_index), headers=headers) as response:
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
            async with session.get(product_api_url.format(product_id), headers=headers) as response:
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
            async with session.get(review_api_url.format(product_id), headers=headers) as response:
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





def adjust_product(product, schema_field):
    e = json.loads(product)
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
    product_id_list = await crawl_product_id()
    save_product_id(product_id_list)
    
    # crawl product and save to file
    product_list = await crawl_product(product_id_list)
    save_raw(product_list, product_data_file)

    product_json_list = [adjust_product(p, schema_product_field) for p in product_list]
    save_json(product_json_list, product_data_import_file)

    # crawl review and save to file
    review_list = await crawl_review(product_id_list)
    save_raw(product_list, review_data_file)

    review_json_list = [adjust_product(r, schema_review_field) for r in review_list]
    save_json(review_json_list, review_data_import_file)
    
    myclient = MongoClient(uri_mongodb)

    # database
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

loop = asyncio.get_event_loop()
loop.run_until_complete(main())