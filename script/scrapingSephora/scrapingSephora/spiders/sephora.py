import scrapy
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import datetime
import json 
import yaml
from yaml.loader import SafeLoader

inputFile = "../../inputFiles/config.yaml"
with open(inputFile,) as f :   
    config = yaml.load(f,Loader=SafeLoader)

class SephoraSpider(scrapy.Spider):
    name = "sephora"
    allowed_domains = ["sephora.com"]
    start_urls = ["https://www.sephora.com/products-sitemap.xml"]
    
    def parse(self, response):
        urls = BeautifulSoup(response.text,'html.parser')
        urls = [i.text for i in urls.find_all('loc') ]
        [i for i in urls if 'no-more-bag' in i]
        for url in urls :
            yield scrapy.Request(url, callback=self.parse_product)
    
    def parse_product(self, response):
        db = MongoClient(f"{config['host']}/{config['mongoCol']}/{config['informationDb']}")
        script = json.loads(response.css('#linkStore::text').get())
        if 'regularChildSkus' in script['page']['product'] :
            files = script['page']['product']['regularChildSkus']
        else : 
            files = [script['page']['product']['currentSku']]
        
        detail = script['page']['product']['productDetails']
        name = detail['displayName']
        Category3 = script['page']['product']['parentCategory']['displayName']
        if  "parentCategory" in script['page']['product']['parentCategory'] :
            Category2 = script['page']['product']['parentCategory']['parentCategory']['displayName']
        if "parentCategory" in script['page']['product']['parentCategory']['parentCategory'] :
            Category1 = script['page']['product']['parentCategory']['parentCategory']['parentCategory']['displayName']
        shortDesc = detail['shortDescription']
        brand = detail['brand']['displayName']
        lovesCount = detail['lovesCount']
        rating =  detail['rating']
        reviews = detail['reviews']
        for item in files :
            if Category1 != None :
                item['Category1'] = Category1
            if Category2 != None :
                item['Category2'] = Category2
            item['Category3'] = Category3
            item['brand'] = brand
            item['lovesCount'] = lovesCount
            item["rating"] = rating
            item["reviews"] = reviews
            item['name'] = name
            item['shortDesc'] = shortDesc
            item['update'] = datetime.datetime.today()
            if db.count_documents({"skuId":item["skuId"],"listPrice":item['listPrice']}) == 0 :
                item['date'] = datetime.datetime.today()
                db.insert_one(item)
            else :
                db.find_one_and_update(filter={'skuId':item['skuId']},update={"$set" : item},sort=[('update',-1)])
            yield item

class SephoraReview(scrapy.Spider):
    name = 'RevSephora'
    allowed_domains = ['api.bazaarvoice.com']
    
    def requests_urls():
        db  = MongoClient(config['host'])
        url = pd.DataFrame(db['Sephora']['Information'].find())
        ProdId = url['targetUrl'].apply(lambda x :x.split('-')[-1].split('?')[0])
        ProdId = set(ProdId)
        url = [f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{i}&Sort=SubmissionTime%3Adesc&Limit=100&Include=Products%2CComments&Stats=Reviews&passkey={config['bazarVoiceApi']}&apiversion=5.4"
               for i in ProdId]
        return url
    
    start_urls = requests_urls()

    def parse(self,response):
        res = json.loads(response.text)
        total = int(res['TotalResults'])
        offset = 0
        if total > 0:
            while True :
                url = f"{response.url}&offset={offset}"
                resp = json.loads(response.text)
                for i in resp['Results'] :
                    db = MongoClient(config['host'])[config['mongoCol']][config["RevDb"]]
                    if db.count_documents({'SubmissionId':i['SubmissionId']})==0 :
                        db.insert_one(i)
                if total <= offset :
                    break
                else : 
                    offset += 100
                if response.status != 200:
                    print(response.url)
                yield scrapy.Request(url,self.parse)
