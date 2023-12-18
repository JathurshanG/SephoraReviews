import scrapy
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import datetime
import json 
import yaml
from yaml.loader import SafeLoader

inputFile = "../../inputFiles/config.yaml"
with open(inputFile,"r") as f :   
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

        db = MongoClient(config['host'])
        db = db[config['mongoCol']][config['informationDb']]
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

class SephoraSpider(scrapy.Spider) :
    name = 'revSephora'
    allowed_domains = ['api.bazaarvoice.com']
    
    InformationDb = config['informationDb']
    reviewDb = config['RevDb']
    api = config['bazarVoiceApi']
    hotline = config['host']

    def requests_urls():
        db  = MongoClient(config['host'])
        url = pd.DataFrame(db['Sephora'][config['informationDb']].find())
        url.sort_values(by='reviews',inplace=True)
        ProdId = url['targetUrl'].apply(lambda x :x.split('-')[-1].split('?')[0])
        ProdId = set(ProdId)
        url = [f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{i}&Sort=SubmissionTime%3Adesc&Limit=100&Include=Products%2CComments&Stats=Reviews&passkey=calXm2DyQVjcCy9agq85vmTJv5ELuuBCF2sdg4BnJzJus&apiversion=5.4"
               for i in ProdId]
        return url
    
    start_urls = requests_urls()

    def parse(self,response):
        mongo_db = 'Sephora'
        mongo_col = self.reviewDb
        while True:
            resp = json.loads(response.text)
            results = resp.get('Results', [])
            
            for review in results:
                db = MongoClient('localhost', 27017)[mongo_db][mongo_col]
                if db.count_documents({'CID': review['CID']}) == 0:
                    db.insert_one(review)
            
            if not results:
                break
            
            offset = resp.get('Offset', 0) + resp.get('Limit', 100)
            url = f"{response.url}&offset={offset}"
            yield scrapy.Request(url,self.parse)