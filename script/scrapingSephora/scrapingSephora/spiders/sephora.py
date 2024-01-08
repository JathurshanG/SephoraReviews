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

class SephoraReviewSpider(scrapy.Spider) :
    name = 'revSephora'
    allowed_domains = ['api.bazaarvoice.com']
    
    InformationDb = config['informationDb']
    reviewDb = config['RevDb']
    api = config['bazarVoiceApi']
    host = config['host']

    def start_requests(self):
        db  = MongoClient(self.host)
        dt = pd.DataFrame(db['Sephora'][self.InformationDb].find())
        dt.sort_values(by='reviews',inplace=True)
        dt['ProdId'] = dt['targetUrl'].apply(lambda x :x.split('-')[-1].split('?')[0])
        df = dt[["ProdId",'reviews']].drop_duplicates()
        df['offsetMax'] = df['reviews'] - (df['reviews'] % 100) 
        df['offsetMax'] = df['offsetMax'].astype(int)
        df.reset_index(inplace=True,drop=True)
        allUrl = []
        for idx,prodId in enumerate(df['ProdId']) :
            for total in range(0,df["offsetMax"][idx],100) :
                allUrl.append(f"https://api.bazaarvoice.com/data/reviews.json?Filter=contentlocale%3Aen*&Filter=ProductId%3A{prodId}&Sort=SubmissionTime%3Adesc&Limit=100&Include=Products%2CComments&Stats=Reviews&passkey={self.api}&apiversion=5.4&offset={total}")
        allReady = list(set(db['Sephora'][self.reviewDb].distinct("url")))
        allUrl = list(set(allUrl) - set(allReady))
        for url in allUrl :
            yield scrapy.Request(url,callback=self.parse_item)


    def parse_item(self,response) :
        resp = json.loads(response.text)
        results = resp.get('Results',[])
        for item in results :
            with MongoClient(self.host) as fp :
                col =fp['Sephora'][self.reviewDb]
                if col.count_documents({"CID" : item["CID"]}) == 0 :
                     item["url"] = response.url
                     col.insert_one(item)
                     yield item

