import pandas as pd 
from pymongo import MongoClient
import yaml
from yaml import SafeLoader

def getconfig() :
    inputFile = "../../inputFiles/config.yaml"
    with open(inputFile,'r') as fp :
        config = yaml.load(fp,Loader=SafeLoader)
    return config

def processAggregateDate() :
    config = getconfig()
    host = config['host']
    db   = config['mongoCol']
    info = config['informationDb']
    with MongoClient(host) as client :
        db = client[db][info]
        dt = pd.DataFrame(list(db.find()))
        dt.drop(columns=['_id'],inplace=True)

    dt['prodID'] = dt['targetUrl'].apply(lambda x : x.split("-")[-1].split('?')[0])
    dt["price"] = dt['listPrice'].apply(lambda x : float(x.replace('$',"")))
    dt["skuImages"] =  dt["skuImages"].apply(lambda x : x['imageUrl'])
    dt['variant'] = 1
    groupByDt = dt.groupby(by=['prodID','name',"brand",'Category1', 'Category2','Category3',"size"],as_index=False).agg({"reviews": "max",
                                                                          "variant" :"count",
                                                                          "rating" : "max",
                                                                          "price"  : "max",
                                                                          "lovesCount" : "max"})
    groupByDt.sort_values(['prodID'],ascending=[False]).reset_index(drop=True,inplace=True)
    groupByDt
    return groupByDt
