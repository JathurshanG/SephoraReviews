
import json
import pandas as pd 
from pymongo import MongoClient
import yaml
from yaml import SafeLoader
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import regexp_extract
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import re


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

    dt['shortDesc'] = dt['shortDesc'].apply(lambda x : re.sub(r"<[^>]*>", ",", x))
    groupByDt = dt.groupby(by=['prodID','name','shortDesc',"brand",'Category1', 'Category2','Category3',"size"],as_index=False).agg({"reviews": "max",
                                                                        "variant" :"count",
                                                                        "rating" : "max",
                                                                        "price"  : "max",
                                                                        "lovesCount" : "max"})
    groupByDt.sort_values(['prodID'],ascending=[False]).reset_index(drop=True,inplace=True)    
    groupByDt.to_json('../../outputFiles/dataset/GroupedData.json')
    return groupByDt

def groupedCat(name,rev):
    dt = rev.groupBy(['ProdID']).pivot(name).count().drop('null')
    return dt


def LogisticRegressions(rev) :
    rev = rev.drop('CID').drop('_id').sort('ProdID').dropna(subset='ReviewText')
    Analysis = rev.filter((rev['Rating']==1) | (rev['Rating']==5))
    Analysis = Analysis.withColumn('label',when(Analysis['Rating']==5,1).when(Analysis['Rating']==1,0)).limit(100000).select('ProdID','ReviewText','label')
    # Création d'un pipeline de traitement de texte
    tokenizer = Tokenizer(inputCol="ReviewText", outputCol="mots")
    hashingTF = HashingTF(inputCol="mots", outputCol="rawFeatures", numFeatures=10000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    pipeline = Pipeline(stages=[tokenizer, hashingTF, idf])
    # Division des données en ensembles d'entraînement et de test
    train_data, test_data = Analysis.randomSplit([0.8, 0.2], seed=42)
    # Entraînement du modèle de régression logistique
    lr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
    pipeline_lr = Pipeline(stages=[pipeline, lr])
    model = pipeline_lr.fit(train_data)
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="label")
    accuracy = evaluator.evaluate(predictions)
    print(f"Précision du modèle : {accuracy}")
    AllRev = rev.select('ProdID','ReviewText')
    predictionAll = model.transform(AllRev)
    predictionSentiment = groupedCat('prediction',predictionAll).withColumnRenamed('0.0','BadSentiment').withColumnRenamed('1.0','GoodSentiment')
    predictionSentiment.show(10)
    return predictionSentiment

def GetReviewsData():
    config = getconfig()
    host = config['host']
    db   = config['mongoCol']
    rev = config['RevDb']
    conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                    .setMaster('local') \
                    .setAppName('ReviewSephora') \
                    .setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
    sc = SparkContext(conf=conf)
    sqlC = SQLContext(sc)
    mongo_uri = f"{host}{db}.{rev}"
    rev = sqlC.read.format('com.mongodb.spark.sql.DefaultSource').option('uri',mongo_uri).load()
    rev = rev.drop('CID').drop('_id').sort('ProdID')
    product_id_regex = r'ProductId%3A(\w+)'
    rev = rev.withColumn('ProdID',regexp_extract('url',product_id_regex,1))
    Sentiment = LogisticRegressions(rev)
    age = groupedCat('age',rev)
    ContentLocale = groupedCat("ContentLocale",rev)
    IsFeatured = groupedCat("IsFeatured",rev).withColumnRenamed('true','Featured').withColumnRenamed('false','NotFeatured')
    IsRecommended = groupedCat('IsRecommended',rev).withColumnRenamed('true','Recomended').withColumnRenamed('false','NotRecomended')
    IsSyndicated = groupedCat('IsSyndicated',rev).withColumnRenamed('true','Syndicated').withColumnRenamed('false','NotSyndicated')
    eyeColor      = groupedCat('eyeColor',rev)
    hairColor     = groupedCat('hairColor',rev)
    hairCondition = groupedCat('hairCondition',rev)
    skinTone      = groupedCat('skinTone',rev)
    skinType      = groupedCat('skinType',rev)
    finalDt = age.join(ContentLocale,on=['ProdID'])\
                .join(IsFeatured,on='ProdID')\
                .join(IsRecommended,on='ProdID')\
                .join(IsSyndicated,on='ProdID')\
                .join(eyeColor.toDF(*[col + "_eye" for col in eyeColor.columns ]).withColumnRenamed('ProdID_eye','ProdID'),on=['ProdID'])\
                .join(hairColor.toDF(*[col + "_hair" for col in hairColor.columns]).withColumnRenamed('ProdID_hair',"ProdID"),on=['ProdID'])\
                .join(hairCondition.toDF(*[col + "_hair" for col in hairCondition.columns]).withColumnRenamed('ProdID_hair',"ProdID"),on='ProdID')\
                .join(skinTone.toDF(*[col + "_skin" for col in skinTone.columns]).withColumnRenamed('ProdID_skin',"ProdID"),on='ProdID')\
                .join(skinType.toDF(*[col + "_skin" for col in skinType.columns]).withColumnRenamed('ProdID_skin',"ProdID"),on='ProdID')\
                .join(Sentiment,on='ProdID')
    
    finalDt.toPandas().to_json('../../outputFiles/dataset/AggregateData.json')
    sc.stop()
    return None

