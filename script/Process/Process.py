import pandas as pd
import numpy as np
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF
from sklearn.metrics.pairwise import cosine_similarity

ReviewData = pd.read_json('https://raw.githubusercontent.com/JathurshanG/SephoraReviews/process/outputFiles/dataset/AggregateData.json')
dt = pd.read_json('https://raw.githubusercontent.com/JathurshanG/SephoraReviews/process/outputFiles/dataset/GroupedData.json')
finalDt = ReviewData.merge(dt,how='left',left_on='ProdID',right_on='prodID')
finalDt.dropna(subset='name',inplace=True)
age = ["ProdID",'13to17', '18to24', '25to34', '35to44', '45to54', 'over54']
country = ["ProdID",'en_CA' , 'en_US']
Recomended = ["ProdID",'NotRecomended', 'Recomended']
eyeColor = ["ProdID",'Grey_eye', 'blue_eye', 'brown_eye', 'gray_eye','green_eye', 'hazel_eye']
hairColor = ["ProdID",'auburn_hair', 'black_hair','blonde_hair','brown_hair', 'brunette_hair', 'gray_hair', 'red_hair']
hairCondition = ["ProdID",'chemicallyTreated_hair','coarse_hair', 'curly_hair', 'dry_hair','fine_hair', 'normal_hair', 'oily_hair']
skinTone = ["ProdID",'dark_skin', 'deep_skin','ebony_skin', 'fair_skin', 'fairLight_skin', 'light_skin',       'lightMedium_skin', 'medium_skin', 'mediumTan_skin', 'notSureST_skin','olive_skin', 'porcelain_skin', 'rich_skin', 'tan_skin',]
skinType = ["ProdID",'combination_skin', 'dry_skin', 'normal_skin', 'oily_skin']
Sentiment = ["ProdID",'BadSentiment', 'GoodSentiment']
information = ["ProdID",'prodID', 'name', 'brand', 'Category1','Category2', 'Category3', 'size', 'reviews', 'variant', 'rating', 'price', 'lovesCount']
stop_words = set(stopwords.words("english"))

def cleanData(sentence) :
    texte = sentence.lower()
    texte = re.sub(r"[^\w\s]", "", texte)
    texte = " ".join([mot for mot in word_tokenize(texte) if mot not in stop_words])
    return texte


def Recomendation(finalDt,name,brand):  
    cat3 = finalDt.loc[(finalDt['name']==name) & (finalDt['brand']==brand),'Category3'].values[0]
    cat1 = finalDt.loc[(finalDt['name']==name) & (finalDt['brand']==brand),'Category1'].values[0]
    ProdID = finalDt.loc[(finalDt['name']==name) & (finalDt['brand']==brand),'ProdID'].values[0]
    df = finalDt[finalDt['Category3']==cat3]
    df = df.drop_duplicates(subset='ProdID').reset_index(drop=True)
    df['texte'] = df['shortDesc'].apply(lambda x : cleanData(x))
    tfidf_vectorizer = TfidfVectorizer()
    product_features = tfidf_vectorizer.fit_transform(df['texte'])
    model = NMF(n_components=5)
    model.fit(product_features)
    cosine_similarities = cosine_similarity(product_features)
    product_index = df[df['ProdID'] == ProdID].index[0]
    similar_products = cosine_similarities[product_index].argsort()[::-1][1:6]  
    recommended_products = df.iloc[similar_products]['ProdID']
    for idx,i in enumerate(recommended_products) :
        df.loc[df['ProdID']==ProdID,f'Reco{idx}'] = i
    df = df.drop(columns=['texte'])
    return df

