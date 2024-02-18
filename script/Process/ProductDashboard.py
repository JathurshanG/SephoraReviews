import Process  # Importé pour accéder à la variable finalDt
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
from PIL import Image
import plotly.express as px

df = Process.finalDt  # Récupération du DataFrame

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1('Sephora Product Dashboard'),
    dbc.Row([
        dbc.Col(dcc.Dropdown(
            id='dropdown_marques',
            options=[{'label': marque, 'value': marque} for marque in df['brand'].unique()],
            value=df['brand'].unique()[0],
        )),
        dbc.Col(dcc.Dropdown(id='dropdown_products',
                             
                             )),
    ]),
    html.P(''),
    dbc.Row([
        dbc.Col([
            html.Img(id='Image')],width=1),
        
        dbc.Col([
            html.P(id='name'),
            html.P(id='price'),
            html.P(id='Cat1'),
            html.P(id='Cat2'),
            html.P(id='Cat3'),
            html.P(id='reviews'),
            html.P(id='rating'),
            html.P(id='lovesCount'),
            
        ],width=2),
        
        dbc.Col(
              [dcc.Graph(id="age")],width=3),
        
        dbc.Col(
              [dcc.Graph(id='figdb')],width=3),
        
        dbc.Col(
            [dcc.Graph(id='SentimentFig')],width=3)
    ]),
    dbc.Row()
    ])


@app.callback(
    [Output('dropdown_products', 'options'),
    Output('dropdown_products', 'value')],
    [Input('dropdown_marques', 'value')])

def update_products_and_price(marque):
    filtered_df = df[(df['brand'] == marque)]
    filtered_df = filtered_df.sort_values(by='name')
    val = filtered_df['name'].unique()[0]
    return [{'label': produit, 'value': produit} for produit in filtered_df['name'].unique()],val

@app.callback(
    [Output('Image', 'src'),
     Output('name','children'),
     Output('price','children'),
     Output('Cat1','children'),
     Output('Cat2','children'),
     Output('Cat3','children'),
     Output('reviews','children'),
     Output('rating','children'),
     Output('lovesCount','children'),
     Output('age','figure'),
     Output('figdb','figure'),
     Output('SentimentFig',"figure")],

    [Input('dropdown_marques', 'value'),
     Input('dropdown_products', 'value')]
)
def getInfo(marque,produit) :
    filtereData = df[(df['brand']==marque) & (df['name']==produit)]
    imageUrl = filtereData['skuImages']
    imageUrl = f"https://sephora.com{imageUrl.values[0]}?imwidth=200"
    
    name = f"Name : {filtereData['name'].values[0]}"
    price = f"Price : ${round(filtereData['price'].values[0],3)}"
    Cat1 = f"Category 1 : {filtereData['Category1'].values[0]}"
    Cat2 = f"Category 2 : {filtereData['Category2'].values[0]}"
    Cat3 = f"Category 3 : {filtereData['Category3'].values[0]}"
    reviews = f"Reviews : {int(filtereData['reviews'].values[0])}"
    rating = f"Rating : {round(filtereData['rating'].values[0],2)}"
    lovesCount = f"LovesCount : {int(filtereData['lovesCount'].values[0])}"
    
    #figure 
    filterDt = Process.Recomendation(df,produit,marque)
    ageDf = filterDt[Process.age].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:]
    ageFig = px.pie(ageDf,names="name",values='values',hole=0.3,title='Age Of Consumer').update_layout(showlegend=False).update_traces(textposition='inside', textinfo='percent+label')

    if filterDt['Category1'].values[0]=="Skincare" :
        db =  filterDt[Process.skinType].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:].sort_values(by='values',ascending=False)
        db['name'] = db['name'].apply(lambda x: x.split('_')[0])
        figdb = px.bar(db,x="name",y="values",title='Skin Type').update_layout(showlegend=False)
    elif filterDt['Category1'].values[0]=="Hair" :
        db =  filterDt[Process.hairCondition].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:].sort_values(by='values',ascending=False)
        db['name'] = db['name'].apply(lambda x: x.split('_')[0])
        figdb = px.bar(db,x="name",y="values",title="Hair Condition").update_layout(showlegend=False)        
    elif filterDt['Category1'].values[0]=="Makeup" :
        db =  filterDt[Process.skinTone].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:].sort_values(by='values',ascending=False)
        db['name'] = db['name'].apply(lambda x: x.split('_')[0])
        figdb = px.bar(db,x="name",y="values",title="Skin Tone").update_layout(showlegend=False)      
    else :
        db =  filterDt[['NotRecomended', 'Recomended']].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:].sort_values(by='values',ascending=False)
        figdb = px.pie(db,names="name",values="values",title="Recomendation",hole=0.3)

    dbSentiment = filterDt[Process.Sentiment].reset_index(drop=True).transpose().reset_index().rename(columns={'index':'name',0:'values'})[1:].sort_values(by='values',ascending=False)
    figSentiment = px.pie(dbSentiment,names="name",values="values",hole=0.3,title='Sentiment Of Consumer').update_layout(showlegend=False).update_traces(textposition='inside', textinfo='percent+label')



    return imageUrl,name,price,Cat1,Cat2,Cat3,reviews,rating,lovesCount,ageFig,figdb,figSentiment
    
if __name__ == '__main__':
    app.run_server(debug=True)
