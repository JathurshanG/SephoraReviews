import Process  # Importé pour accéder à la variable finalDt
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd

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
    dbc.Row([
        dbc.Col([
            html.Img(id='Image')
        ])
    ])
                            ])

@app.callback(
    Output('dropdown_products', 'options'),
    [Input('dropdown_marques', 'value')])

def update_products_and_price(marque):
    filtered_df = df[(df['brand'] == marque)]
    return [{'label': produit, 'value': produit} for produit in filtered_df['name'].unique()]

@app.callback(
    Output('Image', 'children'),
    [Input('dropdown_marques', 'value'),
     Input('dropdown_products', 'value')]
)
def getInfo(marque,produit) :
    filtereData = df[(df['brand']==marque) & (df['name']==produit)]
    imageUrl = filtereData['ProdID'].values[0]
    imageUrl = f'sephora.com{imageUrl}'
    return imageUrl
    
if __name__ == '__main__':
    app.run_server(debug=True)
