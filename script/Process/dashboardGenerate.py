import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import process
import plotly.express as px

# Exemple de DataFrame avec les noms de marques et de produits
df = process.processAggregateDate().sort_values('brand')

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1('Sephora Branded Products'),
    html.Label(children="Brand:"),
    dcc.Dropdown(
        id='dropdown_marques',
        options=[{'label': marque, 'value': marque} for marque in df['brand'].unique()],
        value=df['brand'].unique()[0],
    ),
    html.Div([
        dbc.Row([
            dbc.Col([html.Img(src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/18/Five-pointed_star.svg/220px-Five-pointed_star.svg.png"),
                    html.P(id='numberOfItem',style={"text-align" : "center"})],width=2,style={'text-align': 'center'}),
            dbc.Col(html.P(id='avgRating'),width=2),
            dbc.Col(html.P(id='avgLovesCount'),width=2),
            dbc.Col(dcc.Graph(id='pie_chart'),width=6)
        ,
            dbc.Col(dcc.Graph(id='histogram_chart'),width=6),
            dbc.Col(dcc.Graph(id='scatter_plot'),width=6)
        ])
    ])
])

@app.callback(
    [Output('numberOfItem', 'children'),
     Output('avgRating', 'children'),
     Output('avgLovesCount', 'children'),
     Output('pie_chart','figure'),
     Output('histogram_chart','figure'),
     Output('scatter_plot','figure')

    ],
    [Input('dropdown_marques', 'value')],
)

def update_charts(selected_brand):
    # Filter data based on the selected brand
    filtered_data = df.loc[df['brand'] == selected_brand]
    numberofItem = len(filtered_data['name'].unique())
    numberOfItem = [f"Number of item : \n {numberofItem}"]

    avgLovesCount = round(filtered_data['lovesCount'].mean(),2)
    avgLovesCount = [f"Average of LovesCount : \n {avgLovesCount}"]

    avgRating = round(filtered_data['rating'].mean(),2)
    avgRating = [f"Average of Rating : \n {avgRating}"]


    pie = px.pie(filtered_data, names='Category1')
    histogram = px.histogram(filtered_data.drop_duplicates(subset='name').sort_values('price'), x='name', y='price').update_layout(xaxis_title="Product Name",yaxis_title='Price ($)')
    # Scatter plot
    scatter_plot = px.scatter(filtered_data, x='reviews', y='lovesCount')
    
    return numberOfItem,avgRating,avgLovesCount,pie,histogram, scatter_plot

if __name__ == '__main__':
    app.run_server(debug=True)
