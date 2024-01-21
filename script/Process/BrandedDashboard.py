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
            dbc.Col([
                html.P(children=""),
                html.Img(src="https://github.com/JathurshanG/SephoraReviews/blob/process/inputFiles/DashboardPhotos/ItemNum.png?raw=true",style={"height":'50%'}),
                html.P(id='numberOfItem',style={"text-align" : "center"})],width=2,style={'text-align': 'center'}),
            dbc.Col([html.Img(src="https://github.com/JathurshanG/SephoraReviews/blob/process/inputFiles/DashboardPhotos/Rating.png?raw=true",style={"height":"50%"}),
                    html.P(id='avgRating',style={"text-align" : "center"})],width=2,style={'text-align': 'center'}),
            dbc.Col([html.Img(src="https://github.com/JathurshanG/SephoraReviews/blob/process/inputFiles/DashboardPhotos/Hearth.png?raw=true",style={"height" : "50%"}),
                    html.P(id='avgLovesCount',style={"text-align" : "center"})],width=2,style={'text-align': 'center'}),
            dbc.Col(dcc.Graph(id="pie_chart"),width=6)
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


    pie = px.pie(filtered_data, names='Category1',color_discrete_sequence=px.colors.sequential.gray)
    histogram = px.histogram(filtered_data.drop_duplicates(subset='name').sort_values('price'), x='name', y='price',color_discrete_sequence=['#545454'])\
                .update_layout(yaxis_title='Price ($)',plot_bgcolor='rgba(0, 0, 0, 0)',paper_bgcolor='rgba(0, 0, 0, 0)',xaxis_title=None)
    # Scatter plot
    scatter_plot = px.scatter(filtered_data, x='reviews', y='lovesCount',color_discrete_sequence=['#545454'])\
                .update_layout(plot_bgcolor='rgba(0, 0, 0, 0)',paper_bgcolor='rgba(0, 0, 0, 0)')
    
    return numberOfItem,avgRating,avgLovesCount,pie,histogram, scatter_plot

if __name__ == '__main__':
    app.run_server(debug=True)
