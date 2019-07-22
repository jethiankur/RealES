
# coding: utf-8

# In[16]:


from dash.dependencies import Input, Output, State
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd

df = pd.read_csv(r'C:\Users\ashvi\Desktop\UdataX_Amaxzon\Data\gapminder.csv')
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets=external_stylesheets)
app.layout=html.Div(children=[
    html.H1(id='heading_one',children="Start the test"),
    html.Div(
        [
            html.Div(
                [
                    dcc.Checklist(
                        id='property',
                        #multi=True,
                        options=[
                            {"label": "Afghanistan", "value": "Afghanistan"},
                            {"label": "Zambia", "value": "Zambia"},
                            {"label": "Argentina", "value": "Argentina"},
                            ],
                            value="",
                            )
                            ],

                        style={"width": "48%", "display": "inline-block"},
                    ),
            html.Div(            [
                    dcc.Dropdown(
                        id='year',
                        options=[
                            {"label": "1990", "value": "1990"},
                            {"label": "1980", "value": "1980"},
                            {"label": "1970", "value": "1970"},
                            {"label": "1880", "value": "1880"},
                            ],
                            value="2018",
                            )
                            ],
                            style={"width": "48%", "float": "right", "display": "inline-block"},
                        ),
        ]
        ),
        dcc.Graph(id='graph'),
        html.Button("RUN", id='run_button', n_clicks=0)
        ])

@app.callback(
            Output('graph','figure'),
            [Input('run_button','n_clicks')],
            [State('property',"value"),
             State('year',"value")]
)
def update_graph(input_value,property,year):
    n_clicks=input_value
    #Substitute with input values
    if n_clicks > 0:
        yr=int(year)
        depths = [[] for i in range(len(property))]
        depth_i=0
        print(property)
        traces=[]
        for i in property:
            depths[depth_i]=df.life[(df['Country']==i) & (df['Year'] > yr)]
            depth_i=depth_i+1
        x=df.Year[(df['Country'].isin(property)) & (df['Year'] > yr)]
        ic=0
        for i in property:
            mydict={'x': x, 'y':depths[ic], 'type': 'lines', 'name': property}
            traces.append(mydict)
            ic=ic+1
        print(traces)
    else:
        print("default values to start with")
        traces=[]
        x=[]
        y=[]
    return {
            'data': traces,
        "layout": go.Layout(
            margin={"b":40 , "t": 10, "r": 0}, hovermode="closest"
            )
                    }

if __name__=='__main__':
    app.run_server(debug=False)

