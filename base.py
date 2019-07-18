from dash.dependencies import Input, Output, State
from constants import SparkStates, JobStates, OutputStatus, DashIds
from utils import prettify_json, parse_json, LivyRequests
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from string import Template
import textwrap
import sys
import time
import wrapper
import pandas as pd
#Fullpath of the code file
codefilename = str(sys.argv[1])

#port number for running the ui
portnumber = sys.argv[2]

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__,external_stylesheets=external_stylesheets)

#Read the code file (check existence of the file). Variables that need to be substituted before the spark run should be preceded by $ sign in the
#original file pyspark code file.

with open(codefilename, 'r') as content_file:
    content_raw = content_file.read()
content_dedent=textwrap.dedent(content_raw)
template=Template(content_dedent)
#print(job)

#here it needs to call a function that returns the id and state of an idle sessions
#better to start a session of livy at start of cluster (no wait for first)

#get the session url for livy_host
idle_session_url=wrapper.get_idle_session_url()

if idle_session_url=='':
    print("No idle session provided")
    exit

app.layout=html.Div(children=[
    html.H1(id='heading_one',children="Start the test"),
    html.Div(
        [
            html.Div(
                [
                    dcc.Dropdown(
                        id='property',
                        options=[
                            {"label": "New York", "value": "PROP1001"},
                            {"label": "San Fransisco", "value": "PROP1003"},
                            {"label": "Delhi", "value": "PROP1004"},
                            {"label": "London", "value": "PROP1005"},
                            ],
                            value="New York",
                            )
                            ],
                        style={"width": "48%", "display": "inline-block"},
                    ),
            html.Div(            [
                    dcc.Dropdown(
                        id='year',
                        options=[
                            {"label": "2018", "value": "2018"},
                            {"label": "2017", "value": "2017"},
                            {"label": "2016", "value": "2016"},
                            {"label": "2015", "value": "2015"},
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
        templated_string=template.substitute(modifier=property)
        job={"code":templated_string}
        job_info = LivyRequests().run_job(idle_session_url, job)
        statement_url = job_info["statement-url"]
        #after running the job need to wait for output. status will change to available and output to ok.
        while(True):
            statement_response = LivyRequests().job_info(statement_url)
            if statement_response["state"]=="available":
                while(True):
                 #   print("checking the output object")
                    if statement_response["output"] is not None:
                        print("in none")
                        break
                    else:
                        print("still in while")
                data = statement_response["output"]["data"]
                payload = parse_json(data["text/plain"])
                dfe = pd.DataFrame(list(zip(*payload["x"])),columns=payload["y"])
                x = (dfe['forecast_period_end_date'])
                y = (dfe['avg_net_income_pq'])
                break
            else:
                time.sleep(1)
    else:
        print("default values to start with")
        x=[1,2,3,4]
        y=[3,4,5,6]
    return {
        "data": [
        go.Scatter(
            x=x,
            y=y,
            mode="markers",
            marker={
                "size": 15,
                "line": {"width": 0.5 , "color": "white"},
                },
                )
                ],
        "layout": go.Layout(
            margin={"b":40 , "t": 10, "r": 0}, hovermode="closest"
            )
                    }

if __name__=='__main__':
    app.run_server(debug=False,port=portnumber)
