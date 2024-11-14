from datetime import datetime

import pandas as pd
import sys
from dateutil.relativedelta import relativedelta
import datetime
import config
import plotly.graph_objects as go
import numpy as np
from dbcontext import Context
from jupyter_service import JupyterService
from dash import Dash, dcc, html, callback, Output, Input, State
import dash
print(dash.__version__)

class IscriDash:

    def __init__(self):
        self.actor1_code = "USA"
        self.actor2_code = "CHN"
        self.context = Context()
        self.context.create(echo=False)
        self.service = JupyterService(self.context)
        pd.set_option('display.max_rows', 50)
        pd.set_option('display.max_columns', None)
        self.df = None
        self.get_df_by_code(self.actor1_code, self.actor2_code)
        self.country_df = self.service.get_countries()
        self.fig = None
        self.get_figure()

    def get_df_by_code(self, actor1_code, actor2_code):
        self.df = self.service.get_iscris_by_codes(actor1_code=actor1_code, actor2_code=actor2_code)
        self.df["year_month"] = self.df.apply(lambda row: f"{row.year}-{row.month:02d}", axis=1)
        self.df["date"] = self.df.apply(lambda row: datetime.date(row.year, row.month, 1), axis=1)

    def get_figure(self):
        self.fig = go.Figure()

        self.add_trace(self.fig)


        # Set title
        self.fig.update_layout(
            height=600,
            title=f"ISCRI {self.actor1_code}-{self.actor2_code}",
            yaxis_title="ISCRI",
            xaxis=dict(tickformat="%Y-%m")
        )

        # Add range slider
        self.fig.update_layout(
            xaxis=dict(
                rangeselector=dict(
                    buttons=list([
                        dict(count=1,
                             label="YTD",
                             step="year",
                             stepmode="todate"),
                        dict(count=1,
                             label="1y",
                             step="year",
                             stepmode="backward"),
                        dict(count=5,
                             label="5y",
                             step="year",
                             stepmode="backward"),
                        dict(count=10,
                             label="10y",
                             step="year",
                             stepmode="backward"),
                        dict(step="all")
                    ]),

                ),
                rangeslider=dict(
                    visible=True,
                    # thickness=0.1,
                    # autorange=True,
                    # range=["2018-01-01", "2019-01-01"],
                ),
                # autorange=True,

                range=[self.df.iloc[-1]["date"] + relativedelta(months=-10 * 12), self.df.iloc[-1]["date"]],
                type="date"
            )
        )

    def add_trace(self, fig):
        fig.add_trace(
            go.Scatter(
                x=self.df.date,
                y=np.round(self.df.iscri, 2),
                name="iscri",
                line=dict(color="#0000ff", width=3),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=self.df.date,
                y=np.round(self.df.iscri4, 2),
                name="iscri4",
                line=dict(color="#ff0000", width=2),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=self.df.date,
                y=np.round(self.df.iscri3, 2),
                name="iscri3",
                line=dict(color="#ff8000", width=2),
            )
        )


d = IscriDash()

css = {"font-family": "Arial, Helvetica, sans-serif"}

app = Dash()
app.layout = ([
    html.Div([
        html.Div('ISCRI Dash Demo'),
        dcc.Dropdown(d.country_df.iso3, id='country1-dd', value="USA"),
        dcc.Dropdown(d.country_df.iso3, id='country2-dd', value="CHN"),
        html.Button('Submit', id='submit-val', n_clicks=0),
        html.Div(dcc.Graph(figure=d.fig, id="myfigure")),
        html.Div(id='debug')
        ],
        style=css
    )
    ])

@callback(
    Output("debug", "children"),
    Input('country1-dd', 'value')
)
def country1_update(value):
    actor1_code = value
    return f'Country1: {value}'

@callback(
    Output("debug", "children", allow_duplicate=True),
    Input('country2-dd', 'value'),
    prevent_initial_call=True
)
def country2_update(value):
    actor2_code = value
    return f'Country2: {value}'

@callback(
    Output('myfigure', 'figure', allow_duplicate=True),
    Input('submit-val', 'n_clicks'),
    State("country1-dd", "value"),
    State("country2-dd", "value"),
    prevent_initial_call=True
)
def button_clicked(n_clicks, value1, value2):
    d.actor1_code = value1
    d.actor2_code = value2
    d.get_df_by_code(value1, value2)
    d.get_figure()
    return d.fig

app.run_server(debug=True, use_reloader=False)