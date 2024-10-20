import plotly.graph_objects as go
import numpy as np
import sys
import sqlalchemy
import config
import pandas as pd
import art
import psycopg2
import jupyterlab
import ipywidgets
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly
import numpy as np
from dbcontext import Context
from jupyter_service import JupyterService
art.tprint(config.name, "big")
print(sys.version, config.version, jupyterlab.__version__, plotly.__version__, ipywidgets.__version__)

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', None)
print(config.connection_string)
context = Context()
context.create(echo=False)
service = JupyterService(context)

df = service.get_iscris_by_codes(actor1_code='USA', actor2_code='CHN')
df["year_month"]=df.year.astype(str) + "-" + df.month.astype(str)
fig = go.Figure()

for step in np.arange(len(df)):
    fig.add_trace(
        go.Scatter(
            visible=False,
            line=dict(color="#00CED1", width=6),
            name=f"Month {step}",
            x=df.year_month,
            y=df.iscri[step:],
        ))

fig.data[0].visible = True

steps = []
for index, row in df.iterrows():
    step = dict(
        method="update",
        args=[{"visible": [False] * len(fig.data)},
              # {"title": f"Date: {row['year']}-{row['month']:02d}"},
              ],
        label=f"{row['year']}-{row['month']:02d}",
    )
    step["args"][0]["visible"][index] = True
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue={"prefix": "Date: "},
    pad={"t": len(df)},
    steps=steps
)]

fig.update_layout(
    sliders=sliders,
    height=500,
    title="ISCRI USA-CHN",
    # xaxis_title="Date",
    yaxis_title="ISCRI",
    # legend_title="Legend",
    xaxis=dict(tickformat="%Y-%m")
)
fig.update_yaxes(range=[1.2, 1.75])
# fig.write_html("data/out.html")
# fig.show()

from dash import Dash, dcc, html

app = Dash()
app.layout = [
    html.Div(children='My First App with Dash'),
    html.Div([dcc.Graph(figure=fig)])
    ]

app.run_server(debug=True, use_reloader=False)