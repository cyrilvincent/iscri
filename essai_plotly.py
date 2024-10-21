from datetime import datetime

import numpy as np
import plotly.graph_objects as go
import pandas as pd
import config
from dbcontext import Context
from jupyter_service import JupyterService

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', None)
print(config.connection_string)
context = Context()
context.create(echo=False)
service = JupyterService(context)

actor1_code='USA'
actor2_code='CHN'

df = service.get_iscris_by_codes(actor1_code=actor1_code, actor2_code=actor2_code)
#df["year_month"]=df.year.astype(str) + "-" + df.month.astype(str)
df["year_month"]=df.apply(lambda row: f"{row.year}-{row.month:02d}", axis=1)
#df["date"]=df.apply(lambda row: datetime.date(row.year, row.month, 1), axis=1)

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
    # fig.add_trace(
    #     go.Scatter(
    #         visible=False,
    #         # line=dict(color="#00CED1", width=6),
    #         name=f"Month {step}",
    #         x=df.year_month,
    #         y=df.iscri4[step:],
    #     ))
    # fig.add_trace(
    #     go.Scatter(
    #         visible=False,
    #         # line=dict(color="#00CED1", width=6),
    #         name=f"Month {step}",
    #         x=df.year_month,
    #         y=df.iscri3[step:],
    #     ))
fig.data[0].visible = True
# fig.data[1].visible = True
# fig.data[2].visible = True

steps = []
for index, row in list(df.iterrows())[::1]:
    step = dict(
        method="restyle",
        args=[{"visible": [False] * len(fig.data)},
              # {"title": f"Date: {row['year']}-{row['month']:02d}"},
              ],
        label= row["year_month"]  # f"{row['date'].year}-{row['date'].month:02d}",
    )
    step["args"][0]["visible"][index] = True
    # step["args"][0]["visible"][index + 1] = True
    # step["args"][0]["visible"][index + 2] = True
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue={"prefix": "Date: "},
    # pad={"t": len(df)},
    steps=steps
)]

fig.update_layout(
    sliders=sliders,
    # height=600,
    # title=f"ISCRI {actor1_code}-{actor2_code}",
    # # xaxis_title="Date",
    # yaxis_title="ISCRI",
    # # legend_title="Legend",
    # xaxis=dict(tickformat="%Y-%m")
)
# fig.update_yaxes(range=[1.4, 1.8])
fig.write_html("data/out.html")
fig.show()