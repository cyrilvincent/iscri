# -*- coding: utf-8 -*-

# 1.0 Imports
import dash
from dash import dcc
from dash import html

from dash.dependencies import Output
from dash.dependencies import Input

import plotly.express as px
import dash_bootstrap_components as dbc

import os
import pandas as pd
import random

# 1.1 Read in data
data_fol = "data/"

wc_date_df = pd.read_csv(data_fol + "date_df.csv", sep=",", encoding="utf8")
wc_month_df = pd.read_csv(data_fol + "month_df.csv", sep=",", encoding="utf8")
wc_year_df = pd.read_csv(data_fol + "year_df.csv", sep=",", encoding="utf8")

# get a df sorted by word count. Is useful to only to this once here and
# reuse below whenever eg. only 1000 words are necesary
wc_date_df["word_len"] = wc_date_df["word"].str.len()
words = wc_date_df.groupby(["word", "word_len"]
                           )["word_cnt_page"].sum().reset_index().sort_values(
    ["word_len", "word"], ascending=True)["word"]

# 1.2 load external / dbc stylesheets. Not necessary but its an option
external_stylesheets = [
    {"href": "https://fonts.googleapis.com/css2?"
             "family=Lato:wght@400;700&display=swap",
     "rel": "stylesheet",
     },
    dbc.themes.BOOTSTRAP
]

# 1.3 initiate the app
app = dash.Dash(__name__,
                external_stylesheets=external_stylesheets,
                suppress_callback_exceptions=True,  # sometimes callbacks dont work which is intended behaviour.
                # This suppresses those notifications
                )

app.title = "My Dashboard!"  # name to be dislplayed eg in browser tab

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # 2  SIDEBAR PART # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# https://dash-bootstrap-components.opensource.faculty.ai/examples/simple-sidebar/

sidebar = html.Div(
    [  # add some header text
        html.H2("My App", className="display-4",
                style={'textAlign': 'center', 'font-variant': 'small-caps'}),
        html.Hr(),  # this adds a horizontal line
        html.H3(
            "Data Analytics", className="display-8", style={'textAlign': 'center'}
        ),
        # add the actual links
        dbc.Nav(
            [
                dbc.NavLink("Page 1", href="/", active="exact"),
                dbc.NavLink("Page 2", href="/page2_url", active="exact"),
            ],
            vertical=True,  # well, that means not horizontically
            pills=True,  # adds a blue square around the active selection
            style={"font-size": 20, 'textAlign': 'center'}
        ),
    ],
    className="sidebar_style"
)

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # 3 LAYOUT PART # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# -----------------------------------------------------------------------------
# 3.1 PAGE 1

# store ALL of the following html in a variable page1_content. This is called when switching pages
# wrap everything in an out Div
page1_content = html.Div(
    children=[

        # header, this is a child of the outer div. All header information is then
        # wrapped in its own Div
        html.Div(
            children=[
                html.P(
                    children="🥦", className="header-emoji",
                ),
                # if children are set to only text, this text will be displayed
                html.H1(children="Word Analytics", className="header-title",
                        ),
                html.P(children="A super sophisticated dashboard example",
                       className="header-description",
                       ),
            ],
            className="header",  # optional css style defined in the style.css in /assets
            style={"background-color": "#b11226"}  # css style that overwrites className arguments
        ),

        # add some space between header and content
        html.Div(children="\n  ",
                 style={'min-height': "5px"}),  # if no min-height is specified, empty strings don't work

        # menu
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Choose Word",
                                 style={'display': 'inline-block', 'margin': '5px 5px 5px 5px', 'font-size': '20px'}),
                        # className="menu-title"),

                        # this adds a little [?] box that only displays some text on hover
                        html.Div(title=("This is just a simple and easy way to add some explanations"
                                        "Will show exactly this message when hovering over the [?] with your mouse"),
                                 children="[?]",
                                 style={'display': 'inline-block', 'border-bottom': '1px dotted black'}),

                        # the actual dropdown menu to select a word to display graphs for
                        dcc.Dropdown(
                            id="word-filter",  # id of this graph element. e.g. works as a reference for callbacks
                            options=[  # all values in the dropdown menu
                                {"label": word, "value": word}
                                for word in words
                            ],
                            value="sunken",  # default value
                            clearable=True,
                            className="dropdown",
                            style={'width': '98%', 'display': 'inline-block', 'margin': '0 5px auto'}
                        ),

                    ]
                ),  # these brackets and what they belong to can get confusing fast
            ],  # probably nest them clearer than I did!
            style={"display": 'inline-block', 'width': '49%'},
            className="card",
        ),

        # display number of found words
        html.Div(id='total_word_count',
                 style={'width': '24%', 'display': 'inline-block'}),

        # main/top graph
        html.Div(  # wrap the graph in its own div
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="total_count_chart",  # give it its own recognizable id
                        config={"displayModeBar": False},
                        style={"height": "29vh"}  # height of the graph element, means 29% of viewport
                    ),
                    className="card",  # css style defined in style.css in /assets
                ),
            ]
        ),

        # month/middle graph
        html.Div(  # wrap the graph in its own div
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="month_count_chart",  # give it its own recognizable id
                        config={"displayModeBar": False},
                        style={"height": "22vh"}  # height of the graph element, means 22% of viewport
                    ),
                    className="card",  # css style defined in style.css in /assets
                ),
            ]
        ),

        # year/bottom graph
        html.Div(  # wrap the graph in its own div
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="year_count_chart",  # give it its own recognizable id
                        config={"displayModeBar": False},
                        style={"height": "22vh"}  # height of the graph element, means 22% of viewport
                    ),
                    className="card",  # css style defined in style.css in /assets
                ),

            ]
        ),
    ],
)

# -----------------------------------------------------------------------------
# 3.2 Page2 content
page2_content = html.Div(children=[

    html.Img(id="add_imgx",
             src="assets/RR1.jpg",
             className="center",

             # styling large images can be tricky when watched on diffrent screen sizes
             # use this to have a fixes height and resize images width depending on
             # screen/window size
             style={'display': 'block', 'height': '90vh', 'width': 'auto',
                    'max-width': '100%', 'max-height': '100%',
                    "object-fit": "cover"}
             ),

    html.P(
        children="👍",  # as you saw above, you can use emojis as text as well
        className="header-emoji",
    )
])

# -----------------------------------------------------------------------------
# 3.3 bring it all together. Tell the app how to display all the elements
# content or pages on the right side will be changed when clicking on the sidebar links
# while the sidebar itself stays as it is

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

content = html.Div(id="page-content", style=CONTENT_STYLE)
app.layout = html.Div([dcc.Location(id="url"), sidebar, content])


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # 4  INTERACTIVE PART # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# 4.1 switch between pages. Each callback is connected to the function below
@app.callback(Output("page-content", "children"),  # returns the page content of the clicked page
              [Input("url", "pathname")])  # get the pathname from the url click as input
def render_page_content(pathname):  # function input the the Input() parameter from the callback decorator
    if pathname == "/":
        return page1_content
    elif pathname == "/page2_url":
        return page2_content
    # If the user tries to reach a different page, return an error message
    return html.Div(
        [
            html.H1("Page not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ],
        className="p-3 bg-light rounded-3",
    )


# 4.2 change image when switching to page2
@app.callback(Output(component_id="add_imgx", component_property="src"),
              [Input("url", "pathname")])
def change_werbung(pathname):  # function input the the Input() parameter from the callback decorator
    if pathname == "/page2_url":
        add = random.choice(["RR1", "RR2", "RR3"])  # return a random image from these three options
        return f"assets/{add}.jpg"
    else:
        return None


# 4.3 word trends page
@app.callback(
    # the function below must also have 4 outputs
    [Output("total_count_chart", "figure"),  # updates top graph
     Output("month_count_chart", "figure"),  # updates month/middle graph
     Output("year_count_chart", "figure"),  # updates year/bottom graph
     Output(component_id='total_word_count', component_property="children")
     # last output updates the number counting the hits
     ],
    [
        Input("word-filter", "value"),  # input is the selected word from the dropdown
    ],
)
def update_charts(the_word):  # function input the the Input() parameter from the callback decorator

    # graph 1 on page 1
    # filter df by selected word
    wc_date_filtered_df = wc_date_df.loc[wc_date_df["word"] == the_word]
    total_word_count = wc_date_filtered_df["word_cnt_page"].sum()

    # create a plotly figure using plotly express (higher level plotly api)
    wc_date_figure = px.line(x=wc_date_filtered_df["date"],
                             y=wc_date_filtered_df["word_cnt_page"],
                             title="Wordcount / Magazine",
                             markers=True,
                             template="plotly_white",  # https://plotly.com/python/templates/
                             )
    # change hoverformats, colors, layout...
    wc_date_figure.update_xaxes(title=None, hoverformat="%Y-%m-%d")
    wc_date_figure.update_yaxes(title=None)
    wc_date_figure.update_traces(line_color='#a12232',
                                 line_width=2,
                                 hovertemplate="%{y}",
                                 # "(%{x|%Y-%m-%d})", #https://github.com/d3/d3-3.x-api-reference/blob/master/Time-Formatting.md#format
                                 )
    wc_date_figure.update_layout(hovermode="x",
                                 hoverlabel={'bgcolor': "white"},
                                 margin=dict(l=40, r=20, t=80, b=50),  # graph size within "card"
                                 )

    # SECOND / THIRD GRAPH IN ALTERNATE DICT WAY TO CREATE -> LESS FLEXIBLE (COULDNT GET HOVERLABELS TO WORK WITH THIS)
    # graph 2 on page 1
    wc_month_filtered_df = wc_month_df.loc[wc_month_df["word"] == the_word]
    wc_month_figure = {
        "data": [
            {
                "x": wc_month_filtered_df["month"],
                "y": wc_month_filtered_df["avg. occurences per magazine"],
                "type": "lines",
            },
        ],
        "layout": {
            "title": {
                "text": "Ø Words per Magazine per Month",
                "x": 0.05,
                "xanchor": "left",
            },
            "colorway": ["#a12232"],
            "margin": dict(l=40, r=20, t=80, b=50)
        },
    }

    # graph 3 on page 1
    wc_year_filtered_df = wc_year_df.loc[wc_year_df["word"] == the_word]
    wc_year_figure = {
        "data": [
            {
                "x": wc_year_filtered_df["year"],
                "y": wc_year_filtered_df["avg. occurences per magazine"],
                "type": "lines",
            },
        ],
        "layout": {
            "title": {
                "text": "Ø Words per Magazine per Year",
                "x": 0.05,
                "xanchor": "left",
            },
            "colorway": ['#a12232'],
            "margin": dict(l=40, r=20, t=80, b=50)
        },
    }
    return wc_date_figure, wc_month_figure, wc_year_figure, f'{total_word_count} Hits'


# 5 run the darn thing
if __name__ == "__main__":
    # this will run the app on your local machine: basically dash will run
    # its own dev server on localhost: visit 127.0.0.1:8050 in your browser to
    # see your app
    app.run_server(port=8050, debug=True)

    # HOWEVER, if you are deploying it later, set your host to "0.0.0.0",
    # because dash will not be responsible for managing the server itself
    # app.run_server(host="0.0.0.0", port=8050)