from dash import dcc, html
import dash_bootstrap_components as dbc
from datetime import datetime
import dash_table
import dash_date as dd

yesterday_morning, yesterday_night = dd.get_yesterday()

layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.H4(
                        "Stock Market Tweets Analysis",
                        className="text-center bg-info text-white",                       
                    ),
                    width=12,
                )
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="slct_ticker",
                        options=[
                            {"label": "AAPL", "value": "AAPL"},
                            {"label": "GOOG", "value": "GOOG"},
                            {"label": "AMZN", "value": "AMZN"},
                        ],
                        multi=False,
                        value="AAPL",                                          
                    ),                    
                    width=2,                   
                    align="center",
                ),
                dbc.Col(
                    dcc.DatePickerRange(
                        id="date-picker-range",
                        calendar_orientation="horizontal",
                        day_size=39,
                        with_portal=False,
                        first_day_of_week=0,
                        reopen_calendar_on_clear=True,
                        is_RTL=False,
                        clearable=False,
                        number_of_months_shown=1,
                        min_date_allowed=datetime(2021, 9, 27),
                        max_date_allowed=datetime(2021, 10, 1),                        
                        initial_visible_month=datetime(2021, 9, 30),
                        start_date=datetime(2021, 9, 27, 0,0,0),
                        end_date=datetime(2021, 10, 1,23,59,59),
                        #max_date_allowed=yesterday_morning,
                        #initial_visible_month=yesterday_night,
                        #start_date=yesterday_morning,
                        #end_date=yesterday_night,
                        display_format="MMM Do, YY",
                        month_format="MMMM, YYYY",
                        minimum_nights=0,
                        persistence=True,
                        persisted_props=["start_date", "end_date"],
                        persistence_type="session",
                        updatemode="singledate",                                             
                    ),
                    width=3, align="center" 
                ),
                dbc.Col(
                    html.H6(id="header-info", className="text-muted", style={'font-size': '80%'}),                   
                    align="center"
                ),
            ]
        ),
        dbc.Row(dbc.Col(html.H1(" "))),                             
        dbc.Row(
            [
                dbc.Col(
                    html.H1(
                        " ",
                        className="text-center bg-info text-white",
                        style={"height":"100%"}               
                    )
                )
            ]
        ),
        dbc.Row([dbc.Col([dcc.Graph(id="graph_a")])]),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dash_table.DataTable(
                            id="datatable",
                            columns=[
                                {"name": "tweet_created_at","id":"tweet_created_at"},
                                {"name": "user_name","id":"user_name"},
                                {"name": "tweet_text","id":"tweet_text"},
                                {"name": "sentiment","id":"sentiment"},                          
                            ],
                            page_size=5,
                            style_data={"whiteSpace": "normal", "height": "auto"},
                        )
                    ],
                    width=7,
                ),
                dbc.Col([dcc.Graph(id="graph_b")], width=5),
            ]
        ),
        dbc.Row(),
    ],fluid=True
)