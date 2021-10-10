import dash
import dash_bootstrap_components as dbc
import dash_data_fetch as ddf
import dash_layout as dl
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

datafetch = ddf.DataFetch()
datafetch.fetch()
datafetch.check_update()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
application = app.server

app.layout = dl.layout

@app.callback(
    Output("header-info", "children"),
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("slct_ticker", "value"),
    ],
)
def update_header(*inputs) -> str:
    """Get triggers from date picker and dropdown components, updates header info line."""
    header_info = f"""Timezone: Eastern Time |                    
                     Last Update: 2021-10-02 | 
                     Collected: {datafetch.tweets_count} Tweets 
                     {datafetch.tickers_count} AVG Prices 
                     {datafetch.users_count} Unique Users"""
    return header_info


def get_button() -> str:
    """Checks if any of buttons were triggered."""
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = "No clicks yet"
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return button_id


@app.callback(
    Output("datatable", "data"),
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("graph_a", "relayoutData"),
        Input("slct_ticker", "value"),
    ],
)
def update_datatable(start_date, end_date, relayoutData, option_slctd) -> pd.DataFrame:
    """Depending on date picker, subplots figure and dropdown components inputs, update datatable component."""
    if option_slctd == "AAPL":
        df_table = datafetch.df_tweets_aapl_full
    if option_slctd == "GOOG":
        df_table = datafetch.df_tweets_goog_full
    if option_slctd == "AMZN":
        df_table = datafetch.df_tweets_amzn_full

    button_id = get_button()

    if button_id == "slct_ticker":
        return df_table[start_date:end_date].to_dict("rows")
    elif button_id == "date-picker-range":
        return df_table[start_date:end_date].to_dict("rows")
    else:
        if relayoutData:
            if "xaxis.range[0]" in relayoutData:
                start_date = relayoutData["xaxis.range[0]"].split(".")[0]
                end_date = relayoutData["xaxis.range[1]"].split(".")[0]
                return df_table[start_date:end_date].to_dict("rows")
            return df_table[start_date:end_date].to_dict("rows")
        else:
            return df_table[start_date:end_date].to_dict("rows")


@app.callback(
    Output("graph_b", "figure"),
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("graph_a", "relayoutData"),
        Input("slct_ticker", "value"),
    ],
)
def update_graph_b(start_date, end_date, relayoutData, option_slctd) -> pd.DataFrame:
    """Depending on date picker, subplot figure and dropdown components inputs, update pie figure graph."""

    if option_slctd == "AAPL":
        df_sentiments_sum = datafetch.df_tweets_aapl_sentiments_minute
    if option_slctd == "GOOG":
        df_sentiments_sum = datafetch.df_tweets_goog_sentiments_minute
    if option_slctd == "AMZN":
        df_sentiments_sum = datafetch.df_tweets_amzn_sentiments_minute

    button_id = get_button()

    if button_id == "slct_ticker":
        df_sentiments_sum = df_sentiments_sum[start_date:end_date]
    elif button_id == "date-picker-range":
        df_sentiments_sum = df_sentiments_sum[start_date:end_date]
    else:
        if relayoutData:
            if "xaxis.range[0]" in relayoutData:
                start_date = relayoutData["xaxis.range[0]"].split(".")[0]
                end_date = relayoutData["xaxis.range[1]"].split(".")[0]
                df_sentiments_sum = df_sentiments_sum[start_date:end_date]
            else:
                df_sentiments_sum = df_sentiments_sum[start_date:end_date]
        else:
            df_sentiments_sum = df_sentiments_sum[start_date:end_date]

    count_names = {
        "positive_count": "positive sentiment",
        "negative_count": "negative sentiment",
        "neutral_count": "neutral sentiment",
    }

    df_sentiments_sum = df_sentiments_sum.rename(columns=count_names).sum()

    fig = go.Figure(
        data=[
            go.Pie(
                labels=df_sentiments_sum.keys(),
                values=df_sentiments_sum.values,
                hole=0.5,
            )
        ]
    )
    fig.update_layout(
        title_text="Sentiments percentage per selected time range",
        annotations=[
            dict(
                text=f"{sum(df_sentiments_sum.values)/1000} K",
                font_size=20,
                showarrow=False,
            )
        ],
    )

    fig.update_traces(marker=dict(colors=["#6ef77b", "#fa5f5f", "#9be9db"]))
    return fig


@app.callback(
    Output("graph_a", "figure"),
    [
        Input("date-picker-range", "start_date"),
        Input("date-picker-range", "end_date"),
        Input("slct_ticker", "value"),
    ],
)
def update_graph_a(start_date, end_date, option_slctd) -> pd.DataFrame:
    """Depending on date picker, dropdown components inputs, update subplots figure."""
    
    #if datafetch.check_update():
    #    datafetch.fetch()

    df_tweets_aapl_sentiments_minute = datafetch.df_tweets_aapl_sentiments_minute[
        start_date:end_date
    ]
    df_ticker_aapl_minute = datafetch.df_ticker_aapl_minute[start_date:end_date]

    df_tweets_goog_sentiments_minute = datafetch.df_tweets_goog_sentiments_minute[
        start_date:end_date
    ]
    df_ticker_goog_minute = datafetch.df_ticker_goog_minute[start_date:end_date]

    df_tweets_amzn_sentiments_minute = datafetch.df_tweets_amzn_sentiments_minute[
        start_date:end_date
    ]
    df_ticker_amzn_minute = datafetch.df_ticker_amzn_minute[start_date:end_date]

    if option_slctd == "AAPL":
        df_tweets_sentiments_minute = df_tweets_aapl_sentiments_minute
        df_ticker_minute = df_ticker_aapl_minute

    if option_slctd == "GOOG":
        df_tweets_sentiments_minute = df_tweets_goog_sentiments_minute
        df_ticker_minute = df_ticker_goog_minute

    if option_slctd == "AMZN":
        df_tweets_sentiments_minute = df_tweets_amzn_sentiments_minute
        df_ticker_minute = df_ticker_amzn_minute

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, y_title="Sentiments Count", x_title="Date"
    )

    fig.add_trace(
        go.Bar(
            x=df_tweets_sentiments_minute.index,
            y=df_tweets_sentiments_minute["positive_count"],
            name="positive sentiment",
            marker_color="#6ef77b",
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df_tweets_sentiments_minute.index,
            y=df_tweets_sentiments_minute["negative_count"],
            name="negative sentiment",
            marker_color="#fa5f5f",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=df_tweets_sentiments_minute.index,
            y=df_tweets_sentiments_minute["neutral_count"],
            name="neutral sentiment",
            marker_color="#9be9db",
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df_ticker_minute.index,
            y=df_ticker_minute["price"],
            name="ticker average price per minute",
            marker_color="#ff9d02",
        ),
        row=4,
        col=1,
    )

    fig.update_layout(template="plotly_white", title_text="Sentiments count per minute",height=500)
    fig.update_yaxes(title_text="AVG Price", row=4, col=1)
    fig.update_traces(dict(marker_line_width=0))

    return fig

if __name__ == "__main__":
    application.run(debug=True, port=8000)