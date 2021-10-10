from datetime import datetime, timedelta
import psycopg2
import dash_secrets as ds
import pandas as pd
import numpy as np
import dash_queries as dq
import logging

class DataFetch:
    """
    A class for fetching twitter and stocks data from redshift analytic schema.
    """

    def __init__(self) -> None:
        self.wh_conn = None
        self.wr_update_time = datetime.now().replace(
            hour=5, minute=0, second=0, microsecond=0
        )

    def connect(self) -> None:
        """
        Connects to redshift database.
        Parameters
        ----------
            None
        Returns
        ----------
            None
        """

        if self.wh_conn:
            self.disconnect()
        
        secrets = ds.get_secrets()

        db_name = secrets["DB_NAME"]
        host = secrets["HOST"]
        port_name = secrets["PORT_NAME"]
        user = secrets["USER"]
        password = secrets["PASSWORD"]

        self.wh_conn = psycopg2.connect(
            dbname=db_name, host=host, port=port_name, user=user, password=password
        )

    def disconnect(self) -> None:
        """
        Disconnects from redhsift database.
        Parameters
        ----------
            None
        Returns
        ----------
            None
        """
        self.wh_conn.close()

    def check_update(self) -> bool:
        """
        Checks if data update is needed.If yes returns True and updates next update time variable.
        Parameters
        ----------
            None
        Returns
        ----------
            bool:
                True if there is new data update, No if there is not.
        """
        now_datetime = datetime.now()
        diff = now_datetime - self.wr_update_time
        if diff.total_seconds() > 0:
            self.wr_update_time = self.wr_update_time + timedelta(days=1)
            return True
        else:
            return False

    def ticker_convert_to_datetime(self, df_ticker: pd.DataFrame) -> pd.DataFrame:
        """
        Converts ticker dataframe ticker_minute_freq column to datetime and sets it as index.
        Parameters
        ----------
            None
        Returns
        ----------
            df_ticker: pd.DataFrame
                Converted ticker dataframe.
        """
        df_ticker["ticker_minute_freq"] = pd.to_datetime(
            df_ticker["ticker_minute_freq"]
        )
        df_ticker = df_ticker.set_index("ticker_minute_freq")
        return df_ticker

    def tweets_convert_to_datetime(
        self, df_tweets: pd.DataFrame, column: str
    ) -> pd.DataFrame:
        """
        Converts tweets dataframe given column to datetime and sets it as index.
        Parameters
        ----------
            column:
                column name which contains datetime data.
        Returns
        ----------
            df_tweets: pd.DataFrame
                Converted tweets dataframe.
        """
        df_tweets[column] = pd.to_datetime(df_tweets[column])
        df_tweets = df_tweets.set_index(column,drop=False)
        return df_tweets

    def fetch(self) -> None:
        """
        Connects to redshift database, executes multiple queries and fetch data.
        Parameters
        ----------
            None
        Returns
        ----------
            None
        """
       
        self.connect()
        cur = self.wh_conn.cursor()

        try:
            cur.execute(dq.tweets_aapl_full_query)
            tweets_aapl_full_colnames = [desc[0] for desc in cur.description]
            self.df_tweets_aapl_full = pd.DataFrame(
                cur.fetchall(), columns=tweets_aapl_full_colnames
            )
            self.df_tweets_aapl_full = self.tweets_convert_to_datetime(
                self.df_tweets_aapl_full, "tweet_created_at"
            ).drop(columns=["tweet_id"])
        except psycopg2.DatabaseError as error:
            logging.error(error)     

        try:
            cur.execute(dq.tweets_goog_full_query)
            tweets_goog_full_colnames = [desc[0] for desc in cur.description]
            self.df_tweets_goog_full = pd.DataFrame(
                cur.fetchall(), columns=tweets_goog_full_colnames
            )
            self.df_tweets_goog_full = self.tweets_convert_to_datetime(
                self.df_tweets_goog_full, "tweet_created_at"
            ).drop(columns=["tweet_id"])
        except psycopg2.DatabaseError as error:
            logging.error(error)     

        try:
            cur.execute(dq.tweets_amzn_full_query)
            tweets_amzn_full_colnames = [desc[0] for desc in cur.description]
            self.df_tweets_amzn_full = pd.DataFrame(
                cur.fetchall(), columns=tweets_amzn_full_colnames
            )
            self.df_tweets_amzn_full = self.tweets_convert_to_datetime(
                self.df_tweets_amzn_full, "tweet_created_at"
            ).drop(columns=["tweet_id"])
        except psycopg2.DatabaseError as error:
            logging.error(error)  
            
        try:
            cur.execute(dq.ticker_aapl_minute_query)
            ticker_aapl_minute_colnames = [desc[0] for desc in cur.description]
            self.df_ticker_aapl_minute = pd.DataFrame(
                cur.fetchall(), columns=ticker_aapl_minute_colnames
            )
            self.df_ticker_aapl_minute = self.ticker_convert_to_datetime(
                self.df_ticker_aapl_minute
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            cur.execute(dq.tweets_aapl_sentiments_minute_query)
            tweets_aapl_sentiments_minute_colnames = [
                desc[0] for desc in cur.description
            ]
            self.df_tweets_aapl_sentiments_minute = pd.DataFrame(
                cur.fetchall(), columns=tweets_aapl_sentiments_minute_colnames
            )
            self.df_tweets_aapl_sentiments_minute = self.tweets_convert_to_datetime(
                self.df_tweets_aapl_sentiments_minute, "tweets_minute_freq"
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            cur.execute(dq.ticker_goog_minute_query)
            ticker_goog_minute_colnames = [desc[0] for desc in cur.description]
            self.df_ticker_goog_minute = pd.DataFrame(
                cur.fetchall(), columns=ticker_goog_minute_colnames
            )
            self.df_ticker_goog_minute = self.ticker_convert_to_datetime(
                self.df_ticker_goog_minute
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            cur.execute(dq.tweets_goog_sentiments_minute_query)
            tweets_goog_sentiments_minute_colnames = [
                desc[0] for desc in cur.description
            ]
            self.df_tweets_goog_sentiments_minute = pd.DataFrame(
                cur.fetchall(), columns=tweets_goog_sentiments_minute_colnames
            )
            self.df_tweets_goog_sentiments_minute = self.tweets_convert_to_datetime(
                self.df_tweets_goog_sentiments_minute, "tweets_minute_freq"
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            cur.execute(dq.ticker_amzn_minute_query)
            ticker_amzn_minute_colnames = [desc[0] for desc in cur.description]
            self.df_ticker_amzn_minute = pd.DataFrame(
                cur.fetchall(), columns=ticker_amzn_minute_colnames
            )
            self.df_ticker_amzn_minute = self.ticker_convert_to_datetime(
                self.df_ticker_amzn_minute
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            cur.execute(dq.tweets_amzn_sentiments_minute_query)
            tweets_amzn_sentiments_minute_colnames = [
                desc[0] for desc in cur.description
            ]
            self.df_tweets_amzn_sentiments_minute = pd.DataFrame(
                cur.fetchall(), columns=tweets_amzn_sentiments_minute_colnames
            )
            self.df_tweets_amzn_sentiments_minute = self.tweets_convert_to_datetime(
                self.df_tweets_amzn_sentiments_minute, "tweets_minute_freq"
            )
        except psycopg2.DatabaseError as error:
            logging.error(error)  

        try:
            self.tweets_count = (
                self.df_tweets_aapl_full.count()[0]
                + self.df_tweets_goog_full.count()[0]
                + self.df_tweets_amzn_full.count()[0]
            )
            self.tickers_count = (
                self.df_ticker_aapl_minute.count()[0]
                + self.df_ticker_goog_minute.count()[0]
                + self.df_ticker_amzn_minute.count()[0]
            )
            self.users_count = len(
                np.unique(
                    np.concatenate(
                        [
                            self.df_tweets_aapl_full["user_name"].values,
                            self.df_tweets_goog_full["user_name"].values,
                        ]
                    )
                )
            )
        except NameError:
            logging.error(error) 

        cur.close()
        self.disconnect()
    