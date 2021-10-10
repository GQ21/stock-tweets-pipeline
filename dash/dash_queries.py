ANALYTIC_SCHEMA = 'stwitter_analytic' 

tweets_aapl_full_query = f"""
                        SELECT *
                        FROM {ANALYTIC_SCHEMA}.tweets_aapl
                        ORDER BY tweet_created_at;
                        """

tweets_goog_full_query = f"""
                        SELECT *
                        FROM {ANALYTIC_SCHEMA}.tweets_goog
                        ORDER BY tweet_created_at;
                        """

tweets_amzn_full_query = f"""
                        SELECT *
                        FROM {ANALYTIC_SCHEMA}.tweets_amzn
                        ORDER BY tweet_created_at;
                        """

ticker_aapl_minute_query = f"""
                            SELECT *
                            FROM {ANALYTIC_SCHEMA}.tickers_aapl_minute_avg
                            ORDER BY ticker_minute_freq;
                            """

ticker_goog_minute_query = f"""
                            SELECT *
                            FROM {ANALYTIC_SCHEMA}.tickers_goog_minute_avg
                            ORDER BY ticker_minute_freq
                            """

ticker_amzn_minute_query = f"""
                            SELECT *
                            FROM {ANALYTIC_SCHEMA}.tickers_amzn_minute_avg
                            ORDER BY ticker_minute_freq
                            """

tweets_aapl_sentiments_minute_query = f"""
                                       SELECT *
                                       FROM {ANALYTIC_SCHEMA}.tweets_aapl_sentiments_minute_sum
                                       ORDER BY tweets_minute_freq
                                       """

tweets_goog_sentiments_minute_query = f"""
                                        SELECT *
                                        FROM {ANALYTIC_SCHEMA}.tweets_goog_sentiments_minute_sum
                                        ORDER BY tweets_minute_freq
                                        """


tweets_amzn_sentiments_minute_query = f"""
                                        SELECT *
                                        FROM {ANALYTIC_SCHEMA}.tweets_amzn_sentiments_minute_sum
                                        ORDER BY tweets_minute_freq
                                        """    