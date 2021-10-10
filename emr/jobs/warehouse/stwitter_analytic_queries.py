ANALYTIC_SCHEMA = "stwitter_analtest"

ingest_tweets_aapl_sentiments_minute_sum_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_aapl_sentiments_minute_sum
            SELECT staging_sum.tweets_minute_freq,
                   staging_sum.positive_count,
                   staging_sum.negative_count,
                   staging_sum.neutral_count
            FROM (SELECT date_trunc('minute',tweet_created_at)  AS tweets_minute_freq,
                        SUM(CASE WHEN sentiment = 'positive' THEN 1
                            ELSE 0 END) as positive_count,
                        SUM(CASE WHEN sentiment = 'negative' THEN 1
                            ELSE 0 END) AS negative_count,
                        SUM(CASE WHEN sentiment = 'neutral' THEN 1
                            ELSE 0 END) AS neutral_count
                    FROM stwitter_staging.tweets t
                    JOIN stwitter_staging.sentiments s
                        ON s.tweet_id = t.tweet_id
                    WHERE ticker_ids ILIKE '%AAPL%'
                    GROUP BY tweets_minute_freq
                    ORDER BY tweets_minute_freq) staging_sum
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_aapl_sentiments_minute_sum analytic_sum 
                  ON analytic_sum.tweets_minute_freq = staging_sum.tweets_minute_freq
            WHERE analytic_sum.tweets_minute_freq IS NULL
            """

ingest_tickers_aapl_minute_avg_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tickers_aapl_minute_avg
            SELECT staging_avg.ticker_minute_freq,
                staging_avg.price
            FROM (SELECT  date_trunc('minute',tickers.datetime)  AS ticker_minute_freq,
                        AVG(price) AS price
                FROM stwitter_staging.tickers 
                WHERE ticker_id='AAPL'
                GROUP BY ticker_minute_freq
                ORDER BY ticker_minute_freq) staging_avg
            LEFT JOIN  {ANALYTIC_SCHEMA}.tickers_aapl_minute_avg analytic_avg
                 ON analytic_avg.ticker_minute_freq = staging_avg.ticker_minute_freq
            WHERE analytic_avg.ticker_minute_freq IS NULL
            """

ingest_tweets_goog_sentiments_minute_sum_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_goog_sentiments_minute_sum
            SELECT staging_sum.tweets_minute_freq,
                   staging_sum.positive_count,
                   staging_sum.negative_count,
                   staging_sum.neutral_count
            FROM (SELECT date_trunc('minute',tweet_created_at)  AS tweets_minute_freq,
                        SUM(CASE WHEN sentiment = 'positive' THEN 1
                            ELSE 0 END) as positive_count,
                        SUM(CASE WHEN sentiment = 'negative' THEN 1
                            ELSE 0 END) AS negative_count,
                        SUM(CASE WHEN sentiment = 'neutral' THEN 1
                            ELSE 0 END) AS neutral_count
                    FROM stwitter_staging.tweets t
                    JOIN stwitter_staging.sentiments s
                        ON s.tweet_id = t.tweet_id
                    WHERE ticker_ids ILIKE '%GOOG%'
                    GROUP BY tweets_minute_freq
                    ORDER BY tweets_minute_freq) staging_sum
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_goog_sentiments_minute_sum analytic_sum 
                ON analytic_sum.tweets_minute_freq = staging_sum.tweets_minute_freq
            WHERE analytic_sum.tweets_minute_freq IS NULL
            """


ingest_tickers_goog_minute_avg_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tickers_goog_minute_avg
            SELECT staging_avg.ticker_minute_freq,
                staging_avg.price
            FROM (SELECT  date_trunc('minute',tickers.datetime)  AS ticker_minute_freq,
                        AVG(price) AS price
                FROM stwitter_staging.tickers 
                WHERE ticker_id='GOOG'
                GROUP BY ticker_minute_freq
                ORDER BY ticker_minute_freq) staging_avg
            LEFT JOIN  {ANALYTIC_SCHEMA}.tickers_goog_minute_avg analytic_avg
                 ON analytic_avg.ticker_minute_freq = staging_avg.ticker_minute_freq
            WHERE analytic_avg.ticker_minute_freq IS NULL
            """

ingest_tweets_amzn_sentiments_minute_sum_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_amzn_sentiments_minute_sum
            SELECT staging_sum.tweets_minute_freq,
                   staging_sum.positive_count,
                   staging_sum.negative_count,
                   staging_sum.neutral_count
            FROM (SELECT date_trunc('minute',tweet_created_at)  AS tweets_minute_freq,
                        SUM(CASE WHEN sentiment = 'positive' THEN 1
                            ELSE 0 END) as positive_count,
                        SUM(CASE WHEN sentiment = 'negative' THEN 1
                            ELSE 0 END) AS negative_count,
                        SUM(CASE WHEN sentiment = 'neutral' THEN 1
                            ELSE 0 END) AS neutral_count
                    FROM stwitter_staging.tweets t
                    JOIN stwitter_staging.sentiments s
                        ON s.tweet_id = t.tweet_id
                    WHERE ticker_ids ILIKE '%AMZN%'
                    GROUP BY tweets_minute_freq
                    ORDER BY tweets_minute_freq) staging_sum
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_amzn_sentiments_minute_sum analytic_sum 
                ON analytic_sum.tweets_minute_freq = staging_sum.tweets_minute_freq
            WHERE analytic_sum.tweets_minute_freq IS NULL
            """

ingest_tickers_amzn_minute_avg_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tickers_amzn_minute_avg
            SELECT staging_avg.ticker_minute_freq,
                staging_avg.price
            FROM (SELECT  date_trunc('minute',tickers.datetime)  AS ticker_minute_freq,
                        AVG(price) AS price
                FROM stwitter_staging.tickers 
                WHERE ticker_id='AMZN'
                GROUP BY ticker_minute_freq
                ORDER BY ticker_minute_freq) staging_avg
            LEFT JOIN  {ANALYTIC_SCHEMA}.tickers_amzn_minute_avg analytic_avg
                 ON analytic_avg.ticker_minute_freq = staging_avg.ticker_minute_freq
            WHERE analytic_avg.ticker_minute_freq IS NULL
            """

ingest_tickers_aapl_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_aapl
            SELECT staging_aapl.tweet_created_at, 
                staging_aapl.tweet_id,
                staging_aapl.user_name,
                staging_aapl.tweet_text,
                staging_aapl.sentiment
            FROM (SELECT DISTINCT tweets.tweet_id,tweets.tweet_created_at,users.user_name,tweets.tweet_text,sentiments.sentiment
                FROM stwitter_staging.tweets tweets
                JOIN stwitter_staging.users users
                    ON tweets.user_id = users.user_id
                JOIN stwitter_staging.sentiments sentiments
                    ON tweets.tweet_id = sentiments.tweet_id
                WHERE ticker_ids ILIKE '%AAPL%'
                ORDER BY tweet_created_at) staging_aapl
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_aapl analytic_aapl
                    ON analytic_aapl.tweet_id = staging_aapl.tweet_id
            WHERE analytic_aapl.tweet_id IS NULL
            """

ingest_tickers_goog_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_goog
            SELECT staging_goog.tweet_created_at, 
                staging_goog.tweet_id,
                staging_goog.user_name,
                staging_goog.tweet_text,
                staging_goog.sentiment
            FROM (SELECT DISTINCT tweets.tweet_id,tweets.tweet_created_at,users.user_name,tweets.tweet_text,sentiments.sentiment
                FROM stwitter_staging.tweets tweets
                JOIN stwitter_staging.users users
                    ON tweets.user_id = users.user_id
                JOIN stwitter_staging.sentiments sentiments
                    ON tweets.tweet_id = sentiments.tweet_id
                WHERE ticker_ids ILIKE '%GOOG%'
                ORDER BY tweet_created_at) staging_goog
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_goog staging_analytic
                    ON staging_analytic.tweet_id = staging_goog.tweet_id
            WHERE staging_analytic.tweet_id IS NULL
            """

ingest_tickers_amzn_query = f"""
            INSERT INTO {ANALYTIC_SCHEMA}.tweets_amzn
            SELECT staging_amzn.tweet_created_at, 
                staging_amzn.tweet_id,
                staging_amzn.user_name,
                staging_amzn.tweet_text,
                staging_amzn.sentiment
            FROM (SELECT DISTINCT tweets.tweet_id,tweets.tweet_created_at,users.user_name,tweets.tweet_text,sentiments.sentiment
                FROM stwitter_staging.tweets tweets
                JOIN stwitter_staging.users users
                    ON tweets.user_id = users.user_id
                JOIN stwitter_staging.sentiments sentiments
                    ON tweets.tweet_id = sentiments.tweet_id
                WHERE ticker_ids ILIKE '%AMZN%'
                ORDER BY tweet_created_at) staging_amzn
            LEFT JOIN {ANALYTIC_SCHEMA}.tweets_amzn analytic_amzn
                    ON analytic_amzn.tweet_id = staging_amzn.tweet_id
            WHERE analytic_amzn.tweet_id IS NULL
            """
