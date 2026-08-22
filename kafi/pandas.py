import pandas as pd

from kafi.functional import Functional
from kafi.helpers import explode_normalize

# Constants

ALL_MESSAGES = -1

#

class Pandas(Functional):
    def topic_to_df(self, topic, n=ALL_MESSAGES, **kwargs):
        """Consume up to n messages of a topic into a pandas DataFrame (one row per message value).

        Args:
            topic: topic name
            n: max messages to consume; ALL_MESSAGES for no limit
            **kwargs: passed to foldl(); "explode" flattens list/nested-dict columns

        Returns:
            pandas DataFrame: one row per consumed message value"""
        #
        def foldl_fun(acc, m):
            # df = pd.DataFrame.from_records([m["value"]])
            df = pd.json_normalize(m["value"])
            if "explode" in kwargs and kwargs["explode"] == True:
                df = explode_normalize(df)
            #
            acc = pd.concat([acc, df], ignore_index=True)
            #
            return acc
        #

        (df,  _) = self.foldl(topic, foldl_fun, pd.DataFrame(), n, **kwargs)
        #
        return df

    def df_to_topic(self, df, topic, n=ALL_MESSAGES, **kwargs):
        """Produce each row of a DataFrame as a message to a topic.

        Args:
            df: DataFrame to produce
            topic: target topic name
            n: max rows to produce; ALL_MESSAGES for no limit
            **kwargs: passed to producer()

        Returns:
            int: number of rows produced"""
        n_int = n
        #

        producer = self.producer(topic, **kwargs)
        counter_int = 0
        for _, row in df.iterrows():
            if n_int != ALL_MESSAGES:
                if counter_int >= n_int:
                    break
            #
            producer.produce(row.to_dict())
            #
            counter_int += 1
        producer.close()
        #
        return counter_int
