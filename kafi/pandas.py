from kafi.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Pandas(Functional):
    def to_df(self, topic, n=ALL_MESSAGES, **kwargs):
        import pandas as pd
        #

        def foldl_function(acc, message_dict):
            df = pd.DataFrame.from_records([message_dict["value"]])
            #
            acc = pd.concat([acc, df], ignore_index=True)
            #
            return acc
        #

        (df,  _) = self.foldl(topic, foldl_function, pd.DataFrame(), n, **kwargs)
        #
        return df

    def from_df(self, df, topic, n=ALL_MESSAGES, **kwargs):
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
