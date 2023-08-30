import io
import json
import pandas as pd

dict_list1 = [{"name": "cookie", "calories": 500.0, "colour": "brown"}, {"name": "cake", "calories": 260.0, "colour": "white"}, {"name": "timtam", "calories": 80.0, "colour": "chocolate"}]

df1 = pd.DataFrame.from_records(dict_list1)
parquet_bytes = df1.to_parquet()

#

df2 = pd.read_parquet(io.BytesIO(parquet_bytes))
df2["json"] = df2.apply(lambda x: x.to_json(), axis=1)
dict_list2 = []
for _, row in df2.iterrows():
    dict_list2.append(json.loads(row["json"]))

print(dict_list1 == dict_list2)
