import io
import pathlib

from kafi.pandas import Pandas

# Constants

ALL_MESSAGES = -1

#

#x = [{"name": "cookie", "calories": 500.0, "colour": "brown"}, {"name": "cake", "calories": 260.0, "colour": "white"}, {"name": "timtam", "calories": 80.0, "colour": "chocolate"}]

class Files(Pandas):
    def to_file(self, topic, fs_obj, file, n=ALL_MESSAGES, **kwargs):
        file_str = file
        #
        suffix_str = pathlib.Path(file_str).suffix
        if suffix_str not in [".csv", ".feather", ".json", ".orc", ".parquet", ".xlsx", ".xml"]:
            raise Exception("Only \".csv\", \".feather\", \".json\", \".orc\", \".parquet\", \".xlsx\" and \".xml\" supported.")
        #
        df = self.to_df(topic, n, **kwargs)
        data_bytesIO = io.BytesIO()
        #
        if suffix_str == ".csv":
            df.to_csv(data_bytesIO)
        elif suffix_str == ".feather":
            df.to_feather(data_bytesIO)
        elif suffix_str == ".json":
            df.to_json(data_bytesIO, orient="records")
        elif suffix_str == ".orc":
            df.to_orc(data_bytesIO)
        elif suffix_str == ".parquet":
            df.to_parquet(data_bytesIO)
        elif suffix_str == ".xlsx":
            df.to_excel(data_bytesIO)
        elif suffix_str == ".xml":
            df.to_xml(data_bytesIO)
        #
        data_bytes = data_bytesIO.getvalue()
        abs_path_file_str = fs_obj.admin.get_abs_path_str(f"files/{file_str}")
        fs_obj.admin.write_bytes(abs_path_file_str, data_bytes)
        #
        return len(df)

    def from_file(self, fs_obj, file, topic, n=ALL_MESSAGES, **kwargs):
        import pandas as pd
        import numpy as np
        #
        file_str = file
        #
        suffix_str = pathlib.Path(file_str).suffix
        if suffix_str not in [".csv", ".feather", ".json", ".orc", ".parquet", ".xlsx", ".xml"]:
            raise Exception("Only \".csv\", \".feather\", \".json\", \".orc\", \".parquet\", \".xlsx\" and \".xml\" supported.")
        #
        abs_path_file_str = fs_obj.admin.get_abs_path_str(f"files/{file_str}")
        data_bytes = fs_obj.admin.read_bytes(abs_path_file_str)
        data_bytesIO = io.BytesIO(data_bytes)
        #
        if suffix_str == ".csv":
            df = pd.read_csv(data_bytesIO)
        elif suffix_str == ".feather":
            df = pd.read_feather(data_bytesIO)
        elif suffix_str == ".json":
            df = pd.read_json(data_bytesIO)
        elif suffix_str == ".orc":
            df = pd.read_orc(data_bytesIO)
        elif suffix_str == ".parquet":
            df = pd.read_parquet(data_bytesIO)
        elif suffix_str == ".xlsx":
            df = pd.read_excel(data_bytesIO)
        elif suffix_str == ".xml":
            df = pd.read_xml(data_bytesIO)
        #
        return self.from_df(df, topic, n, **kwargs)
