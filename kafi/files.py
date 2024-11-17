import io
import pathlib

from kafi.pandas import Pandas

# Constants

ALL_MESSAGES = -1

#

#x = [{"name": "cookie", "calories": 500.0, "colour": "brown"}, {"name": "cake", "calories": 260.0, "colour": "white"}, {"name": "timtam", "calories": 80.0, "colour": "chocolate"}]

class Files(Pandas):
    def topic_to_file(self, topic, fs_obj, file, n=ALL_MESSAGES, **kwargs):
        if not fs_obj.__class__.__bases__[0].__name__== "FS":
            raise Exception("The target must be a file system.")
        #
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
            index_bool = kwargs["index"] if "index" in kwargs else False
            df.to_csv(data_bytesIO, index=index_bool)
        elif suffix_str == ".feather":
            df.to_feather(data_bytesIO)
        elif suffix_str == ".json":
            index_bool = kwargs["index"] if "index" in kwargs else None
            df.to_json(data_bytesIO, orient="records")
        elif suffix_str == ".orc":
            index_bool = kwargs["index"] if "index" in kwargs else None
            df.to_orc(data_bytesIO, index=index_bool)
        elif suffix_str == ".parquet":
            index_bool = kwargs["index"] if "index" in kwargs else None
            df.to_parquet(data_bytesIO, index=index_bool)
        elif suffix_str == ".xlsx":
            index_bool = kwargs["index"] if "index" in kwargs else False
            df.to_excel(data_bytesIO, index=index_bool)
        elif suffix_str == ".xml":
            index_bool = kwargs["index"] if "index" in kwargs else False
            df.to_xml(data_bytesIO, index=index_bool)
        #
        data_bytes = data_bytesIO.getvalue()
        file_abs_path_str = fs_obj.admin.get_file_abs_path_str(file_str)
        fs_obj.admin.write_bytes(file_abs_path_str, data_bytes)
        #
        return len(df)

    def file_to_topic(self, file, storage_obj, topic, n=ALL_MESSAGES, **kwargs):
        if not self.__class__.__bases__[0].__name__== "FS":
            raise Exception("The source must be a file system.")
        #
        import pandas as pd
        #
        file_str = file
        #
        suffix_str = pathlib.Path(file_str).suffix
        if suffix_str not in [".csv", ".feather", ".json", ".orc", ".parquet", ".xlsx", ".xml"]:
            raise Exception("Only \".csv\", \".feather\", \".json\", \".orc\", \".parquet\", \".xlsx\" and \".xml\" supported.")
        #
        file_abs_path_str = self.admin.get_file_abs_path_str(file_str)
        data_bytes = self.admin.read_bytes(file_abs_path_str)
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
        return storage_obj.from_df(df, topic, n, **kwargs)
