import duckdb
import pyarrow

from abc import abstractmethod
from abc import ABC
from typing import List


class JobInterface(ABC):
    @abstractmethod
    def run():
        pass


class Job(JobInterface):
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pyarrow.Schema,
        schema_name: str,
        table_name: str,
        data: List[dict],
        queries: List[str],
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.schema_name = schema_name
        self.table_name = table_name
        self.data = data
        self.queries = queries
        self.conn = self.__init_connection(duckdb_schema)

    def __init_connection(self, duckdb_schema: str):
        conn = duckdb.connect(database=':memory:', read_only=False)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")
        conn.execute(f"USE {self.schema_name}")
        conn.execute(duckdb_schema)
        return conn

    def __populate_table(self):
        tmp_table_name = "temp"
        pt = pyarrow.Table.from_pylist(self.data, schema=self.pyarrow_schema)
        self.conn.register(tmp_table_name, pt)
        self.conn.execute(f"INSERT INTO {self.table_name} FROM {tmp_table_name};")
        self.conn.unregister(tmp_table_name)

    def run(self):
        self.__populate_table()
        for q in self.queries:
            query = q.get("sql")
            response = self.conn.execute(
                query.format(table_name=self.table_name)
            ).fetchall()

            if not response:
                print('No results')
                return 

            print(f"\n 📈{q['desc']}:")
            for item in response:
                columns = list(q['columns'].values())
                for i, value in enumerate(item):
                    msg = f"{columns[i]['name']}: {value}"
                    print(msg)
                print("---")
                

