import pandas as pd
from sqlalchemy import create_engine


class MySQL:
    def __init__(self,db_connection:str):
        self.dbt_connection = db_connection
        self.engine = create_engine(self.dbt_connection)
        self.connection = self.engine.raw_connection()
   
    def close_connection(self):
        self.engine.dispose()
        print('Conexão fechada')

    def run_sql(self, sql: str, schema: str = None):
        sql = f"USE {schema}; {sql}"
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)
            print('SQL executado')
            cursor.close()
        except Exception as e:
            print(f'Erro: {e}')

    def df_to_mysql(self, table_name: str, df: pd.DataFrame,schema:str):
        try:
            new_conn = self.dbt_connection+schema
            df.to_sql(table_name, con=new_conn, if_exists='append', index=False)
        except Exception as e:
            print(e)

    def create_schema(self, schema_name: str):
        cursor = self.connection.cursor()
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
        cursor.close()
