import os
import validators

from crate import client
from typing import Callable

FOCUS_PRIMARY_KEYS = ['ResourceId', 'BillingPeriodStart', 'BillingPeriodEnd', 'ChargePeriodStart', 'ChargePeriodEnd']

class db:
    def __init__(self, app):
        self.app = app
        self.connection = None

    def get_db_connection_info(self) -> tuple[str, str, str, str]:
        if self.connection != None:
            return (True, self.host, str(self.port), self.username, self.password)
        else:
            return (False, '', '', '', '')

    def get_db_connection(self, username : str, password : str):
        if self.connection == None:
            self.host = os.getenv('CRATE_HOST')
            self.port = int(os.getenv('CRATE_PORT'))

            self.app.logger.info(f"Connecting to CrateDB on http://{self.host}:{self.port}")

            if self.host == '' or not validators.url(f"http://{self.host}:{self.port}"):
                self.app.logger.error('No host for CrateDB, invalid url')
                return

            self.username = username
            self.password = password
            try:
                self.connection = client.connect(f"http://{self.host}:{self.port}", username=self.username, password=self.password, backoff_factor=6)
                return # stops the decoding attempt of username/password if the connection is successful
            except Exception as e:
                self.app.logger.warning(f"Failed to connect to CrateDB: {str(e)}")
            
            self.app.logger.info('Trying base64 decode of credentials...')
            import base64
            try:    
                self.username = base64.b64decode(username, validate=True).decode('utf-8')
            except Exception as e:
                self.app.logger.info('username is not base64, continuing...')
                self.username = username

            try:    
                self.password = base64.b64decode(password, validate=True).decode('utf-8')
            except Exception as e:
                self.app.logger.info('password is not base64, continuing...')
                self.username = username
            
            try:
                self.connection = client.connect(f"http://{self.host}:{self.port}", username=self.username, password=self.password, backoff_factor=6)
            except Exception as e:
                self.app.logger.error(f"Failed to connect to CrateDB: {str(e)}")
                raise

    def bulk_insert(self, table_name : str, data : list, username : str, password : str) -> tuple[int, str]:
        """Perform bulk insert into specified CrateDB table"""
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()

        # Assuming data is a list of dictionaries
        if not data:
            return 0
        
        # Get columns from the first record
        columns = list(data[0]['labels'].keys())
        column_names = ','.join(columns)
        marks_str = ','.join(['?' for _ in columns])
        values = []
        for row in data:
            record = row["labels"]
            values.append([record[column] for column in columns])
            # Prepare the INSERT statement
        try:
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({marks_str}) ON CONFLICT DO NOTHING"
            #str(marks)[1:-1]
            # Execute insert
            self.app.logger.debug('\n\n' + query + '\n\n')
            result = cursor.executemany(query, values)
            rows_inserted = 0
            error_executemany = ''
            for dictionary in result:
                if 'rowcount' in dictionary.keys() and dictionary['rowcount'] != -2:
                    rows_inserted += dictionary['rowcount']
                elif 'error_message' in dictionary.keys():
                    self.app.logger.error(dictionary['error_message'])
                    if error_executemany != '':
                        error_executemany = dictionary['error_message']
        except Exception as e:
            self.app.logger.error(f"Bulk insert failed: {str(e)}")
            cursor.close()
            raise
        finally:
            cursor.close()
            return rows_inserted, error_executemany

    def insert_notebook(self, table_name : str, notebook_name : str, notebook : str, overwrite : bool, username : str, password : str) -> bool:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()
        if not notebook:
            return False
        self.app.logger.info('Overwrite of ' + notebook_name + ' set to ' + str(overwrite))
        if overwrite:
            cursor.execute(f"INSERT INTO {table_name} (NOTEBOOK_NAME,DATA) VALUES (?,?) ON CONFLICT (NOTEBOOK_NAME) DO UPDATE SET DATA = excluded.DATA;", [notebook_name, notebook])
        else:
            cursor.execute(f"INSERT INTO {table_name} (NOTEBOOK_NAME,DATA) VALUES (?,?) ON CONFLICT DO NOTHING", [notebook_name, notebook])
        cursor.close()

        return True
    
    def delete_notebook(self, table_name : str, notebook_name : str, username : str, password : str) -> bool:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()

        cursor.execute(f"DELETE FROM {table_name} WHERE NOTEBOOK_NAME = '{notebook_name}'")

        cursor.close()
        return True
    
    def get_notebook(self, table_name : str, notebook : str, username : str, password : str) -> str:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()

        cursor.execute(f"SELECT DATA FROM {table_name} WHERE NOTEBOOK_NAME = '{notebook}' LIMIT 1")
        records = cursor.fetchall()
        self.app.logger.debug(records[0][0])
        return records[0][0]

    def list_notebooks(self, table_name : str, username : str, password : str) -> str:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()

        cursor.execute(f"SELECT NOTEBOOK_NAME FROM {table_name}")
        return cursor.fetchall()

    def does_table_exist(self, table_name : str, username : str, password : str) -> bool:
        self.get_db_connection(username, password)

        cursor = self.connection.cursor()

        try: 
            cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{table_name}'")
            records = cursor.fetchall()
            return len(records) == 1
        except Exception as e:
            self.app.logger.error(f"Could not check if table exists: {str(e)}")
        finally:
            cursor.close()

    def create_table(self, table_name : str, username : str, password : str, column_list_fun : Callable[[], str]):
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()
      
        try: 
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_list_fun()}) WITH (column_policy = 'dynamic')")
        except Exception as e:
            self.app.logger.error(f"Could not create table: {str(e)}")
        finally:
            cursor.close()


    
    def close_connection(self):
        self.connection.close()
