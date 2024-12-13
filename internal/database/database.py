from crate import client
from typing import Callable

FOCUS_PRIMARY_KEYS = ['ResourceId', 'BillingPeriodStart', 'BillingPeriodEnd', 'ChargePeriodStart', 'ChargePeriodEnd']

class db:
    def __init__(self, app, host : str, port : int):
        self.app = app
        self.host = host
        self.port = port
        self.connection = None

    def get_db_connection_info(self) -> tuple[str, str, str, str]:
        if self.connection != None:
            return (True, self.host, str(self.port), self.username, self.password)
        else:
            return (False, '', '', '', '')

    def get_db_connection(self, username : str, password : str):
        """Create and return a connection to CrateDB"""
        if self.connection == None:
            self.username = username
            self.password = password
            try:
                self.connection = client.connect(f"http://{self.host}:{self.port}", username=self.username, password=self.password)
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

    def insert_notebook(self, table_name : str, notebook_name : str, notebook : str, username : str, password : str) -> bool:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()
        if not notebook:
            return False
        cursor.execute(f"INSERT INTO {table_name} (NOTEBOOK_NAME,DATA) VALUES (?,?) ON CONFLICT DO NOTHING", [notebook_name, notebook])
        cursor.close()

        return True
    
    def get_notebook(self, table_name : str, notebook : str, username : str, password : str) -> str:
        self.get_db_connection(username, password)
        cursor = self.connection.cursor()

        cursor.execute(f"SELECT DATA FROM {table_name} WHERE NOTEBOOK_NAME = '{notebook}' LIMIT 1")
        records = cursor.fetchall()
        self.app.logger.info(records[0][0])
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
            cursor.execute(f"CREATE TABLE {table_name} ({column_list_fun()}) WITH (column_policy = 'dynamic')")
        except Exception as e:
            self.app.logger.error(f"Could not create table: {str(e)}")
        finally:
            cursor.close()


    
    def close_connection(self):
        self.connection.close()
