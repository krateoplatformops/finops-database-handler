# The notebook is injected with additional lines of code:
# import sys
# from crate import client
# def eprint(*args, **kwargs):
#     print(*args, file=sys.stderr, **kwargs)
# host = sys.argv[1]
# port = sys.argv[2]
# username = sys.argv[3]
# password = sys.argv[4]
# try:
#     connection = client.connect(f"http://{host}:{port}", username=username, password=password)
#     cursor = connection.cursor()
# except Exception as e:
#     eprint('error while connecting to database' + str(e))
#     raise

def main():   
    table_name_arg = sys.argv[5]
    table_name_key_value = str.split(table_name_arg, '=')
    if len(table_name_key_value) == 2:
        if table_name_key_value[0] == 'table_name':
            table_name = table_name_key_value[1]
    try:
        resource_query = f"SELECT * FROM {table_name}"
        cursor.execute(resource_query)
        raw_data = cursor.fetchall()
        print(pd.DataFrame(raw_data))
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()