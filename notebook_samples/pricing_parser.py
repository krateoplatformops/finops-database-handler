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

def main(operation : str, composition_id : str, json_list : str, table_name : str):
    try: 
        cursor.execute(f"CREATE TABLE IF NOT EXISTs {table_name} (composition_id string, keys object, PRIMARY KEY (composition_id)) WITH (column_policy = 'dynamic')")
    except Exception as e:
        print(f"Could not create table: {str(e)}")
    try:
        if operation == 'create':
            cursor.execute(f"INSERT INTO {table_name} (composition_id, keys) VALUES (?,?) ON CONFLICT (composition_id) DO UPDATE SET keys = excluded.keys;", [composition_id, json_list])
        else:
            cursor.execute(f"DELETE FROM {table_name} WHERE composition_id = '{composition_id}'")
    except Exception as e:
        print(f"Could not complete {operation} for {composition_id} in table {table_name}: {str(e)}")
    finally:
        cursor.close()

if __name__ == "__main__":
    args = {'operation': 'create', 'composition_id': '', 'json_list': '', 'annotation_table': 'composition_definition_annotations'}
    for i in range(5, len(sys.argv)):
        key_value = sys.argv[i]
        key_value_split = str.split(key_value, '=')
        if key_value_split[0] in args.keys():
            args[key_value_split[0]] = key_value_split[1] if key_value_split[1] else args[key_value_split[0]]

    for key in args:
        if args[key] == '':
            print('missing agument for call: ' + key)

    main(args['operation'], args['composition_id'], args['json_list'], args['annotation_table'])