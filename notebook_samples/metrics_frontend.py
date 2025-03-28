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

import pip._internal as pip
import json

def install(package):
    pip.main(['install', package])

def main():   
    args = {'table_name': '', 'resource_name': '', 'resource_group_name': ''}
    for i in range(5, len(sys.argv)):
        key_value = sys.argv[i]
        key_value_split = str.split(key_value, '=')
        if key_value_split[0] in args.keys():
            args[key_value_split[0]] = key_value_split[1] if key_value_split[1] else args[key_value_split[0]]

    for key in args:
        if args[key] == '':
            print('missing agument for call: ' + key)

    try:
        resource_query = f"SELECT * FROM {args['table_name']} WHERE resourceid LIKE '%{args['resource_name']}%' AND resourceid LIKE '%{args['resource_group_name']}%' AND timestamp > CURRENT_TIMESTAMP - INTERVAL '1 week'"
        cursor.execute(resource_query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms').dt.strftime("%Y-%m-%d %H:%M:%S")
        df = df.sort_values(by="timestamp")
        json_output = {
            "xAxisName": "Time",
            "yAxisName": df["metricname"][0] if len(df["metricname"]) > 0 else "",
            "legendName": df["metricname"][0] if len(df["metricname"]) > 0 else "",
            "color": "blue",
            "data": [{"xValue": row["timestamp"], "yValue": row["average"]} for _, row in df.iterrows()]
        }

        print(json.dumps(json_output, indent=4))
        
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    try:
        import pandas as pd
    except ImportError:
        install('pandas')
        import pandas as pd
    main()