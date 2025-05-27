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

import json
from datetime import datetime

def main(table_name: str, resource_id: str):
    try:
        metricnames_query = f"SELECT DISTINCT metricname FROM {table_name} WHERE resourceid like '%{resource_id}%'"
        cursor.execute(metricnames_query)
        metrics = cursor.fetchall()

        result = []

        for metric in metrics:
            resource_query = f"SELECT resourceid, timestamp, average, unit FROM {table_name} WHERE resourceid like '%{resource_id}%' AND metricname='{metric[0]}' ORDER BY timestamp ASC"    
            cursor.execute(resource_query)
            raw_data = cursor.fetchall()

            # Indexes come from select position
            partial = {
                "resourceid": raw_data[0][0],
                "metricname": metric[0],
                "unit": raw_data[0][3],
                "timeseries": []
            }

            for row in raw_data:
                if len(partial['timeseries']) == 0 or partial['timeseries'][-1]['timestamp'] != datetime.fromtimestamp(row[1]/1000).strftime('%Y-%m-%dT%H:%M:%SZ'):
                    # Use seconds precision
                    partial['timeseries'].append({"average": row[2], "timestamp": datetime.fromtimestamp(row[1]/1000).strftime('%Y-%m-%dT%H:%M:%SZ')})


            result.append(partial)

        print(json.dumps(result))
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    args = {'table_name': '', 'resource_name': ''}
    for i in range(5, len(sys.argv)):
        key_value = sys.argv[i]
        key_value_split = str.split(key_value, '=')
        if key_value_split[0] in args.keys():
            args[key_value_split[0]] = key_value_split[1] if key_value_split[1] else args[key_value_split[0]]

    missing_param = False
    for key in args:
        if args[key] == '':
            print("{\"error\": \"missing agument for call: "+ key + "\"}")
            missing_param = True
    if not missing_param:
        main(args['table_name'], args['resource_name'])