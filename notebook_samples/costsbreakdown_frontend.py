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
import traceback

def install(package):
    import pip._internal as pip
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
            print('missing argument for call: ' + key)
    
    try:
        resource_query = (
            f"SELECT sum(billedcost) as billedcost, billingcurrency, resourcetype, chargeperiodstart FROM {args['table_name']} "
            f"WHERE (resourcename LIKE '{args['resource_name']}\_%' OR resourcename LIKE '{args['resource_name']}' ) "
            f"AND resourceid LIKE '%resourcegroups/{args['resource_group_name']}%' "
            f"GROUP BY resourcetype, billingcurrency, chargeperiodstart"
        )
        cursor.execute(resource_query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        
        resourcetypes_query = (
            f"SELECT distinct resourcetype FROM {args['table_name']} "
            f"WHERE resourceid LIKE '%{args['resource_name']}%' "
            f"AND resourceid LIKE '%{args['resource_group_name']}%' "
        )
        cursor.execute(resourcetypes_query)
        df_types = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        
        # Convert timestamp to datetime
        df["chargeperiodstart"] = pd.to_datetime(df["chargeperiodstart"], unit='ms')
        
        # Get all unique timestamps across all resource types and sort them
        all_timestamps = sorted(df["chargeperiodstart"].unique())
        
        # Define colors for different resource types
        colors = ["blue", "orange", "green", "red", "purple", "brown", "pink", "gray", "yellow"]
        
        lines = []
        for i, resourcetype in enumerate(df_types["resourcetype"]):
            # Filter data for this resource type
            resource_data = df[df["resourcetype"] == resourcetype]
            
            # Create a dictionary of existing values for this resource type
            value_dict = {ts: 0 for ts in all_timestamps}
            for _, row in resource_data.iterrows():
                value_dict[row.chargeperiodstart] = row.billedcost
            
            # Create data points with cumulative values
            cumulative_value = 0
            data_points = []
            
            for timestamp in all_timestamps:
                cumulative_value += value_dict[timestamp]
                data_points.append({
                    "xValue": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "yValue": cumulative_value
                })
            
            # Create line object with proper JSON formatting
            line = {
                "color": colors[i % len(colors)],
                "legendName": resourcetype,
                "data": json.dumps(data_points)
            }
            lines.append(line)
        
        # Create the final formatted output
        json_output = {
            "lines": lines,
            "xAxisName": "Time",
            "yAxisName": "Cost"
        }
        
        print(json.dumps(json_output, indent=4))
    
    except Exception:
        print(traceback.format_exc())
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