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
    args = {'table_name': '', 'composition_id': '', 'name': '', 'namespace': '', 'opt': ''}
    for i in range(5, len(sys.argv)):
        key_value = sys.argv[i]
        key_value_split = str.split(key_value, '=')
        if key_value_split[0] in args.keys():
            args[key_value_split[0]] = key_value_split[1] if key_value_split[1] else args[key_value_split[0]]

    for key in args:
        if args[key] == '':
            print('missing agument for call: ' + key)

    try:
        # Use only the required columns: billedcost and resourcetype
        resource_query = (
            f"SELECT sum(billedcost) as billedcost "
            f"FROM {args['table_name']} "
            f"WHERE tags['compositionId'] = '{args['composition_id']}' "
        )
        cursor.execute(resource_query)
        # Create dataframe from query results
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        
        # Calculate the total billed cost (for title and series.total)
        total_billed_cost = str(round(df['billedcost'].sum() if not df.empty else 0, 2))

        # Build final JSON output
        json_output = {
            "composition_id": args['composition_id'],
            "billed_cost": total_billed_cost,
            "name": args['name'],
            "namespace": args['namespace'],
            "opt": args['opt'],
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