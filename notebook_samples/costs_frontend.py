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
        # Use only the required columns: billedcost and resourcetype
        resource_query = (
            f"SELECT sum(billedcost) as billedcost, billingcurrency, resourcetype FROM {args['table_name']} "
            f"WHERE resourceid LIKE '%{args['resource_name']}%' "
            f"AND resourceid LIKE '%{args['resource_group_name']}%' "
            f"GROUP BY resourcetype, billingcurrency"
        )
        cursor.execute(resource_query)
        # Create dataframe from query results
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        
        # Calculate the total billed cost (for title and series.total)
        total_billed_cost = round(df['billedcost'].sum() if not df.empty else 0, 3)

        # Define the rotation of colors
        colors = ["blue", "darkBlue", "orange", "gray", "red", "green"]

        # Build the data entries: each row maps to a data entry.
        # The color rotates through the colors list.
        series_data = [
            {
                "color": colors[i % len(colors)],
                "value": round(row["billedcost"], 3),
                "label": row["resourcetype"]
            }
            for i, (_, row) in enumerate(df.iterrows())
        ]

        # Build final JSON output
        json_output = {
            "title": str(total_billed_cost) + " " + df["billingcurrency"][0] if len(df["billingcurrency"]) > 0 else "",
            "description": args["resource_name"],
            "series": [
                {
                    "total": total_billed_cost,
                    "data": series_data
                }
            ]
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