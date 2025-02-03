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

def main(pricing_table : str, annotation_table : str, composition_id : str):
    try:
        cursor.execute(f"SELECT keys FROM {annotation_table} WHERE composition_id = '{composition_id}'")
        records = cursor.fetchall()
        result = {}
        for record in records:
            for row in record:
                for key in row.keys():
                    cursor.execute(f"SELECT listunitprice, pricingunit FROM {pricing_table} WHERE tags['krateo-finops-focus-resource'] = '{key}'")
                    inner_records = cursor.fetchall()
                    for inner_record in inner_records:
                        # 0: listunitprice, 1: pricingunit
                        if inner_record[1] in result.keys():
                            result[inner_record[1]] += float(inner_record[0])
                        else:
                            result[inner_record[1]] = float(inner_record[0])
        print(json.dumps(result))
                        
    except Exception as e:
        print("{\"error\": \"could not obtain pricing: "+ str(e) + "\"}")
    finally:
        cursor.close()

if __name__ == "__main__":
    args = {'composition_id': '', 'pricing_table': 'pricing_table', 'annotation_table': 'composition_definition_annotations'}
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
        main(args['pricing_table'], args['annotation_table'], args['composition_id'])