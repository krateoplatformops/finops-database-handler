
def cratedb_client_injection() -> str:
    return '''
import sys
from crate import client

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

host = sys.argv[1]
port = sys.argv[2]
username = sys.argv[3]
password = sys.argv[4]

try:
    connection = client.connect(f"http://{host}:{port}", username=username, password=password)
    cursor = connection.cursor()
except Exception as e:
    eprint('error while connecting to database' + str(e))
    raise
'''