import subprocess
import tempfile
import internal.database.database as cratedb
import internal.compute.helpers as helpers
from internal.database.helpers import get_notebook_create

NOTEBOOK_TABLE = 'NOTEBOOK_TABLE'

def list(db : cratedb.db, username : str, password : str) -> list:
    return db.list_notebooks(NOTEBOOK_TABLE, username, password)
    

def run(notebook : str, db : cratedb.db, username : str, password : str, parameters : list, engine : str) -> str:
    db.get_db_connection(username, password)
    data = db.get_db_connection_info()
    if not data[0]:
        return 'no connection info for database'
    
    original_script = db.get_notebook(NOTEBOOK_TABLE, notebook, username, password).replace('\\\'', '\'')

    match engine:
        case 'cratedb':
            injection_code = helpers.cratedb_client_injection()

    new_script = f"{injection_code}\n{original_script}"
    # Create a temporary file for the combined script
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp_file:
        tmp_file.write(new_script)
        tmp_file.flush()
        tmp_path = tmp_file.name

    cmd = ['python3', tmp_path, data[1], data[2], data[3], data[4]]
    for key in parameters:
        cmd.append(key+'='+parameters[key])

    result = subprocess.run(cmd, capture_output=True, text = True)
    if result.returncode != 0:
        return result.stderr.strip('\n')
    return result.stdout.strip('\n')


def upload(db : cratedb.db, notebook_name : str, notebook : str, overwrite: bool, username : str, password : str) -> bool:
    if not db.does_table_exist(NOTEBOOK_TABLE, username, password):
        db.create_table(NOTEBOOK_TABLE, username, password, get_notebook_create)
    
    return db.insert_notebook(NOTEBOOK_TABLE, notebook_name, notebook, overwrite, username, password)

def delete(db : cratedb.db, notebook_name : str, username : str, password : str) -> bool:
    return db.delete_notebook(NOTEBOOK_TABLE, notebook_name, username, password)

