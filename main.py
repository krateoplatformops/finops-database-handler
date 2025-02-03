import logging
import os
import time

import internal.database.database as cratedb
import internal.compute.compute as compute_notebook

import traceback

from flask import Flask, request, jsonify, Response
from waitress import serve
from internal.database.helpers import get_focus_create, get_resource_create, format_tags_for_db

""" Error strings for the webservice """
url_IncorrectError = 'URL composition incorrect'
url_Method_IncorrectError = 'URL composition or methods are incorrect'

""" Service initialization """
app = Flask(__name__)
# CrateDB interface object
db = cratedb.db(app)

@app.before_request
def log_request():
    # Log basic info about the incoming request
    app.logger.debug(
        f"Incoming Request: \n{request.method} {request.url} \n"
        f"Headers: {dict(request.headers)} \n"
        f"Body: {request.get_data(as_text=True)} \n"
    )

@app.after_request
def log_response(response):
    # Optionally, log details about the outgoing response
    app.logger.debug(f"Response: {response.status} for {request.method} {request.url}")
    return response

@app.route('/upload', methods=['POST'])
def upload_data():
    app.logger.debug(request.method + ' request on /upload')

    time_start = time.time_ns() / (10 ** 9)
    try:
        if request.authorization.type == 'basic':
            username = request.authorization.get('username')
            password = request.authorization.get('password')
        else:
            app.logger.error("request authorization header is not type basic. Authorization type: " + request.authorization.type)
            return jsonify({'error': 'authorization header is missing or not type basic'}), 401

        # Get table name from request parameters
        app.logger.info('args: ' + str(request.args.to_dict()))

        table_name = request.args.get('table')
        if not table_name:
            return jsonify({'error': 'Table name not specified'}), 400
        
        metric_type = request.args.get('type')
        app.logger.info("data type received: " + metric_type)
        if not metric_type:
            app.logger.warning("metric type not specified, assuming 'cost'")
            metric_type = 'cost'

        # Get data from request body
        if not request.is_json:
            return jsonify({'error': 'Request must be JSON'}), 400
        
        data = request.get_json()
        
        # Validate data
        if not isinstance(data, list):
            return jsonify({'error': 'Data must be a list of records'}), 400
        
        if not data:
            return jsonify({'result': 'No data to process'}), 200
        
        if not db.does_table_exist(table_name, username, password):
            if metric_type == 'cost':
                db.create_table(table_name, username, password, get_focus_create)
            elif metric_type == 'resource':
                db.create_table(table_name, username, password, get_resource_create)
        # Process the bulk insert

        if metric_type == 'cost':
            # Modify Tags column to match DB format
            for i in range(len(data)):
                if 'Tags' in data[i]['labels'].keys():
                    data[i]['labels']['Tags'] = format_tags_for_db(data[i]['labels']['Tags'], app.logger)

        records_inserted, error = db.bulk_insert(table_name, data, username, password)
        total_time = time.time_ns() / (10 ** 9) - time_start
        app.logger.info(f"records inserted: {records_inserted}")        
        if error != '' or records_inserted == 0:
            app.logger.error(error)
            return jsonify({
                'message': error,
                'records_processed': records_inserted,
                'time_taken': total_time
            }), 500
        
        return jsonify({
            'message': 'Data uploaded successfully',
            'records_processed': records_inserted,
            'time_taken': total_time
        }), 200
        
    except Exception as e:
        app.logger.error(f"Upload failed: {str(e)}")
        return jsonify({'error': str(e)}), 500
    
@app.route('/', methods=['GET'])
def home():
    return 'This is the homepage of Krateo\'s CrateDB Webservice: you can reach the service!'

@app.route('/compute', defaults={'path': ''})
@app.route('/compute/<path:path>', methods=['GET', 'POST', 'DELETE'])
def compute(path : str):
    app.logger.debug(request.method + ' request on ' + path)

    if request.authorization != None and request.authorization.type == 'basic':
        username = request.authorization.get('username')
        password = request.authorization.get('password')
    else:
        app.logger.error('authorization header is missing or not type basic')
        return jsonify({'error': 'authorization header is missing or not type basic'}), 401


    if request.method == 'GET':
        if path == 'list':
            return jsonify(compute_notebook.list(db, username, password)), 200
        

        parts = path.split('/')           
        if len(parts) == 2:
            if parts[1] == 'info':
                # return algorithm info for parts[0]
                return jsonify({'result': 'Information for ' + parts[0],}), 200
            else:
                return jsonify({'error': url_IncorrectError}), 400
        else:
            return jsonify({'error': url_IncorrectError}), 400

        
    elif request.method == 'POST':
        parts = path.split('/')

        if len(parts) == 1:
            parameters = request.get_json()
            result = compute_notebook.run(path, db, username, password, parameters, engine='cratedb')
            app.logger.info('notebook call to ' + path + ' has result: ' + result)
            if request.headers.get('Accept') == 'application/json':
                return Response(result, mimetype='application/json')
            else:
                return result, 200

        if len(parts) == 2:
            if parts[1] == 'upload':
                notebook = request.get_data().decode()
                if request.args.get('overwrite') is not None:
                    app.logger.debug('Overwrite from query string ' + request.args.get('overwrite'))
                    overwrite = request.args.get('overwrite') == ('true' or 'True')
                else:
                    overwrite = False
                return jsonify({'result': compute_notebook.upload(db, parts[0], notebook, overwrite, username, password)}), 200
            
    elif request.method == 'DELETE':
        return jsonify({'result': compute_notebook.delete(db, path, username, password)}), 200
        
    return jsonify({'error': url_Method_IncorrectError}), 405


@app.errorhandler(Exception)
def handle_error(error):
    """Global error handler"""
    app.logger.error(f"An error occurred: {str(error)}\n{traceback.format_exc()}")
    return jsonify({'error': str(error)}), 500

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Start the server using Waitress
    app.logger.info("Starting server on port 8088...")
    serve(app, host='0.0.0.0', port=int(os.getenv('PORT_DB_WEBSERVICE', '8088')))