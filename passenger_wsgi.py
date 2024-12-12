import importlib.util
import os
import sys

# Add the project directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Specify the path to the Flask application
app_path = os.path.join(os.path.dirname(__file__), 'bybit_flask.py')

# Load the Flask application module
spec = importlib.util.spec_from_file_location('wsgi', app_path)
wsgi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wsgi)

# Assign the Flask app to 'application' for WSGI
application = wsgi.app
