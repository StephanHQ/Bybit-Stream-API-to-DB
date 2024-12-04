# bybit_listener.py

import os
from os import sep as os_sep
from flask import Flask, render_template, send_from_directory, abort, request, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from functools import wraps
from urllib.parse import unquote
from datetime import datetime, timezone

# Initialize Flask app
app = Flask(__name__)

# Initialize Flask-Limiter
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"]
)

# === Configuration ===

# Toggle authentication
USE_AUTH = False  # Set to True to enable authentication

# Directories for storing CSV files and logs
CSV_FOLDER = "bybit_stream_data"

# Ensure the CSV folder exists
os.makedirs(CSV_FOLDER, exist_ok=True)

# === Authentication Setup ===

# Define your credentials for Basic Authentication (Used only if USE_AUTH is True)
USERNAME = os.getenv('USERNAME', 'admin')        # Change to your desired username
PASSWORD = os.getenv('PASSWORD', 'password')     # Change to your desired password

def check_auth(username, password):
    """Check if a username/password combination is valid."""
    return username == USERNAME and password == PASSWORD

def authenticate():
    """Sends a 401 response that enables basic auth."""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'}
    )

def requires_auth(f):
    """Decorator to handle authentication logic."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if USE_AUTH:
            auth = request.authorization
            if not auth or not check_auth(auth.username, auth.password):
                return authenticate()
        return f(*args, **kwargs)
    return decorated

# Security: Prevent directory traversal by ensuring all paths are within CSV_FOLDER
def secure_path(path):
    # Join the CSV_FOLDER with the requested path
    base_path = os.path.abspath(CSV_FOLDER)
    safe_path = os.path.abspath(os.path.join(base_path, path))
    # Ensure the resulting path is within CSV_FOLDER
    if not safe_path.startswith(base_path):
        abort(403)  # Forbidden
    return safe_path

# === Error Handlers ===

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', message="Resource Not Found.", description="The requested resource could not be found."), 404

@app.errorhandler(403)
def forbidden(error):
    return render_template('error.html', message="Forbidden", description="You don't have permission to access this resource."), 403

@app.errorhandler(401)
def unauthorized(error):
    return render_template('error.html', message="Unauthorized", description="Please provide valid credentials to access this resource."), 401

@app.errorhandler(429)
def ratelimit_handler(error):
    return render_template('error.html', message="Rate Limit Exceeded", description="You have made too many requests. Please try again later."), 429

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', message="Internal Server Error", description="An unexpected error has occurred. Please try again later."), 500

# === Routes ===

# Home route - lists the top-level directories
@app.route('/')
@requires_auth
@limiter.limit("10 per minute")
def index():
    return list_directory('')

# Dynamic route to list directories and files
@app.route('/browse/<path:subpath>')
@requires_auth
@limiter.limit("10 per minute")
def list_directory(subpath):
    # Decode the URL-encoded path
    subpath = unquote(subpath)
    safe_path = secure_path(subpath)

    if not os.path.exists(safe_path):
        abort(404)

    if not os.path.isdir(safe_path):
        abort(404)

    # List directories and files
    items = os.listdir(safe_path)
    dirs = []
    files = []
    for item in sorted(items):
        item_path = os.path.join(safe_path, item)
        if os.path.isdir(item_path):
            dirs.append(item)
        elif os.path.isfile(item_path) and (item.endswith('.csv') or item.endswith('.csv.gz')):
            file_stat = os.stat(item_path)
            files.append({
                'name': item,
                'size': f"{file_stat.st_size / (1024**2):.2f} MB",
                'modified': datetime.fromtimestamp(file_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })

    # Calculate the relative path for navigation
    relative_path = os.path.relpath(safe_path, os.path.abspath(CSV_FOLDER))
    if relative_path == '.':
        relative_path = ''

    return render_template(
        'directory.html',
        dirs=dirs,
        files=files,
        current_path=relative_path,
        path_separator=os_sep  # Pass the path separator to the template
    )

# Route to download files
@app.route('/download/<path:filepath>')
@requires_auth
@limiter.limit("10 per minute")
def download_file(filepath):
    # Decode the URL-encoded path
    filepath = unquote(filepath)
    safe_path = secure_path(filepath)

    if not os.path.exists(safe_path):
        abort(404)

    if not os.path.isfile(safe_path):
        abort(404)

    # Extract directory and filename
    directory = os.path.dirname(safe_path)
    filename = os.path.basename(safe_path)

    return send_from_directory(directory, filename, as_attachment=True)

# === Main Application Runner ===

if __name__ == "__main__":
    # For development purposes; in production, use passenger_wsgi.py as the entry point
    app.run(host='127.0.0.1', port=8000, debug=False)
