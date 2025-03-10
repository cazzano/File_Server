from flask import Flask, request, jsonify, send_from_directory, send_file
from werkzeug.utils import secure_filename
import os
import magic
import time
from functools import wraps
import logging
from flask_cors import CORS
import zipfile
import io
import shutil
import threading
import tempfile
import requests
from werkzeug.serving import is_running_from_reloader

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure upload directories
PICTURES_FOLDER = 'db/pictures'
DOWNLOADS_FOLDER = 'db/downloads'

# Create directories if they don't exist
os.makedirs(PICTURES_FOLDER, exist_ok=True)
os.makedirs(DOWNLOADS_FOLDER, exist_ok=True)

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 100 * \
    1024 * 1024 * 1024  # 100GB max file size
ALLOWED_PICTURE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_DOWNLOAD_EXTENSIONS = {'pdf', 'doc', 'docx', 'txt', 'zip', 'rar'}

# Rate limiting configuration
RATE_LIMIT = 100  # requests
RATE_TIME = 3600  # seconds (1 hour)
request_history = {}

# Backup/Restore configuration
BACKUP_CHUNK_SIZE = 5 * 1024 * 1024  # 5MB chunks for backup streaming
RESTORE_TEMP_DIR = 'restore_temp'
MAX_RETRIES = 3
TIMEOUT = 180  # 3 minutes timeout for operations
active_operations = {}  # Track ongoing backup/restore operations


def is_valid_file_type(file, folder_type):
    """Validate file type using magic numbers"""
    try:
        mime = magic.from_buffer(file.read(1024), mime=True)
        file.seek(0)  # Reset file pointer

        if folder_type == 'pictures':
            return any(ext in mime for ext in ['image/'])
        elif folder_type == 'downloads':
            return any(ext in mime for ext in ['application/', 'text/'])

        return False
    except Exception as e:
        logger.error(f"Error checking file type: {e}")
        return False


def allowed_file(filename, folder_type):
    """Check if the file extension is allowed"""
    ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
    if folder_type == 'pictures':
        return ext in ALLOWED_PICTURE_EXTENSIONS
    return ext in ALLOWED_DOWNLOAD_EXTENSIONS


def rate_limit(f):
    """Rate limiting decorator"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        ip = request.remote_addr
        current_time = time.time()

        # Clean up old requests
        request_history[ip] = [t for t in request_history.get(ip, [])
                               if current_time - t < RATE_TIME]

        if len(request_history.get(ip, [])) >= RATE_LIMIT:
            return jsonify({'error': 'Rate limit exceeded'}), 429

        request_history.setdefault(ip, []).append(current_time)
        return f(*args, **kwargs)
    return decorated_function


def handle_file_operation(folder, operation, filename=None, file=None, book_id=None):
    """Common file operation handler with book_id support"""
    try:
        folder_type = 'pictures' if folder == PICTURES_FOLDER else 'downloads'

        if operation == 'list':
            files = os.listdir(folder)
            files_with_urls = [{
                'filename': file,
                'url': f'/db/{folder_type}/{file}',
                'size': os.path.getsize(os.path.join(folder, file)),
                'modified': os.path.getmtime(os.path.join(folder, file))
            } for file in files]
            return jsonify(files_with_urls)

        elif operation in ['upload', 'update']:
            if not file:
                return jsonify({'error': 'No file provided'}), 400

            original_filename = secure_filename(filename or file.filename)
            if not original_filename:
                return jsonify({'error': 'Invalid filename'}), 400

            # Get file extension
            ext = original_filename.rsplit(
                '.', 1)[1].lower() if '.' in original_filename else ''

            if not allowed_file(original_filename, folder_type):
                return jsonify({'error': 'File type not allowed'}), 400

            if not is_valid_file_type(file, folder_type):
                return jsonify({'error': 'Invalid file content'}), 400

            # If book_id is provided, use it to create the new filename
            if book_id:
                filename = f"{book_id}.{ext}"
            else:
                filename = original_filename

            file_path = os.path.join(folder, filename)
            file.save(file_path)

            return jsonify({
                'message': f'File {"updated" if operation == "update" else "uploaded"} successfully',
                'url': f'/db/{folder_type}/{filename}'
            }), 200 if operation == 'update' else 201

        elif operation == 'delete':
            if filename:
                # Original method - delete by filename
                file_path = os.path.join(folder, secure_filename(filename))
                if not os.path.exists(file_path):
                    return jsonify({'error': 'File not found'}), 404

                os.remove(file_path)
                return jsonify({'message': 'File deleted successfully'}), 200

            elif book_id:
                # New method - delete all files with matching book_id
                deleted_files = []
                not_found = True

                # Get all files in the directory
                files = os.listdir(folder)

                # Iterate through files to find matching book_id
                for file in files:
                    # Check if filename starts with book_id followed by a dot
                    if file.startswith(f"{book_id}."):
                        not_found = False
                        file_path = os.path.join(folder, file)
                        os.remove(file_path)
                        deleted_files.append(file)

                if not_found:
                    return jsonify({'error': f'No files found for book_id {book_id}'}), 404

                return jsonify({
                    'message': 'Files deleted successfully',
                    'deleted_files': deleted_files
                }), 200
            else:
                return jsonify({'error': 'Neither filename nor book_id provided'}), 400

    except Exception as e:
        logger.error(f"Error in file operation: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Static file serving routes with caching


@app.route('/db/pictures/<filename>')
@rate_limit
def serve_picture(filename):
    response = send_from_directory(PICTURES_FOLDER, secure_filename(filename))
    response.headers['Cache-Control'] = 'public, max-age=3600'  # 1 hour cache
    return response


@app.route('/db/downloads/<filename>')
@rate_limit
def serve_download(filename):
    response = send_from_directory(DOWNLOADS_FOLDER, secure_filename(filename))
    response.headers['Cache-Control'] = 'public, max-age=3600'  # 1 hour cache
    return response

# Pictures endpoints


@app.route('/pictures', methods=['GET', 'POST', 'DELETE', 'PUT'])
@rate_limit
def handle_pictures():
    if request.method == 'GET':
        return handle_file_operation(PICTURES_FOLDER, 'list')

    elif request.method == 'POST':
        book_id = request.args.get('book_id')
        return handle_file_operation(
            PICTURES_FOLDER,
            'upload',
            file=request.files.get('file'),
            book_id=book_id
        )

    elif request.method == 'DELETE':
        # Check for book_id first, then fall back to filename
        book_id = request.args.get('book_id')
        filename = request.args.get('filename')

        if book_id:
            return handle_file_operation(
                PICTURES_FOLDER,
                'delete',
                book_id=book_id
            )
        else:
            return handle_file_operation(
                PICTURES_FOLDER,
                'delete',
                filename=filename
            )

    elif request.method == 'PUT':
        book_id = request.args.get('book_id')
        return handle_file_operation(
            PICTURES_FOLDER,
            'update',
            filename=request.args.get('filename'),
            file=request.files.get('file'),
            book_id=book_id
        )

# Downloads endpoints


@app.route('/downloads', methods=['GET', 'POST', 'DELETE', 'PUT'])
@rate_limit
def handle_downloads():
    if request.method == 'GET':
        return handle_file_operation(DOWNLOADS_FOLDER, 'list')

    elif request.method == 'POST':
        book_id = request.args.get('book_id')
        return handle_file_operation(
            DOWNLOADS_FOLDER,
            'upload',
            file=request.files.get('file'),
            book_id=book_id
        )

    elif request.method == 'DELETE':
        # Check for book_id first, then fall back to filename
        book_id = request.args.get('book_id')
        filename = request.args.get('filename')

        if book_id:
            return handle_file_operation(
                DOWNLOADS_FOLDER,
                'delete',
                book_id=book_id
            )
        else:
            return handle_file_operation(
                DOWNLOADS_FOLDER,
                'delete',
                filename=filename
            )

    elif request.method == 'PUT':
        book_id = request.args.get('book_id')
        return handle_file_operation(
            DOWNLOADS_FOLDER,
            'update',
            filename=request.args.get('filename'),
            file=request.files.get('file'),
            book_id=book_id
        )


# Backup and Restore Helper Functions
def generate_operation_id():
    """Generate a unique operation ID"""
    return str(int(time.time() * 1000))


def create_backup_archive(operation_id):
    """Create a zip archive of the db directory"""
    try:
        # Path for the backup file
        backup_path = os.path.join(
            tempfile.gettempdir(), f'db_backup_{operation_id}.zip')

        # Create the zip file
        with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Walk through all directories in the db folder
            for root, dirs, files in os.walk('db'):
                for file in files:
                    # Get the full file path
                    file_path = os.path.join(root, file)
                    # Add file to zip with its relative path
                    zf.write(file_path, arcname=file_path)

        # Update operation status
        active_operations[operation_id]['status'] = 'completed'
        active_operations[operation_id]['file_path'] = backup_path

        logger.info(f"Backup {operation_id} completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating backup {operation_id}: {e}")
        active_operations[operation_id]['status'] = 'failed'
        active_operations[operation_id]['error'] = str(e)
        return False


def process_restore_archive(operation_id, zip_path):
    """Process a restore operation from a zip file"""
    try:
        # Create a backup of the current db folder first
        backup_folder = f'db_backup_before_restore_{operation_id}'
        if os.path.exists(backup_folder):
            shutil.rmtree(backup_folder)

        shutil.copytree('db', backup_folder)

        # Empty the current db folder without deleting the folder structure
        for root, dirs, files in os.walk('db'):
            for f in files:
                os.remove(os.path.join(root, f))

        # Extract the zip file to the db folder
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                # Only extract files that start with 'db/'
                if member.startswith('db/'):
                    zip_ref.extract(member, '.')

        # Update operation status
        active_operations[operation_id]['status'] = 'completed'

        # Clean up temporary files
        if os.path.exists(zip_path):
            os.remove(zip_path)

        logger.info(f"Restore {operation_id} completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error restoring backup {operation_id}: {e}")
        active_operations[operation_id]['status'] = 'failed'
        active_operations[operation_id]['error'] = str(e)

        # Try to restore from backup if something went wrong
        try:
            if os.path.exists(backup_folder):
                shutil.rmtree('db')
                shutil.copytree(backup_folder, 'db')
        except Exception as restore_error:
            logger.error(f"Error restoring from backup: {restore_error}")

        return False


# Enhanced Backup Endpoint
@app.route('/backup', methods=['GET'])
@rate_limit
def backup_db():
    """Create a zip file containing all files in the db directory with support for slow connections"""
    try:
        # Check if client wants to stream the backup or start an async operation
        stream_mode = request.args.get('stream', 'true').lower() == 'true'

        if stream_mode:
            # Original streaming approach
            memory_file = io.BytesIO()

            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                for root, dirs, files in os.walk('db'):
                    for file in files:
                        file_path = os.path.join(root, file)
                        zf.write(file_path, arcname=file_path)

            memory_file.seek(0)

            response = send_file(
                memory_file,
                mimetype='application/zip',
                as_attachment=True,
                download_name='db_backup.zip'
            )
            # Set timeout and chunked transfer encoding
            response.headers['Transfer-Encoding'] = 'chunked'
            return response
        else:
            # Async approach for slow connections
            operation_id = generate_operation_id()
            active_operations[operation_id] = {
                'type': 'backup',
                'status': 'in_progress',
                'start_time': time.time()
            }

            # Start backup process in a separate thread
            thread = threading.Thread(
                target=create_backup_archive,
                args=(operation_id,)
            )
            thread.daemon = True
            thread.start()

            return jsonify({
                'message': 'Backup operation started',
                'operation_id': operation_id,
                'status_url': f'/operation/{operation_id}'
            }), 202

    except Exception as e:
        logger.error(f"Error initiating backup: {e}")
        return jsonify({'error': f'Failed to create backup: {str(e)}'}), 500


# Enhanced Restore Endpoint
@app.route('/restore', methods=['POST'])
@rate_limit
def restore_db():
    """Restore db folder from a zip file with support for slow connections"""
    try:
        # Check for chunk mode or full upload
        chunk_mode = request.args.get('chunk', 'false').lower() == 'true'
        operation_id = request.args.get('operation_id')

        # Full file upload (may fail with slow connections)
        if not chunk_mode:
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400

            file = request.files['file']
            if not file.filename.endswith('.zip'):
                return jsonify({'error': 'File must be a zip archive'}), 400

            # Create a new operation
            operation_id = generate_operation_id()
            active_operations[operation_id] = {
                'type': 'restore',
                'status': 'in_progress',
                'start_time': time.time()
            }

            # Save the uploaded file to a temporary location
            temp_path = os.path.join(
                tempfile.gettempdir(), f'restore_{operation_id}.zip')
            file.save(temp_path)

            # Process the restore in a separate thread
            thread = threading.Thread(
                target=process_restore_archive,
                args=(operation_id, temp_path)
            )
            thread.daemon = True
            thread.start()

            return jsonify({
                'message': 'Restore operation started',
                'operation_id': operation_id,
                'status_url': f'/operation/{operation_id}'
            }), 202

        # Chunked upload handling
        else:
            # Ensure we have an operation ID
            if not operation_id:
                # Start a new chunked upload
                operation_id = generate_operation_id()
                chunk_dir = os.path.join(
                    tempfile.gettempdir(), f'restore_chunks_{operation_id}')
                os.makedirs(chunk_dir, exist_ok=True)

                active_operations[operation_id] = {
                    'type': 'restore_chunked',
                    'status': 'receiving_chunks',
                    'start_time': time.time(),
                    'chunk_dir': chunk_dir,
                    'chunks_received': 0,
                    'total_chunks': int(request.headers.get('X-Total-Chunks', '0'))
                }

                return jsonify({
                    'message': 'Chunked restore initialized',
                    'operation_id': operation_id,
                    'status_url': f'/operation/{operation_id}'
                }), 202

            # Process an existing chunked upload
            if operation_id not in active_operations:
                return jsonify({'error': 'Invalid operation ID'}), 400

            # Get operation info
            op_info = active_operations[operation_id]

            # Handle chunk upload
            if 'file' not in request.files:
                return jsonify({'error': 'No chunk provided'}), 400

            chunk = request.files['file']
            chunk_number = int(request.args.get('chunk_number', '0'))
            chunk_path = os.path.join(
                op_info['chunk_dir'], f'chunk_{chunk_number}')

            # Save the chunk
            chunk.save(chunk_path)
            op_info['chunks_received'] += 1

            # Check if all chunks received
            if op_info['chunks_received'] >= op_info['total_chunks']:
                # Combine chunks
                combined_path = os.path.join(
                    tempfile.gettempdir(), f'restore_{operation_id}.zip')
                with open(combined_path, 'wb') as outfile:
                    for i in range(op_info['total_chunks']):
                        chunk_path = os.path.join(
                            op_info['chunk_dir'], f'chunk_{i}')
                        with open(chunk_path, 'rb') as infile:
                            outfile.write(infile.read())

                # Update status
                op_info['status'] = 'processing'

                # Process the combined file
                thread = threading.Thread(
                    target=process_restore_archive,
                    args=(operation_id, combined_path)
                )
                thread.daemon = True
                thread.start()

                # Clean up chunk directory
                shutil.rmtree(op_info['chunk_dir'], ignore_errors=True)

                return jsonify({
                    'message': 'All chunks received, processing restore',
                    'operation_id': operation_id,
                    'status_url': f'/operation/{operation_id}'
                }), 200

            # Not all chunks received yet
            return jsonify({
                'message': f'Chunk {chunk_number} received',
                'operation_id': operation_id,
                'chunks_received': op_info['chunks_received'],
                'total_chunks': op_info['total_chunks']
            }), 200

    except Exception as e:
        logger.error(f"Error in restore operation: {e}")
        return jsonify({'error': f'Failed to restore: {str(e)}'}), 500


# Operation Status Endpoint
@app.route('/operation/<operation_id>', methods=['GET'])
@rate_limit
def operation_status(operation_id):
    """Check the status of a backup or restore operation"""
    if operation_id not in active_operations:
        return jsonify({'error': 'Operation not found'}), 404

    op_info = active_operations[operation_id]

    # If operation is completed and it's a backup, return the file
    if op_info['type'] == 'backup' and op_info['status'] == 'completed' and request.args.get('download', 'false').lower() == 'true':
        if os.path.exists(op_info['file_path']):
            return send_file(
                op_info['file_path'],
                mimetype='application/zip',
                as_attachment=True,
                download_name='db_backup.zip'
            )

    # Return the operation status
    result = {
        'operation_id': operation_id,
        'type': op_info['type'],
        'status': op_info['status'],
        'start_time': op_info['start_time'],
        'elapsed_time': time.time() - op_info['start_time']
    }

    # Add operation-specific information
    if op_info['type'] == 'restore_chunked' and op_info['status'] == 'receiving_chunks':
        result.update({
            'chunks_received': op_info['chunks_received'],
            'total_chunks': op_info['total_chunks'],
            'progress': (op_info['chunks_received'] / op_info['total_chunks']) * 100 if op_info['total_chunks'] > 0 else 0
        })

    # Add error information if available
    if 'error' in op_info:
        result['error'] = op_info['error']

    # Clean up completed operations after some time
    if op_info['status'] in ['completed', 'failed'] and (time.time() - op_info['start_time']) > 3600:
        # Remove temporary files
        if op_info['type'] == 'backup' and 'file_path' in op_info and os.path.exists(op_info['file_path']):
            os.remove(op_info['file_path'])

        # Schedule for removal from active_operations
        # We don't remove it immediately to allow the client to check the status
        def cleanup_operation():
            if operation_id in active_operations:
                del active_operations[operation_id]

        # Remove after 5 minutes
        threading.Timer(300, cleanup_operation).start()

    return jsonify(result)


# Cleanup function that used to be @app.before_first_request
def cleanup_temp_files():
    """Clean up any temporary files from previous runs"""
    if not is_running_from_reloader():
        temp_dir = tempfile.gettempdir()
        for filename in os.listdir(temp_dir):
            if filename.startswith(('db_backup_', 'restore_')):
                try:
                    file_path = os.path.join(temp_dir, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path, ignore_errors=True)
                except Exception as e:
                    logger.error(
                        f"Error cleaning up temp file {filename}: {e}")


# Execute the cleanup function at startup
with app.app_context():
    cleanup_temp_files()


# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(413)
def too_large_error(error):
    return jsonify({'error': 'File too large'}), 413


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    app.run(debug=False, port=3000)  # Set debug=False for production
