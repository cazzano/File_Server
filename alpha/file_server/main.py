from flask import Flask, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
import os
import magic
import time
from functools import wraps
import logging
from flask_cors import CORS

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
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
ALLOWED_PICTURE_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
ALLOWED_DOWNLOAD_EXTENSIONS = {'pdf', 'doc', 'docx', 'txt', 'zip', 'rar'}

# Rate limiting configuration
RATE_LIMIT = 100  # requests
RATE_TIME = 3600  # seconds (1 hour)
request_history = {}


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
