# wsgi.py
from main import app as application

if __name__ == "__main__":
    application.run(port=3000)
