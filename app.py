from flask import Flask
from app.views import bq_table_comparison
import logging
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s (line %(lineno)d) - %(message)s'
)

app.register_blueprint(bq_table_comparison.view_bqtb, url_prefix='/api/v1/tables')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080,ssl_context='adhoc')
