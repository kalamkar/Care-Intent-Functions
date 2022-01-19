import base64
import json
import logging

import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(event, context):
    logging.info(event)
    logging.info(context)
    message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    logging.info(message)

    return 'OK'

