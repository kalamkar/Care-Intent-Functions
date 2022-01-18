
import google.cloud.logging as logger
logger.handlers.setup_logging(logger.Client().get_default_handler())


def main(request):
    return 'OK'

