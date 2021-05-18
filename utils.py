

ALLOW_HEADERS = 'Access-Control-Allow-Headers, Origin, Accept, X-Requested-With, Content-Type, Authorization, ' \
                'Cookie, Access-Control-Request-Method, Access-Control-Request-Headers, ' \
                'Access-Control-Allow-Credentials, Cache-Control, Pragma, Expires'


def make_response(request, methods='GET'):
    import flask
    response = flask.make_response()
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    origin = request.headers.get('origin')
    response.headers['Access-Control-Allow-Origin'] = origin if origin else '*'

    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = methods
        response.headers['Access-Control-Allow-Headers'] = ALLOW_HEADERS
        response.status_code = 204

    return response

