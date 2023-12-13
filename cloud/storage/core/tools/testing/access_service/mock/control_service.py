import flask


class ControlServer(flask.Flask):
    def __init__(self, mock_state, *args, **kwargs):
        super(ControlServer, self).__init__(*args, **kwargs)

        self._mock_state = mock_state

        self.add_url_rule("/ping", view_func=self._ping_handler)
        self.add_url_rule("/shutdown", view_func=self.shutdown_handler, methods=["POST"])

        self.add_url_rule("/authenticate", methods=["PUT"],
                          view_func=self.put_authenticate_handler)
        self.add_url_rule("/authorize", methods=["PUT"],
                          view_func=self.put_authorize_handler)
        self.add_url_rule("/authenticate", methods=["GET"],
                          view_func=self.get_authenticate_handler)
        self.add_url_rule("/authorize", methods=["GET"],
                          view_func=self.get_authorize_handler)

    def _ping_handler(self):
        return flask.jsonify(status="OK")

    def put_authenticate_handler(self):
        value = flask.request.get_json()

        try:
            self._mock_state.authenticate_subject = value
        except ValueError as e:
            flask.abort(400, description=str(e))

        return flask.jsonify(status="OK")

    def put_authorize_handler(self):
        value = flask.request.get_json()

        try:
            self._mock_state.authorize_subject = value
        except ValueError as e:
            flask.abort(400, description=str(e))

        return flask.jsonify(status="OK")

    def get_authenticate_handler(self):
        return ControlServer._jsonify_subject_value(self._mock_state.authenticate_subject)

    def get_authorize_handler(self):
        return ControlServer._jsonify_subject_value(self._mock_state.authorize_subject)

    @staticmethod
    def shutdown():
        func = flask.request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    def shutdown_handler(self):
        self.shutdown()
        return 'Access Service Mock Control Server is shutting down...'

    @staticmethod
    def _jsonify_subject_value(subject):
        if subject is None:
            return "null"

        return flask.jsonify(subject)
