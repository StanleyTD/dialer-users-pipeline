"""
Based on https://github.com/exageraldo/gchat_logging_handler

Creates a logging handler to push log messages to Google Chat Space
"""
import logging
from json import dumps
from httplib2 import Http

class GoogleSpacesLogger(logging.Handler):
    def __init__(self, url: str) -> None:
        logging.Handler.__init__(self)

        if not url:
            raise ValueError("Webhook url not provided")
        self.url = url
        self.headers = {'Content-Type': 'application/json; charset=UTF-8'}

    def emit(self, record: logging.LogRecord) -> None:
        try:
            Http().request(
                uri = self.url,
                method = 'POST',
                headers = self.headers,
                body = dumps({ 'text': self.format(record) }),
            )
        except Exception:
            try:
                self.handleError(record)
            except Exception as err:
                print(err)
