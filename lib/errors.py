class WebHDFSError(Exception):
    def __init__(self, message='unknown error has occurred'):
        Exception.__init__(self, message)
