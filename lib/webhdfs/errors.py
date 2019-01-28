import sys

class WebHDFSError(Exception):
    def __init__(self, message='unknown error has occurred'):
        if isinstance(message, dict):
            m = message.get('RemoteException', {}).get('message', 'unknown remote error has occurred').split('\n')[0]
            e = message.get('RemoteException', {}).get('exception', 'UnknownException')
            c = getattr(sys.modules[__name__], 'WebHDFS'+e.replace('Exception', 'Error'), WebHDFSUnknownRemoteError)

            raise c(m.encode('utf8') if isinstance(m, unicode) else m)

        Exception.__init__(self, message)

class WebHDFSConnectionError(WebHDFSError):
    pass

class WebHDFSIncompleteTransferError(WebHDFSError):
    pass

class WebHDFSFileNotFoundError(WebHDFSError):
    pass

class WebHDFSIllegalArgumentError(WebHDFSError):
    pass

class WebHDFSAccessControlError(WebHDFSError):
    pass

class WebHDFSSecurityError(WebHDFSError):
    pass

class WebHDFSUnsupportedOperationError(WebHDFSError):
    pass

class WebHDFSUnknownRemoteError(WebHDFSError):
    pass
