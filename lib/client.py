import errno
import logging
import os
import requests
import tempfile
import urlparse
import xml.etree.cElementTree as ET

from errors import WebHDFSError
from attrib import WebHDFSObject

LOG = logging.getLogger()

class WebHDFSClient(object):
    def __init__(self, base, user, conf=None):
        self.user = user
        self._cfg(base, conf)


    def _url(self, url):
        if url.scheme == 'hdfs':
            url = url._replace(scheme='http', netloc='%s:%s' % (url.hostname, url.port or '50070'))

        return url.geturl()

    def _cfg(self, base, conf=None):
        url = urlparse.urlparse(base)
        self.urls = []

        for part in ('hdfs', 'core'):
            try:
                tree = ET.parse('%s/%s-site.xml' % (conf, part))
                name = tree.getroot().find('.//property/[name="dfs.ha.namenodes.%s"]/value' % url.hostname)
                for item in name.text.split(','):
                    addr = tree.getroot().find('.//property/[name="dfs.namenode.http-address.%s.%s"]/value' % (url.hostname, item))
                    self.urls.append(self._url(url._replace(netloc=addr.text)))
                if self.urls:
                    break
            except EnvironmentError as e:
                if e.errno == errno.ENOENT:
                    continue
                else:
                    raise e
        else:
            self.urls.append(self._url(url))

    def _req(self, name, path, kind='get', data=None, **args):
        args['op']        = name
        args['user.name'] = self.user

        try:
            u = '%s/webhdfs/v1/%s' % (self.base, path.lstrip('/'))
            if not data:
                r = getattr(requests, kind)(u, params=args)
                self._log(r)
                r.raise_for_status()
                return r.json()
            elif kind == 'put':
                r = requests.put(u, params=args, allow_redirects=False)
                self._log(r)
                r.raise_for_status()
                r = requests.put(r.headers['location'], headers={'content-type': 'application/octet-stream'}, data=data)
                self._log(r)
                r.raise_for_status()
                return True
            else:
                r = requests.get(u, params=args, stream=True)
                self._log(r)
                for c in r.iter_content(16 * 1024):
                    data.write(c)
                r.raise_for_status()
                return True
        except requests.exceptions.ConnectionError as e:
            raise WebHDFSError(e.message)
        except requests.exceptions.HTTPError as e:
            error = WebHDFSError('%s: %s' % (e.response.reason, path))
            try:
                error = WebHDFSError(e.response.json()['RemoteException']['message'])
            finally:
                raise error

    def _log(self, rsp):
        LOG.debug('url:  %s', rsp.url)
        LOG.debug('code: %d %s', rsp.status_code, rsp.reason)

        w = reduce(lambda x, y: max(x, len(y)), rsp.headers.keys(), 0)
        for k, v in sorted(rsp.headers.iteritems()):
            LOG.debug('  %%-%ds : %%s' % w, k, v)

    def stat(self, path):
        r = self._req('GETFILESTATUS', path)
        return WebHDFSObject(path, r['FileStatus'])

    def ls(self, path, recurse=False):
        l = []
        r = self._req('LISTSTATUS', path)
        for i in r['FileStatuses']['FileStatus']:
            l.append(WebHDFSObject(path, i))
            if recurse and l[-1].is_dir():
                l.extend(self.ls('%s/%s' % (path, l[-1].name), recurse))

        return l

    def du(self, path, real=False):
        r = self._req('GETCONTENTSUMMARY', path)
        return r['ContentSummary']['length'] if not real else r['ContentSummary']['spaceConsumed']

    def mkdir(self, path):
        r = self._req('MKDIRS', path, 'put')
        return r['boolean']

    def rm(self, path):
        r = self._req('DELETE', path, 'delete')
        return r['boolean']

    def repl(self, path, num):
        r = self._req('SETREPLICATION', path, 'put', replication=num)
        return r['boolean']

    def get(self, path, data=None):
        rval = True
        if not data:
            rval = False
            data = tempfile.TemporaryFile()

        self._req('OPEN', path, 'get', data=data)

        data.flush()
        if os.fstat(data.fileno()).st_size != self.stat(path).size:
            raise WebHDFSError('%s: download incomplete' % path)

        if not rval:
            data.seek(0)
            rval = data.read()

        data.close()
        return rval

    def put(self, path, data):
        if isinstance(data, str):
            temp = tempfile.TemporaryFile()
            temp.write(data)
            temp.flush()
            temp.seek(0)

            LOG.debug('%s: saved %d bytes to temp file', temp.name, len(data))
            data = temp

        self._req('CREATE', path, 'put', data=data)
        if os.fstat(data.fileno()).st_size != self.stat(path).size:
            raise WebHDFSError('%s: upload incomplete' % data.name)

        data.close()
        return True
