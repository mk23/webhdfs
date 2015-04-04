#!/usr/bin/env python2.7

import datetime
import stat


class WebHDFSObject(object):
    def __init__(self, path, bits):
        self.path = path
        self.bits = bits
        self.calc = {}

        self._prep()


    def __getstate__(self):
        return {'path': self.path, 'bits': self.bits}

    def __setstate__(self, args):
        self.__init__(**args)

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, repr(self.path), repr(self.bits))

    def __str__(self):
        return self.full

    def _prep(self):
        self.calc['date'] = datetime.datetime.fromtimestamp(self.bits['modificationTime'] / 1000)
        self.calc['perm'] = int(self.bits['permission'], 8)

        pmap = list(reversed('rwxrwxrwx'))
        mode = list(pmap[i] if self.perm & (1 << i) else '-' for i in range(len(pmap) - 1, -1, -1))
        mode.insert(0, 'd' if self.is_dir() else '-')
        for b, c in ((stat.S_IXOTH|stat.S_ISVTX, 't'), (stat.S_ISVTX, 'T'), (stat.S_IXOTH, 'x')):
            if self.calc['perm'] & b == b:
                mode[-1] = c
                break
        else:
            mode[-1] = '-'

        self.calc['mode'] = ''.join(mode)

    def is_dir(self):
        return self.kind == 'DIRECTORY'

    @property
    def owner(self):
        return self.bits['owner']
    @property
    def group(self):
        return self.bits['group']

    @property
    def name(self):
        return self.bits['pathSuffix']
    @property
    def full(self):
        return '%s/%s' % (self.path, self.bits['pathSuffix'])

    @property
    def size(self):
        return self.bits['length']

    @property
    def repl(self):
        return self.bits['replication']

    @property
    def kind(self):
        return self.bits['type']

    @property
    def date(self):
        return self.calc['date']

    @property
    def mode(self):
        return self.calc['mode']

    @property
    def perm(self):
        return self.calc['perm']
