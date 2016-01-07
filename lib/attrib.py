import datetime
import grp
import os
import pwd
import stat

# lifted from python3 stat.py
_permissions = (
    ((stat.S_IFREG,              "-"),
     (stat.S_IFDIR,              "d")),

    ((stat.S_IRUSR,              "r"),),
    ((stat.S_IWUSR,              "w"),),
    ((stat.S_IXUSR|stat.S_ISUID, "s"),
     (stat.S_ISUID,              "S"),
     (stat.S_IXUSR,              "x")),

    ((stat.S_IRGRP,              "r"),),
    ((stat.S_IWGRP,              "w"),),
    ((stat.S_IXGRP|stat.S_ISGID, "s"),
     (stat.S_ISGID,              "S"),
     (stat.S_IXGRP,              "x")),

    ((stat.S_IROTH,              "r"),),
    ((stat.S_IWOTH,              "w"),),
    ((stat.S_IXOTH|stat.S_ISVTX, "t"),
     (stat.S_ISVTX,              "T"),
     (stat.S_IXOTH,              "x"))
)
def perm_to_mode(perm):
    mode = []
    for t in _permissions:
        for b, c in t:
            if perm & b == b:
                mode.append(c)
                break
        else:
            mode.append('-')

    return ''.join(mode)


class WebHDFSObject(object):
    def __init__(self, path, bits):
        self.path = path.rstrip('/')
        self.bits = bits

        if not self.bits['pathSuffix']:
            self.bits['pathSuffix'] = os.path.basename(self.path)
            self.path = os.path.dirname(self.path).rstrip('/')

        self.calc = {
            'date': datetime.datetime.fromtimestamp(self.bits['modificationTime'] / 1000),
            'perm': int(self.bits['permission'], 8) | (stat.S_IFDIR if self.is_dir() else stat.S_IFREG),
        }
        self.calc['mode'] = perm_to_mode(self.calc['perm'])


    def __getstate__(self):
        return {'path': self.path, 'bits': self.bits}

    def __setstate__(self, args):
        self.__init__(**args)

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, repr(self.path), repr(self.bits))

    def __str__(self):
        return self.full

    def is_dir(self):
        return self.kind == 'DIRECTORY'

    def is_empty(self):
        return self.is_dir() and self.bits['childrenNum'] == 0 or not self.is_dir() and self.size == 0

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


class LocalFSObject(object):
    def __init__(self, path, name):
        self.path = path.rstrip('/')
        self.bits = os.stat('%s/%s' % (self.path, name))
        self.calc = {
            'name': name,
            'date': datetime.datetime.fromtimestamp(self.bits.st_mtime),
            'type': 'DIRECTORY' if stat.S_ISDIR(self.bits.st_mode) else 'FILE',
            'mode': perm_to_mode(self.bits.st_mode),
        }

        try:
            self.calc['owner'] = pwd.getpwuid(self.bits.st_uid).pw_name
        except KeyError:
            self.calc['owner'] = str(self.bits.st_uid)

        try:
            self.calc['group'] = grp.getgrgid(self.bits.st_gid).gr_name
        except KeyError:
            self.calc['group'] = str(self.bits.st_gid)

    def __repr__(self):
        return '%s(%s, %s, %s)' % (self.__class__.__name__, repr(self.path), repr(self.name), repr(self.bits))

    def __str__(self):
        return self.full

    def is_dir(self):
        return self.kind == 'DIRECTORY'

    @property
    def owner(self):
        return self.calc['owner']
    @property
    def group(self):
        return self.calc['group']

    @property
    def name(self):
        return self.calc['name']
    @property
    def full(self):
        return '%s/%s' % (self.path, self.name)

    @property
    def size(self):
        return self.bits.st_size

    @property
    def repl(self):
        return self.bits.st_nlink

    @property
    def kind(self):
        return self.calc['type']

    @property
    def date(self):
        return self.calc['date']

    @property
    def mode(self):
        return self.calc['mode']

    @property
    def perm(self):
        return self.bits.st_mode
