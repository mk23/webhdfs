import cmd
import datetime
import getpass
import grp
import os
import pwd
import stat
import urlparse
import zlib

from webhdfs import WebHDFSError
from webhdfs import WebHDFSObject
from webhdfs import WebHDFSClient


class WebHDFSPrompt(cmd.Cmd):
    def __init__(self, base):
        cmd.Cmd.__init__(self)

        self.base = urlparse.urlparse(base)
        self.user = getpass.getuser()
        self.hdfs = WebHDFSClient(base, self.user)

        self.cmds = dict((name[3:], False) for name in dir(self) if name.startswith('do_'))
        self.cmds.update({
            'lcd':  True,
            'lls':  True,
            'put':  True,
        })

        self.do_cd()

    def normalize(self, path, local=False, required=False):
        path = '' if path is None else path.strip()
        rval = []

        if not path and not required:
            path = getattr(self, 'path', '/user/%s' % self.user) if not local else os.getcwd()
        elif not path and required:
            raise WebHDFSError('%s: path not specified' % required)

        if not path.startswith('/'):
            path = '%s/%s' % (self.path if not local else os.getcwd(), path)

        for part in path.split('/'):
            if not part or part == '.':
                continue
            if rval and part == '..':
                rval.pop()
            else:
                rval.append(part)

        return '/'+'/'.join(rval)

    def emptyline(self):
        pass

    def default(self, arg):
        print '%s: unknown command' % arg

    def completedefault(self, _1, line, _2, _3):
        name = line.split(None, 1)[0]
        if name not in self.cmds:
            return []

        part = line[len(name) + 1:]
        path = os.path.dirname(part)

        try:
            if self.cmds[name]:
                if not path.startswith('/'):
                    path = '%s/%s' % (os.getcwd(), path)
                return [i + ('/' if stat.S_ISDIR(os.stat(i).st_mode) else ' ') for i in os.listdir(os.path.dirname(path)) if i.startswith(os.path.basename(part))]
            else:
                if not path.startswith('/'):
                    path = '%s/%s' % (self.path, path)
                return [i.name + ('/' if i.is_dir() else ' ') for i in self.hdfs.ls(path) if i.name.startswith(os.path.basename(part))]
        except (WebHDFSError, OSError):
            return []

    def do_cd(self, path=None):
        try:
            path = self.normalize(path or '/user/%s' % self.user)
            if not self.hdfs.stat(path).is_dir():
                print '%s: not a directory' % path

            self.path = path
            self.prompt = '%s@%s r:%s l:%s> ' % (self.user, self.base.netloc, self.path, os.getcwd())
        except WebHDFSError as e:
            print e

    def do_lcd(self, path=None, local=True):
        try:
            path = self.normalize(path, local=True)
            os.chdir(path)
            self.prompt = '%s@%s r:%s l:%s> ' % (self.user, self.base.netloc, self.path, os.getcwd())
        except OSError as e:
            print e

    def do_ls(self, path=None):
        try:
            path = self.normalize(path)

            objects = []
            columns = ['mode', 'repl', 'owner', 'group', 'size', 'date', 'name']
            lengths = dict(zip(columns, [0] * len(columns)))
            builds = {
                'date': '{:%b %d %Y %H:%M:%S}',
            }
            places = {
                'repl': '>',
                'size': '>',
            }

            for item in self.hdfs.ls(path):
                dfs_obj = {}

                for name in columns:
                    attr = builds.get(name, '{}').format(getattr(item, name))

                    dfs_obj[name] = attr
                    lengths[name] = max(lengths[name], len(attr))

                objects.append(dfs_obj)

            fmts = ' '.join('{%s:%s%s}' % (i, places.get(i, ''), lengths[i]) for i in columns)
            for item in objects:
                print fmts.format(**item)

        except WebHDFSError as e:
            print e

    def do_lls(self, path=None):
        def print_item(names):
            if isinstance(names, str):
                names = [names]

            for name in names:
                item = os.stat(name)
                if not stat.S_ISDIR(item.st_mode) and not stat.S_ISREG(item.st_mode):
                    continue

                try:
                    owner = pwd.getpwuid(item.st_uid).pw_name
                except KeyError:
                    owner = item.st_uid

                try:
                    group = grp.getgrgid(item.st_gid).gr_name
                except KeyError:
                    group = item.st_gid

                perm = (0o0777 & item.st_mode) | (int(stat.S_ISDIR(item.st_mode)) << 9)
                date = datetime.datetime.fromtimestamp(item.st_mtime)
                mode = ''.join( WebHDFSObject.pmap[i] if perm & (1 << (i)) else '-' for i in range(len(WebHDFSObject.pmap) - 1, -1, -1) )

                print '{} {:>3} {:<15} {:<15} {:>15} {:%b %d %Y %H:%M:%S} {}'.format(mode, item.st_nlink, owner, group, item.st_size, date, name)

        try:
            path = self.normalize(path, local=True)
            item = os.stat(path)
            if stat.S_ISDIR(item.st_mode):
                print_item(os.listdir(path))
            else:
                print_item(path)
        except OSError as e:
            print e

    def do_du(self, path=None):
        try:
            path = self.normalize(path)
            print self.hdfs.du(path)
        except WebHDFSError as e:
            print e

    def do_mkdir(self, path):
        try:
            path = self.normalize(path, required='mkdir')
            self.hdfs.mkdir(path)
        except WebHDFSError as e:
            print e

    def do_rm(self, path):
        try:
            path = self.normalize(path, required='rm')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot remove directory' % path)
            self.hdfs.rm(path)
        except WebHDFSError as e:
            print e

    def do_rmdir(self, path):
        try:
            path = self.normalize(path, required='rmdir')
            if not self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: not a directory' % path)
            if self.hdfs.ls(path):
                raise WebHDFSError('%s: directory not empty' % path)
            self.hdfs.rm(path)
        except WebHDFSError as e:
            print e

    def do_get(self, path):
        try:
            path = self.normalize(path, required='get')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot download directory' % path)
            if os.path.exists(os.path.basename(path)):
                raise WebHDFSError('%s: file exists' % path)
            self.hdfs.get(path, data=open('%s/%s' % (os.getcwd(), os.path.basename(path)), 'w'))
        except (WebHDFSError, OSError) as e:
            print e

    def do_put(self, path):
        try:
            path = self.normalize(path, local=True, required='put')
            if stat.S_ISDIR(os.stat(path).st_mode):
                raise WebHDFSError('%s: cannot upload directory' % path)
            self.hdfs.put('%s/%s' % (self.path, os.path.basename(path)), data=open(path, 'r'))
        except (WebHDFSError, OSError) as e:
            print e

    def do_cat(self, path):
        try:
            path = self.normalize(path, required='cat')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot cat directory' % path)
            print self.hdfs.get(path)
        except (WebHDFSError, OSError) as e:
            print e

    def do_zcat(self, path):
        try:
            path = self.normalize(path, required='zcat')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot cat directory' % path)
            print zlib.decompress(self.hdfs.get(path), 16 + zlib.MAX_WBITS)
        except (WebHDFSError, OSError) as e:
            print e

    def do_EOF(self, line):
        print
        return True
