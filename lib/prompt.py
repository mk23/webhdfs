import cmd
import getpass
import os
import pwd
import readline
import stat
import sys
import urlparse
import zlib

from errors import WebHDFSError
from client import WebHDFSClient
from attrib import LocalFSObject

# Work around python's overeager completer delimiters
readline.set_completer_delims(readline.get_completer_delims().translate(None, '-'))

class WebHDFSPrompt(cmd.Cmd):
    def __init__(self, base, conf=None, path=None, task=None, wait=None):
        cmd.Cmd.__init__(self)

        self.base = urlparse.urlparse(base)
        self.user = getpass.getuser()
        self.hdfs = WebHDFSClient(base, self.user, conf, wait)

        self.cmds = dict((name[3:], False) for name in dir(self) if name.startswith('do_'))
        self.cmds.update({
            'lcd':  True,
            'lls':  True,
            'put':  True,
        })

        self.do_cd(path)

        if task:
            self.onecmd(' '.join(task))
            sys.exit(0)

    def _list_dir(self, sources):
        objects = []
        columns = ['mode', 'repl', 'owner', 'group', 'size', 'date', 'name']
        lengths = dict(zip(columns, [0] * len(columns)))
        build = {
            'date': '{:%b %d %Y %H:%M:%S}',
        }
        align = {
            'repl': '>',
            'size': '>',
        }

        for item in sources:
            if not stat.S_ISREG(item.perm) and not stat.S_ISDIR(item.perm):
                continue

            tmp_obj = {}

            for name in columns:
                text = build.get(name, '{}').format(getattr(item, name))

                tmp_obj[name] = text
                lengths[name] = max(lengths[name], len(text))

            objects.append(tmp_obj)

        text = ' '.join('{%s:%s%s}' % (i, align.get(i, ''), lengths[i]) for i in columns)
        for item in objects:
            print text.format(**item)

    def _fix_path(self, path, local=False, required=False):
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

    def _reset_prompt(self):
        self.prompt = '%s@%s r:%s l:%s> ' % (self.user, self.base.netloc, self.path, os.getcwd())

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
            path = self._fix_path(path or '/user/%s' % self.user)
            if not self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: not a directory' % path)
            self.path = path
        except WebHDFSError as e:
            self.path = '/'
            print e
        finally:
            self._reset_prompt()

    def do_lcd(self, path=None):
        try:
            path = self._fix_path(path or pwd.getpwnam(self.user).pw_dir, local=True)
            os.chdir(path)
        except (KeyError, OSError) as e:
            print e
        finally:
            self._reset_prompt()

    def do_ls(self, path=None):
        try:
            path = self._fix_path(path)
            self._list_dir(self.hdfs.ls(path))
        except WebHDFSError as e:
            print e

    def do_lls(self, path=None):
        try:
            path = self._fix_path(path, local=True)
            info = os.stat(path)
            objs = []

            if stat.S_ISDIR(info.st_mode):
                objs = list(LocalFSObject(path, name) for name in os.listdir(path))
            elif stat.S_ISREG(info.st_mode):
                objs = [LocalFSObject(os.path.dirname(path), os.path.basename(path))]

            self._list_dir(objs)
        except OSError as e:
            print e

    def do_du(self, path=None):
        try:
            path = self._fix_path(path)
            print self.hdfs.du(path)
        except WebHDFSError as e:
            print e

    def do_mkdir(self, path):
        try:
            path = self._fix_path(path, required='mkdir')
            try:
                self.hdfs.stat(path)
            except WebHDFSError as e:
                pass
            else:
                raise WebHDFSError('%s: already exists' % path)
            self.hdfs.mkdir(path)
        except WebHDFSError as e:
            print e

    def do_rm(self, path):
        try:
            path = self._fix_path(path, required='rm')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot remove directory' % path)
            self.hdfs.rm(path)
        except WebHDFSError as e:
            print e

    def do_rmdir(self, path):
        try:
            path = self._fix_path(path, required='rmdir')
            if not self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: not a directory' % path)
            if self.hdfs.ls(path):
                raise WebHDFSError('%s: directory not empty' % path)
            self.hdfs.rm(path)
        except WebHDFSError as e:
            print e

    def do_get(self, path):
        try:
            path = self._fix_path(path, required='get')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot download directory' % path)
            if os.path.exists(os.path.basename(path)):
                raise WebHDFSError('%s: file exists' % path)
            self.hdfs.get(path, data=open('%s/%s' % (os.getcwd(), os.path.basename(path)), 'w'))
        except (WebHDFSError, OSError) as e:
            print e

    def do_put(self, path):
        try:
            path = self._fix_path(path, local=True, required='put')
            dest = '%s/%s' % (self.path, os.path.basename(path))
            if stat.S_ISDIR(os.stat(path).st_mode):
                raise WebHDFSError('%s: cannot upload directory' % path)
            try:
                self.hdfs.stat(dest)
            except WebHDFSError as e:
                pass
            else:
                raise WebHDFSError('%s: already exists' % dest)
            self.hdfs.put(dest, data=open(path, 'r'))
        except (WebHDFSError, OSError) as e:
            print e

    def do_cat(self, path):
        try:
            path = self._fix_path(path, required='cat')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot cat directory' % path)
            sys.stdout.write(self.hdfs.get(path))
        except (WebHDFSError, OSError) as e:
            print e

    def do_zcat(self, path):
        try:
            path = self._fix_path(path, required='zcat')
            if self.hdfs.stat(path).is_dir():
                raise WebHDFSError('%s: cannot cat directory' % path)
            sys.stdout.write(zlib.decompress(self.hdfs.get(path), 16 + zlib.MAX_WBITS))
        except (WebHDFSError, OSError) as e:
            print e

    def do_EOF(self, line):
        print
        return True
