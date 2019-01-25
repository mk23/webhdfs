Python WebHDFS
==============

WebHDFS python client library and simple shell.


Table of Contents
-----------------
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [API](#api)
  * [WebHDFSClient](#webhdfsclient)
    * [`__init__()`](#__init__base-user-confnone-waitnone)
    * [`stat()`](#statpath-catchfalse)
    * [`ls()`](#lspath-recursefalse-requestfalse)
    * [`glob()`](#globpath)
    * [`du()`](#dupath-realfalse)
    * [`mkdir()`](#mkdirpath)
    * [`mv()`](#mvpath-dest)
    * [`rm()`](#rmpath)
    * [`repl()`](#replpath-num)
    * [`chown()`](#chownpath-owner-group)
    * [`chmod()`](#chmodpath-perm)
    * [`get()`](#getpath-datanone)
    * [`put()`](#putpath-data)
    * [`calls`](#calls)
  * [WebHDFSObject](#webhdfsobject)
    * [`__init__()`](#__init__path-bits)
    * [`is_dir()`](#is_dir)
    * [`is_empty()`](#is_empty)
    * [`owner`](#owner)
    * [`group`](#group)
    * [`name`](#name)
    * [`full`](#full)
    * [`size`](#size)
    * [`repl`](#repl)
    * [`kind`](#kind)
    * [`date`](#date)
    * [`mode`](#mode)
    * [`perm`](#perm)
* [Usage](#usage)
* [License](#license)


Prerequisites
-------------
* Python 2.7+
* Python [requests](http://docs.python-requests.org/) module


Installation
------------
Install python-webhdfs as a Debian package by building a deb:

    dpkg-buildpackage
    # or
    pdebuild

Install python-webhdfs using the standard setuptools script:

    python setup.py install


API
---
To use the WebHDFS Client API, start by importing the class from the module

```python
>>> from webhdfs import WebHDFSClient
```

All functions may throw a `WebHDFSError` exception or one of these subclasses:

| Exception Type                   | Remote Exception              | Description                                |
|----------------------------------|-------------------------------|--------------------------------------------|
| WebHDFSConnectionError           |                               | Unable to connect to active NameNode       |
| WebHDFSIncompleteTransferError   |                               | Transferred file doesn't match origin size |
| WebHDFSAccessControlError        | AccessControlException        | Access to specified path denied            |
| WebHDFSIllegalArgumentError      | IllegalArgumentException      | Invalid parameter value                    |
| WebHDFSFileNotFoundError         | FileNotFoundException         | Specified path does not exist              |
| WebHDFSSecurityError             | SecurityException             | Failed to obtain user/group information    |
| WebHDFSUnsupportedOperationError | UnsupportedOperationException | Requested operation is not implemented     |
| WebHDFSUnknownRemoteError        |                               | Remote exception unrecognized              |

## `WebHDFSClient` ##

#### `__init__(base, user, conf=None, wait=None)` ####
Creates a new `WebHDFSClient` object

Parameters:
* `base`: base webhdfs url. (e.g. http://localhost:50070)
* `user`: user name with which to access all resources
* `conf`: (_optional_) path to hadoop configuration directory for NameNode HA resolution
* `wait`: (_optional_) floating point number in seconds for request timeout waits

```python
>>> import getpass
>>> hdfs = WebHDFSClient('http://localhost:50070', getpass.getuser(), conf='/etc/hadoop/conf', wait=1.5)
```

#### `stat(path, catch=False)` ####
Retrieves metadata about the specified HDFS item.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=GETFILESTATUS

Parameters:
* `path`: HDFS path to fetch
* `catch`: (_optional_) trap `WebHDFSFileNotFoundError` instead of raising the exception

Returns:
* A single [`WebHDFSObject`](#webhdfsobject) object for the specified path.
* `False` if object not found in HDFS and `catch=True`.

```python
>>> o = hdfs.stat('/user')
>>> print o.full
/user
>>> print o.kind
DIRECTORY
>>> o = hdfs.stat('/foo', catch=True)
>>> print o
False
```


#### `ls(path, recurse=False, request=False)` ####
Lists a specified HDFS path.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=LISTSTATUS

Parameters:
* `path`: HDFS path to list
* `recurse`: (_optional_) descend down the directory tree
* `request`: (_optional_) filter request callback for each returned object

Returns:
* Generator producing children [`WebHDFSObject`](#webhdfsobject) objects for the specified path.

```python
>>> l = list(hdfs.ls('/')) # must convert to list if referencing by index
>>> print l[0].full
/user
>>> print l[0].kind
DIRECTORY
>>> l = list(hdfs.ls('/user', request=lambda x: x.name.startswith('m')))
>>> print l[0].full
/user/max
```


#### `glob(path)` ####
Lists a specified HDFS path pattern.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=LISTSTATUS

Parameters:
* `path`: HDFS path pattern to list

Returns:
* List of [`WebHDFSObject`](#webhdfsobject) objects for the specified pattern.

```python
>>> l = hdfs.glob('/us*')
>>> print l[0].full
/user
>>> print l[0].kind
DIRECTORY
```


#### `du(path, real=False)` ####
Gets the usage of a specified HDFS path.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY

Parameters:
* `path`: HDFS path to analyze
* `real`: (_optional_) specifies return type

Returns:
* If `real` is `None`:          Instance of a `du` object: `du(dirs=, files=, hdfs_usage=, disk_usage=, hdfs_quota=, disk_quota=)`
* If `real` is a string:        Integer for the `du` object attribute name.
* If `real` is boolean `True`:  Integer of hdfs bytes used by the specified path.
* If `real` is boolean `False`: Integer of disk bytes used by the specified path.

```python
>>> u = hdfs.du('/user')
>>> print u
110433
>>> u = hdfs.du('/user', real=True)
>>> print u
331299
>>> u = hdfs.du('/user', real='disk_quota')
>>> print u
-1
>>> u = hdfs.du('/user', real=None)
>>> print u
du(dirs=3, files=5, hdfs_usage=110433, disk_usage=331299, hdfs_quota=-1, disk_quota=-1)
```


#### `mkdir(path)` ####
Creates the specified HDFS path.  Uses this WebHDFS rest request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=MKDIRS

Parameters:
* `path`: HDFS path to create

Returns:
* Boolean `True`

```python
>>> hdfs.mkdir('/user/%s/test' % getpass.getuser())
True
```


#### `mv(path, dest)` ####
Moves/renames the specified HDFS path to specified destination.  Uses this WebHDFS rest request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=RENAME&destination=<DEST>

Parameters:
* `path`: HDFS path to move/rename
* `dest`: Destination path

Returns:
* Boolean `True` on success and `False` on error

```python
>>> hdfs.mv('/user/%s/test' % getpass.getuser(), '/user/%s/test.old' % getpass.getuser())
True
>>> hdfs.mv('/user/%s/test.old' % getpass.getuser(), '/some/non-existant/path')
False
```


#### `rm(path)` ####
Removes the specified HDFS path.  Uses this WebHDFS rest request:

    DELETE <BASE>/webhdfs/v1/<PATH>?op=DELETE

Parameters:
* `path`: HDFS path to remove

Returns:
* Boolean `True`

```python
>>> hdfs.rm('/user/%s/test' % getpass.getuser())
True
```


#### `repl(path, num)` ####
Sets the replication factor for the specified HDFS path.  Uses this WebHDFS rest request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=SETREPLICATION

Parameters:
* `path`: HDFS path to change
* `num`:  new replication factor to apply

Returns:
* Boolean `True` on success, `False` otherwise

```python
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).repl
1
>>> hdfs.repl('/user/%s/test' % getpass.getuser(), 3).repl
True
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).repl
3
```


#### `chown(path, owner='', group='')` ####
Sets the owner and/or group of a specified HDFS path.  Uses this WebHDFS REst request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=SETOWNER[&owner=<OWNER>][&group=<GROUP>]

Parameters:
* `path`:  HDFS path to change
* `owner`: (_optional_) new object owner
* `group`: (_optional_) new object group

Returns:
* Boolean `True` if ownership successfully applied

Raises:
* `WebHDFSIllegalArgumentError` if both owner and group are unspecified or empty

```python
>>> hdfs.chown('/user/%s/test' % getpass.getuser(), owner='other_owner', group='other_group')
True
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).owner
'other_owner'
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).group
'other_group'
```


#### `chmod(path, perm)` ####
Sets the permission of a specified HDFS path.  Uses this WebHDFS REst request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=SETPERMISSION&permission=<PERM>

Parameters:
* `path`: HDFS path to change
* `perm`: new object permission

Returns:
* Boolean `True` if permission successfully applied

Raises:
* `WebHDFSIllegalArgumentError` if permission is not octal integer under 0777

```python
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).mode
'-rwxr-xr-x'
>>> hdfs.chmod('/user/%s/test' % getpass.getuser(), perm=0644)
True
>>> hdfs.stat('/user/%s/test' % getpass.getuser()).mode
'-rw-r--r--'
```


#### `get(path, data=None)` ####
Fetches the specified HDFS path.  Returns a string or writes a file, based on parameters.  Uses this WebHDFS request:

    GET <BASE>/webhdfs/v1/<PATH>?op=OPEN

Parameters:
* `path`: HDFS path to fetch
* `data`: (_optional_) file-like object open for write

Returns:
* Boolean `True` if data is set and written file size matches source
* String contents of the fetched file if data is None

Raises:
* `WebHDFSIncompleteTransferError`


#### `put(path, data)` ####
Creates the specified HDFS file using the contents of a file open for read, or value of the string.  Uses this WebHDFS request:

    PUT <BASE>/webhdfs/v1/<PATH>?op=CREATE

Parameters:
* `path`: HDFS path to fetch
* `data`: file-like object open for read or string

Returns:
* Boolean `True` if written file size matches source

Raises:
* `WebHDFSIncompleteTransferError`

#### `calls` ####
Read-only property that retrieves number of HTTP requests performed so far.

```python
>>> l = list(hdfs.ls('/user', recurse=True))
>>> hdfs.calls
11
```


## `WebHDFSObject` ##

#### `__init__(path, bits)` ####
Creates a new `WebHDFSObject` object

Parameters:
* `path`: HDFS path prefix
* `bits`: dictionary as returned by [`stat()`](#statpath-catchfalse) or [`ls()`](#lspath-recursefalse-requestfalse) call.

```python
>>> o = hdfs.stat('/')
>>> type(o)
<class 'webhdfs.attrib.WebHDFSObject'>
```

#### `is_dir()` ####
Determines whether the HDFS object is a directory or not.

Parameters: None

Returns:
* boolean `True` when object is a directory, `False` otherwise

```python
>>> o = hdfs.stat('/')
>>> o.is_dir()
True
```

#### `is_empty()` ####
Determines whether the HDFS object is empty or not.

Parameters: None

Returns:
* boolean `True` when object is a directory and has no children or a file and is of 0 size, and `False` otherwise

```python
>>> o = hdfs.stat('/')
>>> o.is_empty()
False
```


#### `owner` ####
Read-only property that retreives the HDFS object owner.

```python
>>> o = hdfs.stat('/')
>>> o.owner
'hdfs'
```

#### `group` ####
Read-only property that retreives the HDFS object group.

```python
>>> o = hdfs.stat('/')
>>> o.group
'supergroup'
```

#### `name` ####
Read-only property that retreives the HDFS object base file name.

```python
>>> o = hdfs.stat('/user/max')
>>> o.name
'max'
```

#### `full` ####
Read-only property that retreives the HDFS object full file name.

```python
>>> o = hdfs.stat('/user/max')
>>> o.full
'/user/max'
```

#### `size` ####
Read-only property that retreives the HDFS object size in bytes.

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> o.size
20552
```

#### `repl` ####
Read-only property that retreives the HDFS object replication factor.

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> o.repl
1
```

#### `kind` ####
Read-only property that retreives the HDFS object type (`FILE` or `DIRECTORY`).

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> o.kind
'FILE'
```

#### `date` ####
Read-only property that retreives the HDFS object last modification timestamp as a Python [datetime](https://docs.python.org/2/library/datetime.html) object.

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> o.date
datetime.datetime(2015, 3, 7, 3, 53, 6)
```

#### `mode` ####
Read-only property that retreives the HDFS object symbolic permissions mode.

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> o.mode
'-rw-r--r--'
```

#### `perm` ####
Read-only property that retreives the HDFS object octal permissions mode, usable by Python's [stat](https://docs.python.org/2/library/stat.html#module-stat) module.

```python
>>> o = hdfs.stat('/user/max/snmpy.mib')
>>> oct(o.perm)
'0100644'
>>> stat.S_ISDIR(o.perm)
False
>>> stat.S_ISREG(o.perm)
True
```

Usage
-----
```
usage: webhdfs [-h] [-d CWD] [-l LOG] [-c CFG] [-t TIMEOUT] [-v]
               url [cmd [cmd ...]]

webhdfs shell

positional arguments:
  url                   webhdfs base url
  cmd                   run this command and exit

optional arguments:
  -h, --help            show this help message and exit
  -d CWD, --cwd CWD     initial hdfs directory
  -l LOG, --log LOG     logger destination url
  -c CFG, --cfg CFG     hdfs configuration dir
  -t TIMEOUT, --timeout TIMEOUT
                        request timeout in seconds
  -v, --version         print version and exit

supported logger formats:
  console://?level=LEVEL
  file://PATH?level=LEVEL
  syslog+tcp://HOST:PORT/?facility=FACILITY&level=LEVEL
  syslog+udp://HOST:PORT/?facility=FACILITY&level=LEVEL
  syslog+unix://PATH?facility=FACILITY&level=LEVEL
```

Parameters:
* `url`: base url for the WebHDFS endpoint, supporting http, https, and hdfs schemes
* `cmd`: (_optional_) run the specified command with args and exit without starting the shell
* `-d | --cwd`: (_optional_) initial hdfs directory to switch to on shell invocation
* `-l | --log`: (_optional_) logger destination url as described by supported formats
* `-c | --cfg`: (_optional_) hadoop configuration directory for NameNode HA resolution
* `-t | --timeout`: (_optional_) request timeout in seconds as floating point number
* `-v | --version`: (_optional_) print shell/library version and exit

Environment Variables:
* `HADOOP_CONF_DIR`: alternative to and takes precedence over the `-c | --cfg` command-line parameter

License
-------
[MIT](http://mk23.mit-license.org/2015-2019/license.html)
