Python WebHDFS
==============

WebHDFS python client library and simple shell.


Table of Contents
-----------------
* [Prerequisites](#prerequisites)
* [Installation](#installation)
* [API](#api)
  * [WebHDFSClient](#webhdfsclient)
    * [`__init__()`](#__init__base-user-confnone)
    * [`stat()`](#statpath)
    * [`ls()`](#lspath-recursefalse)
    * [`du()`](#dupath-realfalse)
    * [`mkdir()`](#mkdirpath)
    * [`rm()`](#rmpath)
    * [`repl()`](#replpath-num)
    * [`get()`](#getpath-datanone)
  * [WebHDFSObject](#webhdfsobject)
    * [`__init__()`](#__init__path-bits)
    * [`is_dir()`](#is_dir)
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

All functions may throw a `WebHDFSError` exception:
* URL is not reachable
* Requested path is not found
* Specified user has no permission to requested path

## `WebHDFSClient` ##

#### `__init__(base, user, conf=None)` ####
Creates a new `WebHDFSClient` object

Parameters:
* `base`: base webhdfs url. (e.g. http://localhost:50070)
* `user`: user name with which to access all resources
* `conf`: (_optional_) path to hadoop configuration directory for NameNode HA resolution

```python
>>> import getpass
>>> hdfs = WebHDFSClient('http://localhost:50070', getpass.getuser(), conf='/etc/hadoop/conf')
```

#### `stat(path)` ####
Retrieves metadata about the specified HDFS item.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=GETFILESTATUS

Parameters:
* `path`: HDFS path to fetch

Returns:
* A single [`WebHDFSObject`](#webhdfsobject) object for the specified path.

```python
>>> o = hdfs.stat('/user')
>>> print o.full
/user
>>> print o.kind
DIRECTORY
```


#### `ls(path, recurse=False)` ####
Lists a specified HDFS path.  Uses this WebHDFS REst request:

    GET <BASE>/webhdfs/v1/<PATH>?op=LISTSTATUS

Parameters:
* `path`: HDFS path to list
* `recurse`: (_optional_) descend down the directory tree

Returns:
* List of children [`WebHDFSObject`](#webhdfsobject) objects for the specified path, if it is a directory or a list of a single item otherwise.

```python
>>> l = hdfs.ls('/')
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
* `real`: (_optional_) return machine usage instead of HDFS usage, taking replication factor into account

Returns:
* Integer of bytes used by the specified path.

```python
>>> u = hdfs.du('/user')
>>> print u
110433
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
>>> hdfs.rm('/user/%s/test' % getpass.getuser())
True
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


## `WebHDFSObject` ##

#### `__init__(path, bits)` ####
Creates a new `WebHDFSObject` object

Parameters:
* `path`: HDFS path prefix
* `bits`: dictionary as returned by [`stat()`](#statpath) or [`ls()`](#lspath-recursefalse) call.

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

#### `owner` ####
Read-only property that retreives the HDFS object owner.

```python
>>> o = hdfs.stat('/')
>>> o.owner
u'hdfs'
```

#### `group` ####
Read-only property that retreives the HDFS object group.

```python
>>> o = hdfs.stat('/')
>>> o.group
u'supergroup'
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
u'FILE'
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

License
-------
[MIT](http://mk23.mit-license.org/2015-2015/license.html)
