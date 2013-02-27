#!/usr/bin/python
# -*- coding: utf-8 -*-

import bz2
import os
import stat
import errno
import sys
import hashlib
from sqlobject import *
from sqlobject.inheritance import InheritableSQLObject
import sqlite3
import threading
import time
import urllib2
import atpy

from fuse import Fuse
import fuse

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

SERVER_LOCATION = 'http://pleiades.icrar.org:7777/'

class PseudoStat(fuse.Stat):
    """
    å°‚ç”¨ã®statæ§‹é€ ä½“
    - st_mode ä¿è­·ãƒ¢ãƒ¼ãƒ‰
    - st_ino inodeç•ªå·
    - st_dev deviceç•ªå·
    - st_nlink ãƒãƒ¼ãƒ‰ãƒªãƒ³ã‚¯ã®æ•°
    - st_uid ã‚ªãƒ¼ãƒŠãƒ¼UID
    - st_gid ã‚ªãƒ¼ãƒŠãƒ¼GID
    - st_size ãƒ•ã‚¡ã‚¤ãƒ«ã‚µã‚¤ã‚º(Byte)
    - st_atime æœ€çµ‚ã‚¢ã‚¯ã‚»ã‚¹æ™‚é–“
    - st_mtime æœ€çµ‚æ›´æ–°æ™‚é–“
    - st_ctime ãƒ—ãƒ©ãƒƒãƒˆãƒ•ã‚©ãƒ¼ãƒ ä¾å­˜ã€Unixã§ã¯æœ€çµ‚ãƒ¡ã‚¿ãƒ‡ãƒ¼ã‚¿å¤‰æ›´æ™‚é–“
    ã€€ã€€ã€€ã€€ã€€Windowsã§ã¯ä½œæˆæ™‚é–“
    """
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

class Inode(SQLObject):
    """
    Creates an object to interact with the inode table
    """

    
    inode_num = IntCol(notNone=True) #Uniquely identifies each item
    rev_id = IntCol(notNone=True) #States the item 'version'
    uid = IntCol(notNone=True) #User id
    gid = IntCol(notNone=True) #Group id
    atime = FloatCol(notNone=True) #Access time
    mtime = FloatCol(notNone=True) #Modify time
    ctime = FloatCol(notNone=True) #Creation time
    size = IntCol(notNone=True) #Size of file
    mode = IntCol(notNone=True) #Access permissions
    dev = IntCol(default=0) #Not sure, has always been 0
    inum_r_index = DatabaseIndex("inode_num","rev_id")

class Dentry(SQLObject):
    """
    Creates an object to interact with the dentry table
    """
    parent = ForeignKey("Inode", notNone=True) #primary key of root from inode table
    filename = UnicodeCol(notNone=True) #Name of file
    inode_num = IntCol(notNone=True) #reference to inode_num from inode table
    p_index = DatabaseIndex("parent")
    p_name_index = DatabaseIndex("parent", "filename")

class RawData(SQLObject):
    """
    Interects with a table that holds the raw data, not used in this implementation
    """
    hash_sha256 = StringCol(notNone=True, length=64)
    data = StringCol(notNone=True)
    hash_index = DatabaseIndex("hash_sha256")

    def _set_data(self, value):
        self._SO_set_data(bz2.compress(value).encode("base64"))

    def _get_data(self):
        return bz2.decompress(self._SO_get_data().decode("base64"))

class DataList(SQLObject):
    """
    Interacts with the data_list table
    """
    parent = ForeignKey("Inode", notNone=True) #Reference to inode primary key
    series = IntCol(notNone=True, default=0) #A type of versioning, 0 for new files
    data = ForeignKey("RawData", notNone=True) #Reference to raw_data, not used
    ps_index = DatabaseIndex("parent", "series")

def split_path(path):
    """
    Returns everything after the final '/'
    Removes '/' if it's the last character.
    """
    if path[-1] == "/":
        path = path[:-1]
    return path.split("/")[1:]

class FileSystemError(BaseException):
    """ãƒ•ã‚¡ã‚¤ãƒ«ã‚·ã‚¹ãƒ†ãƒ ãŒè¿”ã™ã™ã¹ã¦ã®ä¾‹å¤–ã®ç¶™æ‰¿å…ƒ"""
    pass

class IllegalInode(FileSystemError):
    """ä¸æ­£ãªi-nodeã‚’æ¤œçŸ¥"""
    pass

class VFile(object):
    """
    Honestly not sure what this is for, have left it unedited and haven't had it interfering.
    """

    BLOCK_SIZE = 32*1024

    def __init__(self, inode_e):
        """Inodeã‚’å—ã‘å–ã£ã¦åˆæœŸåŒ–"""
        self.__inode = inode_e
        if not isinstance(inode_e, Inode):
            raise IllegalInode()
        self.__count = 0
        self.__dirty = False
        self.__lock = threading.Lock()
        self.__list = []

    def open(self):
        self.__count += 1

    def close(self):
        """å‚ç…§ã‚«ã‚¦ãƒ³ã‚¿ã‚’ãƒ‡ã‚¯ãƒªãƒ¡ãƒ³ãƒˆã—ã¦å¿…è¦ãªã‚‰DBã«æ›¸ãè¾¼ã‚€"""
        self.__count -= 1
        if self.__count < 1 & self.__dirty:
            self.fsync()

    def is_close(self):
        if self.__count > 0:
            return False
        return True

    def __read_list(self):
        if not self.__list:
            tmp = DataList.selectBy(parent=self.__inode).orderBy("series")
            cnt = tmp.count()
            if cnt == 0:
                inodes = Inode.selectBy(inode_num=self.__inode.inode_num)\
                            .orderBy("-rev_id")
                for i_n in inodes:
                    tmp = DataList.selectBy(parent=i_n).orderBy("series")
                    cnt = tmp.count()
                    if cnt == 0:
                        break
            if cnt == 0 and self.__inode.size > 0:
                raise FileSystemError(self.__inode,list(inodes))
            for d in tmp:
                self.__list.append(d.data)

    def read(self, length, offset):
        if offset > self.__inode.size:
            raise FileSystemError("Offset is greater than size.")
        if (offset + length) > self.__inode.size:
            length = self.__inode.size - offset
        self.__read_list()
        i_start = int(offset / self.BLOCK_SIZE)
        start = i_start * self.BLOCK_SIZE
        if len(self.__list) == 0:
            return ""
        if len(self.__list) < i_start:
            return ""
        buf = self.__list[i_start].data[offset - start:]
        self.__lock.acquire()
        for d in self.__list[i_start+1:]:
            buf += d.data
            if len(buf) > length:
                break
        self.__lock.release()
        return buf[:length]

    def __write_block(self, blk):
        """ãƒ–ãƒ­ãƒƒã‚¯ã‚’æ›¸ãè¾¼ã‚€
        é‡è¤‡ã®ç¢ºèªã‚’è¡Œã†
        æ›¸ãè¾¼ã¿ã«æˆåŠŸã—ãŸå ´åˆã€RawDataã‚ªãƒ–ã‚¸ã‚§ã‚¯ãƒˆã‚’è¿”ã™
        """
        hash_sha256 = hashlib.sha256(blk).hexdigest()
        if RawData.selectBy(hash_sha256=hash_sha256).count() > 0:
            same_hashs = RawData.selectBy(hash_sha256=hash_sha256)
            hash_md5 = hashlib.md5(blk).digest()
            for same_hash in same_hashs:
                hash = hashlib.md5(same_hash.data).digest()
                if hash_md5 == hash:
                    return same_hash
        return RawData(hash_sha256=hash_sha256, data=blk)

        
    def write_wo_t(self, buf, offset):
        """ä¸€æ™‚ãƒ•ã‚¡ã‚¤ãƒ«ã‚’ç”¨ã„ãšã«æ›¸ãè¾¼ã¿"""
        self.__read_list()
        now = time.time()
        size = offset + len(buf)
        if size < self.__inode.size:
            size = self.__inode.size
        self.__read_list()
        self.__lock.acquire()
        if not self.__dirty:
            max_rev = Inode.selectBy(inode_num=self.__inode.inode_num).\
                                                            max("rev_id")
            if not max_rev == self.__inode.rev_id:
                raise FileSystemError("You use old file.")
            new_i = Inode(inode_num=self.__inode.inode_num,
                    rev_id=(max_rev+1), uid=self.__inode.uid,
                    gid=self.__inode.gid, atime=now,
                    mtime=now, ctime=self.__inode.ctime, size=size,
                    mode=33060)
            self.__inode = new_i
            self.__dirty = True
        else:
            new_i = self.__inode
            new_i.atime = now
            new_i.mtime = now
            new_i.size = size
        tmp_list = []
        pos = 0
        i_start = int(offset / self.BLOCK_SIZE)
        b_offset = offset - (i_start * self.BLOCK_SIZE)
        i_end = int((offset + len(buf)) / self.BLOCK_SIZE)
        b_len = offset + len(buf) - (i_end * self.BLOCK_SIZE)
        for d in self.__list[:i_start-1]:
            tmp_list.append(d)
        tmp_buf = self.__list[i_start].data
        if i_start == i_end:
            tmp_buf[b_offset:b_len] = buf[:]
        else:
            tmp_buf[b_offset:] = buf[:b_offset]
        tmp_list.append(self.__write_block(tmp_buf))
        tmp_buf = buf[b_offset:]
        for d in self.__list[i_start+1:i_end]:
            tb = tmp_buf[:self.BLOCK_SIZE]
            tmp_list.append(self.__write_block(tb))
            tmp_buf = tmp_buf[self.BLOCK_SIZE:]
        if not i_start == i_end:
            tb = self.__list[i_end].data
            tb[len(tmp_buf):] = tmp_buf
            tmp_list.append(self.__write_block(tb))
        for d in self.__list[i_end+1:]:
            tmp_list.append(d)
        new_i.syncUpdate()
        self.__list = tmp_list
        self.__lock.release()

    def write_w_t(self, buf, offset):
        """ä¸€æ™‚ãƒ•ã‚¡ã‚¤ãƒ«ã‚’çµŒç”±ã—ã¦æ›¸ãè¾¼ã¿"""
        self.__read_list()
        import tempfile
        tf = tempfile.TemporaryFile()
        now = time.time()
        size = offset + len(buf)
        if size < self.__inode.size:
            size = self.__inode.size
        self.__lock.acquire()
        if not self.__dirty:
            max_rev = Inode.selectBy(inode_num=self.__inode.inode_num).\
                                                            max("rev_id")
            if not max_rev == self.__inode.rev_id:
                raise FileSystemError("You use old file.")
            new_i = Inode(inode_num=self.__inode.inode_num,
                    rev_id=(max_rev+1), uid=self.__inode.uid,
                    gid=self.__inode.gid, atime=now,
                    mtime=now, ctime=self.__inode.ctime, size=size,
                    mode=33060)
            self.__inode = new_i
            self.__dirty = True
        else:
            new_i = self.__inode
            new_i.atime = now
            new_i.mtime = now
            new_i.size = size
        tmp_list = []
        if len(self.__list) < 10:
            for d in self.__list:
                tf.write(d.data)
            tf.seek(offset)
            tf.write(buf)
            tf.seek(0)
            b_count = int(size / self.BLOCK_SIZE)
            if size > b_count * self.BLOCK_SIZE:
                b_count += 1
            for i in range(b_count):
                tb = tf.read(self.BLOCK_SIZE)
                if len(tb) > self.BLOCK_SIZE:
                    tb = tb + "\0" * (self.BLOCK_SIZE-len(tb))
                sd = RawData.selectBy(hash_sha256=\
                        hashlib.sha256(tb).hexdigest())
                if sd.count() == 0:
                    d = RawData(hash_sha256=hashlib.sha256(tb).hexdigest(),
                            data=tb)
                    tmp_list.append(d)
                else:
                    tmp_list.append(sd[0])
        else:
            i_start = int(offset / self.BLOCK_SIZE)
            b_offset = offset - (i_start * self.BLOCK_SIZE)
            i_end = int((offset + len(buf)) / self.BLOCK_SIZE)
            b_len = offset + len(buf) - (i_end * self.BLOCK_SIZE)
            if blen > 0:
                i_end += 1
            for d in self.__list[i_start:i_end]:
                tf.write(d.data)
            tf.seek(b_offset)
            tf.write(buf)
            tf.seek(0)
            tmp_list = self.__list[:]
            if i_end < len(self.__list):
                for i in range(i_start, i_end+1):
                    tb = tf.read(self.BLOCK_SIZE)
                    tmp_list[i] = self.__write_block(tb)
            else:
                for i in range(i_start, len(self.__list)):
                    tb = tf.read(self.BLOCK_SIZE)
                    tmp_list[i] = self.__write_block(tb)
                for i in range(len(self.__list), i_end+1):
                    tb = tf.read(self.BLOCK_SIZE)
                    tmp_list.append(self.__write_block(tb))
        new_i.syncUpdate()
        self.__list = tmp_list
        self.__lock.release()

    def write(self, buf, offset):
        return self.write_w_t(buf, offset)

    def get_entry(self):
        return self.__inode

    def truncate(self, size):
        self.__read_list()
        now = time.time()
        self.__read_list()
        self.__lock.acquire()
        if not self.__dirty:
            max_rev = Inode.selectBy(inode_num=self.__inode.inode_num).\
                                                            max("rev_id")
            if not max_rev == self.__inode.rev_id:
                raise FileSystemError("You use old file.", \
                        max_rev, self.__inode.rev_id)
            new_i = Inode(inode_num=self.__inode.inode_num,
                    rev_id=(max_rev+1), uid=self.__inode.uid,
                    gid=self.__inode.gid, atime=now,
                    mtime=now, ctime=self.__inode.ctime, size=size,
                    mode=self.__inode.mode)
            self.__inode = new_i
            self.__dirty = True
        else:
            new_i = self.__inode
            new_i.atime = now
            new_i.mtime = now
            new_i.size = size
        new_i.syncUpdate()
        self.__lock.release()

    def chmod(self, mode):
        self.__read_list()
        now = time.time()
        self.__read_list()
        self.__lock.acquire()
        if not self.__dirty:
            max_rev = Inode.selectBy(inode_num=self.__inode.inode_num).\
                                                            max("rev_id")
            if not max_rev == self.__inode.rev_id:
                raise FileSystemError("You use old file.")
            new_i = Inode(inode_num=self.__inode.inode_num,
                    rev_id=(max_rev+1), uid=self.__inode.uid,
                    gid=self.__inode.gid, atime=now,
                    mtime=now, ctime=self.__inode.ctime, size=self.__inode.size,
                    mode=mode)
            self.__inode = new_i
            self.__dirty = True
        else:
            new_i = self.__inode
            new_i.atime = now
            new_i.mtime = now
            new_i.mode = mode
        new_i.syncUpdate()
        self.__lock.release()

    def chown(self, uid, gid):
        self.__read_list()
        now = time.time()
        self.__read_list()
        self.__lock.acquire()
        if not self.__dirty:
            max_rev = Inode.selectBy(inode_num=self.__inode.inode_num).\
                                                            max("rev_id")
            if not max_rev == self.__inode.rev_id:
                raise FileSystemError("You use old file.")
            new_i = Inode(inode_num=self.__inode.inode_num,
                    rev_id=(max_rev+1), uid=uid,
                    gid=gid, atime=now,
                    mtime=now, ctime=self.__inode.ctime, size=self.__inode.size,
                    mode=self.__inode.mode)
            self.__inode = new_i
            self.__dirty = True
        else:
            new_i = self.__inode
            new_i.atime = now
            new_i.mtime = now
            new_i.uid = uid
            new_i.gid = gid
        new_i.syncUpdate()
        self.__lock.release()

    def fsync(self):
        self.__lock.acquire()
        self.__inode.syncUpdate()
        for i, v in enumerate(self.__list):
            DataList(parent=self.__inode, series=i, data=v)
        self.__dirty = False
        self.__lock.release()


class DBDumpFS:
    """
    This is the class for interactions with the dumpfs.sqlite DB.
    Holds all of the overwritten functions used to modify the filesystem.
    """

    BLOCK_SIZE = 32*1024

    def __init__(self, db_scheme=""):
        """
        Connects to and opens DB if exists, creates it if it doesn't
        """
        if not db_scheme:
            import tempfile
            self.__tf = tempfile.NamedTemporaryFile()
            db_scheme = "sqlite:" + self.__tf.name
        conn = connectionForURI(db_scheme)
        sqlhub.processConnection = conn
        Inode.createTable(ifNotExists=True)
        Dentry.createTable(ifNotExists=True)
        RawData.createTable(ifNotExists=True)
        DataList.createTable(ifNotExists=True)
        self.__init_root()
        self.__openfiles = dict()

    def __init_root(self):
        """
        Creates the root node within the database
        """
        if not list(Inode.selectBy(inode_num=0)):
            now = time.time()
            root_node = Inode(inode_num=0, rev_id=0,
                    uid=os.getuid(), gid=os.getgid(),
                    atime=now, ctime=now, mtime=now,
                    size=0, mode=stat.S_IFDIR|0o755)

    def __get_parent_inode(self, path):
        """
        Returns the inode of parent directory.
        Raises error if on root.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        """
        if path == "/":
            raise FileSystemError("No parent directory.")
        parent_dir = Inode.selectBy(inode_num=0).orderBy("-rev_id")[0]
        for fn in split_path(path)[:-1]:
            tmp = Dentry.selectBy(parent=parent_dir, filename=fn)
            if tmp.count() == 0:
                raise FileSystemError("file not found.")
            parent_dir = Inode.selectBy(inode_num=tmp[0].inode_num).\
                    orderBy("-rev_id")[0]
        return parent_dir.inode_num

    def __get_inode(self, path):
        """
        Returns the inode of the object (file/directory).
        Raises an error if the object doesn't exist.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path. 
    
        """
        if path == "/":
            return 0
        parent_dir = Inode.selectBy(inode_num=self.__get_parent_inode(path)).\
                orderBy("-rev_id")[0]
        tmp = Dentry.selectBy(parent=parent_dir,
                filename=split_path(path)[-1])
        if tmp.count() == 0:
            raise FileSystemError("file not found.:",path)
        ret = Inode.selectBy(inode_num=tmp[0].inode_num).orderBy("-rev_id")[0]
        return ret.inode_num

    def fsync(self):
        """
        Ensures all files in VFile are up-to-date.
        Not sure on actual useage.
        """
        for vfile in self.__openfiles.values():
            vfile.fsync()

    def stat(self, path):
        """
        Returns the inode table entry of the object.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        
        """
        if path in self.__openfiles:
            inode_ent = self.__openfiles[path].get_entry()
        else:
            inode = self.__get_inode(path)
            inode_ent = Inode.selectBy(inode_num=inode).orderBy("-rev_id")[0]
        return inode_ent

    def readdir(self, path):
        """
        Reads the directory and returns a list of the items within it.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        
        """
        inode = self.__get_inode(path)
        inode_ent = Inode.selectBy(inode_num=inode).orderBy("-rev_id")[0]
        if not stat.S_ISDIR(inode_ent.mode):
            raise FileSystemError("Not a directory.")
        return list(Dentry.selectBy(parent=inode_ent))

    def mknod(self, path, mode, dev):
        """
        Creates a new file/directory on path

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        mode: interger, file permissions of object
        dev: interger, no idea as I just leave it 0
        """
        try:
            self.__get_inode(path)
        except FileSystemError:
            parent_i_num = self.__get_parent_inode(path)
            parent_i = Inode.selectBy(inode_num=parent_i_num).\
                    orderBy("-rev_id")[0]
            now = time.time()
            inode_num = Inode.select().max("inode_num") + 1
            conn = sqlhub.getConnection()
            trans = conn.transaction()
            ret = Inode(inode_num=inode_num, rev_id=0, uid=os.getuid(),
                    gid=os.getgid(), atime=now, ctime=now,
                    mtime=now, size=0, mode=mode, dev=dev, connection=trans)
            Dentry(parent=parent_i, inode_num=inode_num,
                    filename=split_path(path)[-1], connection=trans)
            trans.commit()
            return ret.inode_num

    def create(self, path, mode):
        """
        Calls mknod to create the file/directory on path.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        mode: interger, file permissions.
        """
        return self.mknod(path, mode, 0)

    def mkdir(self, path, mode):
        """
        Call create to make a directory.
        """
        return self.create(path, mode|stat.S_IFDIR)

    def rmdir(self, path):
        """
        Call remove to remove directory.
        """
        return self.remove(path)

    def open(self, path, flags, mode=0):
        """
        Opens a file for reading and writing depending on permissions.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        flags: binary?, standard flags for file creation.
        mode: octal, used to store file permissions.
        """
        if mode == 0:
            mode = 0o644|stat.S_IFREG
        if flags&os.O_CREAT == os.O_CREAT:
            self.create(path, mode)
        T = atpy.Table(SERVER_LOCATION + 'QUERY?query=files_list&format=list',type='ascii')
        if not (path[1:] in T['col3']):
            if path in self.__openfiles:
                self.__openfiles[path].open()
            else:
                inode = self.__get_inode(path)
                inode_ent = Inode.selectBy(inode_num=inode).orderBy("-rev_id")[0]
                inode_ent.mode = 33060 #Sets the file to read-only mode before opening it.
                self.__openfiles[path]=VFile(inode_ent)
                self.__openfiles[path].open()

    def close(self, path):
        """
        Closes the file/directory

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        """
        if path in self.__openfiles:
            self.__openfiles[path].close()
            if self.__openfiles[path].is_close():
                del self.__openfiles[path]

    def read(self, path, length, offset):
        """
        Reads the selected file.
        If the file is from the server it is downloaded and read without being saved locally.
        If it is a local file, it is read locally.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        length: interger?, amount of the file to read.
        offset: interger?, starting position in file (invalid for reading from server.
        """

        #Create a list of available files
        T = atpy.Table(SERVER_LOCATION + 'QUERY?query=files_list&format=list',type='ascii')
        #Check is file is local
        if not (path[1:] in T['col3']):
            if not path in self.__openfiles:
                self.open(path, 0)
            return self.__openfiles[path].read(length, offset)
        #File is not local so open and read from server
        else:
            urlString = SERVER_LOCATION + 'RETRIEVE?file_id=' + path[1:]
            ht = None
            ht = urllib2.urlopen(urlString)
            return ht.read(length)

    def write(self, path, buf, offset):
        """
        Writes a specified file.
        NOTE: At current implementation, all files are READ-ONLY

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        buf: string?, contains data to be written to file.
        offset: interger?, position in file to start writing.
        """
        if not path in self.__openfiles:
            self.open(path, 0)
        return sqlhub.doInTransaction(self.__openfiles[path].write,
                buf, offset)

    def remove(self, path):
        """
        Removes the file from the directory and database.
        NOTE: This funtion should not be used and therefore has been removed.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        """
        #Create a list of available files
        T = atpy.Table(SERVER_LOCATION + 'QUERY?query=files_list&format=list',type='ascii')
        #Check is file is local
        if not (path[1:] in T['col3']):
            now = time.time()
            i_num = self.__get_inode(path)
            parent_i_num = self.__get_parent_inode(path)
            parent_i = Inode.selectBy(inode_num=parent_i_num).orderBy("-rev_id")[0]
            dl = Dentry.selectBy(parent=parent_i)
            conn = sqlhub.getConnection()
            trans = conn.transaction()
            new_i = Inode(inode_num=parent_i.inode_num,
                    rev_id=parent_i.rev_id+1,
                    uid=parent_i.uid, gid=parent_i.gid,
                    atime=now, mtime=parent_i.mtime,
                    ctime=parent_i.ctime, size=parent_i.size,
                    mode=parent_i.mode, connection=trans)
            for de in dl:
                if de.inode_num != i_num:
                    Dentry(parent=new_i, filename=de.filename,
                            inode_num=de.inode_num, connection=trans)
            trans.commit()
            if path in self.__openfiles:
                while not self.__openfiles[path].is_close():
                    self.__openfiles[path].close()
                del self.__openfiles[path]
        #If not local then file is from server and removal is denied.
        else:
            pass

    def rename(self, oldPath, newPath):
        """
        Removes an object from oldPath and places it on newPath
        NOTE: This function should not be used and has therefore been removed.

        self: object, object(file/directory) that the function was called on.
        oldPath: string, current path of object.
        newPath: string, new path of object.

        """
        
        conn = sqlhub.getConnection()
        trans = conn.transaction()
        now = time.time()
        i_num = self.__get_inode(oldPath)
        parent_i_num = self.__get_parent_inode(oldPath)
        parent_i = Inode.selectBy(inode_num=parent_i_num).orderBy("-rev_id")[0]
        dl = Dentry.selectBy(parent=parent_i)
        new_i = Inode(inode_num=parent_i.inode_num,
                rev_id=parent_i.rev_id+1,
                uid=parent_i.uid, gid=parent_i.gid,
                atime=now, mtime=parent_i.mtime,
                ctime=parent_i.ctime, size=parent_i.size,
                mode=parent_i.mode, connection=trans)
        for de in dl:
            if de.inode_num != i_num:
                Dentry(parent=new_i, filename=de.filename,
                        inode_num=de.inode_num, connection=trans)
        parent_i_num = self.__get_parent_inode(newPath)
        parent_i = Inode.selectBy(inode_num=parent_i_num).orderBy("-rev_id")[0]
        Dentry(parent=new_i, filename=split_path(newPath)[-1],
                inode_num=i_num, connection=trans)
        old_i = Inode.selectBy(inode_num=i_num).orderBy("-rev_id")[0]
        Inode(inode_num=old_i.inode_num,
                rev_id=old_i.rev_id+1,
                uid=old_i.uid, gid=old_i.gid,
                atime=now, mtime=old_i.mtime,
                ctime=old_i.ctime, size=old_i.size,
                mode=old_i.mode, connection=trans)
        trans.commit()
        if oldPath in self.__openfiles:
            while not self.__openfiles[oldPath].is_close():
                self.__openfiles[oldPath].close()
            del self.__openfiles[oldPath]
        

    def chmod(self, path, mode):
        """
        Changes access permissions
        NOTE: Doesn't work.
        """
        if path in self.__openfiles:
            self.__openfiles[path].chmod(mode)
        else:
            inode_num = self.__get_inode(path)
            old_i = Inode.selectBy(inode_num=inode_num).orderBy("-rev_id")[0]
            vfile = VFile(old_i)
            vfile.chmod(mode)
            vfile.close()

    def chown(self, path, uid, gid):
        """
        Changes ownership of object.
        NOTE: Doesn't work, states invalid permissions even if run via root.
        """
        if path in self.__openfiles:
            self.__openfiles[path].chown(uid,gid)
        else:
            inode_num = self.__get_inode(path)
            old_i = Inode.selectBy(inode_num=inode_num).orderBy("-rev_id")[0]
            vfile = VFile(old_i)
            vfile.chown(uid, gid)
            vfile.close()

    def truncate (self, path, size):
        """
        Changes the size of the object.
        NOTE: Doesn't seem to work.
        """
        if path in self.__openfiles:
            self.__openfiles[path].truncate(size)
        else:
            inode_num = self.__get_inode(path)
            old_i = Inode.selectBy(inode_num=inode_num).orderBy("-rev_id")[0]
            vfile = VFile(old_i)
            vfile.truncate(size)
            vfile.close()

    def flush(self, path):
        """
        Syncs the object if it has been opened.

        self: object, object(file/directory) that the function was called on.
        path: string, current working directory path.
        """
        if path in self.__openfiles:
            self.__openfiles[path].fsync()

    def link(self, oldPath, newPath):
        """
        Creates a link from the object at oldPath to an object at newPath.
        NOTE: This does not work on items from the server, the link is unreadable.

        self: object, object(file/directory) that the function was called on.
        oldPath: string, path of original file
        newPath, string, path of link file

        """
        conn = sqlhub.getConnection()
        trans = conn.transaction()
        now = time.time()
        i_num = self.__get_inode(oldPath)
        parent_i_num = self.__get_parent_inode(newPath)
        parent_i = Inode.selectBy(inode_num=parent_i_num).orderBy("-rev_id")[0]
        dl = Dentry.selectBy(parent=parent_i)
        new_i = Inode(inode_num=parent_i.inode_num,
                rev_id=parent_i.rev_id+1,
                uid=parent_i.uid, gid=parent_i.gid,
                atime=now, mtime=parent_i.mtime,
                ctime=parent_i.ctime, size=parent_i.size,
                mode=parent_i.mode, connection=trans)
        for de in dl:
            Dentry(parent=new_i, filename=de.filename,
                        inode_num=de.inode_num, connection=trans)
        Dentry(parent=new_i, filename=split_path(newPath)[-1],
                inode_num=i_num, connection=trans)
        trans.commit()

    def symlink(self, oldPath, newPath):
        """
        Creates a symbolic link from oldPath to newPath.
        NOTE: Raises I/O error.
        """
        mode = 0o644|stat.S_IFLNK
        i_num = self.mknod(newPath, mode, 0)
        self.write(newPath, oldPath, 0)

    def readlink(self, path):
        """
        Reads a symlink.
        NOTE: Can't create symlinks.
        """
        self.open(path, 0)
        a = self.stat(path)
        if (a.mode&stat.S_IFLNK) == stat.S_IFLNK:
            return self.read(path, a.size, 0)
        raise FileSystemError("Not symlink")


class SqliteDumpFS(Fuse):
    """
    Honestly not sure what this is for, have left it unedited and haven't had it interfering.
    """
    
    block_size = 32*1024

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.db_path = "./dumpfs.sqlite"
        self.__backend = DBDumpFS()

    def main(self, *args, **kw):
        import os
        fullpath = os.path.abspath(os.path.expanduser(os.path.expandvars(
            self.db_path)))
        self.__backend = DBDumpFS("sqlite:"+fullpath)
        Fuse.main(self, *args, **kw)

    def getattr(self, path):
        print "*** getattr :", path
        try:
            inode = self.__backend.stat(path)
        except FileSystemError:
            return -errno.ENOENT
        st = PseudoStat()
        st.st_atime = inode.atime
        st.st_ctime = inode.ctime
        st.st_dev = inode.dev
        st.st_gid = inode.gid
        st.st_ino = inode.inode_num
        st.st_mode = inode.mode
        st.st_mtime = inode.mtime
        if inode.mode&stat.S_IFDIR:
            st.st_nlink = 2
        else:
            st.st_nlink = 1
        st.st_size = inode.size
        st.st_uid = inode.uid
        return st

    def readdir(self, path, offset):
        print "*** readdir", path, offset
        rets = [fuse.Direntry("."), fuse.Direntry("..")]
        dlist = self.__backend.readdir(path)
        for de in dlist:
            ret = fuse.Direntry(str(de.filename))
            ret.ino = de.inode_num
            rets.append(ret)
        return rets[offset:]

    def mythread(self):
        """
        ä½•ã®ãŸã‚ã®ãƒ¡ã‚½ãƒƒãƒ‰ã‹ä¸æ˜Ž
        """
        print "*** mythread"
        return -errno.ENOSYS

    def chmod(self, path, mode):
        """
        åŒåã®ã‚³ãƒžãƒ³ãƒ‰ã¨ã»ã¼åŒã˜
        """
        print "*** chmod :", path, oct(mode)
        self.__backend.chmod(path, mode)
        return 0

    def chown (self, path, uid, gid):
        print '*** chown', path, uid, gid
        self.__backend.chown(path, uid, gid)
        return 0

    def fsync (self, path, isFsyncFile):
        print '*** fsync', path, isFsyncFile
        self.__backend.fsync()
        return 0


    def link (self, targetPath, linkPath):
        """
        ãƒãƒ¼ãƒ‰ãƒªãƒ³ã‚¯ã®ç”Ÿæˆ
        """
        print '*** link', targetPath, linkPath
        self.__backend.link(targetPath, linkPath)
        return 0

    def mkdir (self, path, mode):
        """
        ãƒ‡ã‚£ãƒ¬ã‚¯ãƒˆãƒªã®ç”Ÿæˆ
        """
        print '*** mkdir', path, oct(mode)
        self.__backend.mkdir(path, mode)
        return 0

    def mknod (self, path, mode, dev):
        """
        pathã§æŒ‡å®šã•ã‚ŒãŸãƒ‘ã‚¹ã‚’æŒã¤ãƒ•ã‚¡ã‚¤ãƒ«ã‚’ç”Ÿæˆã™ã‚‹
        modeãŒS_IFCHRã¨S_IFBLKä»¥å¤–ãªã‚‰devã¯ç„¡è¦–ã™ã‚‹
        S_IFCHRã¨S_IFBLKãªã‚‰devã¯ä½œæˆã™ã‚‹ãƒ‡ãƒã‚¤ã‚¹ãƒ•ã‚¡ã‚¤ãƒ«ã®
        ãƒ¡ã‚¸ãƒ£ãƒ¼ç•ªå·ã¨ãƒžã‚¤ãƒŠãƒ¼ç•ªå·
        """
        print '*** mknod', path, oct(mode), dev
        self.__backend.mknod(path, mode, dev)
        return 0

    def create(self, path, flags, mode):
        print '*** create', path, flags, oct(mode)
        self.__backend.create(path, mode)
        return 0

    def open (self, path, flags):
        self.__backend.open(path, flags)
        return 0

    def read (self, path, length, offset):
        """
        DBã‹ã‚‰è©²å½“éƒ¨åˆ†ã‚’å–ã‚Šå‡ºã™
        """
        print '*** read', path, length, offset
        return self.__backend.read(path, length, offset)

    def flush(self, path):
        print '*** flush', path
        self.__backend.flush(path)
        return 0

    def readlink (self, path):
        """
        symlinkã®ãƒªãƒ³ã‚¯å…ˆã®æŽ¢ç´¢
        """
        print '*** readlink', path
        return self.__backend.readlink(path)

    def release (self, path, flags):
        """
        openfilesã‹ã‚‰è©²å½“ã‚¨ãƒ³ãƒˆãƒªã®å‰Šé™¤
        dirtyãƒ•ãƒ©ã‚°ãŒç«‹ã£ã¦ã„ã‚‹ãªã‚‰DBã¸ã®æ›¸ãè¾¼ã¿ã‚‚è¡Œã†
        """
        print "*** release :", path, flags
        self.__backend.close(path)
        return 0

    def rename (self, oldPath, newPath):
        """
        åå‰ã®å¤‰æ›´
        """
        print '*** rename', oldPath, newPath
        self.__backend.rename(oldPath, newPath)
        return 0

    def rmdir (self, path):
        """
        ãƒ‡ã‚£ãƒ¬ã‚¯ãƒˆãƒªã®å‰Šé™¤
        """
        print '*** rmdir', path
        self.__backend.remove(path)
        return 0

    def statfs (self):
        """
        ãƒ•ã‚¡ã‚¤ãƒ«ã‚·ã‚¹ãƒ†ãƒ è‡ªä½“ã®stat
        ä»¥ä¸‹ã®6ã¤ã®è¦ç´ ã‚’æŒã£ãŸã‚¿ãƒ—ãƒ«ã‚’è¿”ã™;
         ãƒ–ãƒ­ãƒƒã‚¯ã‚µã‚¤ã‚ºã€€ãƒã‚¤ãƒˆå˜ä½ã®ãƒ–ãƒ­ãƒƒã‚¯ã‚µã‚¤ã‚º
         å…¨ãƒ–ãƒ­ãƒƒã‚¯æ•°ã€€ç¢ºä¿ã—ã¦ã„ã‚‹ãƒ–ãƒ­ãƒƒã‚¯æ•°
         ç©ºããƒ–ãƒ­ãƒƒã‚¯æ•°ã€€ä½¿ã‚ã‚Œã¦ã„ãªã„ãƒ–ãƒ­ãƒƒã‚¯æ•°
         å…¨ãƒ•ã‚¡ã‚¤ãƒ«æ•°ã€€ç¢ºä¿ã—ã¦ã„ã‚‹i-nodeæ•°
         ç©ºããƒ•ã‚¡ã‚¤ãƒ«æ•°ã€€ä½¿ç”¨ã—ã¦ã„ãªã„i-nodeæ•°
         ãƒ•ã‚¡ã‚¤ãƒ«åã®é•·ã•ã€€ãƒ•ã‚¡ã‚¤ãƒ«åã«ä½¿ãˆã‚‹æœ€å¤§é•·ã•
        æœªå®šç¾©ãªã‚‰ãã®è¦ç´ ã‚’0ã«ã—ã¦è¿”ã™
        """
        print '*** statfs'
        stvfs = fuse.StatVfs()
        stvfs.f_bsize = self.block_size
        return stvfs

    def symlink (self, targetPath, linkPath):
        """
        symlinkã®ç”Ÿæˆ
        """
        print '*** symlink', targetPath, linkPath
        return self.__backend.symlink(targetPath, linkPath)

    def truncate (self, path, size):
        """
        æŒ‡å®šã‚µã‚¤ã‚ºã«ãƒ•ã‚¡ã‚¤ãƒ«ã‚’åˆ‡ã‚Šè©°ã‚ã‚‹
        """
        print '*** truncate', path, size
        self.__backend.truncate(path, size)
        return 0

    def unlink (self, path):
        """
        ãƒ•ã‚¡ã‚¤ãƒ«ã‚’å‰Šé™¤ã™ã‚‹
        d_dataã‹ã‚‰è©²å½“ãƒ•ã‚¡ã‚¤ãƒ«åã‚’æ¶ˆã™
        """
        print '*** unlink', path
        try:
            self.__backend.remove(path)
            return 0
        except FileSystemError:
            return -errno.ENOENT

    def utime (self, path, times):
        """
        utimeã®ä¿®æ­£
        """
        print '*** utime', path, times
        return -errno.ENOSYS

    def write (self, path, buf, offset):
        print '*** write', path, len(buf), offset
        try:
            self.__backend.write(path, buf, offset)
            return len(buf)
        except FileSystemError:
            return 0

    def access(self, path, mode):
        """ã‚¢ã‚¯ã‚»ã‚¹æ¨©ã®ç¢ºèª"""
        print '*** access', path, oct(mode)
        try:
            inode = self.__backend.stat(path)
            return 0
        except FileSystemError:
            return -errno.ENOENT



def main():
    """
    Uses FUSE to mount the sqliteFS to the mount point.
    """
    usage = Fuse.fusage
    sdfs = SqliteDumpFS(version="%prog"+fuse.__version__,
            usage=usage,
            dash_s_do='setsingle')
    sdfs.parser.add_option(mountopt="db_path", metavar="PATH",default="")
    sdfs.parse(values=sdfs, errex=1)
    sdfs.main()

if __name__ == '__main__':
    main()


