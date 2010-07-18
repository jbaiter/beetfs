'''
		beetFs
		Copyright 2010 Martin Eve

		This file is part of beetFs.

		beetFs is free software: you can redistribute it and/or modify
		it under the terms of the GNU General Public License as published by
		the Free Software Foundation, either version 3 of the License, or
		(at your option) any later version.

		beetFs is distributed in the hope that it will be useful,
		but WITHOUT ANY WARRANTY; without even the implied warranty of
		MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the
		GNU General Public License for more details.

		You should have received a copy of the GNU General Public License
		along with beetFs.	If not, see <http://www.gnu.org/licenses/>.

'''

from beets.plugins import BeetsPlugin
from beets.ui import Subcommand
import beets
import logging
from string import Template
import operator
import os
import re
import fuse
import errno
import stat
import datetime
import time
import calendar
import mutagen
from mutagen import flac, id3
from mutagen.flac import FLAC, Padding, MetadataBlock
from mutagen.id3 import ID3

path_format = "$artist/$album ($year) [$format_upper]/$track - $artist - $title.$format"

beetFs_command = Subcommand('mount', help='Mount a beets filesystem')
log = logging.getLogger('beets')


# FUSE version at the time of writing. Be compatible with this version.
fuse.fuse_python_api = (0, 2)

""" This is duplicated from Library. Ideally should be exposed from there.
"""

metadata_rw_fields = [
		('title',			'text'),
		('artist',		 'text'),
		('album',			'text'),
		('genre',			'text'),
		('composer',	 'text'),
		('grouping',	 'text'),
		('year',			 'int'),
		('month',			'int'),
		('day',				'int'),
		('track',			'int'),
		('tracktotal', 'int'),
		('disc',			 'int'),
		('disctotal',	'int'),
		('lyrics',		 'text'),
		('comments',	 'text'),
		('bpm',				'int'),
		('comp',			 'bool'),
]
metadata_fields = [
		('length',	'real'),
		('bitrate', 'int'),
] + metadata_rw_fields

metadata_keys = map(operator.itemgetter(0), metadata_fields)


def template_mapping(lib, item):
		""" Builds a template substitution map. Taken from library.py.
		"""
		mapping = {}
		for key in metadata_keys:
				value = getattr(item, key)
				# sanitize the value for inclusion in a path:
				# replace / and leading . with _
				if isinstance(value, basestring):
						value.replace(os.sep, '_')
						value = re.sub(r'[\\/:]|^\.', '_', value)
				elif key in ('track', 'tracktotal', 'disc', 'disctotal'):
						# pad with zeros
						value = '%02i' % value
				else:
						value = str(value)
				mapping[key] = value
				
		format = os.path.splitext(item.path)[1][1:]
		mapping['format'] = re.sub(r'[\\/:]|^\.', '_', format)
		mapping['format_upper'] = re.sub(r'[\\/:]|^\.', '_', format).upper()
		
		# fix dud entries
		if mapping['artist'] == '':
			mapping['artist'] = 'Unknown Artist'

		if mapping['album'] == '':
			mapping['album'] = 'Unknown Album'

		if mapping['year'] == '0':
			mapping['year'] = 'Unknown Year'
			
		if mapping['title'] == '':
			mapping['title'] = 'Unknown Track'

		return mapping

def mount(lib, config, opts, args):
		# check we have a command line argument
		if not args:
			raise beets.ui.UserError('no mountpoint specified')
			
		# build the in-memory folder structure
		global structure_split 
		structure_split = path_format.split("/")
		global structure_depth 
		structure_depth = len(structure_split)
		root = {}
		templates = {}
		global library 
		library = lib
		
		# establish a blank dictionary at each level of depth
		# also create templates for each level
		for level in range(0, structure_depth):
			#log.debug("Creating template %s" % structure_split[level])
			templates[level] = Template(structure_split[level])
	
		global directory_structure 
		directory_structure = FSNode({}, {})
		
		# iterate over items in library
		for item in lib.items():
			# build the template map
			mapping = template_mapping(lib, item)

			# do the substitutions
			level_subbed = {}
			for level in range(0, structure_depth):
				level_subbed[level] = templates[level].substitute(mapping)

			# build a directory structure
			sub_elements = []
			for level in range(0, structure_depth - 1):
				# append the path up to the current depth, minus one, to elements
				# this means that sub_elements contains the current path, except for the folder to be added
				if level-1 in level_subbed:
					sub_elements.append(level_subbed[level-1])					
				# add this directory to the master structure
				directory_structure.adddir(sub_elements, level_subbed[level])		
		
			# add this item as a file
			
			# First, compensate for 2 directories depth taken off above
			# ie. sub_elements now contains the full folder path
			sub_elements.append(level_subbed[structure_depth-3])	
			sub_elements.append(level_subbed[structure_depth-2])

			# do the actual add
			directory_structure.addfile(sub_elements, level_subbed[structure_depth-1], item.id)
			
		server = beetFileSystem(version="%prog " + fuse.__version__,
				usage="", dash_s_do='setsingle')
		server.parse(errex=1)
		
		server.multithreaded = 0
		try:
				server.main()
				pass
		except fuse.FuseError, e:
				log.error(str(e))
		

beetFs_command.func = mount

class beetFs(BeetsPlugin):
		""" The beets plugin hook
		"""
		def commands(self):
				return [beetFs_command]
				
import mutagen
from mutagen import flac
from mutagen.flac import FLAC, Padding, MetadataBlock

def to_int_be(string):
	"""Convert an arbitrarily-long string to a long using big-endian
	byte order."""
	return reduce(lambda a, b: (a << 8) + ord(b), string, 0L)

class InterpolatedID3 (ID3):
    def save(self, filename=None, v1=0):
        """Save changes to a file.

        If no filename is given, the one most recently loaded is used.

        Keyword arguments:
        v1 -- if 0, ID3v1 tags will be removed
              if 1, ID3v1 tags will be updated but not added
              if 2, ID3v1 tags will be created and/or updated

        The lack of a way to update only an ID3v1 tag is intentional.
        """

        # Sort frames by 'importance'
        order = ["TIT2", "TPE1", "TRCK", "TALB", "TPOS", "TDRC", "TCON"]
        order = dict(zip(order, range(len(order))))
        last = len(order)
        frames = self.items()
        frames.sort(lambda a, b: cmp(order.get(a[0][:4], last),
                                     order.get(b[0][:4], last)))

        framedata = [self.__save_frame(frame) for (key, frame) in frames]
        framedata.extend([data for data in self.unknown_frames
                if len(data) > 10])
        

        framedata = ''.join(framedata)
        framesize = len(framedata)

        if filename is None: filename = self.filename
        f = open(filename, 'rb+')
        try:
            idata = f.read(10)
            try: id3, vmaj, vrev, flags, insize = unpack('>3sBBB4s', idata)
            except struct.error: id3, insize = '', 0
            insize = BitPaddedInt(insize)
            if id3 != 'ID3': insize = -10

            if insize >= framesize: outsize = insize
            else: outsize = (framesize + 1023) & ~0x3FF
            framedata += '\x00' * (outsize - framesize)

            framesize = BitPaddedInt.to_str(outsize, width=4)
            flags = 0
            header = pack('>3sBBB4s', 'ID3', 4, 0, flags, framesize)
            data = header + framedata

            if (insize < outsize):
                insert_bytes(f, outsize-insize, insize+10)
            f.seek(0)

            try:
                f.seek(-128, 2)
            except IOError, err:
                from errno import EINVAL
                if err.errno != EINVAL: raise
                f.seek(0, 2) # ensure read won't get "TAG"

            if f.read(3) == "TAG":
                f.seek(-128, 2)
                if v1 > 0: f.write(MakeID3v1(self))
                else: f.truncate()
            elif v1 == 2:
                f.seek(0, 2)
                f.write(MakeID3v1(self))

        finally:
            f.close()

class InterpolatedFLAC (FLAC):
			def get_header(self, filename=None):
				
				if filename == None:
					filename = self.filename
					
				f = open(filename, 'rb+')

				# Ensure we've got padding at the end, and only at the end.
				# If adding makes it too large, we'll scale it down later.
				self.metadata_blocks.append(Padding('\x00' * 1020))
				MetadataBlock.group_padding(self.metadata_blocks)

				header = self.__check_header(f)
				available = self.__find_audio_offset(f) - header # "fLaC" and maybe ID3
				data = MetadataBlock.writeblocks(self.metadata_blocks)

				if len(data) > available:
						# If we have too much data, see if we can reduce padding.
						padding = self.metadata_blocks[-1]
						newlength = padding.length - (len(data) - available)
						if newlength > 0:
								padding.length = newlength
								data = MetadataBlock.writeblocks(self.metadata_blocks)
								assert len(data) == available
								
				elif len(data) < available:
						# If we have too little data, increase padding.
						self.metadata_blocks[-1].length += (available - len(data))
						data = MetadataBlock.writeblocks(self.metadata_blocks)
						assert len(data) == available

				if len(data) != available:
						# We couldn't reduce the padding enough.
						diff = (len(data) - available)
						#insert_bytes(f, diff, header)
						
				f.close()

				self.__offset = len("fLaC" + data)

				return("fLaC" + data)	 
				
			def offset(self):
				return self.__offset
				
			def __find_audio_offset(self, fileobj):
				byte = 0x00
				while not (byte >> 7) & 1:
						byte = ord(fileobj.read(1))
						size = to_int_be(fileobj.read(3))
						fileobj.read(size)
				return fileobj.tell()

			def __check_header(self, fileobj):
				size = 4
				header = fileobj.read(4)
				if header != "fLaC":
						size = None
						if header[:3] == "ID3":
								size = 14 + BitPaddedInt(fileobj.read(6)[2:])
								fileobj.seek(size - 4)
								if fileobj.read(4) != "fLaC": size = None
				if size is None:
						raise FLACNoHeaderError(
								"%r is not a valid FLAC file" % fileobj.name)
				return size
				
class FSNode(object):
		"""A directory node. Contains directories (as a dictionary keyed
		by directory name) and files (dictionary keyed by filename to id).
		"""
		def __init__(self, dirs, files):
				self.dirs = dirs
				self.files = files

		def getnode(self, elements, root=None):
				if root == None:
					root = self
					
				if elements:
						topdir = elements.pop(0)
						return self.getnode(elements, root=root.dirs[topdir])
				else:
						# base case
						return root

		def adddir(self, elements, directory, root=None):
				if root == None:
					root = self
					
				if len(elements) == 1 and elements[0] == '':
						elements = []
				node = self.getnode(elements, root=root)
				if not directory in node.dirs:
					node.dirs[directory] = FSNode({}, {})
				
		def addfile(self, elements, filename, id, root=None):
				if root == None:
					root = self
					
				if len(elements) == 1 and elements[0] == '':
						elements = []
				node = self.getnode(elements, root=root)
				node.files[filename] = id

		def listdir(self, elements, directories, root=None):
				if root == None:
					root = self
					
				if len(elements) == 1 and elements[0] == '':
						elements = []
				node = self.getnode(elements, root=root)
				if directories:
					return node.dirs.keys()
				else:
					return node.files.keys()
					
class FileHandler(object):
	def __init__(self, path, lib):
		self.path = path
		self.lib = lib
		
		pathsplit = path[1:].split('/')		
		subdepth = path.count('/') + 1
			
		# determine the real path
		items = self.lib.get_path(id=directory_structure.getnode(pathsplit[0:structure_depth-1]).files[pathsplit[structure_depth-1]])
		self.real_path = items[0]
		
		# open the on-disk file for reading
		self.file_object = open(self.real_path, 'r+')
		self.instance_count = 1
		
		# now get the bounds of the file_class
		#TODO: this needs to handle other file formats; use mutagen's detection procedure
		format = os.path.splitext(path)[1][1:].lower()
		#logging.info(self.real_path)
		if format == "flac":
			self.inf = InterpolatedFLAC(self.real_path)
			self.header = self.inf.get_header(self.real_path)
			self.bound = len(self.header)
			self.music_offset = self.inf.offset()
		elif format == "mp3":
			pass
		
	def open(self):
		# as init() handles actual opening, just increment instance count here
		self.instance_count = self.instance_count + 1

	def release(self):
		# decrement the instance count
		if self.instance_count > 0:
			self.instance_count = self.instance_count - 1
			return False
		else:
			# if instance count is zero, close the physical file on disk
			self.file_object.close()
			return True
			
	def read(self, size, offset):
		# check if read is within header boundary
		if offset < self.bound:
			
			if offset + size < len(self.header):
			# can just read from header
				ret = self.header[offset:offset+size]
				return ret
			else:
				# get the header + some data from file
				ret = self.header[offset:len(self.header)]
				self.file_object.seek(self.music_offset)
				ret = ret + self.file_object.read(size-(len(self.header) - offset))
				return ret
		
		# otherwise, pass read call to underlying file system
		self.file_object.seek(offset)
		return self.file_object.read(size)
		
	def write(self, offset, buf):
		# determine if offset is within header; if not, discard write
		if offset < self.bound:
			pass
					
class Stat(fuse.Stat):
		DIRSIZE = 4096

		def __init__(self, st_mode, st_size, st_nlink=1, st_uid=None, st_gid=None,
						dt_atime=None, dt_mtime=None, dt_ctime=None):

				self.st_mode = st_mode
				self.st_ino = 0
				self.st_dev = 0
				self.st_nlink = st_nlink
				if st_uid is None:
						st_uid = os.getuid()
				self.st_uid = st_uid
				if st_gid is None:
						st_gid = os.getgid()
				self.st_gid = st_gid
				self.st_size = st_size
				now = datetime.datetime.utcnow()
				self.dt_atime = dt_atime or now
				self.dt_mtime = dt_mtime or now
				self.dt_ctime = dt_ctime or now

		def _get_dt_atime(self):
				return self.epoch_datetime(self.st_atime)
		def _set_dt_atime(self, value):
				self.st_atime = self.datetime_epoch(value)
		dt_atime = property(_get_dt_atime, _set_dt_atime)

		def _get_dt_mtime(self):
				return self.epoch_datetime(self.st_mtime)
		def _set_dt_mtime(self, value):
				self.st_mtime = self.datetime_epoch(value)
		dt_mtime = property(_get_dt_mtime, _set_dt_mtime)

		def _get_dt_ctime(self):
				return self.epoch_datetime(self.st_ctime)
		def _set_dt_ctime(self, value):
				self.st_ctime = self.datetime_epoch(value)
		dt_ctime = property(_get_dt_ctime, _set_dt_ctime)

		@staticmethod
		def datetime_epoch(dt):
				return calendar.timegm(dt.timetuple())
		@staticmethod
		def epoch_datetime(seconds):
				return datetime.datetime.utcfromtimestamp(seconds)
					
class beetFileSystem(fuse.Fuse):
		def __init__(self, *args, **kwargs):	
				LOG_FILENAME = "LOG"
				logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,)
				
				logging.info("Preparing to mount file system")
				super(beetFileSystem, self).__init__(*args, **kwargs)

		def fsinit(self):
				# called after filesystem is mounted
				#self.lib = self.cmdline[1][0]
				self.lib = library
				self.files = {}

				logging.info("Filesystem mounted")
				
		def fsdestroy(self):
				logging.info("Unmounting file system")

		def statfs(self):
				logging.info("statfs")

				# have no way of knowing where the music is stored
				# (disparate locations), so using homedir to fill this in
				return os.statvfs(os.path.expanduser("~"))

				
		def getattr(self, path):
				logging.info("getattr: %s" % path)
				
				try:
					if path == "/":
							logging.info("Returning /")
							mode = stat.S_IFDIR | 0755
							st = Stat(st_mode=mode, st_size=Stat.DIRSIZE, st_nlink=2)
							return st
					else:
						# determine if it's a directory or a file list
						# Split path into components
						pathsplit = path[1:].split('/')

						if len(pathsplit) == structure_depth:
							# it's a file						
							items = self.lib.get_path(id=directory_structure.getnode(pathsplit[0:structure_depth-1]).files[pathsplit[structure_depth-1]])
							item = items[0]							

							if not item:
								# file not found
								logging.error("Returning ENOENT")
								return -errno.ENOENT
							elif item == '':
								logging.error("Returning ENOENT")
								return -errno.ENOENT
							statinfo = os.stat(item)
							st = Stat(st_mode=statinfo.st_mode, st_size=statinfo.st_size, st_uid=statinfo.st_uid, st_gid=statinfo.st_gid, st_nlink=statinfo.st_nlink, dt_atime=datetime.datetime.fromtimestamp(statinfo.st_atime), dt_mtime=datetime.datetime.fromtimestamp(statinfo.st_mtime), dt_ctime=datetime.datetime.fromtimestamp(statinfo.st_ctime))
							return st
						else:
								logging.info("dir")
								# it's a directory
								if not pathsplit[len(pathsplit)-1] in directory_structure.getnode(pathsplit[0:len(pathsplit)-1]).dirs:
									# directory not found
									logging.error("Returning ENOENT")
									return -errno.ENOENT
								else:
									logging.info("gotdir")
									mode = stat.S_IFDIR | 0544
									st = Stat(st_mode=mode, st_size=Stat.DIRSIZE, st_nlink=2)
									return st
									
				except Exception as e:
					logging.error(e)			
					return -errno.ENOENT
										
		# Note: utime is deprecated in favour of utimens.
		# utimens takes precedence over utime, so having this here does nothing
		# unless you delete utimens.
		def utime(self, path, times):

				atime, mtime = times
				logging.info("utime: %s (atime %s, mtime %s)" % (path, atime, mtime))
				return -errno.EOPNOTSUPP

		def utimens(self, path, atime, mtime):

				logging.info("utime: %s (atime %s:%s, mtime %s:%s)"
						% (path,atime.tv_sec,atime.tv_nsec,mtime.tv_sec,mtime.tv_nsec))
				return -errno.EOPNOTSUPP
				
		def access(self, path, flags):
				logging.info("access: %s (flags %s)" % (path, oct(flags)))
				pathsplit = path[1:].split('/')
				if path == "/":
					return 0
				else:
					is_dir = not len(pathsplit) == structure_depth

				# check for existence
				if is_dir:
					logging.info("dir")
					if not pathsplit[len(pathsplit) - 1] in directory_structure.getnode(pathsplit[0:len(pathsplit)-1]).dirs:
						return -errno.EACCES
					else:
						# if exists, always return allowed for directories
						return 0
				else:
					item = self.lib.get_path(id=directory_structure.getnode(pathsplit[0:structure_depth-1]).files[pathsplit[structure_depth-1]])[0]
					if not item:
						return -errno.EACCES			
					else:
						# TODO: actually check the file permissions
						# NB. existence is already tested (os.F_OK)
						if flags | os.R_OK: 
							pass				
						if flags | os.W_OK: 
							pass
						if flags | os.X_OK: 
							pass
							
						return 0
						
		def readlink(self, path):
				"""
				Get the target of a symlink.
				Returns a bytestring with the contents of a symlink (its target).
				May also return an int error code.
				"""
				logging.info("readlink: %s" % path)
				return -errno.EOPNOTSUPP
				
		def mknod(self, path, mode, rdev):
				"""
				Creates a non-directory file (or a device node).
				mode: Unix file mode flags for the file being created.
				rdev: Special properties for creation of character or block special
						devices (I've never gotten this to work).
						Always 0 for regular files or FIFO buffers.
				"""
				# Note: mode & 0770000 gives you the non-permission bits.
				# Common ones:
				# S_IFREG:	0100000 (A regular file)
				# S_IFIFO:	010000	(A fifo buffer, created with mkfifo)

				# Potential ones (I have never seen them):
				# Note that these could be made by copying special devices or sockets
				# or using mknod, but I've never gotten FUSE to pass such a request
				# along.
				# S_IFCHR:	020000	(A character special device, created with mknod)
				# S_IFBLK:	060000	(A block special device, created with mknod)
				# S_IFSOCK: 0140000 (A socket, created with mkfifo)

				# Also note: You can use self.GetContext() to get a dictionary
				#	 {'uid': ?, 'gid': ?}, which tells you the uid/gid of the user
				#	 executing the current syscall. This should be handy when creating
				#	 new files and directories, because they should be owned by this
				#	 user/group.
				logging.info("mknod: %s (mode %s, rdev %s)" % (path, oct(mode), rdev))
				return -errno.EOPNOTSUPP

		def mkdir(self, path, mode):
				"""
				Creates a directory.
				mode: Unix file mode flags for the directory being created.
				"""
				# Note: mode & 0770000 gives you the non-permission bits.
				# Should be S_IDIR (040000); I guess you can assume this.
				# Also see note about self.GetContext() in mknod.
				logging.info("mkdir: %s (mode %s)" % (path, oct(mode)))
				return -errno.EOPNOTSUPP

		def unlink(self, path):
				"""Deletes a file."""
				logging.info("unlink: %s" % path)
				return -errno.EOPNOTSUPP

		def rmdir(self, path):
				"""Deletes a directory."""
				logging.info("rmdir: %s" % path)
				return -errno.EOPNOTSUPP

		def symlink(self, target, name):
				"""
				Creates a symbolic link from path to target.

				The 'name' is a regular path like any other method (absolute, but
				relative to the filesystem root).
				The 'target' is special - it works just like any symlink target. It
				may be absolute, in which case it is absolute on the user's system,
				NOT the mounted filesystem, or it may be relative. It should be
				treated as an opaque string - the filesystem implementation should not
				ever need to follow it (that is handled by the OS).

				Hence, if the operating system creates a link FROM this system TO
				another system, it will call this method with a target pointing
				outside the filesystem.
				If the operating system creates a link FROM some other system TO this
				system, it will not touch this system at all (symlinks do not depend
				on the target system unless followed).
				"""
				logging.info("symlink: target %s, name: %s" % (target, name))
				return -errno.EOPNOTSUPP

		def link(self, target, name):
				"""
				Creates a hard link from name to target. Note that both paths are
				relative to the mounted file system. Hard-links across systems are not
				supported.
				"""
				logging.info("link: target %s, name: %s" % (target, name))
				return -errno.EOPNOTSUPP

		def rename(self, old, new):
				"""
				Moves a file from old to new. (old and new are both full paths, and
				may not be in the same directory).
				
				Note that both paths are relative to the mounted file system.
				If the operating system needs to move files across systems, it will
				manually copy and delete the file, and this method will not be called.
				"""
				logging.info("rename: target %s, name: %s" % (old, new))
				return -errno.EOPNOTSUPP

		def chmod(self, path, mode):
				"""Changes the mode of a file or directory."""
				logging.info("chmod: %s (mode %s)" % (path, oct(mode)))
				return -errno.EOPNOTSUPP

		def chown(self, path, uid, gid):
				"""Changes the owner of a file or directory."""
				logging.info("chown: %s (uid %s, gid %s)" % (path, uid, gid))
				return -errno.EOPNOTSUPP

		def truncate(self, path, size):
				"""
				Shrink or expand a file to a given size.
				If 'size' is smaller than the existing file size, truncate it from the
				end.
				If 'size' if larger than the existing file size, extend it with null
				bytes.
				"""
				logging.info("truncate: %s (size %s)" % (path, size))
				return -errno.EOPNOTSUPP

		### DIRECTORY OPERATION METHODS ###
		# Methods in this section are operations for opening directories and
		# working on open directories.
		# "opendir" is the method for opening directories. It *may* return an
		# arbitrary Python object (not None or int), which is used as a dir
		# handle by the methods for working on directories.
		# All the other methods (readdir, fsyncdir, releasedir) are methods for
		# working on directories. They should all be prepared to accept an
		# optional dir-handle argument, which is whatever object "opendir"
		# returned.
		
		def opendir(self, path):
				"""
				Checks permissions for listing a directory.
				This should check the 'r' (read) permission on the directory.

				On success, *may* return an arbitrary Python object, which will be
				used as the "fh" argument to all the directory operation methods on
				the directory. Or, may just return None on success.
				On failure, should return a negative errno code.
				Should return -errno.EACCES if disallowed.
				"""
				logging.info("opendir: %s" % path)
				pathsplit = path[1:].split('/')
				if path == "/":
						return directory_structure
				else:
						if not pathsplit[len(pathsplit) - 1] in directory_structure.getnode(pathsplit[0:len(pathsplit)-1]).dirs:
							return -errno.EACCES
						else:
							return directory_structure.getnode(pathsplit[0:len(pathsplit)-1]).dirs[pathsplit[len(pathsplit) - 1]]
							
		def releasedir(self, path, dh=None):
				"""
				Closes an open directory. Allows filesystem to clean up.
				"""
				logging.info("releasedir: %s (dh %s)" % (path, dh))

		def fsyncdir(self, path, datasync, dh=None):
				"""
				Synchronises an open directory.
				datasync: If True, only flush user data, not metadata.
				"""
				logging.info("fsyncdir: %s (datasync %s, dh %s)"
						% (path, datasync, dh))
						
		def readdir(self, path, offset, dh=None):
				"""
				Generator function. Produces a directory listing.
				Yields individual fuse.Direntry objects, one per file in the
				directory. Should always yield at least "." and "..".
				Should yield nothing if the file is not a directory or does not exist.
				(Does not need to raise an error).

				offset: I don't know what this does, but I think it allows the OS to
				request starting the listing partway through (which I clearly don't
				yet support). Seems to always be 0 anyway.
				"""
				logging.info("readdir: %s (offset %s, dh %s)" % (path, offset, dh))
				
				yield fuse.Direntry(".")
				yield fuse.Direntry("..")
				
				try:
					pathsplit = path[1:].split('/')
		
			
					if dh == None:
						if path == "/":
							logging.info("dh assigned as root")
							dh = directory_structure
						else:
							dh = directory_structure.getnode(pathsplit[0:len(pathsplit)-1]).dirs[pathsplit[len(pathsplit)]]
			

					if len(pathsplit) == structure_depth - 1:
						# files
						logging.info("Yielding files: %s" % path)
						for files in directory_structure.listdir(pathsplit, False):
							logging.info("Yielding file: %s" % files.encode('utf-8'))
							yield fuse.Direntry(files.encode('utf-8'))
					else:
						# directories
						for files in directory_structure.listdir(pathsplit, True):
							#logging.info("Yielding dir: %s" % files.encode('utf-8'))
							yield fuse.Direntry(files.encode('utf-8'))
							
				except Exception as e:
					logging.error(e)
				
		### FILE OPERATION METHODS ###
		# Methods in this section are operations for opening files and working on
		# open files.
		# "open" and "create" are methods for opening files. They *may* return an
		# arbitrary Python object (not None or int), which is used as a file
		# handle by the methods for working on files.
		# All the other methods (fgetattr, release, read, write, fsync, flush,
		# ftruncate and lock) are methods for working on files. They should all be
		# prepared to accept an optional file-handle argument, which is whatever
		# object "open" or "create" returned.

		def open(self, path, flags):
				"""
				Open a file for reading/writing, and check permissions.
				flags: As described in man 2 open (Linux Programmer's Manual).
						ORing of several access flags, including one of os.O_RDONLY,
						os.O_WRONLY or os.O_RDWR. All other flags are in os as well.

				On success, *may* return an arbitrary Python object, which will be
				used as the "fh" argument to all the file operation methods on the
				file. Or, may just return None on success.
				On failure, should return a negative errno code.
				Should return -errno.EACCES if disallowed.
				"""
				logging.info("open: %s (flags %s)" % (path, oct(flags)))
				
				file_path = None
				
				try:
					if self.files == None:
						self.files = {"x":"y"}
						
					if path in self.files:
						# get a file object
						logging.info("Retrieving an existing File Handler for: %s" % path)
						self.files[path].open()
					else:
						# create a file open
						logging.info("Creating a File Handler for: %s" % path)
						self.files[path] = FileHandler(path, self.lib)
			
					return self.files[path]
				except Exception as e:
					logging.info("Error creating a File Handler: %s" % e)
					return -errno.EACCES
				
				
		def create(self, path, mode, rdev):
				"""
				Creates a file and opens it for writing.
				Will be called in favour of mknod+open, but it's optional (OS will
				fall back on that sequence).
				mode: Unix file mode flags for the file being created.
				rdev: Special properties for creation of character or block special
						devices (I've never gotten this to work).
						Always 0 for regular files or FIFO buffers.
				See "open" for return value.
				"""
				logging.info("create: %s (mode %s, rdev %s)" % (path,oct(mode),rdev))
				return -errno.EOPNOTSUPP

		def fgetattr(self, path, fh=None):
				"""
				Retrieves information about a file (the "stat" of a file).
				Same as Fuse.getattr, but may be given a file handle to an open file,
				so it can use that instead of having to look up the path.
				"""
				logging.info("fgetattr: %s (fh %s)" % (path, fh))
				# We could use fh for a more efficient lookup. Here we just call the
				# non-file-handle version, getattr.
				return self.getattr(path)

		def release(self, path, flags, fh=None):
				"""
				Closes an open file. Allows filesystem to clean up.
				flags: The same flags the file was opened with (see open).
				"""
				logging.info("release: %s (flags %s, fh %s)" % (path, oct(flags), fh))
				if self.files[path].release():
					logging.info("Complete release: %s (flags %s, fh %s)" % (path, oct(flags), fh))
					del self.files[path]

		def fsync(self, path, datasync, fh=None):
				"""
				Synchronises an open file.
				datasync: If True, only flush user data, not metadata.
				"""
				logging.info("fsync: %s (datasync %s, fh %s)" % (path, datasync, fh))

		def flush(self, path, fh=None):
				"""
				Flush cached data to the file system.
				This is NOT an fsync (I think the difference is fsync goes both ways,
				while flush is just one-way).
				"""
				logging.info("flush: %s (fh %s)" % (path, fh))
				
		def read(self, path, size, offset, fh=None):
				"""
				Get all or part of the contents of a file.
				size: Size in bytes to read.
				offset: Offset in bytes from the start of the file to read from.
				Does not need to check access rights (operating system will always
				call access or open first).
				Returns a byte string with the contents of the file, with a length no
				greater than 'size'. May also return an int error code.

				If the length of the returned string is 0, it indicates the end of the
				file, and the OS will not request any more. If the length is nonzero,
				the OS may request more bytes later.
				To signal that it is NOT the end of file, but no bytes are presently
				available (and it is a non-blocking read), return -errno.EAGAIN.
				If it is a blocking read, just block until ready.
				"""
				logging.info("read: %s (size %s, offset %s, fh %s)"
						% (path, size, offset, fh))
						
				
				if fh == None:
					file_path = None
				
					try:
						if self.files == None:
							self.files = {"x":"y"}
						self.files[path] = FileHandler(path, self.lib)
					except:
						return -errno.EPERM
		
				
				return self.files[path].read(size, offset)
			 
		def write(self, path, buf, offset, fh=None):
				"""
				Write over part of a file.
				buf: Byte string containing the text to write.
				offset: Offset in bytes from the start of the file to write to.
				Does not need to check access rights (operating system will always
				call access or open first).
				Should only overwrite the part of the file from offset to
				offset+len(buf).

				Must return an int: the number of bytes successfully written (should
				be equal to len(buf) unless an error occured). May also be a negative
				int, which is an errno code.
				"""
				logging.info("write: %s (offset %s, fh %s)" % (path, offset, fh))
				logging.debug("	buf: %r" % buf)
				
				return -errno.EOPNOTSUPP

		def ftruncate(self, path, size, fh=None):
				"""
				Shrink or expand a file to a given size.
				Same as Fuse.truncate, but may be given a file handle to an open file,
				so it can use that instead of having to look up the path.
				"""
				logging.info("ftruncate: %s (size %s, fh %s)" % (path, size, fh))
				return -errno.EOPNOTSUPP


