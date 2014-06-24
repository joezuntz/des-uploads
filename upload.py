"""
This module enable direct upload of astropy tables
into the DESDM database.

"""
import sys

def requirements(missing):
	sys.stderr.write("""


Could not find some libraries you need to run this.
You need:
	numpy
	astropy
	desdb (https://github.com/esheldon/desdb)

The first missing thing I found was:
	{0}

""".format(missing))
	sys.exit(1)

try:
	import desdb
except:
	requirements('desdb')
try:	
	import numpy as np
except:
	requirements('numpy')
try:
	import astropy.io.ascii
	import astropy.table
except:
	requirements('astropy')

import os
import glob
import itertools
import re

tile_regex = re.compile('DES\d\d\d\d[+-]\d\d\d\d')
MAX_NUMBER_ROWS = 250000


try:
	desdb.Connection	
except AttributeError:
	sys.stderr.write("""


Your environment is not correctly set up
Cx_Oracle was either not found or did not 
have the environment variables LD_LIBRARY_PATH
(DYLD_LIBRARY_PATH on a mac) or ORACLE_HOME set correctly

See the instructions at:
https://cdcvs.fnal.gov/redmine/projects/deswlwg/wiki/Accessing_catalogs_from_DESDM_with_python

""")
	sys.exit(1)

class TableUploaderConnection(desdb.Connection):
	def create_table(self, table_name, fields, primary=None, public=False):
		table_info = ', '.join(["{0}  {1} NOT NULL ".format(*field) for field in fields])

		if isinstance(primary,basestring):
			primary = [primary]
		if primary:
			primary_text = ','.join(primary)
			constraint = ", CONSTRAINT %s_pk PRIMARY KEY (%s)" % (table_name, primary_text)
		else:
			constraint = ""
		query = """ create table {0}({1}{2}) compress """.format(table_name,table_info,constraint)
		print query
		self.quick(query)
		self.quick("commit")
		if public:
			self.quick("grant select on {0} to des_reader".format(table_name))

	def delete_table(self, table_name):
		self.quick("drop table {0}".format(table_name))

	def insert_data(self, table_name, names, arrays):
		nrow = len(arrays[0])
		if nrow > MAX_NUMBER_ROWS:
			n_split = (nrow//MAX_NUMBER_ROWS)+1
			print 'Splitting upload into %d' % n_split
			for i in xrange(n_split):
				start = MAX_NUMBER_ROWS*i
				finish = min(start+MAX_NUMBER_ROWS, nrow)
				if start>=finish:
					#handle the case where nrow is exact multiple of MAX
					continue
				subarrays = [a[start:finish] for a in arrays]
				self.insert_data(table_name, names, subarrays)
			return

		ncol = len(names)
		for array in arrays:
			assert len(array) == nrow
		assert len(arrays) == ncol
		lists = []
		for array in arrays:
			if isinstance(array, np.ndarray):
				array = array.tolist()
			else:
				array = list(array)
			lists.append(array)
		rows = zip(*lists)
		types = [type(l[0]) for l in lists]
		name_code = ", ".join(names)
		insert_code = ', '.join([':{0}'.format(i+1) for i in xrange(ncol)])
		cursor = self.cursor()
		print "nrow = ", nrow
		cursor.bindarraysize = nrow
		cursor.setinputsizes(*types)
		sql = "insert into {0}({1}) values ({2})".format(table_name, name_code, insert_code)
		cursor.executemany(sql, rows)
		self.commit()
		cursor.close()

	def insert_table(self, table_name, table, primary=None, cut_duplicates=False,extra=None):
		names = list(table.colnames)
		ncol = len(names)
		if isinstance(primary, basestring):
			primary = [primary]		
		if cut_duplicates:
			assert primary is not None, "Require a primary key to remove duplicates"
			key_fields = [table[p] for p in primary]
			mask = []
			seen = set()
			for i in xrange(len(key_fields[0])):
				k = tuple([f[i] for f in key_fields])
				if k in seen:
					mask.append(False)
				else:
					mask.append(True)
				seen.add(k)
			mask = np.array(mask)
		else:
			mask = np.ones(len(table), dtype=bool)		
		arrays = [table[name][mask] for name in names]
		if extra:
			key,value=extra.split("=")
			names.append(key)
			arrays.append(np.repeat(value,len(arrays[0])))

		self.insert_data(table_name, names, arrays)

	def insert_new_column(self, table_name, column_name, column_array, match_name, match_array, create=False):
		#Create the new column
		column_dtype = column_array.dtype.kind
		type_code = {'f':'binary_double', 'i':'integer','S':'varchar'}[column_dtype]
		add_column_sql = "alter table {table_name} add ( {column_name} {type_code})".format(**locals())
		if create:
			print add_column_sql
			self.quick(add_column_sql)
		column_array = column_array.tolist()
		match_array = match_array.tolist()
		#Fill in the values
		update_sql = "update {table_name} set {column_name}  = :1 where {match_name} = :2".format(**locals())
		rows = zip(column_array, match_array)

		types = [type(column_array[0]), type(match_array[0])]
                cursor = self.cursor()
		cursor.bindarraysize = len(column_array)
		cursor.setinputsizes(*types)
		cursor.executemany(update_sql, rows)
		self.commit()
		cursor.close()


	def table_with_format(self, filename, format, extension):
		if format is None:
			format=self.guess_file_format(filename)
		if format=='fits':
			return astropy.table.Table.read(filename, format='fits', hdu=extension)
		elif format=='ssv':
			return astropy.io.ascii.read(filename, delimiter=' ', header_start=0, data_start=1)


	def type_codes_for_table(self, table):
		type_codes = [table.dtype[name].kind for name in table.colnames]
		type_map = {'f':'binary_double', 'i':'integer','S':'varchar'}
		oracle_types = []
		for i,type_code in enumerate(type_codes):
			oracle_type = type_map[type_code]
			if oracle_type=='varchar':
				oracle_type += '(%d)'%max(table.columns[i].dtype.itemsize,12)
			oracle_types.append(oracle_type)
		return oracle_types

	def create_table_from_table(self, table, table_name, extra_cols,primary=None, public=False):
		print "Creating table ",table_name
		names = list(table.colnames)
		oracle_types = self.type_codes_for_table(table)
		fields = [(name,oracle_type) for (name,oracle_type) in zip(names, oracle_types)]
		fields += extra_cols
		self.create_table(table_name, fields, primary=primary, public=public)

	def guess_file_format(self, filename):
		if filename.lower().endswith('fits') or filename.lower().endswith('fit'):
			return 'fits'
		else:
			return 'ssv'

	def upload_collection(self, table_name, filenames, format=None, create=False, 
			primary=None, cut_duplicates=False, extension=0, tilename_col=False, public=False):
		for filename in filenames:
			assert os.path.exists(filename)

		for i,filename in enumerate(filenames):
			table = self.table_with_format(filenames[0], format, extension)
			if tilename_col:
				extra_cols = [('TILENAME','VARCHAR(12)')]
				tilename = re.search(tile_regex, filename).group()
				extra = 'TILENAME={0}'.format(tilename)
			else:
				extra_cols = []
				extra=None
			if i==0 and create:
				self.create_table_from_table(table, table_name, extra_cols, primary=primary)
			print "Uploading {0} to {1}".format(filename, table_name)
			self.insert_table(table_name, table, primary=primary, cut_duplicates=cut_duplicates, extra=extra)


			

import argparse
parser = argparse.ArgumentParser(description="Upload some FITS files to an oracle database.  This crashes often because cx_Oracle is crap.")
parser.add_argument('filename_base', help='Upload all files that start with this')
parser.add_argument('table_name', help='Name of Oracle table')
parser.add_argument("-s", "--start", type=int, default=0, help='first file in list to process')
parser.add_argument("-n", "--count", type=int, default=100000000000, help='number of files to process')
parser.add_argument("--create", action='store_true', default=False, help='Create the table')
parser.add_argument("-p", "--primary", type=str, nargs='+',default=[], help='Create the table')
parser.add_argument("-k", "--remove-duplicates", action='store_true', default=False, help='remove duplicated primary keys')
parser.add_argument("-j", "--extension", type=int, default=None, help='extension to get data from')
parser.add_argument("-t", "--tilename-col", action='store_true', help='Add a tilename field based on the filename')
parser.add_argument("-u", "--public", action='store_true', help='Make the table public (if you are just creating it; no effect otherwise)')


if __name__=="__main__":
	args = parser.parse_args()
	filenames = glob.glob(args.filename_base+"*")
	filenames.sort()

	connection = TableUploaderConnection()
	filenames = filenames[args.start:args.start+args.count]
	extra=None

	connection.upload_collection(args.table_name, filenames, 
		create=args.create, primary=args.primary, 
		cut_duplicates=args.remove_duplicates, extension=args.extension, 
		tilename_col=args.tilename_col, public=args.public)

