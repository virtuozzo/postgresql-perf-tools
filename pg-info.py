#!/usr/bin/python

import os
import sys
import time

HAS_PA = True
oldpath = sys.path
try:
	bindir, testscript= os.path.split(sys.argv[0])
	sys.path.append(os.path.join(bindir, "..", "lib"))
	import pa_config
except ImportError:
	HAS_PA = False
finally:
	sys.path = oldpath

import psycopg2
from optparse import OptionParser, OptionGroup
import logging
HAS_SQLPARSE = True
try:
	import sqlparse
except ImportError:
	HAS_SQLPARSE = False


class DB:
	def __init__(self, host, port, database, user, password):
		self.host = host
		self.port = port
		self.database = database
		self.user = user
		self.password = password or ""

	def get_name(self):
		return self.database

	def __str__(self):
		return "%s@%s:%s db %s" % (self.user, self.host, self.port, self.database)

	def connect(self):
		return psycopg2.connect(**(self.__dict__))

	@staticmethod
	def _execute_fetch(con, query, fetchfn, *args):
		cur = con.cursor()
		try:
			if args is None or not len(args):
				logging.debug(query)
				cur.execute(query)
			else:
				logging.debug(query % args)
				cur.execute(query, args)
			return fetchfn(cur)
		finally:
			cur.close()

	@staticmethod
	def execute_fetchone(con, query, *args):
		return DB._execute_fetch(con, query, lambda cur : cur.fetchone(), *args)

	@staticmethod
	def execute_fetchall(con, query, *args):
		return DB._execute_fetch(con, query, lambda cur : cur.fetchall(), *args)

	@staticmethod
	def execute_fetchval(con, query, *args):
		ret = DB.execute_fetchone(con, query, *args)
		if ret is not None and len(ret) > 0:
			return ret[0]


class PgInfo:
	def __init__(self, con, db_name, width, lines_limit, print_sqls=False):
		self.con = con
		self.db_name = db_name
		self.w = width
		self.lines_limit = lines_limit
		self.print_sqls = print_sqls

	def _printSql(self, q):
		if self.print_sqls:
			self._printLine()
			if HAS_SQLPARSE:
				print sqlparse.format(q, reindent=True, keyword_case='upper')
			else:
				print q

	def _printHeader(self, text):
		self._printLine("=")
		print text

	def _printLine(self, char="-"):
		print char * self.w

	def _printTable(self, fmt, sql_ret):
		self._printLine()
		for f in fmt:
			if f[1] != 0:
				print f[0].rjust(f[1]).upper(),
		print
		line = 0
		for s in sql_ret:
			if self.lines_limit and line > self.lines_limit:
				for n in xrange(0, len(fmt)):
					if fmt[n][1] == 0:
						continue
					print "...".rjust(fmt[n][1]),
				print
				break
			line += 1
			n = 0
			for v in s:
				if fmt[n][1] != 0:
					if len(fmt[n]) > 2 and fmt[n][2]:
						try:
							v = fmt[n][2] % v
						except:
							v = "0"
					else:
						v = str(v)
					l = len(v)
					maxl = fmt[n][1]
					if l > maxl:
						v = v[0:maxl-3] + "..."
					print v.rjust(fmt[n][1]),
				n += 1
			print
		print

	def printPGInfo(self):
		print DB.execute_fetchval(self.con, "SELECT version()")
		print

	def printDBSize(self):
		self._printHeader("All schemas sorted by TOTAL_SZ size on disk")
		q = """
		SELECT
			schema,
			pg_size_pretty(total) AS total,
			pg_size_pretty(relation) AS relation,
			pg_size_pretty(indexes) AS indexes,
			case when total > 0
			THEN
				indexes / total
			ELSE
				0
			END
		FROM
		(
			SELECT
				schema,
				sum(pg_total_relation_size(qual_table))::bigint AS total,
				sum(pg_relation_size(qual_table))::bigint AS relation,
				sum(pg_indexes_size(qual_table))::bigint AS indexes
			FROM
			(
				SELECT
					schemaname AS schema,
					tablename AS table,
					('"'||schemaname||'"."'||tablename||'"')::regclass AS qual_table
				FROM
					pg_tables
				WHERE
					schemaname NOT LIKE 'pg_%'
			) s
			GROUP BY schema
			ORDER BY total DESC
		) s"""
		self._printSql(q)
		ret = DB.execute_fetchall(self.con, q)
		fmt = [('schema', 32), ('*total_sz', 11), ('data_sz', 11), ('index_sz', 11), ('index_sz%', 9, "%.0f%%")]
		self._printTable(fmt, ret)

	def printTablesSize(self):
		self._printHeader("All tables sorted by TOTAL_SZ size on disk")
		q = """
		SELECT
			relname,
			pg_total_relation_size(C.oid) AS total_bytes,
			pg_size_pretty(pg_total_relation_size(C.oid)) AS "total",
			pg_size_pretty(pg_relation_size(C.oid)) AS "data_sz",
			pg_size_pretty(pg_indexes_size(C.oid)) AS "index_sz",
			case when pg_indexes_size(C.oid) > 0
			THEN
				100 * pg_indexes_size(C.oid) / pg_total_relation_size(C.oid)
			ELSE
				0
			END,
			reltuples,
			columns,
			indexes,
			case when columns > 0
			THEN
				100 * indexes / columns
			ELSE
				0
			END
		FROM
			pg_class C LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
			LEFT JOIN (
				SELECT
					table_name, count(*) AS columns
				FROM
					information_schema.columns
				GROUP BY
					table_name
			) S ON S.table_name = C.relname
			LEFT JOIN (
				SELECT
					tablename, count(tablename) AS indexes
				FROM
					pg_indexes
				GROUP BY
					tablename
			) I ON (I.tablename = C.relname)
		WHERE
			nspname NOT IN ('pg_catalog', 'information_schema') AND C.relkind = 'r'
		ORDER BY
			total_bytes DESC
		"""
		self._printSql(q)
		ret = DB.execute_fetchall(self.con, q)
		fmt = [('table', 32),
			('total_bytes', 0),
			('*total_sz', 11), ('data_sz', 11), ('index_sz', 11), ('index_sz%', 9, "%d%%"),
			("rows", 11, "%d"), ("columns", 7, "%d"), ("indexes", 7, "%d"), ("idx%", 4, "%d")]

		self._printTable(fmt, ret)
		print "  HINT: The bigger is table TOTAL_SZ the slower are all operations on this table\n"

	def printMissingIndexes(self, pg_rel_size_threshold):
		if pg_rel_size_threshold:
			self._printHeader("Tables with size > %dKB and missing indexes (lots of sequential scans)" %
				(pg_rel_size_threshold / 1024))
		else:
			self._printHeader("All tables with missing indexes")

		q = """
		SELECT
			relname, seq_scan-idx_scan AS too_much_seq,
			case when seq_scan-idx_scan > 0
			THEN
				'Missing Index?'
			ELSE
				'OK'
			END,
			pg_relation_size(relname::regclass)
			AS rel_size, seq_scan, idx_scan
		FROM
			pg_stat_all_tables
		WHERE
			schemaname='public' AND pg_relation_size(relname::regclass) >= %d
		ORDER BY too_much_seq DESC""" % pg_rel_size_threshold

		self._printSql(q)
		ret = DB.execute_fetchall(self.con, q)
		fmt = [('table', 32), ('*too_much_seq', 14), ('case', 15), ('rel_size', 10),
			('seq_scan', 10), ('idx_scan', 10)]
		self._printTable(fmt, ret)
		print \
			"  HINT: The more TOO_MUCH_SCAN value the more frequently sequential scan was performed on the\n" + \
			"        given table\n"

	def printDeadIndexes(self):
		self._printHeader("Less frequently accessed indexes ordered by IDX_SIZE")
		q = """
		SELECT
			relid::regclass AS table,
			indexrelid::regclass AS index,
			pg_size_pretty(pg_relation_size(indexrelid::regclass)) AS index_size,
			pg_relation_size(indexrelid::regclass) AS index_size_bytes,
			idx_scan,
			idx_tup_read,
			idx_tup_fetch
		FROM
			pg_stat_user_indexes
			JOIN pg_index USING (indexrelid)
		WHERE
			indisunique IS FALSE
		ORDER BY idx_scan ASC, index_size_bytes DESC
		"""
		self._printSql(q)
		ret = DB.execute_fetchall(self.con, q)

		fmt = [('table', 32), ('index', 40), ('*idx_size', 10), ('idx size bytes', 0),
			('*idx_scan', 9), ('tup_read', 9), ('tup_fetch', 9)]
		self._printTable(fmt, ret)

		print \
			"  HINT: The more IDX_SIZE value the more size is occupied for given index, if IDX_SCAN == 0 then\n" + \
			"        this index is unused and can be just removed to increase INSERT/UPDATE performance\n"

	def printMostWritableTables(self):
		self._printHeader("Most frequently modified tables")
		q = """
		SELECT
			relname AS TABLE,
			pg_size_pretty( pg_relation_size(relid) ) AS tsize,
			n_tup_upd + n_tup_ins + n_tup_del AS WRITE,
			seq_scan + idx_scan AS READ,
			case when seq_scan + idx_scan > 0
			THEN
				100 * (n_tup_upd + n_tup_ins + n_tup_del) / (n_tup_upd + n_tup_ins + n_tup_del + seq_scan + idx_scan)
			ELSE
				0
			END,
			n_tup_ins AS INS,
			n_tup_upd AS UPD,
			n_tup_del AS DEL
		FROM
			pg_stat_user_tables
		ORDER BY
			( n_tup_upd + n_tup_ins + n_tup_del ) DESC
		"""
		self._printSql(q)
		ret = DB.execute_fetchall(self.con, q)

		fmt = [('table', 32), ('tsize', 10), ('*writes', 11), ('reads', 11),
			('write%', 10, "%.0f%%"), ('ins', 10), ('upd', 10), ('del', 10)]
		self._printTable(fmt, ret)
		print "  HINT: Tables with significant amount of WRITEs can reveal bad application design\n"

def main():
	test_description = "%prog [options]"

	p = OptionParser(test_description)
	p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
	p.add_option("-s", "--sql",     action="store_true", help="print SQLs that were used to obtain the stats")
	p.add_option("-l", "--lines",   type=int, default=20, help="num of lines in output (default %default)")
	p.add_option("",   "--min-tab-size", type=int, default=(32*1024),
		help="min size of tables when analyze missing indexes (default %default)")

	defdb = ""
	defusr = "postgres"
	if HAS_PA:
		g = OptionGroup(p, "If you have pa.conf")
		g.add_option("-c", "--config",  type="string", default=None, help="PA config file [default: %default]")
		g.add_option("", "--pba",       action="store_true", help = "connect to PBA (POA is default)")
		p.add_option_group(g)
		defdb = "plesk"
		defusr = "plesk"

	g = OptionGroup(p, "If you don't have pa.conf" if HAS_PA else "Database credentials")
	g.add_option("", "--db-host",   type="string", help="database hostname/IP")
	g.add_option("", "--db-port",   type="string", default=5432, help="database port")
	g.add_option("", "--db-name",   type="string", default=defdb, help="database name [default: %default]")
	g.add_option("", "--db-user",   type="string", default=defusr, help="database username [default: %default]")
	g.add_option("", "--db-pass",   type="string", default="", help="database password")
	p.add_option_group(g)

	opts, args = p.parse_args()
	loglevel = logging.DEBUG if opts.verbose else logging.WARNING
	logging.basicConfig(level=loglevel, format="%(asctime)s - %(module)s - %(levelname)s - %(message)s")

	if HAS_PA:
		if not opts.config and not opts.db_host:
			p.error("either -c or --db-host option must be provided")
	else:
		if not opts.db_host:
			p.error("--db-host option must be provided")

	if HAS_PA and opts.config:
		pa_config.init(opts.config)
		if opts.pba:
			b = pa_config.get().pba_db
		else:
			b = pa_config.get().poa_db

		opts.db_vendor = b.db_vendor
		opts.db_host = b.ip
		opts.db_port = b.db_port
		opts.db_name = b.db_name
		opts.db_user = b.db_user
		opts.db_pass = b.db_pass

	db = DB(opts.db_host, opts.db_port, opts.db_name, opts.db_user, opts.db_pass)
	print "Connecting to %s ..." % str(db)
	con = db.connect()
	pi = PgInfo(con, opts.db_name, 110, opts.lines, opts.sql)

	pi.printPGInfo()
	pi.printDBSize()
	pi.printTablesSize()
	pi.printMissingIndexes(opts.min_tab_size)
	pi.printDeadIndexes()
	pi.printMostWritableTables()

if __name__ == "__main__":
	main()
