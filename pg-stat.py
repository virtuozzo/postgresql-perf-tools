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
import inspect


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


class PgVersion:
	def __init__(self, con):
		self.str = DB.execute_fetchval(con, "SELECT version()")
		self.maj = DB.execute_fetchval(con, "SELECT substring(version() from $$(\d+)\.\d+\.\d+$$)::smallint;")
		self.min = DB.execute_fetchval(con, "SELECT substring(version() from $$\d+\.(\d+)\.\d+$$)::smallint;")

	def ge(self, maj, min):
		return self.maj > maj or (self.maj >= maj and self.min >= min)


opts = None
con = None

class PgStatStore:
	def __init__(self, table=None, cols=None):
		self.table = table
		self.cols = cols

	def update(self):
		query = ["SUM(%s)" % c for c in self.cols]

		ret = DB.execute_fetchone(con, "SELECT %s FROM %s" % (", ".join(query), self.table))
		self.store = {}
		n = 0
		for c in self.cols:
			self.store[c] = ret[n]
			n += 1

class PgStatStoreBigUserTables(PgStatStore):
	def __init__(self, cols):
		self.cols = cols

	def update(self):
		query = ["SUM(p.%s)" % c for c in self.cols]

		ret = DB.execute_fetchone(con,
			"SELECT %s FROM pg_stat_user_tables p, pg_class c "
			"WHERE p.relname = c.relname AND c.reltuples > %d" %
			(", ".join(query), int(opts.scan_threshold)))
		self.store = {}
		n = 0
		for c in self.cols:
			self.store[c] = ret[n]
			n += 1

class PgStatStoreProc(PgStatStore):
	def update(self):
		ret = DB.execute_fetchall(con,
			"SELECT current_query, COUNT(*) FROM pg_stat_activity " + \
			"WHERE datname = '%s' GROUP BY current_query" % opts.db_name)

		self.store = {}
		for row in ret:
			if "pg_stat_activity" in row[0]:
				continue
			self.store[row[0]] = row[1]

class DbStatCounter:
	width = 5
	rate_fmt = None
	absolute = False
	def __init__(self, store=None):
		metric_len = len(self.metric)
		if not self.absolute:
			metric_len += 2 # '/s'
		self.width = max(self.width, len(self.title), metric_len)
		self.store = store

		self.val_initial = 0
		self.val = 0
		self.rate = 0
		self.time = None

	def update(self):
		prev_val = float(self.val)
		self.update_action()
		prev_time = self.time
		self.time = time.time()
		logging.debug("%s raw val: %d" % (self.title, self.val))
		self.rate = 0
		if self.absolute:
			self.rate = self.val
		else:
			if prev_time:
				dt = self.time - prev_time
				if dt:
					self.rate = (float(self.val) - prev_val) / dt
		if not self.val_initial:
			self.val_initial = self.val
		con.commit()

	def update_action(self):
		# virtual
		self.val = 0

	def abs(self):
		if self.absolute:
			return self.val
		return self.val - self.val_initial

class pgsDbSize(DbStatCounter):
	title = "DBSize"
	metric = "KB"
	width = 8
	help = "size of database in kilobytes"
	def update_action(self, **kwargs):
		self.val = DB.execute_fetchval(con, "select pg_database_size('%s')" % opts.db_name)
		self.val /= 1024

class pgsWrIns(DbStatCounter):
	title = "INS"
	metric = "rows"
	help = "number of rows inserted into user tables [pg_stat_user_tables.n_tup_ins]"
	rate_fmt = "%.1f"
	def update_action(self):
		self.val = self.store.store["n_tup_ins"]

class pgsWrUpd(DbStatCounter):
	title = "UPD"
	metric = "rows"
	help = "number of rows updated in the user tables [pg_stat_user_tables.n_tup_upd]"
	rate_fmt = "%.1f"
	def update_action(self):
		self.val = self.store.store["n_tup_upd"]

class pgsWrDel(DbStatCounter):
	title = "DEL"
	metric = "rows"
	help = "number of rows deleted in the user tables [pg_stat_user_tables.n_tup_del]"
	rate_fmt = "%.1f"
	def update_action(self):
		self.val = self.store.store["n_tup_del"]

class pgsScanIdx(DbStatCounter):
	title = "IDX"
	metric = "scan"
	help = "total number of index scans [pg_stat_database.idx_scan]"
	def update_action(self):
		self.val = self.store.store["idx_scan"]

class pgsScanSeq(DbStatCounter):
	title = "SEQ"
	metric = "scan"
	help = "total number of sequential scans [pg_stat_database.seq_scan]"
	def update_action(self):
		self.val = self.store.store["seq_scan"]

class pgsScanIdxPerc(DbStatCounter):
	title = "SEQ%"
	metric = "scan%"
	help = "percentage of sequential scans [100 * pg_stat_database.seq_scan / (.idx_scan + .seq_scan)]"
	absolute = True
	def update_action(self):
		idx = int(self.store.store['idx_scan'])
		seq = int(self.store.store['seq_scan'])

		if hasattr(self, 'prev_idx'):
			d_idx = idx - self.prev_idx
			if d_idx < 0:
				d_idx = 0
			d_seq = seq - self.prev_seq
			if d_seq < 0:
				d_seq = 0
			tot = d_idx + d_seq
			self.val = ((100 * int(d_seq)) / float(tot)) if tot else 0
		else:
			self.val = 0
		self.prev_idx = idx
		self.prev_seq = seq


class pgsScanSeqRows(DbStatCounter):
	title = "SEQ_ROWS"
	metric = "rows"
	help = "total number of live rows fetched by seq scans [pg_stat_database.seq_tup_read]"
	rate_fmt = "%.0f"
	def update_action(self):
		self.val = self.store.store["seq_tup_read"]

class pgsCacheMiss(DbStatCounter):
	title = "MISS"
	metric = "blk"
	help = "total number of shmem block miss [pg_stat_database.blks_read - pg_stat_database.blks_hit]"
	def update_action(self):
		self.val = self.store.store["blks_read"]

class pgsCacheHit(DbStatCounter):
	title = "HIT"
	metric = "blk"
	width = 6
	help = "total number of shmem block hits [pg_stat_database.blks_hit]"
	def update_action(self):
		self.val = self.store.store['blks_hit']

class pgsIoReadWa(DbStatCounter):
	title = "READWA"
	metric = "wait%"
	help = "percent of time spent on IO read's wait [100 * pg_stat_database.blk_read_time / wall_time] (>= 9.2)"
	def update_action(self):
		td = time.time() - self.time
		wa = self.store.store['blk_read_time']
		self.val = 100 * wa / dt if dt else 0

class pgsIoWriteWa(DbStatCounter):
	title = "WRITEWA"
	metric = "wait%"
	help = "percent of time spent on IO write's wait [100 * pg_stat_database.blk_write_time / wall_time] (>= 9.2)"
	def update_action(self):
		td = time.time() - self.time
		wa = self.store.store['blk_write_time']
		self.val = 100 * wa / dt if dt else 0

class pgsTxnCommit(DbStatCounter):
	title = "COMMIT"
	metric = "txn"
	help = "number of committed transactions [pg_stat_database.xact_commit]"
	def update_action(self):
		self.val = self.store.store['xact_commit']

class pgsTxnRollback(DbStatCounter):
	title = "RLLBCK"
	metric = "txn"
	help = "number of rolled back transactions [pg_stat_database.xact_rollback]"
	def update_action(self):
		self.val = self.store.store['xact_rollback']

class pgsLockWait(DbStatCounter):
	width = 5
	title = "LOCK"
	metric = "cnt"
	help = "number of processes waiting for lock [COUNT(*) FROM pg_locks WHERE NOT granted]"
	rate_fmt = "%d"
	absolute = True
	def update_action(self):
		self.val = DB.execute_fetchval(con, "SELECT COUNT(*) FROM pg_locks WHERE NOT granted")

class pgsDeadlocks(DbStatCounter):
	title = "DEADLOCK"
	metric = "cnt"
	help = "total number of deadlocks [pg_stat_database.deadlocks] (>= 9.2)"
	def update_action(self):
		self.val = self.store.store['deadlocks']

class pgsProcsIdletxn(DbStatCounter):
	width = 5
	title = "PROC"
	metric = "idltxn"
	help = "total number of 'idle in transaction' processes"
	absolute = True
	rate_fmt = "%d"
	def update_action(self):
		key = "<IDLE> in transaction"
		self.val = int(self.store.store[key]) if self.store.store.has_key(key) else 0

class pgsProcsLive(DbStatCounter):
	width = 3
	title = "PROC"
	metric = "live"
	help = "total number of live processes"
	absolute = True
	rate_fmt = "%d"
	def update_action(self):
		self.val = 0
		for key, val in self.store.store.items():
			if key.startswith("<IDLE>"):
				continue
			self.val += int(val)

class PgStats:
	def __init__(self):
		self.sep = " |"
		self.hdr_titles = ""
		self.hdr_metrics = ""
		self.fmt = ""

		pg_ver = PgVersion(con)
		print pg_ver.str

		s_db = PgStatStore("pg_stat_database", ["xact_commit", "xact_rollback", "blks_read", "blks_hit"])
		s_ut = PgStatStore("pg_stat_user_tables", ["n_tup_ins", "n_tup_upd", "n_tup_del"])
		s_utb = PgStatStoreBigUserTables(["idx_scan", "seq_scan", "seq_tup_read"])
		s_pr = PgStatStoreProc()

		if pg_ver.ge(9, 2):
			s_db.append("deadlocks")
			s_db.append("blk_read_time")
			s_db.append("blk_write_time")

		self.stores = [s_db, s_ut, s_utb, s_pr]

		self.groups = [
			("DataBase",  [pgsDbSize()]),
			("Write Ops", [pgsWrIns(s_ut), pgsWrUpd(s_ut), pgsWrDel(s_ut)]),
			("Scan (tables with >%dK rows)" % (opts.scan_threshold / 1000),
				[pgsScanIdx(s_utb), pgsScanSeq(s_utb), pgsScanIdxPerc(s_utb), pgsScanSeqRows(s_utb)]),
			("CacheRead", [pgsCacheHit(s_db), pgsCacheMiss(s_db)]),
			("Locks", [pgsLockWait()] + ([pgsDeadlocks(s_db)] if pg_ver.ge(9, 2) else [])),
			("Transactions", [pgsTxnCommit(s_db), pgsTxnRollback(s_db)]),
			("Proc", [pgsProcsIdletxn(s_pr), pgsProcsLive(s_pr)]),
		]

		if pg_ver.ge(9, 2):
			self.groups.append(("Disk Wait", [pgsIoReadWa(s_db), pgsIoWriteWa(s_db)]))

		self.init()

	def init(self):
		self.counters = []

		for group in self.groups:
			for c in group[1]:
				self.counters.append(c)
				self.hdr_metrics += " " + c.title.rjust(c.width)
				self.fmt += " %%%ds" % c.width
			self.hdr_titles += " " + group[0].rjust(sum(c.width + 1 for c in group[1]) - 1) + self.sep
			self.hdr_metrics += self.sep
			self.fmt += self.sep

	def header(self):
		print "=" * len(self.hdr_titles)
		print self.hdr_titles
		print self.hdr_metrics
		if opts.abs:
			metrics = [c.metric for c in self.counters]
		else:
			metrics = [c.metric if c.absolute else "%s/s" % c.metric for c in self.counters]
		print self.fmt % tuple(metrics)
		print "+" * len(self.hdr_titles)

	def print_row(self):
		if opts.abs:
			vals = ["%d" % c.abs() for c in self.counters]
		else:
			vals = []
			for c in self.counters:
				r = c.val if c.absolute else c.rate
				fmt = c.rate_fmt if c.rate_fmt else ("%.1f" if r < 100 else "%.0f")
				vals.append(fmt % r)
		print self.fmt % tuple(vals)

	def update(self):
		for s in self.stores:
			s.update()
		for c in self.counters:
			c.update()

def pg_usage():
	ps = PgStats()
	ps.header()
	ps.update()
	try:
		i = 0
		while True:
			time.sleep(opts.delay)
			ps.update()
			ps.print_row()
			if opts.count:
				i += 1
				if opts.count <= i:
					break
	except KeyboardInterrupt, e:
		pass

def main():
	global opts
	global con

	test_description = "%prog [options]"

	epilog = "\nCounters description:"
	for name, obj in inspect.getmembers(sys.modules[__name__]):
		if inspect.isclass(obj) and issubclass(obj, DbStatCounter) and hasattr(obj, "title"):
			epilog += "\n%18s - %s" % (obj.title + " (" + obj.metric + ")", obj.help)

	class PgOptParser(OptionParser):
		def format_epilog(self, formatter):
			return self.epilog + "\n"

	p = PgOptParser(test_description, epilog=epilog)
	p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
	p.add_option("-d", "--delay",   type=int, default=2, help="delay between database poll (sec)")
	p.add_option("-n", "--count",   type=int, default=0, help="exit after COUNT iterations")
	p.add_option("-a", "--abs",     action="store_true", help="show absolute values, not rates")
	p.add_option("-r", "--scan-threshold", type=int, default=5000,
		help="skip tables with fewer rows when collect IDX and SEQ scan stats")

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
	pg_usage()

if __name__ == "__main__":
	main()
