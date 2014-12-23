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

import threading
import curses
import mutex
import StringIO
import traceback
import copy


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
	def _execute_fetch(con, query, *args):
		cur = con.cursor()

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


KEY_LEFT = 68
KEY_RIGHT = 67

USER_COL_NAME = 0
USER_COL_WIDTH = 1
USER_COL_TYPE  = 2
USER_COL_ABS   = 3
USER_COL_METRIC = 4
USER_COL_SQL_NAME = 5
USER_COL_HELP = 6

user_cols_def = [
 # title #width  #type    #abs   #metric   #sql_name        #help
 ["Table",     0, "str",   True,  "",       "tablename",     "table name"],
 ["DB",        5, "str",   True,  "",       "dbname",        "database"],
 ["Write",     6, "int",   False, "row/s",  "writes",        "total number of inserted/updated/deleted rows per sec"],
 ["Ins",       6, "int",   False, "row/s",  "n_tup_ins",     "number of inserted rows per second"],
 ["Upd",       6, "int",   False, "row/s",  "n_tup_upd",     "number of updated rows per second"],
 ["Del",       6, "int",   False, "row/s",  "n_tup_del",     "number of deleted rows per second"],
 ["UpdIdx",    8, "float", False, "row/s",  "n_tup_idx_upd", "number of rows updated with index update"],
 ["IdxScan",   9, "int",   False, "scan/s", "idx_scan",      "number of index scans per second"],
 ["SeqScan",   9, "int",   False, "scan/s", "seq_scan",      "number of sequential scans per second"],
 ["SeqRows",   9, "int",   False, "row/s",  "seq_tup_read",  "number of rows per second fetched by seq scans"],
 ["Locks",     6, "int",   True,  "count",  "locks",         "number of processes waiting for lock"],
 ["Reltuples", 10, "int",   True,  "count", "reltuples",     "approximate number of rows in table"]
]

user_cols_select_query = """
SELECT
	%s
FROM
	pg_stat_user_tables U

	LEFT JOIN (
		SELECT
			case WHEN schemaname = 'public'
			THEN
				relname
			ELSE
				schemaname || '.' || relname
			END tablename,
			schemaname AS T_schemaname,
			relname AS T_relname,
			current_database() dbname,
			(n_tup_ins + n_tup_upd + n_tup_del) writes,
			(n_tup_upd - n_tup_hot_upd) n_tup_idx_upd
		FROM
			pg_stat_user_tables
	) T ON (T_relname = relname AND T_schemaname = U.schemaname)

	LEFT JOIN (
		SELECT
			relname AS L_relname,
			nspname AS L_schemaname,
			reltuples,
			case WHEN L_locks > 0
			THEN
				L_locks
			ELSE
				0
			END locks
		FROM
			pg_class C

			LEFT JOIN (
				SELECT
					relation,
					COUNT(*) L_locks
				FROM
					pg_locks
				WHERE
					NOT granted
				GROUP BY
					relation
			) L ON L.relation = C.oid

			LEFT JOIN pg_namespace n ON n.oid = C.relnamespace

	) L ON (L_relname = relname AND L_schemaname = U.schemaname)
"""

user_cols_select_query_for_schema = user_cols_select_query + "WHERE U.schemaname = '%s'"


class PgTop:
	def __init__(self):
		self.scr = None
		self.opts = None
		self.con = []

		self.paused = 0
		self.terminate = False

		self.mutex = threading.Lock()
		self.prev_time = 0

		if sys.stderr.isatty():
			sys.stderr = StringIO.StringIO()

		self.init_user_cols()

	def init_user_cols(self):
		self.user_cols_sorted = 0
		self.user_cols_hash = {}
		self.user_cols_data_prev = {}
		self.user_cols_meta = []
		self.user_cols_view_data = []
		self.user_cols_view_ctime = time.ctime()

		for col in user_cols_def:

			# hide dB if there is only one DB
			if col[USER_COL_NAME].lower() == "db" and len(self.con) == 1:
				continue

			self.user_cols_meta.append(col)

		for n in xrange(0, len(self.user_cols_meta)):
			self.user_cols_hash[self.user_cols_meta[n][USER_COL_NAME]] = n
			if self.opts and self.user_cols_meta[n][USER_COL_NAME] == self.opts.sort:
				self.user_cols_sorted = n

	def init(self, scr, con, opts):
		self.scr = scr
		self.con = con
		self.opts = opts
		self.init_user_cols()

	def fetch_user_cols(self):
		cols = ", ".join([c[USER_COL_SQL_NAME] for c in self.user_cols_meta])
		data = []
		for name, con in self.con.items():
			if self.opts.schema:
				data += DB.execute_fetchall(con, user_cols_select_query_for_schema % (cols, self.opts.schema))
			else:
				data += DB.execute_fetchall(con, user_cols_select_query % cols)
			con.commit()
		return data

	def update_user_cols_view(self):
		self.user_cols_view_ctime = time.ctime()
		sql_data = self.fetch_user_cols()

		total = [0] * len(self.user_cols_meta)
		total[0] = "Total"

		for data in sql_data:
			for n in xrange(1, len(data)):
				if self.user_cols_meta[n][USER_COL_TYPE] == "str":
					total[n] = ""
				else:
					total[n] += data[n] if data[n] else 0
		sql_data = [total] + sql_data

		user_data = {}
		for r in sql_data:
			user_data[r[0]] = r

		if not self.prev_time:
			self.user_cols_data_prev = user_data
			self.prev_time = time.time()
			return None

		self.user_cols_view_data = []

		t = time.time()
		for data in sql_data:
			out = []

			table = data[0]

			for n in xrange(0, len(data)):
				if self.user_cols_meta[n][USER_COL_TYPE] == "str":
					s = str(data[n])
					w = self.user_cols_meta[n][USER_COL_WIDTH]
					if len(s) > w:
						s = s[0:w-3] + "..."
					out.append(s)
				elif self.user_cols_meta[n][USER_COL_ABS]:
					out.append(data[n])
				else:
					new = data[n] if data[n] else 0
					old = self.user_cols_data_prev[table][n] if self.user_cols_data_prev.has_key(table) and \
						self.user_cols_data_prev[table][n] else 0
					out.append(new - old)

			for n in xrange(0, len(out)):
				if self.user_cols_meta[n][USER_COL_METRIC].endswith("/s"):
					if t - self.prev_time:
						out[n] = int(out[n]) / (t - self.prev_time)
				if self.user_cols_meta[n][USER_COL_TYPE] == "int":
					out[n] = round(out[n])

			self.user_cols_view_data.append(out)

		self.user_cols_data_prev = user_data
		self.prev_time = t

	def get_user_cols_view_data(self):
		return sorted(self.user_cols_view_data, key=lambda x:
			(x[self.user_cols_sorted],
			x[self.user_cols_hash['Write']],
			x[self.user_cols_hash['Reltuples']]),
			reverse=True)

	def _refresh(self):
		if not self.scr or self.terminate:
			return
		(max_y, max_x) = self.scr.getmaxyx()

		s = sum([c[1] + 1 for c in self.user_cols_meta])
		s -= self.user_cols_meta[0][USER_COL_WIDTH]
		self.user_cols_meta[0][USER_COL_WIDTH] = max_x - s

		fmt = []
		for c in self.user_cols_meta:
			if c[USER_COL_TYPE] == "int":
				fmt.append("%%%dd" % c[1])
			elif c[USER_COL_TYPE] == "float":
				fmt.append("%%%d.1f" % c[1])
			else:
				fmt.append("%%%ds" % c[1])
		fmt_data = " ".join(fmt)
		fmt_header = " ".join(["%%%ds" % c[USER_COL_WIDTH] for c in self.user_cols_meta])

		self.scr.erase()
		self.scr.addstr(0, 0, "%s | Use: 'left' and 'right' keys - select sortable col; 'p' pause; 'q' quit; 'space' refresh"
			% self.user_cols_view_ctime)
		if self.paused:
			self.scr.addstr(0, 0, "PAUSED! ")
		self.scr.addstr(1, 0, "=" * max_x)
		columns = []
		metrics = []

		for n in xrange(0, len(self.user_cols_meta)):
			c = self.user_cols_meta[n]
			metrics.append(c[USER_COL_METRIC])
			if self.user_cols_sorted == n:
				columns.append("*" + c[USER_COL_NAME])
			else:
				columns.append(c[USER_COL_NAME])

		self.scr.addstr(2, 0, fmt_header % tuple(columns))
		self.scr.addstr(3, 0, fmt_header % tuple(metrics))

		if not self.paused:
			self.update_user_cols_view()
		view = self.get_user_cols_view_data()

		if view == None:
			return

		self.scr.addstr(4, 0, "-" * max_x)
		r = 4
		for row in view:
			r += 1
			if r == max_y:
				break
			self.scr.addstr(r, 0, fmt_data % tuple(row))

		self.scr.refresh()

	def refresh(self):
		self.mutex.acquire()
		try:
			self._refresh()
		except:
			self.mutex.release()
			raise
		self.mutex.release()

	def shift_sorted_col(self, shift):
		self.user_cols_sorted = (len(self.user_cols_meta) + self.user_cols_sorted + shift) % len(self.user_cols_meta)

	def handle_key(self, key):
		if ord(key) == KEY_LEFT:
			self.shift_sorted_col(-1)
		elif ord(key) == KEY_RIGHT:
			self.shift_sorted_col(1)
		elif key == 'p':
			self.paused = self.paused ^ 1
		elif key == ' ':
			if self.paused:
				self.paused = 0
		else:
			return
		self.refresh()

	def getkey(self):
		try:
			# return chr(self.scr.getch()) - thread unsafe
			key = sys.stdin.read(1)
		except KeyboardInterrupt:
			raise
		except:
			return chr(0)

		self.handle_key(key)
		return key

	def handle_exc(self):
		print >> sys.stderr, traceback.format_exc()
		self.mutex.acquire()
		curses.endwin()
		self.mutex.release()
		self.deinit()

	def deinit(self):
		if isinstance(sys.stderr, StringIO.StringIO):
			print sys.stderr.getvalue()
			sys.stderr = sys.stdout
		sys.stdout.flush()

	def __del__(self):
		self.deinit()


def main_loop(pgt):
	try:
		pgt.refresh()
		time.sleep(0.3)
		while 1:
			if not pgt.paused:
				pgt.refresh()
			time.sleep(pgt.opts.delay)
			if pgt.terminate:
				return
	except:
		pgt.handle_exc()
		os._exit(1)

def pg_top(scr, pgt, con, opts):

	curses.noecho()		# disable echo
	curses.cbreak()		# keys are read directly, without hitting Enter
#	curses.curs_set(0)	# disable mouse

	pgt.init(scr, con, opts)
	t = threading.Thread(target=main_loop, args=(pgt,))
	t.daemon = True
	t.start()

	while 1:
		try:
			key = pgt.getkey()
			if key == 'q':
				pgt.terminate = True
				break
		except KeyboardInterrupt:
			break
	pgt.terminate = True

def main():
	test_description = "%prog [options]"
	pgt = PgTop()

	epilog = "\nCounters description:"
	for c in pgt.user_cols_meta:
		epilog += "\n%9s - %s" % (c[USER_COL_NAME], c[USER_COL_HELP])

	class PgOptParser(OptionParser):
		def format_epilog(self, formatter):
			return self.epilog + "\n"

	p = PgOptParser(test_description, epilog=epilog)
	p.add_option("-v", "--verbose", action="store_true", help="enable verbose mode")
	p.add_option("-d", "--delay",   type=int, default=2, help="delay between database poll (sec)")
	p.add_option("-n", "--count",   type=int, default=0, help="exit after COUNT iterations")
	p.add_option("-a", "--abs",     action="store_true", help="show absolute values, not rates")
	p.add_option("-s", "--sort",    type="choice", default="Write",
		choices=tuple([c[USER_COL_NAME] for c in user_cols_def]), help="sort by given column (default is '%default')")
	p.add_option("-S", "--schema",  type="string",
		help="take into account only given schema (default: all schemas)")

	defdb = ""
	defusr = "postgres"
	if HAS_PA:
		g = OptionGroup(p, "If you have pa.conf")
		g.add_option("-c", "--config",  type="string", default=None, help="PA config file [default: %default]")
		g.add_option("", "--poa",       action="store_true", help = "connect to POA only")
		g.add_option("", "--pba",       action="store_true", help = "connect to PBA only")
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

	dbs = []
	con = {}

	if HAS_PA:
		if not opts.config and not opts.db_host:
			p.error("either -c or --db-host option must be provided")
	else:
		if not opts.db_host:
			p.error("--db-host option must be provided")

	if HAS_PA and opts.config:
		non = not opts.pba and not opts.poa
		pa_config.init(opts.config)
		cfg = pa_config.get()
		if (non or opts.pba) and cfg.pba_db.host:
			dbs.append(DB(cfg.pba_db.ip, cfg.pba_db.db_port, cfg.pba_db.db_name, cfg.pba_db.db_user, cfg.pba_db.db_pass))
		if (non or opts.poa) and cfg.poa_db.host:
			dbs.append(DB(cfg.poa_db.ip, cfg.poa_db.db_port, cfg.poa_db.db_name, cfg.poa_db.db_user, cfg.poa_db.db_pass))
	else:
		dbs.append(DB(opts.db_host, opts.db_port, opts.db_name, opts.db_user, opts.db_pass))

	for db in dbs:
		print "Connecting to %s..." % str(db)
		try:
			con[db.get_name()] = db.connect()
		except Exception, x:
			print "failed to connect: ", type(x), str(x)

	if con:
		try:
			curses.wrapper(pg_top, pgt, con, opts)
		except:
			pgt.handle_exc()

if __name__ == "__main__":
	main()
