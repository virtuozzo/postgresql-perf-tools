## PostgreSQL Performance Monitoring Tools
This package includes three useful scripts aimed to help to pinpoint
performance  issues on systems with PostgreSQL as database backend.

All scritps are written in Python.
Requirements:
* Python 2.6+
* psycopg2 module (http://initd.org/psycopg/)

All scripts require a connect to PostgreSQL database. They take traditional
set of DB credentials: host address, port (5432 by default), database name,
database user and password.

Note: you may need to modify pg_hba.conf file to allow the scripts to
connect to the server. Please refer to official documentaion:
http://www.postgresql.org/docs/9.3/static/auth-pg-hba-conf.html

For detailed list of command line options use --help / -h option.

### pg-top
*pg-top.py* uses pg_stat_user_tables system table to get current statistics
of server activity. The information is represented in 'top'-like screen,
per table and total for the server, and updated dynamically.

The following data is reported:
* total number of inserted/updated/deleted rows per sec
* number of inserted rows per second
* number of updated rows per second
* number of deleted rows per second
* number of rows updated with index update
* number of index scans per second
* number of sequential scans per second
* number of rows per second fetched by seq scans
* number of processes waiting for lock
* approximate number of rows in table

### pg-stat
*pg-stat.py* is a command-line tool to get advanced server statistics in
real-time. The information is represented in tabular form, similar to
'vmstat' output. By default, new data row is printed each 2 seconds.

The following data is reported:
* size of database in kilobytes
* write operations: number of rows inserted/updated/deleted (into user tables)
* total number of index scans
* total number of sequential scans
* percentage of sequential scans
* total number of live rows fetched by seq scans
* cache: total number of shmem block hit/miss
* IO: percent of time spent on waiting to read/write the device (>=9.2)
* number of processes waiting for lock
* total number of deadlocks (>= 9.2)
* number of transactions committed/rolled back
* total number of 'idle in transaction' processes
* total number of live processes

### pg-info
*pg-info.py* script gathers static performance-related information
from the pg_stat_xxx tables and tries to identify potential problem sources.

In particular, the following data is reported:
* Size of entire database on disk
* Top tables sorted by size on disk
* Large tables with missing indexes
* Less frequently accessed indexes ordered by size
* Most frequently modified tables


## Authors And Contributors
These scripts were created as inhouse tools at Parallels (www.parallels.com),
by Alexander Andreev (aandreev@parallels.com).

Do not hesitate to send your patches, issues and proposals!

## License
Released under [GPLv2 License](https://github.com/CloudServer/postgresql-perf-tools/blob/master/LICENSE)


