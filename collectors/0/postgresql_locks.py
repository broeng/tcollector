#!/usr/bin/env python
"""
Mutex stats collector for PostgreSQL.

Please, set login/password at etc/postgresql.conf . Collector uses socket
file for DB connection so set 'unix_socket_directory' at postgresql.conf.
Alternative the collector will try some default locations and abort if the
postgres socket could not be found.
"""

from collectors.lib import utils
import sys
import os
import time
import socket
import errno
import pwd

try:
    import psycopg2
except ImportError:
    psycopg2 = None  # handled in main()

COLLECTION_INTERVAL = 15  # seconds
CONNECT_TIMEOUT = 2  # seconds


# Directories under which to search socket files
SEARCH_DIRS = (
    "/var/run/postgresql",  # Debian default
    "/var/pgsql_socket",  # MacOS default
    "/usr/local/var/postgres",  # custom compilation
    "/tmp",  # custom compilation
)


def find_sockdir():
    """Returns a path to PostgreSQL socket file to monitor."""
    for directory in SEARCH_DIRS:
        for dirpath, _, dirfiles in os.walk(directory, followlinks=True):
            for name in dirfiles:
                # ensure selection of PostgreSQL socket only
                if utils.is_sockfile(os.path.join(dirpath, name)) and "PGSQL" in name:
                    return(dirpath)
    return None


def find_owner(filename):
    """Find socket owner"""
    return pwd.getpwuid(os.stat(filename).st_uid).pw_name


def change_user(user):
    """Change uid of the executing user to a named one"""
    uid = pwd.getpwnam(user)[2]
    os.setuid(uid)

def default(value):
    return value.replace(" ", "_") if value is not None else "NA"

def postgres_connect(sockdir):
    """ Connects to the PostgreSQL server using the specified socket file.

        If user is not configured in postgresqlconf use postgres as user with
        no password.
    """

    try:
        from collectors.etc import postgresqlconf
        user, password = postgresqlconf.get_user_password()
    except ImportError:
        user, password = "postgres", ""

    try:
        return psycopg2.connect(
            "host='%s' user='%s' password='%s' "
            "connect_timeout='%s' dbname=postgres" % (sockdir, user, password, CONNECT_TIMEOUT))
    except (EnvironmentError, EOFError, RuntimeError, socket.error), e:
        utils.err("Couldn't connect to DB :%s" % (e))


def collect(db):
    """
    Collects and prints mutex states.

    Here we collect information about the transactions and their locks
    see https://www.postgresql.org/docs/current/view-pg-locks.html
        http://www.postgresql.org/docs/9.2/static/monitoring-stats.html
    """

    try:
        cursor = db.cursor()

        cursor.execute(
            "SELECT a.datname,"
            "l.mode,"
            "l.GRANTED,"
            "a.usename,"
            "a.pid, "
            "age(now(), a.query_start) AS age "
            "FROM pg_stat_activity a "
            "JOIN pg_locks l ON l.pid=a.pid "
            "ORDER BY a.query_start;"
        )

        stats = cursor.fetchall()

        for dbname, mode, granted, user, pid, age in stats:
            # Calculate total seconds spend in milliseconds and cap to positive values
            total_age = age.total_seconds() if age is not None else 0
            age_milli = max([total_age * 1000, 0])
            ts = time.time()
            # Print the datum, tscollector will listen on std out
            print("postgresql.locks %i %i database=%s mode=%s granted=%s user=%s pid=%s" %
                  (ts, age_milli, default(dbname), default(mode), default(granted), default(user), default(pid)))

    except (EnvironmentError, EOFError, RuntimeError, socket.error), e:
        if isinstance(e, IOError) and e[0] == errno.EPIPE:
            # exit on a broken pipe. There is no point in continuing
            # because no one will read our stdout anyway.
            utils.err("error: failed to collect data: %s" % e)
            sys.exit(2)
        utils.err("error: failed to collect data: %s" % e)


def main(_):
    """Collects and dumps stats from a PostgreSQL server."""

    if psycopg2 is None:
        utils.err("error: Python module 'psycopg2' is missing")
        return 13  # Ask tcollector to not respawn us

    sockdir = find_sockdir()
    if sockdir is None:  # Nothing to monitor
        utils.err("error: Postgresql installation is missing, it is not possible to find socket file")
        return 13

    # Find the postgres user and deescalate from root to pg user.
    owner = find_owner(sockdir)
    change_user(owner)

    # Connect to the db
    db = postgres_connect(sockdir)
    db.autocommit=True

    # Collect lock stats
    while True:
        collect(db)
        sys.stdout.flush()
        time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
    sys.stdin.close()
    sys.exit(main(sys.argv))
