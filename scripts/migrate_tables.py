from __future__ import absolute_import, print_function
from __future__ import unicode_literals, division

import argparse
import json
import logging
import sqlite3
import subprocess
import sys
from datetime import datetime
from time import sleep

from six.moves import input

logger = logging.getLogger(__name__)


try:
    import textwrap
    textwrap.indent
except AttributeError:
    def indent(text, amount, ch=' '):
        padding = amount * ch
        return ''.join(padding + line for line in text.splitlines(True))
else:
    def indent(text, amount, ch=' '):
        return textwrap.indent(text, amount * ch)


def parse_date(s, default=None):
    if not s:
        return default

    try:
        return datetime.strptime(s, "%Y-%m-%d").date().replace(day=1)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


class Database(object):
    def __init__(self, db_path):
        self.db_path = db_path

    def execute(self, query, *params):
        with self.db:
            res = self.db.execute(query, params)
            return res.fetchall()

    def get_table(self, table_name):
        rows = self.execute('select * from tables where source_table = ?', table_name)
        if not rows:
            raise Exception('No table found with name "{}"'.format(table_name))

        row = rows[0]
        if row['migrated']:
            raise Exception('Table "{}" has already been migrated'.format(table_name))

        return row['source_table'], row['target_table']

    def get_tables(self, start_date=None, end_date=None):
        params = []
        filters = []
        if start_date and end_date:
            filters.append('date is not null')

        if start_date:
            if not end_date:
                filters.append("date is null OR date >= ?")
            else:
                filters.append("date >= ?")
            params.append(start_date)

        if end_date:
            filters.append("date < ?")
            params.append(end_date)

        query = 'SELECT * FROM tables WHERE migrated is null'
        if filters:
            query += ' and ({})'.format(' and '.join(filters))
        return [
            (row['source_table'], row['target_table'])
            for row in self.execute(query, *params)
        ]

    def mark_migrated(self, tables):
        with self.db:
            for table in tables:
                self.db.execute('update tables set migrated = 1 where source_table = ?', [table])

    def __enter__(self):
        self.db = sqlite3.connect(self.db_path)
        self.db.row_factory = sqlite3.Row

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()


class CommandWithContext(object):
    def __init__(self, command, context, callback=None):
        self.context = context
        self.command = command
        self.process = None
        self.callback = callback

    def run(self):
        self.process = subprocess.Popen(self.command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return self

    def poll(self):
        ret = self.process.poll()
        if ret is not None:
            self.call_callback(ret)
        return ret

    def wait(self):
        ret = self.process.wait()
        self.call_callback(ret)
        return ret

    def call_callback(self, ret_code):
        if self.callback:
            stdout = self.process.stdout.read()
            stderr = self.process.stderr.read()
            self.callback(self.context, ret_code, stdout, stderr)

    def __repr__(self):
        return 'CommandWithContext({})'.format(self.context)


class MigrationPool(object):
    def __init__(self, max_size, callback, stop_on_error=True):
        self.max_size = max_size
        self._pool = set()
        self.has_errors = False
        self.callback = callback
        self.stop_on_error = stop_on_error

    def run(self, command, context):
        if self.has_errors and self.stop_on_error:
            return

        self.wait_for_capacity()

        process = CommandWithContext(command, context, self.callback)
        self._pool.add(process.run())

    def wait_for_capacity(self):
        while len(self._pool) >= self.max_size:
            sleep(0.5)
            for process in frozenset(self._pool):
                code = process.poll()
                if code is not None:
                    self._pool.remove(process)
                    self.has_errors |= code != 0

    def wait(self):
        for process in frozenset(self._pool):
            code = process.poll()
            if code is None:
                process.wait()


class Migrator(object):
    def __init__(self, db_path, source_db, source_host, source_user,
                 target_db, target_host, target_user,
                 start_date, end_date,
                 start_table, only_table, confirm, dry_run,
                 stop_on_error):
        self.db_path = db_path
        self.source_db = source_db
        self.source_host = source_host
        self.source_user = source_user
        self.target_db = target_db
        self.target_host = target_host
        self.target_user = target_user
        self.start_date = start_date
        self.end_date = end_date
        self.start_table = start_table
        self.only_table = only_table
        self.confirm = confirm
        self.dry_run = dry_run
        self.stop_on_error = stop_on_error

        self.error_log = 'icds_citus_migration_errors-{}.log'.format(datetime.utcnow().isoformat())

        self.db = Database(self.db_path)

    def run(self, max_concurrent):
        with self.db:
            if self.only_table:
                table = self.db.get_table(self.only_table)
                tables = [table]
            else:
                tables = self.db.get_tables(self.start_date, self.end_date)

            if not tables:
                raise Exception("No table to migrate")

            if self.dry_run or _confirm('Preparing to migrate {} tables.'.format(len(tables))):
                self.migrate_tables(tables, max_concurrent)

    def write_error(self, context, ret_code, stdout, stderr):
        stderr = stderr.decode()
        try:
            lines = stderr.splitlines()
            if len(lines) == 2:
                error, detail = lines
                error = error.split(':  ')[1]
                detail = detail.split(':  ')[1]
        except Exception:
            error = None
            detail = None

        error_json = context.copy()
        error_json.update({
            'ret_code': ret_code,
            'stdout': stdout.decode(),
            'stderr': stderr,
            'ERROR': error,
            'DETAIL': detail,
        })
        with open(self.error_log, 'a') as out:
            out.write('{}\n'.format(json.dumps(error_json)))

    def migrate_tables(self, tables, max_concurrent):
        table_sizes = None
        total_size = None
        progress = []
        completed = []
        total_tables = len(tables)
        start_time = datetime.now()

        if not self.dry_run:
            table_sizes = get_table_sizes(self.source_db, self.source_host, self.source_user)

            if table_sizes is not None:
                source_tables = {source_table for source_table, target_table in tables}
                total_size = sum([size for table, size in table_sizes.items() if table in source_tables])

        def _update_progress(context, ret_code, stdout, stderr):
            completed.append(context['source_table'])
            success = ret_code == 0
            print('{} {}'.format('[ERROR] ' if not success else '', context['cmd']))
            if stdout:
                print(stdout.decode())
            if stderr:
                print(stderr.decode())
            if not success:
                self.write_error(context, ret_code, stdout, stderr)
            else:
                self.db.mark_migrated([context['source_table']])
                table_progress = len(completed) + 1
                elapsed = datetime.now() - start_time
                if table_sizes:
                    progress.append(context['size'])
                    data_progress = sum(progress)
                    remaining = elapsed // data_progress * total_size if data_progress else 'unknown'
                    print(
                        '\nProgress: {:.1f}% data ({} of {}), '
                        '{:.1f}% tables ({} of {}) in {} ({} remaining)\n'.format(
                            (100 * float(data_progress) / total_size) if total_size else 100,
                            data_progress, total_size,
                            100 * float(table_progress) / total_tables,
                            table_progress, total_tables,
                            elapsed, remaining
                        )
                    )
                else:
                    print('\nProgress: {:.1f}% tables ({} of {}) in {}\n'.format(
                        100 * float(table_progress) / total_tables,
                        table_progress, total_tables, elapsed
                    ))

        commands = self.get_dump_load_commands(tables)

        pool = MigrationPool(max_concurrent, _update_progress, stop_on_error=self.stop_on_error)
        for table_index, [source_table, target_table, cmd] in enumerate(commands):
            confirm_msg = 'Migrate {} to {}'.format(source_table, target_table)
            if not self.dry_run and (not self.confirm or _confirm(confirm_msg)):
                pool.run(cmd, {
                    'cmd': cmd,
                    'source_table': source_table,
                    'target_table': target_table,
                    'size': table_sizes[source_table] if table_sizes else 0
                })
            else:
                print(cmd)

        pool.wait()

        if pool.has_errors:
            sys.exit(1)

    def get_dump_load_commands(self, tables):
        for source_table, target_table in tables:
            cmd = 'pg_dump -h {} -U {} -t {} {} --data-only --no-acl'.format(
                self.source_host, self.source_user, source_table, self.source_db
            )
            if target_table:
                cmd += ' | sed "s/\\"\\?{}\\"\\?/\\"{}\\"/g"'.format(source_table, target_table)
            cmd += ' | psql -h {} -U {} -v ON_ERROR_STOP=1 {} --single-transaction'.format(
                self.target_host, self.target_user, self.target_db
            )

            yield source_table, target_table, cmd


def get_table_sizes(source_db, source_host, source_user):
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print('\nsqlalchemy not installed. Progress not supported.\n')
        return

    engine = create_engine("postgresql://{}:@{}/{}".format(source_user, source_host, source_db))
    with engine.begin() as conn:
        res = conn.execute("select relname, n_live_tup from pg_stat_user_tables")
        return {
            row.relname: row.n_live_tup
            for row in res
        }


def _confirm(msg):
    confirm_update = input(msg + ' [yes / no] ')
    return confirm_update == 'yes'


def main():
    parser = argparse.ArgumentParser(description="""
        Migrate DB tables from one DB to another using pg_dump.
        Compainion to custom/icds_reports/management_commands/generate_migration_tables.py
    """)
    parser.add_argument('db_path', help='Path to sqlite DB containing list of tables to migrate')
    parser.add_argument('-D', '--source-db', required=True, help='Name for source database')
    parser.add_argument('-O', '--source-host', required=True, help='Name for source database')
    parser.add_argument('-U', '--source-user', required=True, help='Name for source database')
    parser.add_argument('-d', '--target-db', required=True, help='Name for target database')
    parser.add_argument('-o', '--target-host', required=True, help='Name for target database')
    parser.add_argument('-u', '--target-user', required=True, help=(
        'PG user to connect to target DB as. This user should be able to connect to the target'
        'DB without a password.'
    ))
    parser.add_argument('--start-date', type=parse_date, help=(
        'Only migrate tables with date on or after this date. Format YYYY-MM-DD'
    ))
    parser.add_argument('--end-date', type=parse_date, help=(
        'Only migrate tables with date before this date. Format YYYY-MM-DD'
    ))
    parser.add_argument('--start-after-table', help='Skip all tables up to and including this one')
    parser.add_argument('--table', help='Only migrate this table')
    parser.add_argument('--confirm', action='store_true', help='Confirm before each table.')
    parser.add_argument('--dry-run', action='store_true', help='Only output the commands.')
    parser.add_argument('--parallel', type=int, default=1, help='How many commands to run in parallel')
    parser.add_argument('--no-stop-on-error', action='store_true', help=(
        'Do not stop the migration if an error is encountered'
    ))

    args = parser.parse_args()

    migrator = Migrator(
        args.db_path, args.source_db, args.source_host, args.source_user,
        args.target_db, args.target_host, args.target_user,
        args.start_date, args.end_date,
        args.start_after_table, args.table, args.confirm, args.dry_run,
        not args.no_stop_on_error
    )
    migrator.run(args.parallel)


if __name__ == "__main__":
    main()
