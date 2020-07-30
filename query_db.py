# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import time
import shutil
import tempfile
import threading
import subprocess

import sublime
import sublime_plugin


BQ_CMD = shutil.which("bq")


PROGRESS = "⡇⣆⣰⢸⠹⠏"


# https://regex101.com/r/hO3eH6/10
CONNECT_URL_RE = re.compile(r"""
^
(?P<scheme>[^:]+)
://
(?:
    (?:
        (?P<user>[^:@]+)
        @
        (?P<host>[^:/]+)
        (?:
            \:
            (?P<port> \d+)
        )?
    )?
    (?:\/ (?P<db> .+))?
)
$
""", flags=re.VERBOSE)


QUERY_DB_URL_RE = re.compile(r"^--\s?db(?:_url)?=(.*)$", flags=re.MULTILINE)

QUERY_DB_ID_RE = re.compile(r"^--\s?db=(\w+)$", flags=re.MULTILINE)


def parse_db_url(url):
    match = CONNECT_URL_RE.match(url)
    if match is None:
        errmsg = "Invalid URL for db connecion: '{}'".format(url)
        raise ValueError(errmsg)
    return match.groupdict()


def mk_time_str(duration):
    if duration < 1:
        return "{:}ms".format(int(duration * 1000))

    if duration < 60:
        return "{:5.1f}s".format(duration)

    mins = int(duration) // 60
    secs = duration % 60
    return "{:02}:{:02} min".format(mins, round(secs))


class StatusUpdateThread(threading.Thread):

    def __init__(self):
        self.queries = []
        self.messages = {}  # message -> lifetime
        self.status_linger = 2.0
        super(StatusUpdateThread, self).__init__()

    def status_id(self, thread):
        return "db_query_{}".format(hex(id(thread)))

    def erase_status(self, thread):
        thread.view.erase_status(self.status_id(thread))

    def update_status(self, thread):
        totaltime = (thread.t2 or time.time()) - thread.t0
        totaltime_str = mk_time_str(totaltime)

        indicator_tick = int(totaltime * 10) % len(PROGRESS)
        indicator = PROGRESS[indicator_tick]

        thread.view.set_status(self.status_id(thread), (
            "[db query {} {:} read {}]"
        ).format(indicator, totaltime_str, 0))

    def run(self):
        while True:
            if self.queries:
                running_query_threads = []
                for t in self.queries:
                    is_alive = not t.t2
                    is_linger = t.t2 and (time.time() - t.t2) < self.status_linger
                    if is_alive or is_linger:
                        running_query_threads.append(t)
                        self.update_status(t)
                    else:
                        self.erase_status(t)
                self.queries = running_query_threads

                time.sleep(0.1)
            else:
                time.sleep(0.5)


STATUS_UPDATE_THREAD = StatusUpdateThread()
STATUS_UPDATE_THREAD.daemon = True
STATUS_UPDATE_THREAD.start()


class QueryThread(threading.Thread):

    def __init__(self, query, window, settings, view=None):
        self.query = query
        self.window = window
        self.settings = settings
        self.view = view
        self.t0 = time.time()
        self.t1 = None
        self.t2 = None
        super(QueryThread, self).__init__()

    def parse_db_connect_params(self, query):
        db_url_match = QUERY_DB_URL_RE.search(self.query)
        if db_url_match:
            db_url = db_url_match.group(1)
        else:
            db_id_match = QUERY_DB_ID_RE.search(self.query)
            if db_id_match:
                db_id = db_id_match.group(1)
                if db_id in self.settings['urls']:
                    db_url = self.settings['urls'][db_id]
                else:
                    raise KeyError("Invalid -- db={}".format(db_id))
            else:
                db_url = self.settings['urls'].get('default')

        if db_url:
            return parse_db_url(db_url)
        else:
            raise Exception("No DB Connection Configured")

    def query_file(self):
        query = self.query
        if not query.endswith(";"):
            query += ";"

        fobj = tempfile.NamedTemporaryFile(mode="wt", encoding='utf-8', suffix=".sql")
        fobj.write(query)
        fobj.flush()
        return fobj

    def run(self):
        try:
            db_connect_params = self.parse_db_connect_params(self.query)
            scheme = db_connect_params['scheme']

            PSQL_SCHEMES = ("pg", "psql", "postgres", "postgresql", "rs", "redshift")
            BIGQUERY_SCHEMES = ("bq", "bigquery")

            env = os.environ.copy()
            with self.query_file() as fobj:
                if scheme in PSQL_SCHEMES:
                    cmd = [
                        self.settings['executables']['psql'],
                        "--no-password",
                        "--host", db_connect_params['host'],
                        "--port", db_connect_params['port'],
                        "--user", db_connect_params['user'],
                        "--dbname", db_connect_params['db'],
                        "--file", fobj.name,
                        # "--output", out_path,
                    ]
                    env['PGCONNECT_TIMEOUT'] = "3"
                    shell = False
                elif scheme in BIGQUERY_SCHEMES:
                    bq_cmd = self.settings['executables'].get('bq', BQ_CMD)
                    cmd = "cat {} | {} query --nouse_legacy_sql --max_rows=10000".format(fobj.name, bq_cmd)
                    if db_connect_params['host']:
                        cmd += " --project_id=" + db_connect_params['host']
                    if db_connect_params['db']:
                        cmd += " --dataset_id=" + db_connect_params['db']

                    env['CLOUDSDK_ACTIVE_CONFIG_NAME'] = db_connect_params['user']
                    shell = True
                else:
                    errmsg = "Unknown scheme: {}".format(scheme)
                    raise Exception(errmsg)

                # TODO (mb 2016-08-19): write to out path and check
                #   its file size instead of doing proc.communicate()
                # out_path = "/tmp/tmp_pgsql_output_{}.txt".format(id(self))

                self.proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    shell=shell,
                )

                # TODO (mb 2016-05-23): truncate large output and write to
                #   separate file.
                out_buffer = []
                err_buffer = []

                stdout_value, stderr_value = self.proc.communicate()
                ttfb = time.time()
                out_buffer.append(stdout_value.decode('utf-8'))
                err_buffer.append(stderr_value.decode('utf-8'))

                ret_code = self.proc.wait()

            # TODO (mb 2016-08-24): write to file and check st_mtime/st_size
            #   rather than reading from stdout.
            self.t1 = time.time()

            response_time = (ttfb - self.t0)
            total_time = (time.time() - self.t0)
            response_time_str = mk_time_str(response_time)
            total_time_str = mk_time_str(total_time)

            out_text = "".join(out_buffer).rstrip()
            err_text = "".join(err_buffer).rstrip()

            output = (
                "-" * 20 + " QUERY " + "-" * 20 + "\n" +
                self.query + "\n" +
                "-" * 20 + " TIMING " + "-" * 20 + "\n" +
                "Response Time: {:9.2f}ms ({})\nTotal Time   : {:9.2f}ms ({})\n".format(
                    response_time * 1000, response_time_str,
                    total_time * 1000, total_time_str,
                ) +
                "-" * 20 + " RESULT " + "-" * 20 + "\n" +
                out_text
            )

            if ret_code or err_buffer:
                output += (
                    "\n\nExit Code: " + str(ret_code) + "\n" +
                    err_text + "\n" +
                    "-" * 48 + "\n"
                )

            if self.view is None:
                self.view = self.window.new_file()
                self.view.set_syntax_file("Packages/SQL/SQL.sublime-syntax")

            self.view.run_command("query_db_output", {
                "output" : output
            })
        finally:
            self.t2 = time.time()
            self.view.set_status("psql_running", "")


class QueryDbCommand(sublime_plugin.TextCommand):

    def get_settings(self):
        executables = {}
        urls = {}

        settings = self.view.settings()
        default_executables = settings.get('query_db_executables')
        default_urls = settings.get('query_db_connection_urls')
        if default_executables:
            executables.update(default_executables)
        if default_urls:
            urls.update(default_urls)

        user_settings = sublime.load_settings("SublimeQueryDB.sublime-settings")
        user_executables = user_settings.get('query_db_executables')
        user_urls = user_settings.get('query_db_connection_urls')

        if isinstance(user_executables, dict):
            executables.update(user_executables)
        if isinstance(user_urls, dict):
            urls.update(user_urls)

        return {'executables': executables, 'urls': urls}

    def run(self, edit, connection_id='default'):
        sels = self.view.sel()
        for sel in sels:
            sel_text = self.view.substr(sel)
            break

        if not sel_text:
            # see if we are in a query file
            sel_text = self.view.substr(sublime.Region(0, self.view.size()))
            if not sel_text.startswith("-------------------- QUERY --------------------"):
                return
            if "-------------------- TIMING --------------------" not in sel_text:
                return
            if "-------------------- RESULT --------------------" not in sel_text:
                return

        query = sel_text
        query = query.split("-------------------- QUERY --------------------")[-1]
        query = query.split("-------------------- TIMING --------------------")[0]
        query = query.strip()

        t = QueryThread(
            query, self.view.window(), settings=self.get_settings(), view=self.view
        )
        t.start()
        STATUS_UPDATE_THREAD.queries.append(t)


class QueryDbOutputCommand(sublime_plugin.TextCommand):

    def run(self, edit, output):
        self.view.replace(edit, sublime.Region(0, self.view.size()), output)
        self.view.settings().set("word_wrap", False)
