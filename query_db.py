import io
import re
import time
import threading
import subprocess

import sublime
import sublime_plugin


# https://regex101.com/r/hO3eH6
DB_URL_RE = re.compile(r"""
^
    (?P<connector>[^:]+)
    ://
    (?:
        (?:
            (?P<username>[^:@]+)
            @
            (?P<host>[^:/]+)
            (?:
                \:
                (?P<port> \d+)
            )?
        )?
        (?: (?P<database> \/ .+))
    )
$
""", re.VERBOSE)


def parse_credentials_url(url):
    match = DB_URL_RE.match(url)
    if match is None:
        raise ValueError("Invalid URL for db connecion: '{}'".format(url))
    return match.groupdict()


class StatusUpdateThread(threading.Thread):

    def __init__(self):
        self.queries = []
        self.messages = {}  # message -> lifetime
        self.status_linger = 2.0
        super(StatusUpdateThread, self).__init__()

    def status_id(self, thread):
        return "psql_query_{}".format(hex(id(thread)))

    def erase_status(self, thread):
        thread.view.erase_status(self.status_id(thread))

    def update_status(self, thread):
        totaltime = (thread.t2 or time.time()) - thread.t0
        if totaltime < 1:
            totaltime_str = "{:}ms".format(int(totaltime * 1000))
        else:
            totaltime_str = "{:.3f}s".format(totaltime)
        thread.view.set_status(self.status_id(thread), (
            "[psql {:} read {}]"
        ).format(totaltime_str, 0))

    def run(self):
        while True:
            if self.queries:
                running_query_threads = []
                for t in self.queries:
                    is_alive = not t.t2 or (time.time() - t.t2) < self.status_linger
                    if is_alive:
                        running_query_threads.append(t)
                        self.update_status(t)
                    else:
                        self.erase_status(t)
                self.queries = running_query_threads

            time.sleep(.07)


STATUS_UPDATE_THREAD = StatusUpdateThread()
STATUS_UPDATE_THREAD.daemon = True
STATUS_UPDATE_THREAD.start()


class QueryThread(threading.Thread):

    def __init__(self, query, window, view=None):
        self.query = query
        self.window = window
        self.view = view
        self.t0 = time.time()
        self.t1 = None
        self.t2 = None
        super(QueryThread, self).__init__()

    def run(self):
        # TODO (mb 2016-05-25): get configuration from
        #   plugin config file, or parse it from the query
        user = "ddmuser"
        port = "5439"
        db = "ddm"
        host = "consul.nt.vc"
        # db = "adm"
        # host = "senator.nt.vc"
        # language = "sql"

        # config_path = "/tmp/.pgpass"
        # with io.open(config_path, encoding='utf-8', mode='w') as fh:
        #     fh.write("\n".join([
        #         "user=" + user,
        #         "password=" + password,
        #         "language=" + language,
        #     ]))

        query_path = "/tmp/tmp_pgsql_query.sql"
        with io.open(query_path, encoding='utf-8', mode='w') as fh:
            query = self.query
            if not query.endswith(";"):
                query += ";"
            fh.write(query)

        out_path = "/tmp/tmp_pgsql_output_{}.txt".format(id(self))

        self.proc = subprocess.Popen(
            [
                "psql",
                "--no-password",
                "--host", host,
                "--port", port,
                "--user", user,
                "--dbname", db,
                "--file", query_path,
                # TODO (mb 2016-08-19): write to out path and check
                #   its file size instead of doing proc.communicate()
                # "--output", out_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # env=env,
        )

        # TODO (mb 2016-05-23): trunkate large output and write to
        #   separate file.
        out_buffer = []
        err_buffer = []

        stdout_value, stderr_value = self.proc.communicate()
        t_first_bytes = time.time()
        out_buffer.append(stdout_value.decode('utf-8'))
        err_buffer.append(stderr_value.decode('utf-8'))

        ret_code = self.proc.wait()

        # TODO (mb 2016-08-24): write to file and check st_mtime/st_size
        #   rather than reading from stdout.
        self.t1 = time.time()

        output = ""
        if ret_code == 0:
            output += (
                "-" * 20 + " QUERY " + "-" * 20 + "\n" +
                self.query + "\n" +
                "-" * 20 + " TIMING " + "-" * 20 + "\n" +
                "Response Time: {:9.2f}ms\nTotal Time   : {:9.2f}ms\n".format(
                    (t_first_bytes - self.t0) * 1000,
                    (time.time() - self.t0) * 1000,
                ) +
                "-" * 20 + " RESULT " + "-" * 20 + "\n" +
                "".join(out_buffer).rstrip()
            )

        if any(err_buffer) or ret_code:
            output += (
                "\n\nExit Code: " + str(ret_code) + "\n" +
                "".join(err_buffer) + "\n" +
                "-" * 48 + "\n"
            )

        self.t2 = time.time()

        self.view.set_status("psql_running", "")

        if self.view is None:
            self.view = self.window.new_file()
            self.view.set_syntax_file("Packages/SQL/SQL.sublime-syntax")

        self.view.run_command("query_db_output", {
            "output" : output
        })


# class PsqlDbQueryCommand(sublime_plugin.TextCommand):

#     def run(self, edit):
#         # TODO (mb 2016-05-25): if the selection is empty/not a valid
#         # sql statement, extend the selection to see if it is within
#         # a valid sql statement and execute that.
#         sels = self.view.sel()
#         for sel in sels:
#             sel_text = self.view.substr(sel)
#             break
#         if not sel_text:
#             # get whole text insted of selection
#             sel_text = self.view.substr(sublime.Region(0, self.view.size()))
#         if not sel_text:
#             return

#         query = sel_text.strip()

#         t = QueryThread(query, self.view.window())
#         t.start()
#         STATUS_UPDATE_THREAD.queries.append(t)


class QueryDbCommand(sublime_plugin.TextCommand):

    def run(self, edit, connection_id='default'):
        settings = self.view.settings()
        # print(settings.get('query_db_executables'))
        # print(settings.get('query_db_connector_defaults'))
        # print(settings.get('query_db_connection_urls'))
        # print(settings.get('tab_size'))
        sels = self.view.sel()
        for sel in sels:
            sel_text = self.view.substr(sel)
            break

        if not sel_text:
            # see if we are in a query file
            in_place = True
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

        t = QueryThread(query, self.view.window(), view=self.view)
        t.start()
        STATUS_UPDATE_THREAD.queries.append(t)


class QueryDbOutputCommand(sublime_plugin.TextCommand):

    def run(self, edit, output):
        self.view.replace(edit, sublime.Region(0, self.view.size()), output)
        self.view.settings().set("word_wrap", False)
