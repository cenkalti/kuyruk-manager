import json
import os
import socket
import logging
import threading
from datetime import datetime
from time import sleep
from functools import total_ordering, wraps, partial

from flask import Flask, render_template, redirect, request, url_for, jsonify
from werkzeug.serving import run_simple
import rpyc
from rpyc.utils.server import ThreadedServer

import kuyruk
from kuyruk.signals import worker_start, worker_init

logger = logging.getLogger(__name__)

CONFIG = {
    "MANAGER_HOST": "127.0.0.1",
    "MANAGER_PORT": 16501,
    "MANAGER_HTTP_PORT": 16500,
    "SENTRY_PROJECT_URL": None,
}

ACTION_WAIT_TIME = 1  # seconds


def start_daemon_thread(target, args=()):
    t = threading.Thread(target=target, args=args)
    t.daemon = True
    t.start()
    return t


def retry(sleep_seconds=1, stop_event=threading.Event(),
          on_exception=lambda e: logger.debug(e)):
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            while not stop_event.is_set():
                try:
                    f(*args, **kwargs)
                except Exception as e:
                    if on_exception:
                        on_exception(e)
                    if sleep_seconds:
                        sleep(sleep_seconds)
        return inner
    return decorator


def _connect_rpc(worker):
    conn = rpyc.connect(worker.kuyruk.config.MANAGER_HOST,
                        worker.kuyruk.config.MANAGER_PORT,
                        service=_worker_service_class(worker),
                        config={"allow_pickle": True})
    rpyc.BgServingThread(conn)._thread.join()


def start_rpc_thread(sender, worker=None):
    start_daemon_thread(retry()(_connect_rpc), args=(worker, ))


def add_exit_method(sender, worker=None):
    worker.manager_exit = partial(os._exit, 0)


class Manager(Flask):

    def __init__(self, kuyruk):
        super(Manager, self).__init__(__name__)
        self.debug = True
        self.kuyruk = kuyruk
        kuyruk.extensions["manager"] = self
        self.workers = {}
        self.requeue = kuyruk.extensions.get("requeue")

        self.has_sentry = "sentry" in kuyruk.extensions
        if self.has_sentry and not kuyruk.config.SENTRY_PROJECT_URL:
            raise Exception("SENTRY_PROJECT_URL is not set")

        worker_start.connect(start_rpc_thread, sender=kuyruk, weak=False)
        worker_init.connect(add_exit_method, sender=kuyruk, weak=False)

        @self.route('/')
        def index():
            return redirect(url_for('workers'))

        @self.route('/workers')
        def workers():
            return render_template('workers.html', sockets=self.workers)

        @self.route('/failed-tasks')
        @self.route('/api/failed-tasks', endpoint="api_failed_tasks")
        def failed_tasks():
            tasks = self.requeue.redis.hvals('failed_tasks')
            decoder = json.JSONDecoder()
            tasks = map(decoder.decode, tasks)
            if request.path.startswith("/api/"):
                ret = {"tasks": tasks}
                return jsonify(**ret)
            return render_template('failed_tasks.html', tasks=tasks)

        @self.route('/action', methods=['POST'])
        def action():
            addr = (request.args['host'], int(request.args['port']))
            client = self.workers[addr]
            f = getattr(client._conn.root, request.form['action'])
            rpyc.async(f)()
            sleep(ACTION_WAIT_TIME)
            return redirect_back()

        @self.route('/action_all', methods=['POST'])
        def action_all():
            for addr, client in self.workers.items():
                f = getattr(client._conn.root, request.form['action'])
                rpyc.async(f)()
            sleep(ACTION_WAIT_TIME)
            return redirect_back()

        @self.route('/requeue', methods=['POST'])
        def requeue_task():
            task_id = request.form['task_id']
            redis = self.requeue.redis

            if task_id == 'ALL':
                self.requeue.requeue_failed_tasks()
            else:
                failed = redis.hget('failed_tasks', task_id)
                failed = json.loads(failed)
                self.requeue.requeue_task(failed)

            return redirect_back()

        @self.route('/delete', methods=['POST'])
        def delete_task():
            task_id = request.form['task_id']
            self.requeue.redis.hdel('failed_tasks', task_id)
            return redirect_back()

        @self.context_processor
        def inject_helpers():
            return {
                'manager': self,
                'now': str(datetime.utcnow())[:19],
                'hostname': socket.gethostname(),
                'has_requeue': self.requeue is not None,
                'has_sentry': self.has_sentry,
            }

        @self.template_filter('sentry_url')
        def do_sentry_url(sentry_id):
            if sentry_id:
                url = self.kuyruk.config.SENTRY_PROJECT_URL
                if not url.endswith('/'):
                    url += '/'
                url += '?query=%s' % sentry_id
                return url

        @self.template_filter('human_time')
        def do_human_time(seconds, suffixes=['y', 'w', 'd', 'h', 'm', 's'],
                          add_s=False, separator=' '):
            """
            Takes an amount of seconds and
            turns it into a human-readable amount of time.

            """
            # the formatted time string to be returned
            time = []

            # the pieces of time to iterate over (days, hours, minutes, etc)
            # - the first piece in each tuple is the suffix (d, h, w)
            # - the second piece is the length in seconds (a day is 60s * 60m * 24h)
            parts = [
                (suffixes[0], 60 * 60 * 24 * 7 * 52),
                (suffixes[1], 60 * 60 * 24 * 7),
                (suffixes[2], 60 * 60 * 24),
                (suffixes[3], 60 * 60),
                (suffixes[4], 60),
                (suffixes[5], 1)]

            # for each time piece, grab the value and remaining seconds, and add it to
            # the time string
            for suffix, length in parts:
                value = seconds / length
                if value > 0:
                    seconds %= length
                    time.append('%s%s' % (str(value),
                                (suffix, (suffix, suffix + 's')[value > 1])[add_s]))
                if seconds < 1:
                    break

            return separator.join(time)


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'


def _manager_service_class(manager):
    @total_ordering
    class _Service(rpyc.Service):
        def __lt__(self, other):
            return self.sort_key < other.sort_key

        @property
        def sort_key(self):
            order = ('hostname', 'queues', 'uptime', 'pid')
            # TODO replace get_stat with operator.itemgetter
            return tuple(self.get_stat(attr) for attr in order)

        @property
        def addr(self):
            return self._conn._config['endpoints'][1]

        def on_connect(self):
            print "Client connected:", self.addr
            self.stats = {}
            manager.workers[self.addr] = self
            start_daemon_thread(target=self.read_stats)

        def on_disconnect(self):
            print "Client disconnected:", self.addr
            del manager.workers[self.addr]

        def read_stats(self):
            while True:
                try:
                    self.stats = rpyc.classic.obtain(self._conn.root.get_stats())
                except Exception as e:
                    print e
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    return
                sleep(1)

        def get_stat(self, name):
            return self.stats.get(name, None)
    return _Service


def _worker_service_class(worker):
    class _Service(rpyc.Service):
        def exposed_get_stats(self):
            return {
                'hostname': socket.gethostname(),
                'uptime': int(worker.uptime),
                'pid': os.getpid(),
                'version': kuyruk.__version__,
                'current_task': getattr(worker.current_task, "name", None),
                'current_args': worker.current_args,
                'current_kwargs': worker.current_kwargs,
                'consuming': worker.consuming,
                'queues': worker.queues,
            }
        exposed_warm_shutdown = worker.shutdown
        exposed_cold_shutdown = worker.manager_exit
        exposed_quit_task = worker.drop_task
    return _Service


def run_manager(kuyruk, args):
    manager = kuyruk.extensions["manager"]
    server = ThreadedServer(_manager_service_class(manager),
                            hostname=kuyruk.config.MANAGER_HOST,
                            port=kuyruk.config.MANAGER_PORT)
    t = start_daemon_thread(server.start)
    logger.info("Manager running in thread: %s", t.name)
    run_simple(kuyruk.config.MANAGER_HOST,
               kuyruk.config.MANAGER_HTTP_PORT,
               manager, threaded=True, use_debugger=True)

help_text = "see and manage kuyruk workers"

command = (run_manager, help_text, None)
