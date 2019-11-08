from __future__ import division
import os
import json
import socket
import logging
import operator
import threading
from datetime import datetime, timedelta
from time import sleep
from functools import total_ordering, wraps, partial

import amqp
from flask import Flask, Blueprint
from flask import render_template, redirect, request, url_for, jsonify
import waitress

import kuyruk
from kuyruk.signals import worker_start, worker_shutdown

logger = logging.getLogger(__name__)

CONFIG = {
    "MANAGER_LISTEN_HOST_HTTP": "127.0.0.1",
    "MANAGER_LISTEN_PORT_HTTP": 16500,
    "MANAGER_STATS_INTERVAL": 1,
    "SENTRY_PROJECT_URL": None,
}

ACTION_WAIT_TIME = 1  # seconds


def start_thread(target, args=(), daemon=False, stop_event=threading.Event()):
    target = _retry(stop_event=stop_event)(target)
    t = threading.Thread(target=target, args=args)
    t.daemon = daemon
    t.start()
    return t


def _retry(sleep_seconds=1, stop_event=threading.Event(),
           on_exception=lambda e: logger.error(e)):
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


def _connect(worker):
    def handle_manager_message(message):
        logger.info("Action received from manager: %s", message.body)
        message = json.loads(message.body)
        action = message['action']
        handlers = {
                'warm_shutdown': worker.shutdown,
                'cold_shutdown': partial(os._exit, 0),
                'quit_task': worker.drop_task,
        }
        try:
            handler = handlers[action]
        except KeyError:
            logger.error("Unknown action: %s", action)
            return

        handler()

    with worker.kuyruk.channel() as ch:
        ch.basic_consume('amq.rabbitmq.reply-to', no_ack=True,
                         callback=handle_manager_message)
        while not worker._manager_connector_stopped.is_set():
            stats = _get_stats(worker)
            body = json.dumps(stats)
            msg = amqp.Message(body=body, type='stats',
                               reply_to='amq.rabbitmq.reply-to')
            ch.basic_publish(msg, routing_key='kuyruk_manager')
            try:
                ch.connection.heartbeat_tick()
                ch.connection.drain_events(
                        timeout=worker.kuyruk.config.MANAGER_STATS_INTERVAL)
            except socket.timeout:
                pass

        msg = amqp.Message(type='shutdown', reply_to='amq.rabbitmq.reply-to')
        ch.basic_publish(msg, routing_key='kuyruk_manager')


def start_connector(sender, worker=None):
    worker._manager_connector_stopped = threading.Event()
    worker._manager_connector_thread = start_thread(
            _connect, args=(worker, ),
            stop_event=worker._manager_connector_stopped)


def stop_connector(sender, worker=None):
    worker._manager_connector_stopped.set()
    worker._manager_connector_thread.join()


class Manager:

    def __init__(self, kuyruk):
        self.kuyruk = kuyruk
        self.workers = {}
        self.lock = threading.Lock()
        self.requeue = kuyruk.extensions.get("requeue")

        self.has_sentry = "sentry" in kuyruk.extensions
        if self.has_sentry and not kuyruk.config.SENTRY_PROJECT_URL:
            raise Exception("SENTRY_PROJECT_URL is not set")

        worker_start.connect(start_connector, sender=kuyruk, weak=False)
        worker_shutdown.connect(stop_connector, sender=kuyruk, weak=False)

        kuyruk.extensions["manager"] = self

    def _accept(self):
        with self.kuyruk.channel() as ch:
            ch.queue_declare('kuyruk_manager', exclusive=True)
            ch.basic_consume('kuyruk_manager', no_ack=True,
                             callback=self._handle_worker_message)
            while True:
                try:
                    ch.connection.heartbeat_tick()
                    ch.connection.drain_events(timeout=1)
                except socket.timeout:
                    pass

    def _handle_worker_message(self, message):
        message_type = message.type
        reply_to = message.reply_to

        if message_type == 'stats':
            stats = json.loads(message.body)
            with self.lock:
                try:
                    worker = self.workers[reply_to]
                except KeyError:
                    worker = _Worker(reply_to, stats)
                    self.workers[reply_to] = worker
                else:
                    worker.update(stats)

        elif message_type == 'shutdown':
            with self.lock:
                try:
                    del self.workers[reply_to]
                except KeyError:
                    pass

    def _clean_workers(self):
        while True:
            sleep(self.kuyruk.config.MANAGER_STATS_INTERVAL)
            with self.lock:
                now = datetime.utcnow()
                for worker in list(self.workers.values()):
                    if now - worker.updated_at > timedelta(seconds=10):
                        del self.workers[worker.reply_to]

    def flask_blueprint(self):
        b = Blueprint("kuyruk_manager", __name__)
        b.add_url_rule('/', 'index', self._get_index)
        b.add_url_rule('/workers', 'workers', self._get_workers)
        b.add_url_rule('/failed-tasks', 'failed_tasks',
                       self._get_failed_tasks)
        b.add_url_rule('/api/failed-tasks', 'api_failed_tasks',
                       self._api_get_failed_tasks)
        b.add_url_rule('/action', 'action',
                       self._post_action, methods=['POST'])
        b.add_url_rule('/action-all', 'action_all',
                       self._post_action_all, methods=['POST'])
        b.add_url_rule('/requeue', 'requeue_task',
                       self._post_requeue, methods=['POST'])
        b.add_url_rule('/delete', 'delete_task',
                       self._post_delete, methods=['POST'])
        b.context_processor(self._context_processors)
        return b

    def flask_application(self):
        app = Flask(__name__)
        app.debug = True
        app.register_blueprint(self.flask_blueprint())
        return app

    def _get_index(self):
        return redirect(url_for('kuyruk_manager.workers'))

    def _get_workers(self):
        hostname = request.args.get('hostname')
        queue = request.args.get('queue')
        consuming = request.args.get('consuming')
        working = request.args.get('working')

        workers = {}
        with self.lock:
            for reply_to, worker in self.workers.items():
                if hostname and hostname != worker.stats.get('hostname', ''):
                    continue
                if queue and queue not in worker.stats.get('queues', []):
                    continue
                if consuming and not worker.stats.get('consuming', False):
                    continue
                if working and not worker.stats.get('current_task', None):
                    continue
                workers[reply_to] = worker

        return render_template('workers.html', workers=workers)

    def _failed_tasks(self):
        tasks = self.requeue.redis.hvals('failed_tasks')
        tasks = [t.decode('utf-8') for t in tasks]
        decoder = json.JSONDecoder()
        tasks = map(decoder.decode, tasks)
        return tasks

    def _get_failed_tasks(self):
        tasks = list(self._failed_tasks())
        return render_template('failed_tasks.html', tasks=tasks)

    def _api_get_failed_tasks(self):
        return jsonify(tasks=self._failed_tasks())

    def _post_action(self):
        body = json.dumps({'action': request.form['action']})
        msg = amqp.Message(body)
        with self.kuyruk.channel() as ch:
            ch.basic_publish(msg, '', request.args['id'])

        sleep(ACTION_WAIT_TIME)
        return redirect_back()

    def _post_action_all(self):
        body = json.dumps({'action': request.form['action']})
        msg = amqp.Message(body)
        with self.kuyruk.channel() as ch:
            with self.lock:
                for id in self.workers:
                    ch.basic_publish(msg, '', id)

        sleep(ACTION_WAIT_TIME)
        return redirect_back()

    def _post_requeue(self):
        task_id = request.form['task_id']
        redis = self.requeue.redis

        if task_id == 'ALL':
            self.requeue.requeue_failed_tasks()
        else:
            failed = redis.hget('failed_tasks', task_id)
            failed = json.loads(failed)
            self.requeue.requeue_task(failed)

        return redirect_back()

    def _post_delete(self):
        task_id = request.form['task_id']
        self.requeue.redis.hdel('failed_tasks', task_id)
        return redirect_back()

    def _context_processors(self):
        return {
            'manager': self,
            'now': str(datetime.utcnow())[:19],
            'hostname': socket.gethostname(),
            'has_requeue': self.requeue is not None,
            'has_sentry': self.has_sentry,
            'sentry_url': self._sentry_url,
            'human_time': self._human_time,
        }

    def _sentry_url(self, sentry_id):
        if not sentry_id:
            return

        url = self.kuyruk.config.SENTRY_PROJECT_URL
        if not url.endswith('/'):
            url += '/'

        url += '?query=%s' % sentry_id
        return url

    def _human_time(self, seconds, suffixes=['y', 'w', 'd', 'h', 'm', 's'],
                    add_s=False, separator=' '):
        """
        Takes an amount of seconds and
        turns it into a human-readable amount of time.

        """
        # the formatted time string to be returned
        time = []

        # the pieces of time to iterate over (days, hours, minutes, etc)
        # the first piece in each tuple is the suffix (d, h, w)
        # the second piece is the length in seconds (a day is 60s * 60m * 24h)
        parts = [
            (suffixes[0], 60 * 60 * 24 * 7 * 52),
            (suffixes[1], 60 * 60 * 24 * 7),
            (suffixes[2], 60 * 60 * 24),
            (suffixes[3], 60 * 60),
            (suffixes[4], 60),
            (suffixes[5], 1)]

        # for each time piece, grab the value and remaining seconds,
        # and add it to the time string
        for suffix, length in parts:
            value = seconds // length
            if value > 0:
                seconds %= length
                time.append('%s%s' % (str(value), (
                    suffix, (suffix, suffix + 's')[value > 1])[add_s]))
            if seconds < 1:
                break

        return separator.join(time)


def redirect_back():
    referrer = request.headers.get('Referer')
    if referrer:
        return redirect(referrer)
    return 'Go back'


_hostname = socket.gethostname()
_pid = os.getpid()


def _get_stats(worker):
    return {
        'hostname': _hostname,
        'uptime': int(worker.uptime),
        'pid': _pid,
        'version': kuyruk.__version__,
        'current_task': getattr(worker.current_task, "name", None),
        'current_args': worker.current_args,
        'current_kwargs': worker.current_kwargs,
        'consuming': worker.consuming,
        'queues': worker.queues,
    }


@total_ordering
class _Worker:

    def __init__(self, reply_to, stats):
        self.reply_to = reply_to
        self.update(stats)

    def __lt__(self, other):
        a, b = self._sort_key, other._sort_key
        return _lt_tuples(a, b)

    @property
    def _sort_key(self):
        order = ('hostname', 'queues', 'uptime', 'pid')
        return operator.itemgetter(*order)(self.stats)

    def update(self, stats):
        self.stats = stats
        self.updated_at = datetime.utcnow()


def _lt_tuples(t1, t2):
    for i in range(min(len(t1), len(t2))):
        a, b = t1[i], t2[i]
        if not a:
            return False
        if not b:
            return True
        return a < b


def run_manager(kuyruk, args):
    manager = kuyruk.extensions["manager"]
    app = manager.flask_application()

    start_thread(manager._accept, daemon=True)
    start_thread(manager._clean_workers, daemon=True)
    waitress.serve(
            app,
            host=kuyruk.config.MANAGER_LISTEN_HOST_HTTP,
            port=kuyruk.config.MANAGER_LISTEN_PORT_HTTP)


help_text = "see and manage kuyruk workers"

command = (run_manager, help_text, None)
