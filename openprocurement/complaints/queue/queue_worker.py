# -*- coding: utf-8 -*-

import os
import sys
import argparse
import atexit
import fcntl
import time
import signal
import logging
import logging.config
from threading import Thread
from multiprocessing import Process
from ConfigParser import ConfigParser, Error as ConfigParserError
from openprocurement.complaints.queue.mysql import ComplaintsToMySQL

logger = logging.getLogger(__name__)


class Watchdog:
    class TimeoutError(Exception):
        pass
    _thread = None
    counter = 0
    timeout = 0
    prntpid = 0


def sigalrm_handler(signum, frame):
    if Watchdog.counter >= Watchdog.timeout:
        os._exit(1)


def watchdog_thread():
    while True:
        Watchdog.counter += 1
        time.sleep(1)
        if Watchdog.counter >= Watchdog.timeout:
            logger.warning("Watchdog counter %d", Watchdog.counter)
        if Watchdog.counter == Watchdog.timeout:
            os.kill(os.getpid(), signal.SIGTERM)
        if Watchdog.counter >= Watchdog.timeout + 2:
            os._exit(1)
            break
        if Watchdog.prntpid and Watchdog.prntpid != os.getppid():
            raise SystemExit("Parent pid changed")


def setup_watchdog(timeout):
    if not timeout or int(timeout) < 5:
        return
    signal.signal(signal.SIGALRM, sigalrm_handler)
    Watchdog.timeout = int(timeout)
    thread = Thread(target=watchdog_thread)
    thread.daemon = True
    thread.start()
    Watchdog._thread = thread


def sigterm_handler(signo, frame):
    logger.warning("Signal received %d", signo)
    if Watchdog.counter >= Watchdog.timeout:
        sys.exit(1)
    sys.exit(0)


def daemonize(logfile):
    if os.fork() > 0:
        sys.exit(0)

    os.chdir("/")
    os.setsid()

    if os.fork() > 0:
        sys.exit(0)

    if not logfile:
        logfile = '/dev/null'

    fout = file(logfile, 'a+')
    ferr = file(logfile, 'a+', 0)
    sys.stdin.close(), os.close(0)
    os.dup2(fout.fileno(), 1)
    os.dup2(ferr.fileno(), 2)


def remove_pidfile(lock_file, filename, mypid):
    if mypid != os.getpid():
        return
    logger.info("Remove pidfile %s", filename)
    lock_file.close()
    os.remove(filename)


def write_pidfile(filename):
    if not filename:
        return
    # try get exclusive lock to prevent second start
    mypid = os.getpid()
    logger.info("Save %d to pidfile %s", mypid, filename)
    lock_file = open(filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX + fcntl.LOCK_NB)
    lock_file.write(str(mypid) + "\n")
    lock_file.flush()
    atexit.register(remove_pidfile, lock_file, filename, mypid)
    return lock_file


class MyConfigParser(ConfigParser):
    def get(self, section, option, default=None):
        try:
            value = ConfigParser.get(self, section, option)
            if value and isinstance(value, str):
                value = value.strip(' \t\'"')
        except ConfigParserError:
            value = default
        return value

    def getboolean(self, section, option, default=False):
        try:
            value = ConfigParser.getboolean(self, section, option)
        except AttributeError:
            value = default
        return value


def run_app(config, descending=False):
    setup_watchdog(config.get('general', 'watchdog'))

    client_config = config.items('client')
    mysql_config = config.items('mysql')

    if descending:
        logger.info("Start in descending mode")
        client_config.append(('descending', 1))

    app = ComplaintsToMySQL(client_config, mysql_config)
    app.watchdog = Watchdog
    try:
        app.run()
    except (KeyboardInterrupt, SystemExit):
        sys.exit(2)
    except Exception as e:
        logger.exception("%s %s", type(e).__name__, str(e))
        Watchdog.counter = Watchdog.timeout - 1
        sys.exit(1)

    logger.info("Leave worker")
    return 0


def run_child(config, descending, proc_pool):
    # clear proc_pool to prevent self-kill
    proc_pool.clear()
    Watchdog.prntpid = os.getppid()
    return run_app(config, descending)


def stop_workers(pool, mypid):
    if mypid != os.getpid():
        return
    for k, p in pool.items():
        process = p.get('process', None)
        if process and process.is_alive():
            logger.info("Stop child %s %d", process.name, process.pid)
            process.terminate()
            time.sleep(0.1)
    pool = {}


def run_workers(config):
    workers = int(config.get('general', 'workers') or 0)

    if not workers:
        logger.info("Starting in signle process")
        return run_app(config)

    logger.info("Starting watcher with %d workers", workers)

    pool = {}

    if workers > 0:
        pool['fwd'] = dict(target=run_child, args=(config, 0, pool), name='Worker.Forward')
    if workers > 1:
        pool['bwd'] = dict(target=run_child, args=(config, 1, pool), name='Worker.Backward')

    atexit.register(stop_workers, pool, os.getpid())

    while pool:
        for k, p in pool.items():
            process = p.get('process', None)
            if not process:
                logger.info("Start child %s", p['name'])
                process = Process(**p)
                process.daemon = True
                process.start()
                p['process'] = process
                time.sleep(0.5)
            if process.is_alive():
                process.join(1)
            else:
                logger.warning("Child %s exited with error %d",
                    process.name, process.exitcode)
                p.pop('process')
            time.sleep(0.5)

    logger.info("Leave watcher")
    return 0


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('config', nargs=1, help='config.ini file')
    parser.add_argument('-d', '--daemon', action='store_true', help='run as daemon in background')
    parser.add_argument('-w', '--workers', help='start as watcher and fork child workers (max 2)')
    parser.add_argument('-p', '--pidfile', help='store pid in file, remove file on exit')
    parser.add_argument('-l', '--logfile', help='redirect stdout and stderr to logfile')
    return parser.parse_args()


def update_config(config, args):
    for opt in ['daemon', 'workers', 'pidfile', 'logfile']:
        if getattr(args, opt, None):
            config.set('general', opt, str(getattr(args, opt)))


def main():
    args = parse_args()

    logging.config.fileConfig(args.config[0])

    config = MyConfigParser(allow_no_value=True)
    config.read(args.config[0])

    update_config(config, args)

    signal.signal(signal.SIGTERM, sigterm_handler)

    if config.getboolean('general', 'daemon'):
        daemonize(config.get('general', 'logfile'))

    write_pidfile(config.get('general', 'pidfile'))

    try:
        run_workers(config)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(2)
    except Exception as e:
        logger.exception("%s %s", type(e).__name__, str(e))
        sys.exit(1)

    logger.info("Leave main")
    return 0

if __name__ == "__main__":
    sys.exit(main())
