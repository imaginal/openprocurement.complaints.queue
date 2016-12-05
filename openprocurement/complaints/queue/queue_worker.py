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
from multiprocessing import Process
from ConfigParser import ConfigParser, Error as ConfigParserError
from openprocurement.complaints.queue.mysql import ComplaintsToMySQL

logger = logging.getLogger(__name__)


class Watchdog:
    class TimeoutError(Exception):
        pass
    counter = 0
    timeout = 0
    prntpid = 0


def sigalrm_handler(signo, frame):
    logger.info("Watchdog %d", Watchdog.counter)
    Watchdog.counter += 1
    if Watchdog.timeout:
        signal.alarm(Watchdog.timeout)
    if Watchdog.counter > 3:
        os._exit(1)
    if Watchdog.counter > 2:
        sys.exit(1)
    if Watchdog.counter > 1:
        raise Watchdog.TimeoutError()
    if Watchdog.prntpid and Watchdog.prntpid != os.getppid():
        raise RuntimeError("Parent pid changed")

def sigalrm(timeout):
    if not timeout or int(timeout) < 1:
        return
    signal.signal(signal.SIGALRM, sigalrm_handler)
    Watchdog.timeout = int(timeout)
    signal.alarm(Watchdog.timeout)


def sigterm_handler(signo, frame):
    logger.warning("Signal received %d", signo)
    # also setup reserve plan via SIGALRM
    signal.signal(signal.SIGALRM, sigalrm_handler)
    Watchdog.counter = 5
    signal.alarm(2)
    # and exit
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


def remove_pidfile(lock_file, filename):
    logger.info("Remove pidfile %s", filename)
    lock_file.close()
    os.remove(filename)


def write_pidfile(filename):
    if not filename:
        return
    # try get exclusive lock to prevent second start
    lock_file = open(filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX + fcntl.LOCK_NB)
    lock_file.write(str(os.getpid()) + "\n")
    lock_file.flush()
    atexit.register(remove_pidfile, lock_file, filename)
    return lock_file


class MyConfigParser(ConfigParser):
    def get(self, section, option, default=None):
        try:
            value = ConfigParser.get(self, section, option)
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
    sigalrm(config.get('general', 'sigalrm'))

    client_config = config.items('client')
    mysql_config = config.items('mysql')

    if descending:
        logger.info("Start in descending mode")
        client_config.append(('descending', 1))

    app = ComplaintsToMySQL(client_config, mysql_config)
    app.watchdog = Watchdog
    try:
        app.run()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Unhandled Exception %s %s", type(e).__name__, str(e))
        sys.exit(1)

    return 0


def run_child(config, descending, proc_pool):
    # clear proc_pool to prevent self-kill
    proc_pool.clear()
    Watchdog.prntpid = os.getppid()
    return run_app(config, descending)


def stop_workers(pool):
    for k, p in pool.items():
        process = p.get('process', None)
        if process and process.is_alive():
            logger.info("Stop child %s", process.name)
            process.terminate()
    pool = {}


def run_workers(config):
    workers = int(config.get('general', 'workers') or 0)

    if not workers:
        return run_app(config)

    pool = {}

    if workers > 0:
        pool['fwd'] = dict(target=run_child, args=(config, 0, pool), name='WorkerForward')
    if workers > 1:
        pool['bwd'] = dict(target=run_child, args=(config, 1, pool), name='WorkerBackward')

    atexit.register(stop_workers, pool)

    while pool:
        for k, p in pool.items():
            process = p.get('process', None)
            if not process:
                logger.info("Start child %s", p['name'])
                process = Process(**p)
                process.start()
                p['process'] = process
            if process.is_alive():
                process.join(1)
            elif process.exitcode == 0:
                logger.info("Success stop child %s", process.name)
                pool.pop(k)
            else:
                logger.warning("Child %s exited with error %d",
                    process.name, process.exitcode)
                p.pop('process')
            time.sleep(0.5)

    logger.info("Leave watcher")


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

    signal.signal(signal.SIGINT, sigterm_handler)
    signal.signal(signal.SIGTERM, sigterm_handler)

    if config.getboolean('general', 'daemon'):
        daemonize(config.get('general', 'logfile'))

    write_pidfile(config.get('general', 'pidfile'))

    try:
        run_workers(config)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.exception("Unhandled Exception %s %s", type(e).__name__, str(e))
        sys.exit(1)

    return 0

if __name__ == "__main__":
    sys.exit(main())
