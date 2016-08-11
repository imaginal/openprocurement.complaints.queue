# -*- coding: utf-8 -*-
#from gevent import monkey
#monkey.patch_all()

import os
import sys
import fcntl
import atexit
import signal
import logging
import logging.config

from ConfigParser import ConfigParser, Error as ConfigParserError
from openprocurement.complaints.queue.mysql import ComplaintsToMySQL


class Watchdog:
    class TimeoutError(Exception):
        pass
    counter = 0
    timeout = 0


def sigalrm_handler(signo, frame):
    if Watchdog.timeout:
        signal.alarm(Watchdog.timeout)
    if Watchdog.counter > 0:
        raise Watchdog.TimeoutError()
    Watchdog.counter += 1
    print "Watchdog.counter", Watchdog.counter


def sigalrm(timeout=None):
    if timeout and timeout != '0':
        signal.signal(signal.SIGALRM, sigalrm_handler)
        Watchdog.timeout = int(timeout)
        signal.alarm(Watchdog.timeout)


def daemonize(filename=False):
    if not filename or filename == 'no':
        return

    if os.fork() > 0:
        sys.exit(0)

    os.chdir("/")
    os.setsid()

    if os.fork() > 0:
        sys.exit(0)

    fout = file(filename, 'a+')
    ferr = file(filename, 'a+', 0)
    sys.stdin.close(), os.close(0)
    os.dup2(fout.fileno(), 1)
    os.dup2(ferr.fileno(), 2)


def delpid(lock_file, filename):
    lock_file.close()
    os.remove(filename)


def pidfile(filename):
    if not filename:
        return
    # try get exclusive lock to prevent second start
    lock_file = open(filename, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX+fcntl.LOCK_NB)
    lock_file.write(str(os.getpid())+"\n")
    lock_file.flush()
    atexit.register(delpid, lock_file, filename)
    return lock_file


def sigterm_handler(signo, frame):
    sys.exit(0)


class MyConfigParser(ConfigParser):
    def get(self, section, option, default=None):
        try:
            value = ConfigParser.get(self, section, option)
        except ConfigParserError:
            value = default
        return value


def main():
    if len(sys.argv) < 2:
        print("Usage: complaints_queue config.ini")
        sys.exit(1)

    logging.config.fileConfig(sys.argv[1])

    parser = MyConfigParser(allow_no_value=True)
    parser.read(sys.argv[1])

    signal.signal(signal.SIGTERM, sigterm_handler)

    daemonize(parser.get('general', 'daemonize'))
    pidfile(parser.get('general', 'pidfile'))
    sigalrm(parser.get('general', 'sigalrm'))

    client_config = parser.items('client')
    mysql_config = parser.items('mysql')

    app = ComplaintsToMySQL(client_config, mysql_config)
    app.watchdog = Watchdog
    app.run()


if __name__ == "__main__":
    main()
