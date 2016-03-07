# -*- coding: utf-8 -*-
import atexit, fcntl, os, sys
import logging, logging.config

from ConfigParser import ConfigParser, Error as ConfigParserError
from openprocurement.complaints.queue.mysql import ComplaintsToMySQL


def daemonize(filename=False):
    if not filename:
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

    parser = MyConfigParser()
    parser.read(sys.argv[1])

    daemonize(parser.get('general', 'daemonize'))
    pidfile(parser.get('general', 'pidfile'))

    client_config = parser.items('client')
    mysql_config = parser.items('mysql')

    app = ComplaintsToMySQL(client_config, mysql_config)
    app.run()


if __name__ == "__main__":
    main()
