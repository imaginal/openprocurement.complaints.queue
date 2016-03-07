# -*- coding: utf-8 -*-
import fcntl, os, sys
import logging.config
from ConfigParser import ConfigParser

from openprocurement.complaints.queue.mysql import ComplaintsToMySQL

default_config = {
    'pidfile': None,
}


def pidfile(name):
    if not name:
        return
    # try get exclusive lock to prevent second start
    lock_file = open(name, "w")
    fcntl.lockf(lock_file, fcntl.LOCK_EX+fcntl.LOCK_NB)
    lock_file.write(str(os.getpid())+"\n")
    lock_file.flush()
    return lock_file


def main():
    if len(sys.argv) < 2:
        print("Usage: complaints_queue config.ini")
        sys.exit(1)

    logging.config.fileConfig(sys.argv[1])

    parser = ConfigParser(defaults=default_config)
    parser.read(sys.argv[1])

    pidfile(parser.get('general', 'pidfile'))

    client_config = parser.items('client')
    mysql_config = parser.items('mysql')

    app = ComplaintsToMySQL(client_config, mysql_config)
    app.run()


if __name__ == "__main__":
    main()
