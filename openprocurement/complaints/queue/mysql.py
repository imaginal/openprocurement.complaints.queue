# -*- coding: utf-8 -*-
import MySQLdb
import warnings
import simplejson as json
from openprocurement.complaints.queue.client import ComplaintsClient
from openprocurement.complaints.queue.utils import getboolean, retry

import logging
logger = logging.getLogger(__name__)


class ComplaintsToMySQL(ComplaintsClient):
    """Complaints to MySQL bridge"""

    mysql_config = {
        'host': 'localhost',
        'user': 'complaints',
        'passwd': '',
        'db': 'complaints',
        'table': 'complaints',
        'charset': 'utf8',
        'init_command': 'SET NAMES utf8',
        'connect_timeout': 300,
        'drop_cache': False,
        'keep_alive': True,
        'max_packet': 500000,
    }

    def __init__(self, client_config=None, mysql_config=None):
        super(ComplaintsToMySQL, self).__init__(client_config)
        if mysql_config:
            self.mysql_config.update(mysql_config)
        # remove passwd before dump config to log
        self.mysql_passwd = self.mysql_config.pop('passwd')
        self.table_name = self.mysql_config.pop('table')
        self.drop_cache = self.mysql_config.pop('drop_cache')
        self.keep_alive = self.mysql_config.pop('keep_alive')
        self.max_packet = int(self.mysql_config.pop('max_packet'))
        for k in ['init_command']:
            self.mysql_config[k] = self.mysql_config[k].strip(' \t"')
        for k in ['connect_timeout']:
            self.mysql_config[k] = int(self.mysql_config[k] or 0)
        self.create_cursor()
        self.create_tables()
        self.restore_skip_until()

    @retry(tries=5, delay=5, logger=logger)
    def create_cursor(self):
        logger.info("Connect to mysql {} table '{}'".format(
            self.mysql_config, self.table_name))
        self.reset_watchdog()
        # close db handle if present
        if getattr(self, 'dbcon', None):
            dbcon, self.dbcon = self.dbcon, None
            dbcon.close()
        self.mysql_config.pop('connect_timeout', None)
        self.dbcon = MySQLdb.Connect(passwd=self.mysql_passwd,
            **self.mysql_config)
        self.cursor = self.dbcon.cursor()

    def handle_error(self, error):
        super(ComplaintsToMySQL, self).handle_error(error)
        if isinstance(error, MySQLdb.MySQLError):
            self.cursor = None
            self.create_cursor()

    def execute_query(self, sql, *args):
        return self.cursor.execute(sql.format(table_name=self.table_name), *args)

    def query_and_fetchone(self, sql, *args):
        self.execute_query(sql, *args)
        return self.cursor.fetchone()

    def create_tables(self):
        SQL = """CREATE TABLE IF NOT EXISTS {table_name} (
                  tender_id char(32) NOT NULL,
                  tender_status varchar(40) NOT NULL,
                  tender_procurementMethod varchar(40) NOT NULL,
                  tender_procurementMethodType varchar(40) NOT NULL,
                  tender_dateModified varchar(40) NOT NULL,
                  tender_mode varchar(40) default NULL,
                  complaint_id char(32) NOT NULL,
                  complaint_complaintID varchar(40) NOT NULL,
                  complaint_path varchar(80) NOT NULL,
                  complaint_acceptance tinyint(1) default NULL,
                  complaint_dateSubmitted varchar(40) default NULL,
                  complaint_dateAccepted varchar(40) default NULL,
                  complaint_status varchar(40) NOT NULL,
                  complaint_json longblob NOT NULL,
                  cancellation_json longblob default NULL,
                  cancellation_dateDecision varchar(40) default NULL,
                  PRIMARY KEY (complaint_id),
                  KEY complaint_complaintID (complaint_complaintID),
                  KEY complaint_dateSubmitted (complaint_dateSubmitted),
                  KEY complaint_status (complaint_status),
                  KEY tender_procurementMethod (tender_procurementMethod)
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
            """
        warnings.filterwarnings('error', category=MySQLdb.Warning)
        try:
            self.query_and_fetchone("SELECT 1 FROM {table_name} LIMIT 1")
        except MySQLdb.MySQLError:
            logger.warning("Create table '%s'", self.table_name)
            self.execute_query(SQL)
            self.dbcon.commit()
            self.drop_cache = True
        # drop cache if we create main table
        if getboolean(self.drop_cache):
            try:
                self.query_and_fetchone("SELECT 1 FROM {table_name}_cache LIMIT 1")
                logger.warning("Drop cache table %s_cache", self.table_name)
                self.execute_query("DROP TABLE IF EXISTS {table_name}_cache")
                self.dbcon.commit()
            except MySQLdb.MySQLError:
                self.dbcon.rollback()
        # create tenders cache
        SQL = """CREATE TABLE IF NOT EXISTS {table_name}_cache (
                  tender_id char(32) NOT NULL,
                  tender_dateModified varchar(40) NOT NULL,
                  PRIMARY KEY (tender_id)
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
            """
        try:
            self.query_and_fetchone("SELECT 1 FROM {table_name}_cache LIMIT 1")
        except MySQLdb.MySQLError:
            logger.warning("Create table '%s_cache'", self.table_name)
            self.execute_query(SQL)
            self.dbcon.commit()

    def clear_cache(self):
        if getattr(self, 'dbcon', None) is None:
            return
        logger.warning("Clear cache table '%s_cache'", self.table_name)
        try:
            self.execute_query("TRUNCATE TABLE {table_name}_cache")
            self.dbcon.commit()
        except MySQLdb.MySQLError as e:
            logger.error("Can't clear cache %s", str(e))
            self.dbcon.rollback()

    def restore_skip_until(self):
        if self.descending_mode:
            return
        row = self.query_and_fetchone("SELECT MAX(complaint_dateSubmitted) FROM {table_name}")
        if not row or not row[0]:
            return
        row_date = row[0][:10]
        if row_date < self.skip_until:
            logger.info("Ignore offset from database '%s' use from config '%s'",
                row_date, self.skip_until)
            return
        logger.info("Restore offset from database '%s'", row_date)
        self.set_client_skip_until(row_date, skip_days=1)

    def check_cache(self, tender):
        row = self.query_and_fetchone(("SELECT tender_dateModified FROM {table_name}_cache " +
            "WHERE tender_id=%s LIMIT 1"), (tender.id,))
        return row and row[0] == tender.dateModified

    def finish_tender(self, tender):
        SQL = ("INSERT INTO {table_name}_cache (tender_id, tender_dateModified) " +
               "VALUES (%s, %s) ON DUPLICATE KEY UPDATE tender_dateModified=%s")
        # logger.debug(SQL)
        self.execute_query(SQL, (tender.id, tender.dateModified, tender.dateModified))
        self.dbcon.commit()

    def ping_backend(self):
        if not getboolean(self.keep_alive):
            return
        try:
            self.dbcon.ping(True)
        except MySQLdb.MySQLError as e:
            logger.error("Error ping mysql %s", str(e))
            self.handle_error(e)

    def check_exists(self, tender, complaint_path, complaint):
        row = self.query_and_fetchone(("SELECT tender_status, tender_dateModified " +
            "FROM {table_name} WHERE complaint_id=%s LIMIT 1"), (complaint.id,))
        # don't update rows in terminal status
        if row and row[0] == "cancelled":
            logger.info("Ignore T=%s P=%s C=%s by TS=cancelled",
                tender.id, complaint_path, complaint.id)
            return True
        if row and row[1] == tender.dateModified:
            logger.info("Exists T=%s P=%s C=%s by DM=%s",
                tender.id, complaint_path, complaint.id, tender.dateModified)
            return True
        return False

    def store(self, complaint, complaint_path):
        complaint_json = json.dumps(complaint)
        if len(complaint_json) > 65000:
            logger.warning("Too big T=%s P=%s C=%s size=%d", complaint.tender.id,
                complaint_path, complaint.id, len(complaint_json))
        if len(complaint_json) > self.max_packet:
            max_title = int(self.max_packet / 125)
            max_descr = int(self.max_packet / 6.25)
            complaint.title = complaint.title[:max_title] + " (truncated)"
            complaint.description = complaint.description[:max_descr] + " (truncated)"
            complaint_json = json.dumps(complaint)
            logger.warning("Complaint T=%s P=%s C=%s truncated to size=%d",
                complaint.tender.id, complaint_path, complaint.id, len(complaint_json))
        insert_data = [
            ('tender_id', complaint.tender.id),
            ('tender_status', complaint.tender.status),
            ('tender_procurementMethod', complaint.tender.procurementMethod),
            ('tender_procurementMethodType', complaint.tender.procurementMethodType),
            ('tender_dateModified', complaint.tender.dateModified),
            ('tender_mode', complaint.tender.get('mode', None)),
            ('complaint_id', complaint.id),
            ('complaint_complaintID', complaint.complaintID),
            ('complaint_path', complaint_path),
            ('complaint_acceptance', complaint.get('acceptance', None)),
            ('complaint_dateSubmitted', complaint.get('dateSubmitted', None)),
            ('complaint_dateAccepted',  complaint.get('dateAccepted', None)),
            ('complaint_status', complaint.status),
            ('complaint_json', complaint_json),
        ]
        update_data = [
            ('tender_status', complaint.tender.status),
            ('tender_dateModified', complaint.tender.dateModified),
            ('complaint_status', complaint.status),
            ('complaint_acceptance', complaint.get('acceptance', None)),
            ('complaint_dateAccepted', complaint.get('dateAccepted', None)),
            ('complaint_json', complaint_json),
        ]
        insert_cols = ", ".join([k for k,_ in insert_data])
        insert_fmts = ", ".join(["%s" for _ in insert_data])
        update_cols = ", ".join([k+"=%s" for k,_ in update_data])
        values_args = [v for _,v in insert_data+update_data]

        SQL = "INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s" % (
            self.table_name, insert_cols, insert_fmts, update_cols)
        #logger.debug(SQL)
        self.execute_query(SQL, values_args)
        self.dbcon.commit()
