# -*- coding: utf-8 -*-
from time import sleep
import MySQLdb
import simplejson as json
from openprocurement.complaints.queue.client import ComplaintsClient, logger


class ComplaintsToMySQL(ComplaintsClient):
    """Complaints to MySQL bridge"""

    mysql_config = {
        'host': 'localhost',
        'user': 'complaints',
        'passwd': '',
        'db': 'complaints',
        'table': 'complaints',
    }

    def __init__(self, client_config=None, mysql_config=None):
        super(ComplaintsToMySQL, self).__init__(client_config)
        if mysql_config:
            self.mysql_config.update(mysql_config)
        # remove passwd before dump config to log
        self.mysql_passwd = self.mysql_config.pop('passwd')
        self.table_name = self.mysql_config.pop('table')
        self.create_cursor()
        self.create_table()
        self.update_skip_until()

    def create_cursor(self):
        logger.info("Connect to mysql {} table '{}'".format(
            self.mysql_config, self.table_name))
        # close db handle if present
        if getattr(self, 'dbcon', None):
            dbcon, self.dbcon = self.dbcon, None
            dbcon.close()
        self.dbcon = MySQLdb.Connect(passwd=self.mysql_passwd,
            **self.mysql_config)
        self.cursor = self.dbcon.cursor()

    def handle_error(self, error):
        super(ComplaintsToMySQL, self).handle_error(error)
        if isinstance(error, MySQLdb.Error):
            self.cursor = None
            while not self.cursor:
                try:
                    self.create_cursor()
                except MySQLdb.Error as e:
                    logger.error("Can't connect {}".format(e))
                    sleep(10)

    def execute_query(self, sql, *args, **kwargs):
        return self.cursor.execute(sql.format(table_name=self.table_name), *args)

    def create_table(self):
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
        try:
            self.execute_query("SELECT 1 FROM {table_name} LIMIT 1")
            clear_cache = False
        except MySQLdb.Error:
            logger.warning("Create table '%s'", self.table_name)
            self.execute_query(SQL)
            self.dbcon.commit()
            clear_cache = True
        # clear cache table
        if clear_cache:
            self.execute_query("DROP TABLE IF EXISTS {table_name}_tenders")
            self.dbcon.commit()
        # create tenders cache
        SQL = """CREATE TABLE IF NOT EXISTS {table_name}_tenders (
                  tender_id char(32) NOT NULL,
                  tender_dateModified varchar(40) NOT NULL,
                  PRIMARY KEY (tender_id),
                  KEY tender_dateModified (tender_dateModified),
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
            """
        try:
            self.execute_query("SELECT 1 FROM {table_name}_tenders LIMIT 1")
        except MySQLdb.Error:
            logger.warning("Create table '%s_tenders'", self.table_name)
            self.execute_query(SQL)
            self.dbcon.commit()

    def clear_cache(self):
        logger.warning("Clear cache table '%s_tenders'", self.table_name)
        self.execute_query("TRUNCATE TABLE {table_name}_tenders")
        self.dbcon.commit()

    def update_skip_until(self):
        self.execute_query("SELECT MAX(complaint_dateSubmitted) FROM {table_name}")
        row = self.cursor.fetchone()
        if not row or not row[0]:
            return
        row_date = row[0][:10]
        if row_date < self.skip_until:
            logger.info("Ignore offset from database '%s' use from config '%s'",
                row_date, self.skip_until)
            return
        logger.info("Update offset from database, set to '%s'", row_date)
        self.client_skip_until(row_date)

    def check_tender_exists(self, tender):
        self.execute_query(("SELECT tender_dateModified FROM {table_name}_tenders "+
            "WHERE tender_id=%s LIMIT 1"), (tender.id,))
        row = self.cursor.fetchone()
        return row and row[0] == tender.dateModified

    def finish_tender(self, tender):
        SQL = ("INSERT INTO {table_name}_tenders (tender_id, tender_dateModified) "+
                "VALUES (%s, %s) ON DUPLICATE KEY UPDATE tender_dateModified=%s")
        #logger.debug(SQL)
        self.execute_query(SQL, (tender.id, tender.dateModified, tender.dateModified))
        self.dbcon.commit()

    def check_exists(self, tender, complaint_path, complaint):
        self.execute_query(("SELECT tender_status, tender_dateModified "+
            "FROM {table_name} WHERE complaint_id=%s LIMIT 1"), (complaint.id,))
        row = self.cursor.fetchone()
        # don't update rows in terminal status
        if row and row[0] == "cancelled":
            logger.warning("Exists T=%s P=%s C=%s by TS=cancelled",
                tender.id, complaint_path, complaint.id)
            return True
        if row and row[1] == tender.dateModified:
            logger.warning("Exists T=%s P=%s C=%s by dateModified",
                tender.id, complaint_path, complaint.id)
            return True
        return False

    def store(self, complaint, complaint_path):
        complaint_json = json.dumps(complaint)
        if len(complaint_json) > 65000:
            logger.warning("Too big T=%s P=%s C=%s size=%d", complaint.tender.id,
                complaint_path, complaint.id, len(complaint_json))
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
