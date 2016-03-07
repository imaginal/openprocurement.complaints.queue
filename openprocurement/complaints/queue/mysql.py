# -*- coding: utf-8 -*-
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
        passwd = self.mysql_config.pop('passwd')
        logger.info("Connect to mysql {}".format(self.mysql_config))
        self.table_name = self.mysql_config.pop('table')
        self.db = MySQLdb.Connect(passwd=passwd, **self.mysql_config)
        self.cursor = self.db.cursor()
        self.create_table()
        self.update_skip_until()

    def execute_query(self, sql, *args):
        return self.cursor.execute(sql.format(table_name=self.table_name), *args)

    def create_table(self):
        SQL = """CREATE TABLE IF NOT EXISTS {table_name} (
                  tender_tenderID varchar(32) NOT NULL,
                  tender_id char(32) NOT NULL,
                  complaint_id char(32) NOT NULL,
                  complaint_path varchar(80) NOT NULL,
                  complaint_date varchar(32) NOT NULL,
                  complaint_status varchar(32) NOT NULL,
                  complaint_json blob NOT NULL,
                  PRIMARY KEY (complaint_id),
                  KEY complaint_date (complaint_date),
                  KEY complaint_status (complaint_status),
                  KEY tender_tenderID (tender_tenderID)
                ) DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
            """
        try:
            self.execute_query("SELECT 1 FROM {table_name} LIMIT 1")
        except MySQLdb.Error:
            logger.warning("Create table '%s'", self.table_name)
            self.execute_query(SQL)

    def update_skip_until(self):
        self.execute_query("SELECT MAX(complaint_date) FROM {table_name}")
        row = self.cursor.fetchone()
        if row and row[0]:
            row_date = row[0][:10]
            logger.info("Update skip_until from database, set to %s", row_date)
            self.skip_until = row_date

    def test_exists(self, tender_id, complaint_id, complaint_date):
        self.execute_query(("SELECT complaint_date FROM {table_name} "+
            "WHERE complaint_id=%s LIMIT 1"), (complaint_id,))
        row = self.cursor.fetchone()
        return row and row[0] == complaint_date

    def store(self, tender, complaint, complaint_path, complaint_date):
        complaint_json = json.dumps(complaint)
        self.execute_query(("INSERT INTO {table_name} "+
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "+
            "ON DUPLICATE KEY UPDATE "+
            "complaint_date=%s, complaint_status=%s, complaint_json=%s"),
            (tender.tenderID, tender.id, complaint.id, complaint_path,
            complaint_date, complaint.status, complaint_json,
            complaint_date, complaint.status, complaint_json))
        self.db.commit()
