# -*- coding: utf-8 -*-
from time import sleep, time
from munch import munchify
from datetime import datetime
from openprocurement_client.client import TendersClient

import socket
import traceback
import logging
logger = logging.getLogger(__name__)


class ComplaintsClient(object):
    """OpenProcurement Complaints client"""

    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '0.12',
        'params': {},
        'timeout': 30,
        'skip_until': None,
    }

    complaint_date_fields = ['dateSubmitted', 'dateAnswered',
        'dateEscalated', 'dateDecision', 'dateCanceled']
    store_tender_fields = ['id', 'tenderID', 'title', 'status',
        'procuringEntity', 'procurementMethod', 'procurementMethodType']

    should_stop = False

    def __init__(self, client_config=None):
        if client_config:
            self.client_config.update(client_config)
        self.conf_skip_until = self.client_config.pop('skip_until', None)
        self.conf_timeout = float(self.client_config.pop('timeout', 0))
        self.reset_client()

    def test_exists(self, complaint_id, complaint_date):
        return False

    def store(self, complaint, complaint_path, complaint_date):
        logger.debug("Fake Store C=%s P=%s D=%s S=%s", complaint.id,
            complaint_path, complaint_date, complaint.status)

    def complaint_date(self, complaint):
        date = complaint.date
        for k in self.complaint_date_fields:
            if complaint.get(k, None) > date:
                date = complaint[k]
        return date

    def update_before_store(self, tender, complaint):
        if 'complaintID' not in complaint:
            complaint['complaintID'] = "{}.{}".format(tender.tenderID, complaint.id[:4])
        tender_info = dict((k, tender.get(k)) for k in self.store_tender_fields)
        complaint.tender = munchify(tender_info)

    def process_complaint(self, tender, complaint_path, complaint):
        complaint_date = self.complaint_date(complaint)

        logger.info("Process T=%s P=%s C=%s D=%s CS=%s TS=%s", tender.id, complaint_path,
            complaint.id, complaint_date, complaint.status, complaint.tender.status)

        if not self.test_exists(complaint.id, complaint_date):
            self.update_before_store(tender, complaint)
            self.store(complaint, complaint_path, complaint_date)

    def process_tender(self, tender):
        logger.debug("Process T=%s D=%s", tender.id, tender.dateModified)
        data = self.client.get_tender(tender.id)['data']

        for comp in data.get('complaints', []):
            self.process_complaint(data, 'complaints', comp)

        for award in data.get('awards', []):
            if 'complaints' in award:
                path = "awards/{}/complaints".format(award.id)
                for comp in award.complaints:
                    self.process_complaint(data, path, comp)

        for qual in data.get('qualifications', []):
            if hasattr(qual, 'complaints'):
                path = "qualifications/{}/complaints".format(qual.id)
                for comp in qual.complaints:
                    self.process_complaint(data, path, comp)

    def process_all(self, sleep_time=1):
        while not self.should_stop:
            if self.conf_timeout > 1e6:
                socket.setdefaulttimeout(self.conf_timeout)
            try:
                tenders_list = self.client.get_tenders()
            except Exception as e:
                logger.error("Fail get_tenders {}".format(self.client_config))
                traceback.print_exc()
                sleep(10*sleep_time)
                self.handle_error(e)
                continue

            if not tenders_list:
                break

            for tender in tenders_list:
                if self.should_stop:
                    break
                if self.skip_until and self.skip_until > tender.dateModified:
                    logger.debug("Ignore T=%s D=%s", tender.id, tender.dateModified)
                    continue
                try:
                    self.process_tender(tender)
                except Exception as e:
                    logger.error("Fail on {} error {}: {}".format(tender, type(e), e))
                    traceback.print_exc()
                    sleep(10*sleep_time)
                    self.handle_error(e)

            if sleep_time:
                sleep(sleep_time)

    def need_reindex(self):
        if time() - self.reset_time > 20*3600:
            return datetime.now().hour <= 6
        return False

    def reset_client(self):
        logger.info("Client {} skip_until {}".format(
            self.client_config, self.conf_skip_until))
        self.client = TendersClient(**self.client_config)
        self.client.params.pop('offset', None)
        self.skip_until = self.conf_skip_until
        self.reset_time = time()
        self.client_errors = 0

    def handle_error(self, error):
        self.client_errors += 1
        if self.client_errors > 100:
            self.reset_client()

    def run(self, sleep_time=10):
        while not self.should_stop:
            if self.need_reindex():
                self.reset_client()
            self.process_all()
            sleep(sleep_time)
