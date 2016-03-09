# -*- coding: utf-8 -*-
from time import sleep, time
from iso8601 import parse_date
from datetime import datetime
from openprocurement_client.client import Client

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
        'skip_until': None,
    }

    complaint_date_fields = ['dateSubmitted', 'dateAnswered',
        'dateEscalated', 'dateDecision', 'dateCanceled']
    store_tender_fields = ['id', 'tenderID', 'title', 'procuringEntity',
        'procurementMethod', 'procurementMethodType']

    should_stop = False

    def __init__(self, client_config=None):
        if client_config:
            self.client_config.update(client_config)
        logger.info("Create client {}".format(self.client_config))
        self.conf_skip_until = self.client_config.pop('skip_until')
        assert(parse_date(self.conf_skip_until or '1970-01-01'))
        self.client = Client(**self.client_config)
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
        complaint['tender'] = dict((k, tender.get(k)) for k in self.store_tender_fields)

    def process_complaint(self, tender, complaint_path, complaint):
        complaint_date = self.complaint_date(complaint)

        logger.info("Process T=%s P=%s C=%s D=%s S=%s", tender.id, complaint_path,
            complaint.id, complaint_date, complaint.status)

        if not self.test_exists(complaint.id, complaint_date):
            path = "{}/{}".format(tender.id, complaint_path)
            self.update_before_store(tender, complaint)
            self.store(complaint, path, complaint_date)

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
            try:
                tenders_list = self.client.get_tenders()
            except Exception as e:
                logger.error("Fail get_tenders {}".format(self.client_config))
                sleep(10*sleep_time)
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

            if sleep_time:
                sleep(sleep_time)

    def need_reindex(self):
        if time() - self.reset_time > 20*3600:
            return datetime.now().hour <= 6
        return False

    def reset_client(self):
        logger.info("Reset client params, set skip_until to %s", self.conf_skip_until)
        self.client.params.pop('offset', None)
        self.skip_until = self.conf_skip_until
        self.reset_time = time()

    def run(self, sleep_time=10):
        while not self.should_stop:
            if self.need_reindex():
                self.reset_client()
            self.process_all()
            sleep(sleep_time)
