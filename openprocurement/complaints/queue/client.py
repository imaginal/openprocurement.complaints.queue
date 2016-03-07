# -*- coding: utf-8 -*-
from time import sleep, time
from datetime import datetime
from openprocurement_client.client import Client

import logging
logger = logging.getLogger(__name__)


class ComplaintsClient(object):
    """OpenProcurement Complaints client"""

    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '0.12',
        'params': {},
    }

    complaint_date_fields = ['dateSubmitted', 'dateAnswered',
        'dateEscalated', 'dateDecision', 'dateCanceled']

    should_stop = False

    def __init__(self, client_config=None):
        if client_config:
            self.client_config.update(client_config)
        logger.info("Create client {}".format(self.client_config))
        self.client = Client(**self.client_config)
        self.reset_client()

    def test_exists(self, tender_id, complaint_id, complaint_date):
        return False

    def store(self, tender, complaint, complaint_path, complaint_date):
        logger.debug("Fake Store T=%s C=%s P=%s D=%s S=%s", tender.id, complaint.id,
            complaint_path, complaint_date, complaint.status)

    def complaint_date(self, complaint):
        date = complaint.date
        for k in self.complaint_date_fields:
            if complaint.get(k, None) > date:
                date = complaint[k]
        return date

    def process_complaint(self, tender, complaint_path, complaint):
        complaint_date = self.complaint_date(complaint)

        logger.info("Process T=%s C=%s P=%s D=%s S=%s", tender.id, complaint.id,
            complaint_path, complaint_date, complaint.status)

        if not self.test_exists(tender.id, complaint_path, complaint_date):
            self.store(tender, complaint, complaint_path, complaint_date)

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
                sleep(sleep_time)
                continue

            if not tenders_list:
                break

            for tender in tenders_list:
                if self.should_stop:
                    break
                if self.skip_till > tender.dateModified:
                    logger.debug("Ignore T=%s D=%s", tender.id, tender.dateModified)
                    continue
                try:
                    self.process_tender(tender)
                except Exception as e:
                    logger.error("Fail on {} error {}: {}".format(tender, type(e), e))

            if sleep_time:
                sleep(sleep_time)

    def need_reindex(self):
        if time() - self.reset_time > 20*3600:
            return datetime.now().hour <= 6
        return False

    def reset_client(self):
        logger.info("Reset client params")
        self.client.params.pop('offset', None)
        self.reset_time = time()
        self.skip_till = None

    def run(self, sleep_time=10):
        while not self.should_stop:
            if self.need_reindex():
                self.reset_client()
            self.process_all()
            sleep(sleep_time)
