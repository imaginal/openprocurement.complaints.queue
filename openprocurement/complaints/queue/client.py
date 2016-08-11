# -*- coding: utf-8 -*-
from time import sleep, time
from munch import munchify
from datetime import datetime
from openprocurement_client.client import TendersClient

import socket
import logging
logger = logging.getLogger(__name__)

CONFIG_BOOL = {'yes': True, 'no': False, 'true': True, 'false': False}


class ComplaintsClient(object):
    """OpenProcurement Complaints client"""

    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '0',
        'params': {},
        'timeout': 30,
        'store_claim': False,
        'store_draft': False,
        'skip_until': None,
        'sleep': 10,
    }

    store_tender_fields = ['id', 'tenderID', 'title', 'status', 'mode',
        'procuringEntity', 'procurementMethod', 'procurementMethodType']

    should_stop = False
    watchdog = None

    def __init__(self, client_config=None):
        if client_config:
            self.client_config.update(client_config)
        self.conf_skip_until = self.client_config.pop('skip_until', None)
        self.conf_store_claim = self.client_conf_bool('store_claim', False)
        self.conf_store_draft = self.client_conf_bool('store_draft', False)
        self.conf_timeout = float(self.client_config.pop('timeout', 0))
        self.conf_sleep = float(self.client_config.pop('sleep', 10))
        self.reset_client()

    def client_conf_bool(self, name, default=False):
        if name not in self.client_config:
            return default
        value = self.client_config.pop(name)
        try:
            return int(value)
        except ValueError:
            value = value.strip().lower()
        return CONFIG_BOOL[value]

    def test_exists(self, tender, complaint):
        return False

    def store(self, complaint, complaint_path):
        logger.debug("Fake Store T=%s P=%s C=%s", complaint.tender.id,
            complaint_path, complaint.id)

    def delete(self, tender, complaint_path, complaint):
        logger.debug("Fake Delete T=%s P=%s C=%s", complaint.tender.id,
            complaint_path, complaint.id)

    def related_lot_status(self, tender, complaint):
        relatedLot = complaint.get('relatedLot', None)
        if relatedLot:
            for lot in tender.lots:
                if lot.id == relatedLot:
                    return lot.status
        return None

    def patch_before_store(self, tender, complaint, complaint_path):
        if 'complaintID' not in complaint:
            complaint['complaintID'] = "{}.{}".format(tender.tenderID, complaint.id[:4])
        tender_info = dict()
        for k in self.store_tender_fields:
            if k in tender:
                tender_info[k] = tender[k]
        # July 26, 2016 by Andriy Kucherenko, patch tender.status to cancelled if
        # ... relatedLot.status is cancelled
        relatedLot_status = self.related_lot_status(tender, complaint)
        if relatedLot_status == "cancelled" and tender_info['status'] != "cancelled":
            logger.warning("Patch T=%s tID=%s P=%s C=%s TS=%s by relatedLot status LS=%s",
                tender.id, tender.tenderID, complaint_path, complaint.id, tender.status,
                relatedLot_status)
            tender_info['tenderStatus'] = tender_info['status']
            tender_info['status'] = relatedLot_status
        # munchify result tender_info
        complaint.tender = munchify(tender_info)

    def filter_complaint(self, tender, complaint_path, complaint):
        """return false if we should not store this complaint in queue"""
        # July 2, 2016 by Julia Dvornyk, don't store complaint.type == 'claim'
        if complaint.get('type', '') == 'claim' and not self.conf_store_claim:
            logger.warning("Ignore T=%s tID=%s P=%s C=%s by type CT=%s", tender.id,
                tender.tenderID, complaint_path, complaint.id, complaint.get('type', ''))
            return False
        # July 26, 2016 by Andriy Kucherenko, don't store complaint.status == 'draft'
        if complaint.get('status', '') == 'draft' and not self.conf_store_draft:
            logger.warning("Ignore T=%s tID=%s P=%s C=%s by status S=%s", tender.id,
                tender.tenderID, complaint_path, complaint.id, complaint.get('status', ''))
            return False
        # Aug 11, 2016 by Julia Dvornyk, don't store w/o dateSubmitted
        if not complaint.get('dateSubmitted', '') and not self.conf_store_draft:
            logger.warning("Ignore T=%s tID=%s P=%s C=%s cause dateSubmitted not set",
                tender.id, tender.tenderID, complaint_path, complaint.id)
            return False

        return True

    def process_complaint(self, tender, complaint_path, complaint):
        if not self.filter_complaint(tender, complaint_path, complaint):
            return

        if self.test_exists(tender, complaint):
            logger.warning("Ignore T=%s tID=%s P=%s C=%s by status TS=%s",
                tender.id, tender.tenderID, complaint_path, complaint.id, "cancelled")
            return

        logger.info("Complaint T=%s tID=%s P=%s C=%s DS=%s S=%s CT=%s TS=%s DM=%s M=%s",
            tender.id, tender.tenderID, complaint_path, complaint.id, complaint.dateSubmitted,
            complaint.status, complaint.get('type', ''), tender.status, tender.dateModified,
            tender.get('mode', ''))

        self.patch_before_store(tender, complaint, complaint_path)
        self.store(complaint, complaint_path)


    def process_tender(self, tender):
        logger.debug("Tender T=%s DM=%s", tender.id, tender.dateModified)
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
        while True:
            if self.watchdog:
                self.watchdog.counter = 0
            try:
                tenders_list = self.client.get_tenders()
            except Exception as e:
                logger.exception("Fail get_tenders {}".format(self.client_config))
                sleep(10*sleep_time)
                self.handle_error(e)
                continue

            if not tenders_list:
                break

            for tender in tenders_list:
                if self.watchdog:
                    self.watchdog.counter = 0
                if self.skip_until and self.skip_until > tender.dateModified:
                    logger.debug("Ignore T=%s DM=%s", tender.id, tender.dateModified)
                    continue
                try:
                    self.process_tender(tender)
                except Exception as e:
                    logger.exception("Fail on {} error {}: {}".format(tender, type(e), e))
                    sleep(10*sleep_time)
                    self.handle_error(e)

            if sleep_time:
                sleep(sleep_time)

    def need_reindex(self):
        if time() - self.reset_time > 20*3600:
            return datetime.now().hour <= 6
        return False

    def reset_client(self):
        logger.info("Client {} skip_until '{}'".format(
            self.client_config, self.conf_skip_until))
        if self.conf_timeout > 0.01:
            socket.setdefaulttimeout(self.conf_timeout)
        self.client = TendersClient(**self.client_config)
        self.client.params.pop('offset', None)
        self.skip_until = self.conf_skip_until
        self.reset_time = time()
        self.client_errors = 0

    def handle_error(self, error):
        self.client_errors += 1
        if self.client_errors > 100:
            self.reset_client()

    def run(self):
        while not self.should_stop:
            if self.need_reindex():
                self.reset_client()
            self.process_all()
            sleep(self.conf_sleep)
