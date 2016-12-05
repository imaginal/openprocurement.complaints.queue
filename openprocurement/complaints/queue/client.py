# -*- coding: utf-8 -*-
from time import sleep, time
from munch import munchify
from retrying import retry
from iso8601 import parse_date
from datetime import datetime, timedelta
from openprocurement_client.client import TendersClient

import socket
import logging
logger = logging.getLogger(__name__)


def getboolean(value):
    bool_dict = {'y': 1, 'n': 0, 'yes': 1, 'no': 0,
        'on': 1, 'off': 0, 'true': 1, 'false': 0}
    try:
        return bool_dict[value.strip().lower()]
    except:
        return int(value or 0)


class ComplaintsClient(object):
    """OpenProcurement Complaints client"""

    client_config = {
        'key': '',
        'host_url': "https://api-sandbox.openprocurement.org",
        'api_version': '0',
        'mode': '',
        'feed': 'changes',
        'limit': 1000,
        'timeout': 30,
        'use_cache': False,
        'store_claim': False,
        'store_draft': False,
        'fast_rewind': False,
        'skip_until': None,
        'reset_hour': 22,
        'clear_cache': 6,
        'sleep': 10,
    }

    reset_client_hour = 22
    clear_cache_wday = 7
    last_reset_time = 0
    client_errors = 0
    tenders_count = 0

    store_tender_fields = ['id', 'tenderID', 'title', 'status', 'mode',
        'procuringEntity', 'procurementMethod', 'procurementMethodType',
        'dateModified']

    watchdog = None
    needstop = False

    def __init__(self, client_config=None):
        if client_config:
            self.client_config.update(client_config)
        self.conf_timeout = float(self.client_config['timeout'] or 30)
        self.conf_sleep = float(self.client_config['sleep'] or 10)
        for k in ['use_cache', 'store_claim', 'store_draft', 'fast_rewind']:
            self.client_config[k] = getboolean(self.client_config.get(k))
        self.descending_mode = getboolean(self.client_config.get('descending'))
        self.reset_client_hour = int(self.client_config['reset_hour'])
        self.clear_cache_wday = int(self.client_config['clear_cache'])
        self.reset_client()

    @property
    def should_stop(self):
        if self.watchdog and self.watchdog.counter > 1:
            raise self.watchdog.WatchdogError("should_stop")
        return self.needstop

    def sleep(self, seconds):
        for i in range(int(2 * seconds)):
            if self.should_stop:
                break
            if self.watchdog:
                self.watchdog.counter = 0
            sleep(0.5)

    def clear_cache(self):
        logger.debug("Fake clear cache")

    def check_cache(self, tender):
        return False

    def check_exists(self, tender, complaint_path, complaint):
        return False

    def store(self, complaint, complaint_path):
        logger.debug("Fake Store T=%s P=%s C=%s", complaint.tender.id,
            complaint_path, complaint.id)

    def delete(self, tender, complaint_path, complaint):
        logger.debug("Fake Delete T=%s P=%s C=%s", complaint.tender.id,
            complaint_path, complaint.id)

    def finish_tender(self, tender):
        logger.debug("Finish tender T=%s DM=%s", tender.id, tender.dateModified)

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
            logger.warning("Patch T=%s P=%s C=%s TS=%s by relatedLot status LS=%s",
                tender.id, complaint_path, complaint.id, tender.status, relatedLot_status)
            tender_info['tenderStatus'] = tender_info['status']
            tender_info['status'] = relatedLot_status
        # munchify result tender_info
        complaint.tender = munchify(tender_info)

    def check_nostore(self, tender, complaint_path, complaint):
        """return True if we should not store this complaint in queue"""
        # July 2, 2016 by Julia Dvornyk, don't store complaint.type == 'claim'
        if complaint.get('type', '') == 'claim' and not self.client_config['store_claim']:
            logger.info("Ignore T=%s P=%s C=%s by type CT=%s", tender.id,
                complaint_path, complaint.id, complaint.get('type', ''))
            return True
        # July 26, 2016 by Andriy Kucherenko, don't store complaint.status == 'draft'
        if complaint.get('status', '') == 'draft' and not self.client_config['store_draft']:
            logger.info("Ignore T=%s P=%s C=%s by status S=%s", tender.id,
                complaint_path, complaint.id, complaint.get('status', ''))
            return True
        # Aug 11, 2016 by Julia Dvornyk, don't store w/o dateSubmitted
        if not complaint.get('dateSubmitted', ''):
            logger.info("Ignore T=%s P=%s C=%s dateSubmitted not set",
                tender.id, complaint_path, complaint.id)
            return True

        return False

    def process_complaint(self, tender, complaint_path, complaint):
        if self.check_nostore(tender, complaint_path, complaint):
            return

        if self.check_exists(tender, complaint_path, complaint):
            return

        logger.info("Complaint T=%s P=%s C=%s DS=%s S=%s TS=%s DM=%s M=%s",
            tender.id, complaint_path, complaint.id, complaint.get('dateSubmitted', ''),
            complaint.status, tender.status, tender.dateModified, tender.get('mode', ''))

        self.patch_before_store(tender, complaint, complaint_path)
        self.store(complaint, complaint_path)

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def get_tender_data(self, tender_id):
        tender = self.client.get_tender(tender_id)
        return tender['data']

    def process_tender(self, tender):
        if self.client_config['use_cache'] and self.check_cache(tender):
            logger.debug("Exists T=%s DM=%s", tender.id, tender.dateModified)
            return

        logger.debug("Tender T=%s DM=%s", tender.id, tender.dateModified)
        data = self.get_tender_data(tender.id)

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

        if self.client_config['use_cache']:
            self.finish_tender(data)

    def process_all(self, sleep_time=1):
        while not self.should_stop:
            if self.watchdog:
                self.watchdog.counter = 0
            try:
                feed = self.client_config['feed'] or 'changes'
                tenders_list = self.client.get_tenders(feed=feed)
            except Exception as e:
                logger.exception("Fail get_tenders")
                self.sleep(10 * sleep_time)
                self.handle_error(e)
                continue

            if not tenders_list:
                break

            tender = None
            for tender in tenders_list:
                self.tenders_count += 1
                if self.should_stop:
                    break
                if self.watchdog:
                    self.watchdog.counter = 0
                if self.skip_until and self.skip_until > tender.dateModified:
                    logger.debug("Ignore T=%s DM=%s", tender.id, tender.dateModified)
                    continue
                try:
                    self.process_tender(tender)
                except Exception as e:
                    logger.exception("Fail on {} error {}: {}".format(tender, type(e), e))
                    self.sleep(10 * sleep_time)
                    self.handle_error(e)

            if tender:
                logger.info("Processed %d tenders last %s",
                    self.tenders_count, tender.dateModified)

            if sleep_time:
                self.sleep(sleep_time)

    def need_clear_cache(self):
        if not self.client_config.get('use_cache', False):
            return False
        if datetime.now().isoweekday() == self.clear_cache_wday:
            return self.need_reset_client()

    def need_reset_client(self):
        if datetime.now().hour == self.reset_client_hour:
            return time() - self.last_reset_time > 3600
        return False

    def client_rewind(self, skip_until, skip_days=0):
        if self.descending_mode:
            logger.warning("Don't rewind in descending_mode")
            return
        date = datetime.now() - timedelta(days=10)
        if skip_until < date.strftime("%Y-%m-%d"):
            logger.info("%s is too old for fast_rewind", skip_until)
            return
        if skip_days:
            date = parse_date(skip_until) - timedelta(days=skip_days)
            skip_until = date.strftime("%Y-%m-%d")
        self.client.params.pop('offset', None)
        self.client.params['descending'] = "1"
        logger.info("Start fast_rewind to %s", skip_until)
        for i in range(101):
            if self.watchdog:
                self.watchdog.counter = 0
            tenders_list = self.client.get_tenders()
            if not tenders_list or i >= 99:
                logger.error("Failed rewind to %s", skip_until)
                self.client.params.pop('offset', None)
                break
            for item in tenders_list:
                if item['dateModified'] > skip_until:
                    break
            if item['dateModified'] < skip_until:
                logger.info("Rewind success to %s", item['dateModified'])
                break
            logger.debug("Rewind client, last %s", item['dateModified'])
        self.client.params.pop('descending')

    def update_offset_before_start(self):
        if self.descending_mode:
            return
        if self.skip_until and self.client_config['fast_rewind']:
            if self.client_config['feed'] == 'dateModified':
                self.client.params['offset'] = self.skip_until
            if self.client_config['feed'] == 'changes':
                self.client_rewind(self.skip_until)

    def client_skip_until(self, skip_until=None, skip_days=0):
        if self.descending_mode and skip_until:
            logger.info("Ignore skip_until %s in descending mode", skip_until)
            return
        if not skip_until:
            skip_until = self.client_config['skip_until']
        if skip_until and skip_until[:2] == "20":
            skip_until = skip_until[:10]
        if skip_days:
            date = parse_date(skip_until) - timedelta(days=skip_days)
            skip_until = date.strftime("%Y-%m-%d")
        self.skip_until = skip_until

    @retry(stop_max_attempt_number=5, wait_fixed=5000)
    def reset_client(self):
        logger.info("Reset Client {}".format(self.client_config))
        if self.client_config['mode'] not in ['', '_all_', 'test']:
            logger.warning("Unknown client mode '%s'", self.client_config['mode'])
        if self.client_config['feed'] not in ['changes', 'dateModified']:
            logger.warning("Unknown client feed '%s'", self.client_config['feed'])
        if self.conf_timeout:
            socket.setdefaulttimeout(self.conf_timeout)
        client_options = {
            'key': self.client_config['key'],
            'host_url': self.client_config['host_url'],
            'api_version': self.client_config['api_version'],
            'params': {
                'mode': self.client_config['mode'],
                'limit': self.client_config['limit'],
            },
        }
        if self.descending_mode:
            client_options['params']['descending'] = "1"
        self.client = TendersClient(**client_options)
        self.client_skip_until()
        self.last_reset_time = time()
        self.client_errors = 0
        self.tenders_count = 0

    def handle_error(self, error):
        self.client_errors += 1
        if self.client_errors >= 10:
            self.reset_client()

    def run(self):
        self.update_offset_before_start()
        while not self.should_stop:
            if self.need_clear_cache():
                self.clear_cache()
            if self.need_reset_client():
                self.reset_client()
            self.process_all()
            self.sleep(self.conf_sleep)
