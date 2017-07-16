#!/usr/bin/env python

import lxml.html
import requests
import re
import shutil
import tempfile
import hashlib
import datetime
import io
import json


from pprint import pprint
from collections import OrderedDict
import logging

from os.path import join as pjoin, dirname, splitext, basename

DATA_DIR = pjoin(dirname(__file__), '..', 'data')

BASE_URL = 'https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/'  # noqa

MONTHLY_LINK_PATTERN = '\s*Monthly A&E Attendances and Emergency Admissions \d{4}-\d{2}\s*'  # noqa
WEEKLY_LINK_PATTERN = '\s*Weekly A&E Attendances and Emergency Admissions \d{4}-\d{2}\s*' # noqa


MONTHS = 'January|February|March|April|May|June|July|August|September|October|November|December'  # noqa

MONTHLY_DATA_LINK_PATTERN = '\s*Monthly A&E (' + MONTHS + ').+(XLS.*)\s*'
WEEKLY_DATA_LINK_PATTERN = '\s*A&E Week Ending .+(XLS.*)\s*'


def main():
    logging.basicConfig(level=logging.DEBUG)
    start_datetime = datetime.datetime.now()

    http_getter = HttpGetterWithSessionAndUserAgent()

    base_page = load_as_lxml(http_getter.get(BASE_URL))

    monthly_page_urls = list(get_monthly_page_urls(base_page))
    weekly_page_urls = list(get_weekly_page_urls(base_page))

    monthly_data_urls = []
    weekly_data_urls = []

    for monthly_url in monthly_page_urls:
        monthly_page = load_as_lxml(http_getter.get(monthly_url))
        monthly_data_urls.extend(get_monthly_data_urls(monthly_page))

    for weekly_url in weekly_page_urls:
        weekly_page = load_as_lxml(http_getter.get(weekly_url))
        weekly_data_urls.extend(get_weekly_data_urls(weekly_page))

    pprint(monthly_page_urls)
    pprint(weekly_page_urls)
    pprint('got {} monthly data files: {}'.format(
        len(monthly_data_urls), monthly_data_urls))
    pprint('got {} weekly data files: {}'.format(
        len(weekly_data_urls), weekly_data_urls))

    end_datetime = datetime.datetime.now()

    spider_log = OrderedDict([
        ('spider_start_datetime',  start_datetime.isoformat()),
        ('spider_end_datetime',  end_datetime.isoformat()),
        ('data_files_discovered',  []),
    ])

    for url in monthly_data_urls:
        filename = download_data_url(http_getter, url, prefix='monthly')

        spider_log['data_files_discovered'].append({
            'url': url,
            'data_filename': basename(filename)
        })

    for url in weekly_data_urls:
        filename = download_data_url(http_getter, url, prefix='weekly')

        spider_log['data_files_discovered'].append({
            'url': url,
            'data_filename': basename(filename)
        })

    with io.open(pjoin(DATA_DIR, 'log_{}.json'.format(start_datetime)), 'w') as f:  # noqa
        json.dump(spider_log, f, indent=4)


def load_as_lxml(page_content):
    return lxml.html.fromstring(page_content)


def get_monthly_page_urls(base_page_lxml):
    for text, url in get_all_links(base_page_lxml):
        if re.match(MONTHLY_LINK_PATTERN, text) is not None:
            yield url


def get_weekly_page_urls(base_page_lxml):
    for text, url in get_all_links(base_page_lxml):
        if re.match(WEEKLY_LINK_PATTERN, text) is not None:
            yield url


def get_monthly_data_urls(monthly_page_lxml):
    for text, url in get_all_links(monthly_page_lxml):
        if re.match(MONTHLY_DATA_LINK_PATTERN, text) is not None:
            logging.info('{} : {}'.format(text, url))
            yield url


def get_weekly_data_urls(weekly_page_lxml):
    for text, url in get_all_links(weekly_page_lxml):
        if re.match(WEEKLY_DATA_LINK_PATTERN, text) is not None:
            logging.info('{} : {}'.format(text, url))
            yield url


def get_all_links(page_lxml):
    for a_tag in page_lxml.xpath('//a'):
        href = a_tag.attrib.get('href', None)

        if href is not None:
            yield a_tag.text_content().strip(), href


def download_data_url(http_getter, url, prefix):

    with tempfile.NamedTemporaryFile('wb', delete=False) as f:
        logging.info(f.name)
        content = http_getter.get(url)
        f.write(content)

        f.seek(0)
        hasher = hashlib.sha1(content)
        sha1 = hasher.hexdigest()
        base_filename, extension = splitext(url.split('/')[-1])

        full_filename = pjoin(
            DATA_DIR, prefix, '{}.{}{}'.format(base_filename, sha1, extension)
        )

        shutil.move(f.name, full_filename)
        logging.info('Downloaded {} to {}'.format(url, full_filename))
        return full_filename


class HttpGetterWithSessionAndUserAgent:
    def __init__(self):
        self.session = requests.Session()

    def get(self, url, *args, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['user-agent'] = (
            'NHS Hackday July 2017 spider bot@paulfurley.com'
        )

        response = self.session.get(url, *args, **kwargs)
        response.raise_for_status()
        return response.content


if __name__ == '__main__':
    main()
