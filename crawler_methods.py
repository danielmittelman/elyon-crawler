import urllib
import datetime
import sys
import os
import time
import re
import base64
import math
from urllib.request import OpenerDirector
from enum import Enum
from bs4 import BeautifulSoup

# Crawler-wide constants
SEARCH_URL = 'http://elyon1.court.gov.il/verdictssearch/HebrewVerdictsSearch.aspx'
EXTENDED_INFO_URL_PREFIX = 'http://elyon2.court.gov.il/scripts9/mgrqispi93.dll' \
                           '?Appname=eScourt&Prgname=GetFileDetails&Arguments=-N'

# HTTP requests headers
headers = {
    "User-Agent": "Mozilla/5.0 ElyonCrawler v1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-US,en;q=0.8,he;q=0.6"
    }

# Log message levels
class LogLevel(Enum):
    VERBOSE  = 1
    INFO     = 2
    ERROR    = 3

# Crawler fault increments
class FaultLevel(Enum):
    INTERVAL    = 75
    RESULT_PAGE = 25
    VERDICT     = 3


def test_connectivity(cookie_jar, timeout=30) -> (OpenerDirector, BeautifulSoup):
    """Tests the initial connectivity of the remote server.

    The cookie_jar argument is used to collect any cookies that may be
    set by the server.

    Returns the BeautifulSoup instance of the page and the request object so
    it can be reused
    """

    # Build an opener with the cookie jar and try to get the response from the server
    req = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    req.addheaders = list(headers.items())
    res = req.open(SEARCH_URL, timeout=timeout)

    # Upon 200 OK, try to read the response and return the BeautifulSoup object.
    # Otherwise, raise a server exception with the specified HTTP code
    if res.code == 200:
        soup = BeautifulSoup(res)
    else:
        raise RuntimeError("Server returned code: " + str(res.code))

    return req, soup


def get_first_search_result_page(req, soup, start_date, end_date, technical=False, timeout=30) -> BeautifulSoup:
    """Retrieves the first result page from the server."""

    # Modify the HTTP request headers to suit the request
    search_headers = headers.copy()
    search_headers["Origin"] = "http://elyon1.court.gov.il"
    search_headers["Referer"] = "http://elyon1.court.gov.il/verdictssearch/HebrewVerdictsSearch.aspx"
    req.addheaders = list(search_headers.items())

    post_map = {
        "Search$ddlYear": "",
        "Search$txtCaseNumber": "",
        "Search$txtText": "",
        "Search$ddlOper": 0,
        "Search$txtJudges": "",
        "Search$txtSides": "",
        "Search$txtLawyers": "",
        "Search$ddlMadors": "",
        "Search$ddlPages": 0,
        "Search$chkTechnical": "on" if technical else "",  # Turns technical resolutions search on/off
        "Search$txtDateFrom": start_date,
        "Search$txtDateTo": end_date,
        "__VIEWSTATE": soup.find_all(id="__VIEWSTATE")[0]['value'],
        "__EVENTTARGET": "Search$lnkSearch",
        "__LASTFOCUS": "",
        "__EVENTARGUMENT": "",
        "__EVENTVALIDATION": soup.find_all(id="__EVENTVALIDATION")[0]['value']
    }
    post_data = urllib.parse.urlencode(post_map).encode("UTF-8")

    # Request the search results page and create a BeautifulSoup object from the HTML response
    res = req.open(SEARCH_URL, data=post_data, timeout=timeout)
    if res.code == 200:
        soup = BeautifulSoup(res)
    else:
        raise RuntimeError("Server returned code: " + str(res.code))

    return soup


def calculate_page_count(soup):
    """Calculates the number of pages the search has yielded.

        This is required in order to calculate the number of iterations
        required to go over the input"""

    try:
        # Decode the VIEWSTATE as UTF-8, ignoring any malformed bytes
        view_state = base64.b64decode(
            soup.find_all(id="__VIEWSTATE")[0]['value']
        ).decode("utf-8", "ignore")

        # The information is located after the XML document, so remove anything before it
        view_state = re.sub(r'^([\s\S]+?)(</Results>|<Results />)', '', view_state)

        # The number of pages is calculated using two fields: The number of results
        # and the results per page. These appear in the following format:
        # |--|<result_count>|--|TRUE/FALSE|--|1|--|<results_per_page>|...
        result_count = view_state[:36].split("|")[2]
        results_per_page = view_state[:36].split("|")[8]

        # Calculate the number of pages
        return math.ceil(int(result_count) / int(results_per_page)), result_count
    except:
        return 0

def generate_page_postmap(soup):
    post_map = {
        "__EVENTTARGET": "setPage",
        "__VIEWSTATE": soup.find_all(id="__VIEWSTATE")[0]['value'],
        "__EVENTVALIDATION": soup.find_all(id="__EVENTVALIDATION")[0]['value']
    }
    return post_map

# Worker thread function for fetching a results page
def fetch_page(param):
    """Returns the from the search that can be obtained using the post_map, or get_number upon failure

    Argument should be a tuple or a list containing the following arguments
    in the following order:
    post_map -- POST headers map (map)
    req -- the OpenerDirector used to make the request (OpenerDirector)
    get_number -- the requested page's number (int)
    threads -- The crawler's thread count (int)
    timeout -- The crawler's timeout (int)"""

    # Unpack the parameters
    post_map = param[0]
    req = param[1]
    get_number = param[2]
    threads = param[3]
    timeout = param[4]

    post_map["__EVENTARGUMENT"] = get_number
    post_data = urllib.parse.urlencode(post_map).encode("UTF-8")

    time.sleep(((get_number - 1) % threads) / 10.0)

    try:
        res = req.open(SEARCH_URL, data=post_data, timeout=timeout)
        req.close()
        if res.code == 200:
            return get_number, res.read()
    except:
        return get_number, None

    return get_number, None

def generate_interval_list(start_date, end_date):
    # Split the date range into equal, week-long subranges
    days_count = (end_date - start_date).days
    if days_count <= 7:
        r = [(start_date, end_date)]
    else:
        r = [(start_date + datetime.timedelta(7*i), start_date + datetime.timedelta(7*(i+1)-1))
                  for i in range(0, math.ceil(days_count / 7))]
        r[len(r)-1] = (r[len(r)-1][0], end_date)  # Fix the last range

    return r

def get_entity_type(fault_entity):
    """Read the FaultEntity and use its fields to determine its type"""
    if fault_entity.interval is None:
        raise ValueError("The fault_entity object is malformed")

    if fault_entity.page is None:
        return FaultLevel.INTERVAL

    if fault_entity.verdict is None:
        return FaultLevel.RESULT_PAGE

    return FaultLevel.VERDICT


# Stores an entity (interval / page / single verdict) or a range of entities that failed
class FaultEntity:
    interval = ()  # (start, end)
    page = None   #
    verdicts = None  # [verdict_1, verdict_2, ...]

    def __init__(self, interval, page=None, verdicts=None):
        self.interval = interval
        self.page = page
        self.verdicts = verdicts
