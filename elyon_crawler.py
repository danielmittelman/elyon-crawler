import math, base64, re, datetime, sys, os, time
import urllib.request
import urllib.parse
import http.cookiejar
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from case import CourtCase
from db import ElyonDatabase
from enum import Enum
from multiprocessing import Pool
from functools import partial

class ElyonCrawler:
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

    # General progress
    start_time = 0
    downloaded_bytes = 0
    downloaded_verdicts = 0

    # Globals - cookie jar for retaining cookies throughout requests, thread pool
    cookie_jar = http.cookiejar.CookieJar()
    thread_pool = None
    pool_progress, pool_total = 0, 0

    def __init__(self, start, end, logfile=None, outputfile='elyon.db',
                 verbose=False, quiet=False, skip_confidential=False,
                 timeout=30, threads=2, technical=False, full_text=False):
        self.start = start
        self.end = end
        self.logfile = logfile
        self.outputfile = outputfile
        self.verbose = verbose
        self.quiet = quiet
        self.skip_confidential = skip_confidential
        self.timeout = timeout
        self.threads = threads
        self.technical = technical
        self.full_text = full_text

        self.start_time = datetime.datetime.now()

    # Searches the new website (elyon1) during the given time period
    # and calls the subsequent methods which handle extraction and insertion
    # into the database
    def start_search(self, start_date, end_date, percent):
        self.log_message(self.LogLevel.INFO, "Starting interval: " + start_date + " - " + end_date +
                         " ({0:.2f}%)".format(percent), trunc=True)

        # Test connectivity and collect any possible cookies that may be sent
        req = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))
        req.addheaders = list(self.headers.items())

        # Try to connect and retrieve the contents into a BeautifulSoup object if possible
        try:
            res = req.open(self.SEARCH_URL, timeout=self.timeout)
            if res.code == 200:
                soup = BeautifulSoup(res)
            else:
                self.log_message(self.LogLevel.ERROR, "Initial connect failed, server returned code " + str(res.code))
                return
        except Exception as e:
            self.log_message(self.LogLevel.ERROR, "Initial connect failed, exception: " + str(e))
            return

        # Perform a wildcard search and retrieve the first result page
        self.headers["Origin"] = "http://elyon1.court.gov.il"
        self.headers["Referer"] = "http://elyon1.court.gov.il/verdictssearch/HebrewVerdictsSearch.aspx"
        req.addheaders = list(self.headers.items())

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
            "Search$chkTechnical": "on" if self.technical else "",  # Turns technical resolutions search on/off
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
        try:
            res = req.open(self.SEARCH_URL, data=post_data, timeout=self.timeout)
            if res.code == 200:
                soup = BeautifulSoup(res)
            else:
                self.log_message(self.LogLevel.ERROR, "Interval search failed, server returned code " + str(res.code))
                return
        except Exception as e:
            self.log_message(self.LogLevel.ERROR, "Interval search failed, exception: " + str(e))
            return

        # Calculate the number of results and pass the results to run_crawl_loop()
        page_count, result_count = self.calculate_page_count(soup)
        self.pool_total = result_count
        self.run_crawl_loop(req, soup, page_count)

    # Iterate over the search results pages. For each page, extract the verdicts
    # and insert them into the database
    def run_crawl_loop(self, req, soup, page_count):
        # Reset the number of verdicts processed for this interval
        self.pool_progress = 0

        # Prepare the POST fields to request the result pages
        post_map = {
            "__EVENTTARGET": "setPage",
            "__VIEWSTATE": soup.find_all(id="__VIEWSTATE")[0]['value'],
            "__EVENTVALIDATION": soup.find_all(id="__EVENTVALIDATION")[0]['value']
        }

        # Fetch the results page simultaneously
        thread_info = []
        for i in range(1, page_count + 1):
            thread_info.append((post_map.copy(), req, i))

        self.log_message(self.LogLevel.VERBOSE, "Retrieving all results pages for this interval...")
        pool = Pool(self.threads)
        pages = pool.map(self.get_page, thread_info)
        pool.terminate()
        self.log_message(self.LogLevel.VERBOSE, str(len(pages)) + " results pages retrieved")

        # Parse each search results page and push the results into the database
        # Avoid None objects which are pages that have not been downloaded correctly
        for p in [p for p in pages if p is not None]:
            results = self.handle_result_page(BeautifulSoup(p))
            self.log_message(self.LogLevel.VERBOSE, "Writing buffer to database")
            self.insert_results_to_db(results)

    # Worker thread function for fetching a results page
    def get_page(self, param):
        # Unpack the parameters
        post_map = param[0]
        req = param[1]
        get_number = param[2]

        post_map["__EVENTARGUMENT"] = get_number
        post_data = urllib.parse.urlencode(post_map).encode("UTF-8")

        time.sleep(((get_number - 1) % self.threads) / 10.0)

        try:
            res = req.open(self.SEARCH_URL, data=post_data, timeout=self.timeout)
            req.close()
            if res.code == 200:
                return res.read()
            else:
                self.log_message(self.LogLevel.ERROR, "Page parse iterator failed on page "
                                 + str(get_number) + ", server returned code " + str(res.code))
                return None
        except Exception as e:
            self.log_message(self.LogLevel.ERROR, "Page parse iterator failed on page "
                             + str(get_number) + ", exception: " + str(e))
            return None

    # Decodes and parses the IIS VIEWSTATE hidden field, then extracts the XML search data
    # and uses it to generate CourtCase object instances with the verdict's information
    def handle_result_page(self, soup):
        return_list = []

        # Extract and decode the XML search results from the VIEWSTATE
        view_state = base64.b64decode(
            soup.find_all(id="__VIEWSTATE")[0]['value']
        ).decode("utf-8", "ignore")
        results_mask = re.compile(r'<Results>([\s\S]+?)</Results>')
        data_xml = results_mask.search(view_state).group(0)

        # Pass each child (search result) to CourtCase's constructor as an ElementTree object
        data_tree = ET.fromstring(data_xml)

        # Create a list of data tree elements with the required delay on startup
        # This is used to space the requests instead of sending them all at the same time
        data_tree_numbered = []
        for i in range(len(data_tree)):
            data_tree_numbered.append((i, data_tree[i]))

        # Initialize a thread pool and execute the jobs concurrently
        thread_pool = Pool(self.threads)
        callback = partial(self.print_status_line)
        tasks = [thread_pool.apply_async(self.get_case, (x, ), callback=callback) for x in data_tree_numbered]
        tasks_results = [task.get() for task in tasks]
        thread_pool.terminate()
        return tasks_results

    # Retrieves and parses the information for a single case.
    # This is the work performed by each worker threads
    def get_case(self, thread_info):
        # Unpack the parameters
        queue_pos = thread_info[0]
        row = thread_info[1]

        # Use queue_pos to space the worker threads. Assuming each thread takes approximately
        # the same time, space out the first round of worker threads by using incremental
        # sleep delays. Remember that queue_pos is 0-based, so the first thread is not delayed
        if queue_pos < self.threads:
            time.sleep(queue_pos / 20.0)  # Space out threads by 50ms

        # Fetch the document text from the server and
        doc_filename = CourtCase.extract_filename(row)
        doc_text = CourtCase.get_document_text(doc_filename.replace(".doc", ".txt"), self.timeout)
        extended_info = self.get_verdict_extended_info(
            CourtCase.extract_case_id(row), CourtCase.extract_filename(row))

        self.pool_progress += 1
        return CourtCase(row, extended_info, doc_text)

    # Calculates the number of pages, meaning the number of loop iterations required
    # to complete going over the results of a specific search
    def calculate_page_count(self, soup):
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

    # Retrieves the extended information about a case by directly querying the script
    # on the old website (elyon2)
    def get_verdict_extended_info(self, case_id, filename):
        # Generate the query URL
        try:
            [sn, year] = case_id.split(r"/")  # Split the case ID into the year and serial number
        except:
            return '', [], []
        sn = sn.zfill(6)  # Pad the serial number to 6 characters
        year = "19" + year if (int(year) >= 77) else "20" + year  # Extend the year to its full format
        query_url = self.EXTENDED_INFO_URL_PREFIX + year + "-" + sn + "-0"

        # Query the URL and retrieve the HTML into a BeautifulSoup object
        try:
            res = urllib.request.urlopen(query_url, timeout=self.timeout)
            self.log_message(self.LogLevel.VERBOSE, "Retrieving information for case " + case_id)

            if res.code == 200:
                soup = BeautifulSoup(res)
            else:
                self.log_message(self.LogLevel.ERROR, "Failed to pull extended information for verdict "
                                 + case_id, ", server returned code " + str(res.code))
                return '', [], []
        except Exception as e:
            self.log_message(self.LogLevel.ERROR, "Failed to pull extended information for verdict "
                             + case_id, ", exception: " + str(e))
            return '', [], []

        # Check if the case is confidential, if it is the information will not be available
        if self.is_case_confidential(soup):
            if self.skip_confidential:
                self.log_message(self.LogLevel.VERBOSE, "Verdict " + case_id + " is confidential, skipping")
            return "חסוי", [], []

        # Try extracting the information from the page, on failure return empty results
        try:
            case_status = soup.find(id="General").table.tr.td.table.find_all("tr")[3].find_all("td")[1].text

            case_parties = []
            parties_soup = soup.find(id="Parties").table.find_all("tr")
            parties_iter = iter(parties_soup)
            next(parties_iter)
            for row in parties_iter:
                row_cells = row.find_all("td")
                case_parties.append((row_cells[0].text, row_cells[2].text, row_cells[3].text))

            decisions_soup = soup.find(id="Decisions").table.tr.td.div.table
            dec_index = 0
            for dec in decisions_soup.find_all("tr"):
                dec_index += 1
                remote_filename = dec.find_all("td")[3].a['href'][-18:-6]
                if re.search(remote_filename.lower(), filename.lower()) is not None:
                    break

            case_judges = soup.find(id="DecisionsMore" + str(dec_index)).table.tr.td.table.find_all("tr")
            judges_iter = iter(case_judges)
            next(judges_iter)
            judges = [j.td.text for j in judges_iter]
        except Exception as e:
            self.log_message(self.LogLevel.ERROR, "Unable to extract extended verdict information for verdict "
                            + case_id + ", exception: " + str(e))
            return '', [], []

        return case_status, case_parties, judges

    # Insert the result list into the database, making sure to skip confidential verdicts if the
    # -S/--skip-confidential flag is on
    def insert_results_to_db(self, results):
        db = ElyonDatabase(self.outputfile, self.full_text)
        for r in results:
            if not(r.status == 'חסוי' and self.skip_confidential):
                db.insert_case(r)

    # Returns whether a verdict is confidential or not
    def is_case_confidential(self, soup):
        return str(soup).find("חסויים") > 0

    # Handles writing log messages to the standard output and/or file
    def log_message(self, level, msg, trunc=False):
        # Set the minimum log levels based on user input
        file_min_level = self.LogLevel.VERBOSE if self.verbose else self.LogLevel.INFO
        stderr_min_level = self.LogLevel.ERROR if self.quiet else file_min_level

        # Set the trunc variable to either truncate the line or not (applicable to standard output only)
        trunc_char = '\r' if trunc else ''

        # Write messages to log file - Use full date and time format
        if level.value >= file_min_level.value:
            with open(self.logfile, 'a') as file:
                file.write("[{0}] ({1}) ".format(
                    level.name, datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")
                ) + msg + os.linesep)

        # Write messages to the standard output - Display only time
        if level.value >= stderr_min_level.value:
            print("{0}[{1}] ({2}) ".format(
                trunc_char, level.name, datetime.datetime.now().strftime("%H:%M:%S")
            ) + msg, file=(sys.stderr if level.value == self.LogLevel.ERROR else sys.stderr))

    # Print a status line if the crawler is not running in verbose mode
    def print_status_line(self, _):
        if not self.verbose:
            self.pool_progress += 1
            # Prepare the progress bar
            percent = float(self.pool_progress) / int(self.pool_total)
            hashes = '#' * int(percent * 20)
            spaces = ' ' * (19 - len(hashes))
            bar = "[{0}] {1}%".format((hashes + spaces), round(percent * 100))

            sys.stdout.write('\r[INFO] Processing interval ({0} / {1} verdicts processed)       {2}'.format(
                str(self.pool_progress), str(self.pool_total), bar)
            )

            if percent == 1.0:
                sys.stdout.write('\x1b[2K') # Erase the progress line

            sys.stdout.flush()

    # Starts the crawler between the provided start and end dates and writes the results
    # to the output database file and/or to the output log file
    def crawl(self):
        # Notify start
        self.log_message(self.LogLevel.INFO, "ElyonCrawler started, using " + str(self.threads) + " concurrent threads")

        # Split the date range into equal, week-long subranges
        days_count = (self.end - self.start).days
        if days_count <= 7:
            r = [(self.start, self.end)]
        else:
            r = [(self.start + datetime.timedelta(7*i), self.start + datetime.timedelta(7*(i+1)-1))
                      for i in range(0, math.ceil(days_count / 7))]
            r[len(r)-1] = (r[len(r)-1][0], self.end)  # Fix the last range

        # Run start_search() for every interval
        it = 0
        total = len(r)
        for (s, e) in r:
            self.start_search(s.strftime("%d/%m/%Y"), e.strftime("%d/%m/%Y"), (it * 100.0 / total))
            it += 1

        # Print a success line to the standard output and log
        self.log_message(self.LogLevel.INFO, "ElyonCrawler done, exiting                 ", trunc=True)
