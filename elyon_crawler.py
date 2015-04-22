import base64
import re
import datetime
import sys
import time
import os
import urllib.request
from urllib.request import OpenerDirector
import urllib.parse
import http.cookiejar
import xml.etree.ElementTree as ET
from multiprocessing import Pool
from functools import partial

from bs4 import BeautifulSoup

from case import CourtCase
from db import ElyonDatabase
import crawler_methods
from crawler_methods import FaultEntity, LogLevel


class ElyonCrawler:
    FAULT_THRESHOLD = 150

    # Global fault variables
    fault_count = 0
    fault_pause_duration = 1
    fault_list = []

    # Globals - cookie jar for retaining cookies throughout requests, thread pool
    cookie_jar = http.cookiejar.CookieJar()
    thread_pool = None
    pool_progress, pool_total = 0, 0

    def __init__(self, start, end, logfile='elyon.log', outputfile='elyon.db',
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

    def perform_search(self, start_date, end_date, percent=0) -> (OpenerDirector, BeautifulSoup, int):
        """Checks for connectivity and performs a search over the given interval.

        Returns the request object used to perform the request, the BeautifulSoup object containing
        the first search results page, and the number of pages the search results are spread over"""
        self.log_message(LogLevel.INFO, "Starting interval: " + start_date + " - " + end_date +
                         " ({0:.2f}%)".format(percent), trunc=True)

        # Test initial connection and get the search form page
        try:
            req, soup = crawler_methods.test_connectivity(self.cookie_jar, self.timeout)
        except Exception as e:
            self.add_failure(FaultEntity(start_date, end_date))
            self.log_message(LogLevel.ERROR, "Initial connection failed, exception: " + str(e))
            return None, None, None

        # Perform the search and get the first search results page
        try:
            soup = crawler_methods.get_first_search_result_page(
                req, soup, start_date, end_date, self.technical, self.timeout)
        except Exception as e:
            self.add_failure(FaultEntity(start_date, end_date))
            self.log_message(LogLevel.ERROR, "Initial search failed, exception: " + str(e))
            return None, None, None

        # Calculate the number of results and pass the results to run_crawl_loop()
        page_count, result_count = crawler_methods.calculate_page_count(soup)
        self.pool_total = result_count

        return req, soup, page_count



    def get_pages_for_search(self, req, soup, page_count=None, specific_pages=None) -> (list, list):
        """Returns a list of the search results pages for the search starting at soup.

        If all pages are to be fetched, provide the page_count argument. If only specific pages
        are to be fetched, provide their number through the specific_pages list argument"""

        # Reset the number of verdicts processed for this interval
        self.pool_progress = 0

        # Prepare the POST fields to request the result pages
        post_map = crawler_methods.generate_page_postmap(soup)

        # Prepare the argument tuple for each worker thread. If specific_pages was provided,
        # provide only those numbers
        thread_info = []
        for i in (range(1, page_count + 1) if specific_pages is None else specific_pages):
            thread_info.append((post_map.copy(), req, i, self.threads, self.timeout))

        # Fetch the result pages simultaneously
        self.log_message(crawler_methods.LogLevel.VERBOSE, "Retrieving all results pages for this interval...")
        pool = Pool(self.threads)
        pages = pool.map(crawler_methods.fetch_page, thread_info)
        pool.terminate()
        self.log_message(crawler_methods.LogLevel.VERBOSE, str(len(pages)) + " results pages retrieved")

        # Split the result list into pages that were fetched and not fetched
        failed_pages = [a for (a, b) in pages if type(a) == int and b is None]
        success_pages = [(a, b) for (a, b) in pages if type(a) == int and b is not None]

        return success_pages, failed_pages


    def handle_result_page(self, soup, start_date, end_date, page_number, specific_verdicts=None) -> (list, FaultEntity):
        """Decodes and parses the IIS VIEWSTATE hidden field, then extracts the XML search data
        and uses it to generate CourtCase object instances with the verdict's information"""
        return_list = []

        # Extract and decode the XML search results from the VIEWSTATE
        view_state = base64.b64decode(
            soup.find_all(id="__VIEWSTATE")[0]['value']
        ).decode("utf-8", "ignore")
        results_mask = re.compile(r'<Results>([\s\S]+?)</Results>')
        data_xml = results_mask.search(view_state).group(0)

        # Pass each child (search result) to CourtCase's constructor as an ElementTree object
        data_tree = ET.fromstring(data_xml)

        # If specific_verdicts was passed, filter data_tree only to the requested verdicts
        if specific_verdicts is not None:
            data_tree = [d for d in data_tree if CourtCase.extract_case_id(d) in specific_verdicts]

        # Create a list of data tree elements with the required delay on startup
        # This is used to space the requests instead of sending them all at the same time
        data_tree_numbered = []
        for i in range(len(data_tree)):
            item = {
                "index": i,
                "row": data_tree[i],
                "start_date": start_date,
                "end_date": end_date,
                "page_number": page_number
            }
            data_tree_numbered.append(item)

        # Initialize a thread pool and execute the jobs concurrently
        thread_pool = Pool(self.threads)
        callback = partial(self.print_status_line)
        tasks = [thread_pool.apply_async(self.get_case, (x, ), callback=callback) for x in data_tree_numbered]
        tasks_results = [task.get() for task in tasks]
        thread_pool.terminate()

        failed_verdicts = [v for v in tasks_results if type(v) == str]
        success_verdicts = [v for v in tasks_results if type(v) == CourtCase]

        if len(failed_verdicts) > 0:
            return success_verdicts, FaultEntity((start_date, end_date), page_number, failed_verdicts)
        else:
            return success_verdicts, None

    # Retrieves and parses the information for a single case.
    # This is the work performed by each worker threads
    def get_case(self, thread_info):
        # Unpack the parameters
        queue_pos = thread_info["index"]
        row = thread_info["row"]
        start_date = thread_info["start_date"]
        end_date = thread_info["end_date"]
        page_number = thread_info["page_number"]

        case_id = CourtCase.extract_case_id(row)

        # Use queue_pos to space the worker threads. Assuming each thread takes approximately
        # the same time, space out the first round of worker threads by using incremental
        # sleep delays. Remember that queue_pos is 0-based, so the first thread is not delayed
        if queue_pos < self.threads:
            time.sleep(queue_pos / 10.0)  # Space out threads by 100ms

        # Fetch the document text from the server create a CourtCase upon success, or add a failure
        try:
            doc_filename = CourtCase.extract_filename(row)
            doc_text = CourtCase.get_document_text(doc_filename.replace(".doc", ".txt"), self.timeout)
            extended_info = self.get_verdict_extended_info(
                CourtCase.extract_case_id(row), CourtCase.extract_filename(row))
        except Exception as e:
            self.log_message(LogLevel.ERROR, "Error fetching verdict for case " + case_id + ": " + str(e))
            return case_id

        self.pool_progress += 1
        return CourtCase(row, extended_info, doc_text)

    # Retrieves the extended information about a case by directly querying the script
    # on the old website (elyon2)
    def get_verdict_extended_info(self, case_id, filename):
        # Generate the query URL
        [sn, year] = case_id.split(r"/")  # Split the case ID into the year and serial number
        sn = sn.zfill(6)  # Pad the serial number to 6 characters
        year = "19" + year if (int(year) >= 77) else "20" + year  # Extend the year to its full format
        query_url = crawler_methods.EXTENDED_INFO_URL_PREFIX + year + "-" + sn + "-0"

        # Query the URL and retrieve the HTML into a BeautifulSoup object
        res = urllib.request.urlopen(query_url, timeout=self.timeout)
        self.log_message(LogLevel.VERBOSE, "Retrieving information for case " + case_id)

        if res.code == 200:
            soup = BeautifulSoup(res)
        else:
            raise RuntimeError("Server returned code: " + str(res.code))

        # Check if the case is confidential, if it is the information will not be available
        if self.is_case_confidential(soup):
            if self.skip_confidential:
                self.log_message(LogLevel.VERBOSE, "Verdict " + case_id + " is confidential, skipping")
            return "חסוי", [], []

        # Extract the information from the page
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

    # Prints a status line if the crawler is not running in verbose mode
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

    # Keeps track of faults and their frequency and pauses the script if a large number
    # of faults occur within a short period of time.
    # fault_scale is used to describe the gravity of the fault (a problem with a single verdict would
    # have a low scale number, a major connectivity issue would result in a high number).
    # This method is called every time a faultable method succeeds/fails. Whenever a method fails, this
    # should be called with the appropriate FaultLevel value, and whenever a method succeeds, this should
    # be called with no parameters.
    """def manage_faults(self, fault_scale=None):
        # First, adjust the fault variables according to the call
        if fault_scale is None:
            # Reduce fault_count and fault_pause_duration by 1 for every success
            if self.fault_count > 0: self.fault_count -= 1
            if self.fault_pause_duration > 1: self.fault_pause_duration -= 1
        else:
            self.fault_count += fault_scale.value

        # If the call was due to a failure and the fault variables are above the permitted threshold,
        # pause the crawler
        #if fault_scale is not None and self.fault_count >= self.FAULT_THRESHOLD:"""

    def add_failure(self, fault_entity):
        self.fault_list.append(fault_entity)
        # TODO manage the fault count correctly - self.fault_count += <Fault_Score>

    def get_retry_list(self):
        return self.fault_list

    # Handles writing log messages to the standard output and/or file
    def log_message(self, level, msg, trunc=False):
        # Set the minimum log levels based on user input
        file_min_level = LogLevel.VERBOSE if self.verbose else LogLevel.INFO
        stderr_min_level = LogLevel.ERROR if self.quiet else file_min_level

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
            ) + msg, file=(sys.stderr if level.value == LogLevel.ERROR else sys.stderr))

    # Starts the crawler between the provided start and end dates and writes the results
    # to the output database file and/or to the output log file
    def crawl(self):
        # Notify start
        self.log_message(crawler_methods.LogLevel.INFO, "ElyonCrawler started, using " + str(self.threads) + " concurrent threads")

        # Generate a list of week-long intervals to run over between the given dates
        intervals = crawler_methods.generate_interval_list(self.start, self.end)

        # For each interval, get the search page, extract the pages and get the verdicts
        it = -1
        total = len(intervals)
        for (s, e) in intervals:
            it += 1

            # Get the first search results page. Upon error, continue
            req, soup, page_count = self.perform_search(
                s.strftime("%d/%m/%Y"),
                e.strftime("%d/%m/%Y"),
                (it * 100.0 / total))

            if req is None:
                continue

            # Get all the search results pages for the interval
            success_pages, failed_pages = self.get_pages_for_search(req, soup, page_count)

            # For each failed page, add a FaultEntity to the failures list
            for fn in failed_pages:
                self.add_failure(FaultEntity((s, e), fn))

            # For each successful page, fetch the verdicts from the page and store them on the database
            for sn, sp in success_pages:
                page_soup = BeautifulSoup(sp)
                success_verdicts, failed_verdicts = self.handle_result_page(page_soup, s, e, sn)
                self.insert_results_to_db(success_verdicts)
                self.log_message(LogLevel.VERBOSE, "Writing buffer to database")

                if failed_verdicts is not None:
                    self.add_failure(failed_verdicts)

        # Print a success line to the standard output and log
        self.log_message(LogLevel.INFO, "ElyonCrawler done, exiting                 ", trunc=True)

    def retry_crawl(self, faults):
        # Starts the crawler on the list of failures generated by a previous crawl
        # Fail if the deserialization failed
        if type(faults) != list:
            self.log_message(LogLevel.ERROR, "File deserialization failed, the file is probably corrupt")
            return

        # Fail if deserialization was successful but the generated list is empty
        if len(faults) == 0:
            self.log_message(LogLevel.INFO, "The deserialized list is empty, exiting")
            return

        # Notify start
        self.log_message(crawler_methods.LogLevel.INFO, "ElyonCrawler started, retrying previously failed verdicts, "
                                                        "using " + str(self.threads) + " concurrent threads")
        for fault in faults:
            # fault can represent one of three entities: A single verdict, a page inside an interval
            # or a complete interval. Each of these needs to be handled a little differently
            start_date, end_date = fault.interval[0].strftime("%d/%m/%Y"), fault.interval[1].strftime("%d/%m/%Y")
            req, soup, page_count = self.perform_search(start_date, end_date)

            if req is None:
                continue

            if fault.page is None:  # If the entity is a complete interval, fetch all the pages
                success_pages, failed_pages = self.get_pages_for_search(req, soup, page_count)
            else:  # If the entity is smaller than a complete interval, fetch only the specific page
                success_pages, failed_pages = self.get_pages_for_search(req, soup, specific_pages=[fault.page])

            for fp in failed_pages:
                self.add_failure(FaultEntity(fault.interval, fp.page))
                continue

            if fault.verdicts is None:  # If the entity is a complete page, fetch all the verdicts
                # For each successful page, fetch the verdicts from the page and store them on the database
                for sn, sp in success_pages:
                    page_soup = BeautifulSoup(sp)
                    success_verdicts, failed_verdicts = self.handle_result_page(
                        page_soup, start_date, end_date, sn)
                    self.insert_results_to_db(success_verdicts)
                    self.log_message(LogLevel.VERBOSE, "Writing buffer to database")

                    for fv in failed_verdicts:
                        self.add_failure(fv)
            elif len(success_pages) > 0:
                page_soup = BeautifulSoup(success_pages[0][1])
                success_verdicts, failed_verdicts = self.handle_result_page(
                    page_soup, start_date, end_date, fault.page, specific_verdicts=fault.verdicts)
                self.insert_results_to_db(success_verdicts)
                self.log_message(LogLevel.VERBOSE, "Writing buffer to database")

                if failed_verdicts is not None:
                    self.add_failure(failed_verdicts)

        # Print a success line to the standard output and log
        self.log_message(LogLevel.INFO, "ElyonCrawler done, exiting                 ", trunc=True)

