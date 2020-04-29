import time
import random
import datetime
import signal

import bs4
import requests

PROXY_SOURCES = [
    "https://www.free-proxy-list.net",
    "http://www.spys.one/en/",
    "https://www.hide-my-ip.com/proxylist.shtml"
]
UA_SOURCES = [
    "https://deviceatlas.com/blog/list-of-user-agent-strings#desktop"
]
MAX_RETRIES = 15
RETRY_DELAY = 1
TIMEOUT_LENGTH = 10


class Snake:
    """The thread_count variable defines how many user-agent strings and
    IP Addresses should be pulled into your project. There are literally
    hundreds of both so go nuts but be aware of your thread limits and
    how much memory you have available. You can also read in all of
    the available user-agents and IPs by passing "all" to thread_count
    rather than an integer. Using futures is recommended here
    to help ease the management of threads and processes. Don't fret too
    much as this is an IO bound not CPU bound utility so you can use many
    more worker threads than you have CPU threads. Trust me, there is a lot
    of system waiting involved in http requests.
    """

    def __init__(self, thread_count='all', verbose=1):
        self.verbose = verbose
        self.ips = Retriever(thread_count).thread_ips
        self.log("Finished getting IPs.")
        self.uas = UserAgent(thread_count).thread_uas
        self.log("Finished getting user-agents.")
        self.masks = []
        for i, u in zip(self.ips, self.uas):
            self.masks.append({"address": i, "user-agent": u})

    def log(self, message, verbosity=1):
        """Log a message if it is not too verbose."""
        if verbosity < self.verbose:
            print(datetime.datetime.strftime(datetime.datetime.now(), "%H:%M:%S: ") + message)


class Retriever:
    def __init__(self, thread_count='all'):
        self.s = requests.Session()
        self.sources = PROXY_SOURCES
        if thread_count == 'all':
            self.thread_ips = self.clean_and_sort(self.connect_and_parse())
        else:
            self.thread_ips = random.choices(self.clean_and_sort(self.connect_and_parse()), k=thread_count)

    def __repr__(self):
        return f'<ProxyRetriever object containing {len(self.thread_ips)} addresses>'

    def connect_and_parse(self):
        website = self.sources[0]
        r = self.s.get(website)
        soup = bs4.BeautifulSoup(r.text, 'html.parser')
        proxy_table = soup.find('tbody')
        proxy_list = proxy_table.find_all('tr')
        elites = [tr for tr in proxy_list if 'elite' in tr.text]
        tds = []
        for tr in elites:
            tds.append([td.text.strip() for td in tr])
        return tds

    def clean_and_sort(self, data_set):
        """Converts IP strings to a usable format."""
        ip_list = []
        for item in data_set:
            ip_list.append(f'{item[0]}:{item[1]}')
        return ip_list


class Source:
    url = None

    def __init__(self):
        self.s = requests.Session()

    def get_markup(self, source):
        r = self.s.get(source)
        soup = bs4.BeautifulSoup(r.text, 'html.parser')
        return soup


class FreeProxyList(Source):
    url = 'https://www.free-proxy-list.net'

    def connect_and_parse(self, soup_obj):
        """Fetches a list of 'elite' proxies from Free-Proxy-List."""
        proxy_table = soup_obj.find('tbody')
        proxy_list = proxy_table.find_all('tr')
        elites = [tr for tr in proxy_list if 'elite' in tr.text]
        tds = []
        for tr in elites:
            tds.append([td.text.strip() for td in tr])
        return tds


class HideMyIp(Source):
    url = "https://www.hide-my-ip.com/proxylist.shtml"

    def get_markup(self, **kwargs):
        r = self.s.get(self.url)
        soup = bs4.BeautifulSoup(r.text, 'html.parser')
        return soup

    def connect_and_parse(self):
        """Fetches a list of proxies from HideMyIP"""
        proxy_table = self.get_markup().find('tbody')
        proxy_list = proxy_table.find_all('tr')
        proxies = [td for td in proxy_list if td.text]
        addresses = []
        for proxy in proxies:
            addresses.append(f'{proxy[0].text.strip()}:{proxy[1].text.strip()}')
        return addresses


class UserAgent:
    ua_source_url = UA_SOURCES[0]

    def __init__(self, thread_count=1):
        if thread_count == 'all':
            self.thread_uas = self.get_ua_list()
        else:
            self.thread_uas = random.choices(self.get_ua_list(), k=thread_count)

    def __repr__(self):
        return self.thread_uas

    def get_ua_list(self, source=ua_source_url):
        """Fetches a list of user-agents."""
        r = requests.get(source)
        soup = bs4.BeautifulSoup(r.content, 'html.parser')
        tables = soup.find_all('table')
        return [table.find('td').text for table in tables]


class Timeout:
    def __init__(self, seconds=TIMEOUT_LENGTH, error_message="Timeout"):
        """Sets an alarm to raise a TimeoutError if not canceled in time."""
        self.error_message = error_message
        self.canceled = False
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(seconds)

    def handle_timeout(self, ignore_1, ignore_2):
        """Raises a TimeoutError when time is up if not canceled."""
        if not self.canceled:
            raise TimeoutError(self.error_message)

    def cancel(self):
        """Prevents a TimeoutError from being raised by this timeout."""
        self.canceled = True


class Scraper:
    """A scraper has masks and opens pages. It's all automated in here to allow automatic retries when pages
    inevitably don't load the first time.
    """

    def __init__(self, thread_count, verbose=1):
        self.session = requests.Session()
        self.masks = Snake(thread_count=thread_count, verbose=verbose).masks
        self.thread_count = thread_count
        self.verbose = verbose
        self.last_soup = None

    def open_page(self, url, retries_left=MAX_RETRIES):
        """Opens the page at a given URL and returns a soup of its content."""
        while retries_left > 0:
            # if there are no masks left, get a new set of masks
            if not self.has_mask():
                self.masks = Snake(thread_count=self.thread_count).masks

            # remove the first mask in the list and get its headers and IP
            mask = self.masks[0]
            del self.masks[0]
            headers = {
                'User-Agent': mask['user-agent']
            }
            ip = mask['address']

            try:
                self.log(f"Fetching page. (URL: {url})")

                # create a timeout in case the request takes forever
                timeout = Timeout()
                self.log(f"Timeout set. (URL: {url})", 5)

                # open the page
                response = self.session.get(url, proxies={'https': ip, 'http': ip}, headers=headers)

                if response.status_code != 200:
                    # if the page doesn't load, cancel any timeouts, sleep, and retry
                    timeout.cancel()
                    if retries_left <= 0:
                        self.log(f"Done retrying. (URL: {url})", 1)
                    else:
                        self.log(f"Page load failed. (URL: {url})")
                        retries_left -= 1
                        time.sleep(RETRY_DELAY)
                else:
                    # if success, add the mask back into the list, cancel any timeouts, and return the soup
                    self.masks.append(mask)
                    timeout.cancel()
                    soup = bs4.BeautifulSoup(response.content, 'html.parser')
                    self.last_soup = soup
                    return soup
            except (TimeoutError, requests.exceptions.ProxyError, IndexError, ConnectionResetError,
                    requests.exceptions.ChunkedEncodingError) as e:
                # if the page fails to load, cancel any timeouts, sleep, and retry
                timeout.cancel()
                if retries_left <= 0:
                    self.log(f"Done retrying. (URL: {url})", 1)
                else:
                    self.log(f"Page load failed: '{e}' (URL: {url})")

                    # if it's an IndexError, get a new set of masks.
                    if isinstance(e, IndexError):
                        self.masks = Snake(thread_count=self.thread_count).masks
            retries_left -= 1
            time.sleep(RETRY_DELAY)

    def has_mask(self):
        """Returns whether any masks are available."""
        return len(self.masks) != 0

    def log(self, message, verbosity=3):
        """Log a message if it is not too verbose."""
        if verbosity < self.verbose:
            print(datetime.datetime.strftime(datetime.datetime.now(), "%H:%M:%S: ") + message)
