### IMPORTS ###


import pytest
import src.scrape_util
import src.scrape_games
import src.scrape_rosters
import time
from bs4 import BeautifulSoup as bs


### FUNCTIONS ###


@pytest.mark.slow
def test_timeout_error():
    with pytest.raises(TimeoutError):
        t = src.scrape_util.Timeout(1)
        time.sleep(1)


@pytest.mark.slow
def test_cancel():
    t = src.scrape_util.Timeout(1)
    t.cancel()
    time.sleep(1)


def main():
    test_timeout_error()
    test_cancel()


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
