### IMPORTS ###


import pytest
import src.scrape_util
import src.scrape_games
import src.scrape_rosters
import time


### FUNCTIONS ###


class TestTimeout:
    def test_timeout_error(self):
        with pytest.raises(TimeoutError):
            t = src.scrape_util.Timeout(1)
            time.sleep(1)

    def test_cancel(self):
        t = src.scrape_util.Timeout(1)
        t.cancel()
        time.sleep(1)


def main():
    t = TestTimeout()
    t.test_timeout_error()
    t.test_cancel()


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
