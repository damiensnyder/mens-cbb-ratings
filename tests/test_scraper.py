### IMPORTS ###


import pytest
import src.scrape_util
import src.scrape_games
import src.scrape_rosters
import time
from bs4 import BeautifulSoup as bs


### FUNCTIONS ###


class TestTimeout:
    def __init__(self):
        self.test_timeout_error()
        self.test_cancel()

    def test_timeout_error(self):
        with pytest.raises(TimeoutError):
            t = src.scrape_util.Timeout(1)
            time.sleep(1)

    def test_cancel(self):
        t = src.scrape_util.Timeout(1)
        t.cancel()
        time.sleep(1)


class TestSoupParser:
    def __init__(self):
        self.test_box_parser()

    def test_box_parser(self):
        with open('webpages/box_1602674.html', 'r') as file_1602674:
            soup_1602674 = bs(file_1602674, 'html.parser')
            game_1602674 = src.scrape_games.RawGame(None, None)
            boxes_1602674 = game_1602674.parse_box_score(soup_1602674)
            print(boxes_1602674)


def main():
    t1 = TestSoupParser()
    t2 = TestTimeout()


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
