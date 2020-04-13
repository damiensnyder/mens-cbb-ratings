### IMPORTS ###


import pytest
import src.scrape_games
import src.scrape_rosters
from bs4 import BeautifulSoup as bs


### FUNCTIONS ###


def test_find_pbp_id(soup, correct_pbp_id):
    assert src.scrape_games.find_pbp_id(soup) == correct_pbp_id


def test_find_referees(soup, correct_referees):
    assert src.scrape_games.find_referees(soup) == correct_referees


def main():
    with open('webpages/box_1602674.html', 'r') as file_1602674:
        soup_1602674 = bs(file_1602674, 'html.parser')
        test_find_pbp_id(soup_1602674, 4654374)
        test_find_referees(soup_1602674, ["Mike O'Neill", "Roland Simmons", "Greg Rennegarbe"])

    with open('webpages/box_1610092.html', 'r') as file_1602674:
        soup_1602674 = bs(file_1602674, 'html.parser')
        test_find_pbp_id(soup_1602674, 4654374)
        test_find_referees(soup_1602674, ["Mike O'Neill", "Roland Simmons", "Greg Rennegarbe"])


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()