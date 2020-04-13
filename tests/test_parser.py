### IMPORTS ###


# import pytest
import src.scrape_games
import src.scrape_rosters
from bs4 import BeautifulSoup as bs


### CONSTANTS ###


correct_box_values = {
    1602674: {
        'pbp id': 4654374,
        'game time': "2018/11/06 19:00",
        'location': "Doug Collins Court at Redbird Arena",
        'attendance': 4764,
        'referees': ["Mike O'Neill", "Roland Simmons", "Greg Rennegarbe"]
    },
    1610092: {
        'pbp id': 4738602,
        'game time': "2019/01/05",
        'location': "Elmore Gymnasium",
        'attendance': 898,
        'referees': [None] * 3
    },
    1614866: {
        'pbp id': 4696618,
        'game time': "2019/02/04 19:00",
        'location': None,
        'attendance': 5251,
        'referees': [None] * 3
    }
}


### FUNCTIONS ###


def test_find_pbp_id(soup, correct_pbp_id):
    assert src.scrape_games.find_pbp_id(soup) == correct_pbp_id


def test_find_game_time(soup, correct_game_time):
    assert src.scrape_games.find_game_time(soup) == correct_game_time


def test_find_location(soup, correct_location):
    assert src.scrape_games.find_location(soup) == correct_location


def test_find_attendance(soup, correct_attendance):
    assert src.scrape_games.find_attendance(soup) == correct_attendance


def test_find_referees(soup, correct_referees):
    assert src.scrape_games.find_referees(soup) == correct_referees


def main():
    for box_id in correct_box_values:
        with open(f'webpages/box_{box_id}.html', 'r') as file:
            soup = bs(file, 'html.parser')
            test_find_pbp_id(soup, correct_box_values[box_id]['pbp id'])
            test_find_game_time(soup, correct_box_values[box_id]['game time'])
            test_find_location(soup, correct_box_values[box_id]['location'])
            test_find_attendance(soup, correct_box_values[box_id]['attendance'])
            test_find_referees(soup, correct_box_values[box_id]['referees'])


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
