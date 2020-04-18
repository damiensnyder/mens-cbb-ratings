import src.scrape_games as sg
from bs4 import BeautifulSoup as bs

correct_box_values = {
    1602674: {
        'pbp id': 4654374,
        'game time': "2018/11/06 19:00",
        'location': "Doug Collins Court at Redbird Arena",
        'attendance': 4764,
        'referees': ["Mike O'Neill", "Roland Simmons", "Greg Rennegarbe"],
        'raw box 1': [1853705, True, 'Carlyle, Christian', '*', '1', '33:00', '1', '3', '', '1', '3', '4', '5', '2',
                      '7', '9', '2', '4', '', '', '3', '']
    },
    1610092: {
        'pbp id': 4738602,
        'game time': "2019/01/05",
        'location': "Elmore Gymnasium",
        'attendance': 898,
        'referees': [None] * 3,
        'raw box 1': [2099975, True, 'Howell, Chris', '', '1', '38:00', '8', '20', '', '3', '1', '2', '17', '4', '10',
                      '14', '', '2', '2', '', '2']
    },
    1614866: {
        'pbp id': 4696618,
        'game time': "2019/02/04 19:00",
        'location': None,
        'attendance': 5251,
        'referees': [None] * 3,
        'raw box 1': [1968272, True, 'Cruz, Jesus', 'G', '1', '30:43', '2', '9', '', '3', '1', '1', '5', '3', '2', '5',
                      '3', '2', '', '', '3']
    }
}


# Below are functions that test the functions intended to gather raw information from BeautifulSoup
# objects representing stats.ncaa.org box score webpages.


def test_find_pbp_id(soup, correct_pbp_id):
    assert sg.find_pbp_id(soup) == correct_pbp_id


def test_find_game_time(soup, correct_game_time):
    assert sg.find_game_time(soup) == correct_game_time


def test_find_location(soup, correct_location):
    assert sg.find_location(soup) == correct_location


def test_find_attendance(soup, correct_attendance):
    assert sg.find_attendance(soup) == correct_attendance


def test_find_referees(soup, correct_referees):
    assert sg.find_referees(soup) == correct_referees


def test_find_raw_boxes(soup, correct_raw_box_1):
    given_raw_box_1 = sg.find_raw_boxes(soup)[0]
    assert given_raw_box_1 == correct_raw_box_1
    return given_raw_box_1


# Below are functions that test the functions used to clean the raw data extracted from NCAA
# webpages.


def test_clean_name():
    """Tests relevant cases of clean_name."""
    assert sg.clean_name("Last, First") == "First Last"
    assert sg.clean_name("Last, Jr., First") == "First Last, Jr."
    assert sg.clean_name("Last,First") == "First Last"
    assert sg.clean_name("Last, Jr.,First") == "First Last, Jr."


def test_clean_position():
    """Tests relevant cases of clean_position."""
    assert sg.clean_position("G") == "G"
    assert sg.clean_position("F") == "F"
    assert sg.clean_position("C") == "C"
    assert sg.clean_position("g") == "G"
    assert sg.clean_position("f") == "F"
    assert sg.clean_position("c") == "C"
    assert sg.clean_position(" G") == "G"
    assert sg.clean_position("F ") == "F"
    assert sg.clean_position("Center") == "C"
    assert sg.clean_position("*") is None
    assert sg.clean_position("") is None
    assert sg.clean_position(" ") is None


def test_clean_time():
    """Tests relevant edge cases of the function for parsing duration strings."""
    assert sg.clean_time("") == 0
    assert sg.clean_time(" ") == 0
    assert sg.clean_time(" : ") == 0
    assert sg.clean_time(" 1:01") == 61
    assert sg.clean_time("10:13") == 613
    assert sg.clean_time("1:01") == 61
    assert sg.clean_time("1:16 ") == 76
    assert sg.clean_time("0:59") == 59
    assert sg.clean_time("0:09") == 9
    assert sg.clean_time("0:00") == 0
    assert sg.clean_time("00:02") == 2
    assert sg.clean_time("--") == 0


def test_clean_stat():
    """Tests relevant edge cases of clean_stat."""
    assert sg.clean_stat("") == 0
    assert sg.clean_stat(" ") == 0
    assert sg.clean_stat("2 ") == 2
    assert sg.clean_stat("2") == 2
    assert sg.clean_stat(" 2") == 2
    assert sg.clean_stat("23") == 23


def main():
    test_clean_name()
    test_clean_position()
    test_clean_time()
    test_clean_stat()

    for box_id in correct_box_values:
        with open(f'webpages/box_{box_id}.html', 'r') as file:
            soup = bs(file, 'html.parser')
            test_find_pbp_id(soup, correct_box_values[box_id]['pbp id'])
            test_find_game_time(soup, correct_box_values[box_id]['game time'])
            test_find_location(soup, correct_box_values[box_id]['location'])
            test_find_attendance(soup, correct_box_values[box_id]['attendance'])
            test_find_referees(soup, correct_box_values[box_id]['referees'])
            test_find_raw_boxes(soup, correct_box_values[box_id]['raw box 1'])


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
