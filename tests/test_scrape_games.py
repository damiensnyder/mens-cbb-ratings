import bs4
import json
import pymysql

import src.scrape_games as sg

PATH_BOX_HTML_VALUES = "../tests/test_cases/box_html_values.json"
PATH_SCOREBOARD_HTML_VALUES = "../tests/test_cases/scoreboard_html_values.json"
PATH_PARSED_PLAY_VALUES = "../tests/test_cases/parsed_play_values.json"


# Test cases for functions that clean stats.ncaa.org scoreboard pages.


def test_clean_scoraboard():
    """Opens the JSON file with the dates and desired outputs for the test
    cases and runs all tests for each date."""
    with open(PATH_SCOREBOARD_HTML_VALUES, 'r') as correct_scoreboard_file:
        dates = json.load(correct_scoreboard_file)
        for date in dates:
            with open(f'webpages/scoreboard_{date}.html', 'r')\
                    as scoreboard_html_file:
                soup = bs4.BeautifulSoup(scoreboard_html_file, 'html.parser')
                test_find_box_ids(soup, dates[date]['box IDs'])


def test_find_box_ids(soup, correct_box_ids):
    assert sg.find_box_ids(soup) == correct_box_ids


# Test cases for functions that clean stats.ncaa.org box score pages.


def test_clean_box_score():
    """Opens the JSON file with the box IDs and desired outputs for the test
    cases and runs all tests for each box ID."""
    with open(PATH_BOX_HTML_VALUES, 'r') as correct_box_file:
        box_ids = json.load(correct_box_file)
        for box_id in box_ids:
            with open(f'webpages/box_{box_id}.html', 'r') as box_html_file:
                soup = bs4.BeautifulSoup(box_html_file, 'html.parser')
                test_find_pbp_id(soup, box_ids[box_id]['pbp ID'])
                test_find_game_time(soup, box_ids[box_id]['game time'])
                test_find_location(soup, box_ids[box_id]['location'])
                test_find_attendance(soup, box_ids[box_id]['attendance'])
                test_find_referees(soup, box_ids[box_id]['referees'])
                test_find_team_ids(soup, box_ids[box_id]['team IDs'])
                test_find_names_and_exhibition(soup,
                                               box_ids[box_id]['team names'])
                test_find_raw_boxes(soup, box_ids[box_id]['raw box 1'])


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


def test_find_team_ids(soup, correct_team_ids):
    assert list(sg.find_team_ids(soup)) == correct_team_ids


def test_find_names_and_exhibition(soup, correct_team_names):
    assert list(sg.find_names_and_exhibition(soup)) == correct_team_names


def test_find_raw_boxes(soup, correct_raw_box_1):
    given_raw_box_1 = sg.find_raw_boxes(soup)[0]
    assert given_raw_box_1 == correct_raw_box_1
    return given_raw_box_1


# Test cases for functions that interact with the database.


def test_db_interaction():
    """Runs all tests of database interaction."""
    conn = test_connect_to_db()
    cursor = conn.cursor()
    test_fetch_division_code(cursor)


def test_connect_to_db():
    conn = sg.connect_to_db()
    assert isinstance(conn, pymysql.connections.Connection)
    return conn


def test_fetch_division_code(cursor):
    assert sg.fetch_division_code(cursor, 2019) == 16700


# Test cases for functions that to clean the raw data extracted from
# stats.ncaa.org box score pages.


def test_clean_raw_box_data():
    """Runs all tests of functions that clean raw box score data."""
    test_clean_name()
    test_clean_position()
    test_clean_time()
    test_clean_stat()
    test_score_name_similarity()


def test_clean_name():
    """Tests relevant cases of clean_name."""
    assert sg.clean_name("Last, First") == "First Last"
    assert sg.clean_name("Last, Jr., First") == "First Last, Jr."
    assert sg.clean_name("Last,First") == "First Last"
    assert sg.clean_name("Last, Jr.,First") == "First Last, Jr."
    assert sg.clean_name("D, R") == "R D"
    assert sg.clean_name("LAST,FIRST") == "First Last"
    assert sg.clean_name("LAST, FIRST") == "First Last"
    assert sg.clean_name("LAST,CJ") == "CJ Last"
    assert sg.clean_name("Last, First \"Nickname\"") == "First Last"


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


def test_score_name_similarity():
    assert sg.score_name_similarity("F L", "First Last") == 8
    assert sg.score_name_similarity("First Last", "F L") == 8
    assert sg.score_name_similarity("First Last", "F D") == -7
    assert sg.score_name_similarity("First Last", "Firs Last") == 9
    assert sg.score_name_similarity("First Last", "First Last") == 16
    assert sg.score_name_similarity("First Last, Jr", "First Last, Jr.") == 24
    assert sg.score_name_similarity("First Last, Jr", "First... Last, Jr.") == 24
    assert sg.score_name_similarity("Firs Last", "First Last") == 9
    assert sg.score_name_similarity("first last", "First Last") == 16
    assert sg.score_name_similarity("first LAST", "First Last") == 16
    assert sg.score_name_similarity("First LAST", "FIrst LaSt") == 16
    assert sg.score_name_similarity("Last First", "First Last") == 12
    assert sg.score_name_similarity("Last Firs", "Firs Last") == 8
    assert sg.score_name_similarity("Longerfirst Last", "Longerirst Last") == 21
    assert sg.score_name_similarity("Longerfirst Last", "Longerfirst Last") == 28


# Test cases for functions that parse plays from stats.ncaa.org play-by-play
# pages.


def test_clean_raw_play_data():
    """Runs all tests of functions for parsing play-by-play data."""
    test_clean_centi_time()
    test_parse_play()


def test_clean_centi_time():
    """Tests relevant cases of clean_centi_time."""
    assert sg.clean_centi_time('11:12') == 672
    assert sg.clean_centi_time('01:12') == 72
    assert sg.clean_centi_time('01:02') == 62
    assert sg.clean_centi_time('01:12:63') == 72.63
    assert sg.clean_centi_time('00:12:63') == 12.63
    assert sg.clean_centi_time('00:02:63') == 2.63
    assert sg.clean_centi_time('00:02:03') == 2.03


def test_parse_play():
    """Test that the outputs of the parse_play function match the outputs
    specified in parsed_play_values.json."""
    with open(PATH_PARSED_PLAY_VALUES, 'r') as correct_values_file:
        plays = json.load(correct_values_file)
        for play in plays:
            try:
                assert sg.parse_play(play) == plays[play]
            except AssertionError:
                print(f"Assertion error in parsing play. Play: '{play}'")
                print("Correct value: " + str(plays[play]))
                print("Received value: " + str(sg.parse_play(play)))


# Main method. Runs all functions that run tests.


def main():
    test_clean_scoraboard()
    test_clean_box_score()
    test_db_interaction()
    test_clean_raw_box_data()
    test_clean_raw_play_data()


if __name__ == '__main__':
    main()
