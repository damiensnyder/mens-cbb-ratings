import time
import datetime
import re

import pymysql

import src.scrape_util

CRAWL_DELAY = 1
VERBOSE = 3
MAX_RETRIES = 15
DEFAULT_THREAD_COUNT = 25

YEAR_DIVISIONS = [
    {'year': 2011, 'code': 10220},
    {'year': 2012, 'code': 10480},
    {'year': 2013, 'code': 10883},
    {'year': 2014, 'code': 11700},
    {'year': 2015, 'code': 12320},
    {'year': 2016, 'code': 12700},
    {'year': 2017, 'code': 13100},
    {'year': 2018, 'code': 13533},
    {'year': 2019, 'code': 16700},
    {'year': 2020, 'code': 17060}
]
CLOCK_RESETTING_ACTIONS = ["jump ball", "possession arrow", "shot", "turnover", "steal",
                           "foul committed", "free throw"]

UPLOAD_GAME_QUERY = "INSERT INTO games (game_id, h_team_season_id, a_team_season_id, h_name," \
                    "a_name, start_time, location, attendance, referee1, referee2, referee3," \
                    "is_exhibition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
NULLABLE_BOX_FIELDS = ["player ID", "name", "is away", "position", "time played", "FGM", "FGA",
                       "3PM", "3PA", "FTM", "FTA", "ORB", "DRB", "AST", "TOV", "STL", "BLK", "PF"]
UPLOAD_BOX_QUERY = "INSERT INTO boxes (game_id, box_in_game, player_id, player_name, is_away," \
                   "position, time_played, fgm, fga, 3pm, 3pa, ftm, fta, orb, drb, ast, tov, stl" \
                   "blk, pf) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                   "%s, %s, %s, %s, %s);"
NULLABLE_PLAY_FIELDS = ["period", "time", "shot clock", "home score", "away score", "is away",
                        "action", "flag 1", "flag 2", "flag 3", "flag 4", "flag 5", "flag 6"]
UPLOAD_PLAY_QUERY = ("INSERT INTO plays (game_id, play_in_game, period,"
                     "time_remaining, shot_clock, h_score, a_score,"
                     "agent_is_away, action, flag1, flag2, flag3, flag4,"
                     "flag5, flag6, agent_id, agent_name, h_p1_id, h_p1_name,"
                     "h_p2_id, h_p2_name, h_p3_id, h_p3_name, h_p4_id,"
                     "h_p4_name, h_p5_id, h_p5_name, a_p1_id, a_p1_name,"
                     "a_p2_id, a_p2_name, a_p3_id, a_p3_name, a_p4_id,"
                     "a_p4_name, a_p5_id, a_p5_name) VALUES (%s, %s, %s, %s,"
                     "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                     "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                     "%s, %s, %s, %s, %s, %s")
FETCH_TEAM_SEASON_ID_QUERY = ("SELECT (team_season_id) FROM team_seasons WHERE"
                              "school_id = %s AND season_year = %s")
FETCH_DIVISION_CODE_QUERY = "SELECT division_code FROM seasons WHERE year = %s"
FETCH_ROSTER_QUERY = "SELECT (player_id, player_name) FROM player_seasons WHERE team_season_id =" \
                     "%s"


# Below are functions for scraping game information from stats.ncaa.org.


def scrape_range(start_year, start_month, start_day, end_year, end_month,
                 end_day):
    """Scrape each game in the given date range and upload the results to the
    database.

    Args:
        start_year: The year of the first date of games to scrape, inclusive.
        start_month: The month of the first date of games to scrape, inclusive.
        start_day: The day of the first date of games to scrape, inclusive.
        end_year: The year of the last date of games to scrape, exclusive.
        end_month: The month of the last date of games to scrape, exclusive.
        end_day: The day of the last date of games to scrape, exclusive."""
    scraper = src.scrape_util.Scraper(thread_count=DEFAULT_THREAD_COUNT,
                                      verbose=VERBOSE)
    conn = connect_to_db()
    cursor = conn.cursor()

    # make the dates into datetime objects
    start_date = datetime.datetime(start_year, start_month, start_day)
    end_date = datetime.datetime(end_year, end_month, end_day)

    # get the season code
    season = start_year
    if start_month > 6:
        season += 1
    season_code = [division['code'] for division in YEAR_DIVISIONS
                   if division['year'] == season][0]

    # iterate through each day in the date range
    while start_date < end_date:
        # get the current day and increment
        year = start_date.year
        month = start_date.month
        day = start_date.day
        start_date += datetime.timedelta(1)

        # scrape all games from that day
        scrape_day(scraper, cursor, year, month, day, season, season_code)
        conn.commit()

    scraper.log("Finished scraping all days in range.", 0)


def scrape_day(scraper, cursor, year, month, day, season, season_code):
    """Scrapes all games on the given date.

    Args:
        scraper: The src.scrape_util.Scraper object used to scrape webpages.
        cursor: The pymysql cursor of the database connection.
        year: The year of the date of games to scrape.
        month: The month of the date of games to scrape.
        day: The day of the date of games to scrape.
        season: The year of the season that the date is in.
        season_code: The stats.ncaa.org code of the season."""
    scraper.log(f"Started parsing day. (Date: {month}/{day}/{year})", 0)
    box_ids = scrape_box_ids(scraper, year, month, day, season_code)
    for box_id in box_ids:
        scrape_game(scraper, cursor, season, box_id)
    time.sleep(CRAWL_DELAY)


def scrape_box_ids(scraper, year, month, day, season_code):
    """Gets all box score IDs from games on the given date.

    Args:
        scraper: The src.scrape_util.Scraper object used to scrape webpages.
        year: The year of the date of games to scrape.
        month: The month of the date of games to scrape.
        day: The day of the date of games to scrape.
        season_code: The stats.ncaa.org code of the season."""
    retries_left = MAX_RETRIES
    while retries_left > 0:
        # open the page
        url = f"http://stats.ncaa.org/season_divisions/{season_code}/scoreboards?" \
              f"game_date={month}%2F{day}%2F{year}"
        soup = scraper.open_page(url=url)

        # inexplicably, sometimes the soup will be set in the scraper but return None anyway
        if soup is None:
            scraper.log(f"Soup did not return. (URL: {url})")
            soup = scraper.last_soup

        try:
            return find_box_ids(soup)
        except AttributeError as e:
            scraper.log(f"Error parsing day: '{e}' (Date: {month}/{day}/{year})")
            retries_left -= 1
            if retries_left <= 0:
                scraper.log(f"Done retrying. (Date: {month}/{day}/{year})", 0)
                return None
        time.sleep(CRAWL_DELAY)


def scrape_game(scraper, cursor, season, box_id, by_pbp=False):
    """Gets and uploads all information from the game at the given box ID.

    Args:
        scraper: The src.scrape_util.Scraper object used to scrape webpages.
        cursor: The pymysql cursor of the database connection.
        season: The year of the season in which the game was played.
        box_id: The box ID of the game (or PBP ID, if by_pbp is True
        by_pbp: True if the game is being scraped by PBP ID instead of box
            ID."""
    box_soup = scrape_box_score(scraper, box_id, by_pbp=by_pbp)
    if box_soup is not None:
        pbp_id = find_pbp_id(box_soup)
        game_time = find_game_time(box_soup)
        location = find_location(box_soup)
        attendance = find_attendance(box_soup)
        referees = find_referees(box_soup)
        h_team_season_id, a_team_season_id, h_school_id, a_school_id = find_team_ids(box_soup)
        h_name, a_name, is_exhibition = find_names_and_exhibition(box_soup)
        h_roster = fetch_roster(cursor, h_team_season_id, h_school_id, season)
        a_roster = fetch_roster(cursor, a_team_season_id, a_school_id, season)
        upload_game(cursor, pbp_id, h_team_season_id, a_team_season_id, h_name, a_name, game_time,
                    location, attendance, referees, is_exhibition)

        raw_boxes = find_raw_boxes(box_soup)
        boxes = clean_raw_boxes(raw_boxes, h_roster, a_roster)
        upload_boxes(cursor, boxes)

        pbp_soup = scrape_plays(scraper, pbp_id)
        if pbp_soup is not None:
            raw_plays = find_raw_plays(pbp_soup, pbp_id)
            plays = parse_all_plays(raw_plays)
            track_shot_clock(plays)
            track_partic(plays)
            correct_minutes(boxes, plays)
            upload_plays(cursor, pbp_id, plays)


def scrape_box_score(scraper, box_id, by_pbp=False):
    """Gets box score information for the game.

    Args:
        scraper: The src.scrape_util.Scraper object used to scrape webpages.
        box_id: The NCAA box ID of the game (or PBP ID, if by_pbp is True)
        by_pbp: True if the game is identified by PBP ID instead of box ID.

    Returns:
        None if a viable soup could not be found; if a soup could be found,
        returns a bs4.BeautifulSoup object of the box score webpage."""
    retries_left = MAX_RETRIES
    while retries_left > 0:
        # open the page
        if by_pbp:
            url = f"http://stats.ncaa.org/game/box_score/{box_id}"
        else:
            url = f"http://stats.ncaa.org/contests/{box_id}/box_score"
        soup = scraper.open_page(url=url)

        # inexplicably, sometimes the soup will not return anything despite existing
        if soup is None:
            scraper.log(f"Soup did not return. Box ID: {box_id}", 4)
            soup = scraper.last_soup

        try:
            find_raw_boxes(soup)    # janky bellwether for whether the soup is usable
            return soup
        except AttributeError as e:
            scraper.log(f"Error parsing box score: '{e}' (Box ID: {box_id})")
            if retries_left <= 0:
                scraper.log(f"Done retrying. (Box ID: {box_id})", 1)
                return None
        retries_left -= 1
        time.sleep(CRAWL_DELAY)


def scrape_plays(scraper, pbp_id):
    """Gets all plays from the game with the given PBP ID.

    Args:
        scraper: The src.scrape_util.Scraper object used to scrape webpages.
        pbp_id: The NCAA PBP ID of the game.

    Returns:
        None if a viable soup could not be found; if a soup could be found,
        returns a bs4.BeautifulSoup object of the play-by-play webpage."""
    retries_left = MAX_RETRIES
    while retries_left > 0:
        # open the page
        url = f"http://stats.ncaa.org/game/play_by_play/{pbp_id}"
        soup = scraper.open_page(url=url)

        # inexplicably, sometimes the soup will be set in the scraper but return None anyway
        if soup is None:
            scraper.log(f"Soup did not return. PBP ID: {pbp_id}")
            soup = scraper.last_soup

        try:
            find_raw_plays(soup, pbp_id)  # janky bellwether for whether the soup is usable
            return soup
        except AttributeError as e:
            scraper.log(f"Error parsing play-by-play: '{e}' (PBP ID: {pbp_id})")
            retries_left -= 1
            if retries_left <= 0:
                scraper.log(f"Done retrying. (PBP ID: {pbp_id})", 1)
                return None
        time.sleep(CRAWL_DELAY)


# Below are functions dedicated to extracting information from BeautifulSoup
# representations of box score webpages scraped from stats.ncaa.org. These
# functions do little or no pre-processing of the values extracted.


def find_box_ids(soup):
    """Given a scoreboard page, find the box IDs of every game played on that day.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org scoreboard
            webpage.

    Returns:
        A list of the box IDs of all games listed on the scoreboard."""
    box_ids = []
    el_table = soup.find('table', attrs={'style': 'border-collapse: collapse'})
    for el_box_cell in el_table.find_all('tr', attrs={'style': 'border-bottom: 1px solid #cccccc'}):
        el_link = el_box_cell.find('a', class_='skipMask')

        # some games do not have box score links, this prevents those from breaking everything
        if el_link is not None:
            box_ids.append(int(el_link.attrs['href'][10:-10]))

    return box_ids


def find_pbp_id(soup):
    """Given a box score page, find the PBP ID of the game.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        The PBP ID of the game."""
    el_pbp = soup.find('ul', class_='level1').find_all('li')[-5].find('a')
    return int(el_pbp.attrs['href'][-7:])


def find_game_time(soup):
    """Given a box score page, find the start time of the game.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        The start time of the game as a string in ISO-8601 format. If the time
        is not listed, returns only the date, in ISO-8601 format."""
    el_metadata = soup.find_all('table',
                                attrs={'width': '50%', 'align': 'center'})[2]
    el_game_date = el_metadata.find('tr').find_all('td')[1]
    raw_date_text = el_game_date.get_text().strip()

    # Some dates list TBA instead of a specific time. Does not return a time
    # for those dates.
    if 'M' in raw_date_text:
        as_date = datetime.datetime.strptime(raw_date_text, '%m/%d/%Y %I:%M %p')
        return as_date.strftime('%Y/%m/%d %H:%M')
    else:
        index_space = raw_date_text.index(' ')
        as_date = datetime.datetime.strptime(raw_date_text[:index_space], '%m/%d/%Y')
        return as_date.strftime('%Y/%m/%d')


def find_location(soup):
    """Given a box score page, find the location of the game, if it is listed.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        The location of the game, if one is listed. Otherwise, returns None."""
    el_metadata = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]

    # location is not listed for all games
    if 'Location:' in el_metadata.get_text():
        el_location = el_metadata.find_all('tr')[1].find_all('td')[1]
        return el_location.get_text().strip()
    else:
        return None


def find_attendance(soup):
    """Given a box score page, find the attendance of the game.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        The attendance of the game."""
    el_metadata = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]

    # location appears above attendance in games where it is listed, but it is not listed for all
    # games
    if 'Location:' in el_metadata.get_text():
        el_attendance = el_metadata.find_all('tr')[2].find_all('td')[1]
    else:
        el_attendance = el_metadata.find_all('tr')[1].find_all('td')[1]
    return int(el_attendance.get_text().strip().replace(',', ''))


def find_referees(soup):
    """Given a box score page, find the referees if they are listed.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        A list of the names of the referees of the game. If none were listed,
        returns a list of three None values."""
    el_referees = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[3].find_all('td')[1]
    referees_text = el_referees.get_text().strip()
    if referees_text.count("\n") == 4:
        index_first_newline = referees_text.find("\n")
        index_last_newline = referees_text.rfind("\n")
        referee1 = referees_text[:index_first_newline].strip()
        referee2 = referees_text[index_first_newline:index_last_newline].strip()
        referee3 = referees_text[index_last_newline:].strip()
        return [referee1, referee2, referee3]
    else:
        return [None] * 3


def find_team_ids(soup):
    """Given a soup of the box score of a game, find the team season IDs or
    school IDs of each team.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        A tuple of the team season IDs or school IDs of each team. Only one
        of them will be found for a team (or none if the team is not NCAA), and
        the other will be set to None. Values will be returned in the order
        (home team season ID, home school ID, away team season ID,
        away school ID)."""
    el_table = soup.find('table', class_='mytable')
    el_h_link = el_table.find_all('tr')[2].find('a')
    el_a_link = el_table.find_all('tr')[1].find('a')
    h_team_season_id = None
    h_school_id = None
    a_team_season_id = None
    a_school_id = None
    if el_h_link is not None:
        h_url = el_h_link.attrs['href']
        if "teams" in h_url:
            h_team_season_id = int(h_url[h_url.rfind("/") + 1:])
        else:
            h_school_id = int(h_url[6:h_url.rfind("/")])
    if el_a_link is not None:
        a_url = el_a_link.attrs['href']
        if "teams" in a_url:
            a_team_season_id = int(a_url[a_url.rfind("/") + 1:])
        else:
            a_school_id = int(a_url[6:a_url.rfind("/")])
    return h_team_season_id, a_team_season_id, h_school_id, a_school_id


def find_names_and_exhibition(soup):
    """Given a soup of the box score of a game, find the names of each team and
    whether the game was an exhibition.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        A tuple containing the names of each team and whether the game was an
        exhibition, in the order (home team name, away team name,
        is exhibition)."""
    is_exhibition = False
    el_headings = soup.find_all('tr', class_='heading')
    h_name = el_headings[1].get_text()
    a_name = el_headings[0].get_text()

    # exhibition games have a telltale string starting with "<i>" after the team's name. remove
    # that if present and note that it is an exhibition game
    h_index_italics = h_name.find(" <i>")
    if h_index_italics >= 0:
        h_name = h_name[:h_index_italics]
        is_exhibition = True
    a_index_italics = a_name.find(" <i>")
    if a_index_italics >= 0:
        a_name = a_name[:a_index_italics]
        is_exhibition = True
    return h_name.strip(), a_name.strip(), is_exhibition


def find_raw_boxes(soup):
    """Given a box score page, find the box scores.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org box score webpage.

    Returns:
        The box score information of the game as a list of lists. Each sub-list
        represents one player's stats, in the format:
        [NCAA player id (as int),
        True if away or False if home,
        name as written, usually in the format 'Last, First',
        position (e.g. 'G') -- sometimes not listed,
        games played (nearly always '1' -- can safely be discarded),
        duration played in 'M:ss' format (e.g. '6:04'),
        'FGM', 'FGA', '3PM', '3PA', 'FTM', 'FTA', 'PTS', 'ORB', 'DRB', 'TRB',
        'AST', 'TOV', 'STL', 'BLK', 'PF', 'DQ'] (each stat as a string)"""
    is_away = True
    boxes = []

    # each team's box scores are in a different table, so use that to infer which team each player
    # is on
    for el_team in soup.find_all('table', class_='mytable')[-2:]:
        for el_box in el_team.find_all('tr', class_='smtext'):
            el_player_id = el_box.find('a')
            if el_player_id is None:
                player_id = None
            else:
                player_id = int(el_player_id.attrs['href'][-7:])
            box = [player_id, is_away]

            for el_stat in el_box.find_all('td'):
                box.append(el_stat.get_text().strip())

            boxes.append(box)

        is_away = False  # the first table of boxes is away, so set the next one to home

    return boxes


def find_raw_plays(soup):
    """Given a play-by-play page, find the plays in the game.

    Args:
        soup: A bs4.BeautifulSoup object of a stats.ncaa.org play-by-play
            webpage.

    Returns:
        The plays of the game as a list of lists, each sub-list representing
        one play in the format:
        [period (first half = 0, second half = 1, 1st overtime = 2,
            2nd overtime = 3, etc),
        time remaining in the format MM:SS:cc, MM:SS, or M:SS,
        away team play,
        score formatted like "46-41" with away team first,
        home team play]"""
    plays = []
    period = 0
    for el_table in soup.find_all('table', class_='mytable')[1:]:
        for el_tr in el_table.find_all('tr'):
            play_row = [period]
            for el_td in el_tr.find_all('td'):
                play_row.append(el_td.get_text().strip())
            while len(play_row) < 5:
                play_row.append("")
            plays.append(play_row)
        period += 1
    return plays


# Below are functions dedicated to cleaning values found by the raw box score parsing functions,
# specifically parsing the actual box scores themselves and not the game metadata.


def clean_raw_boxes(raw_boxes, home_roster, away_roster):
    """Given raw box scores and the rosters of the two teams playing, create pre-processed box
    scores as dicts."""
    boxes = []
    for raw_box in raw_boxes:
        if raw_box[1]:  # select roster based on whether player is home or away
            boxes.append(clean_single_box(raw_box, away_roster))
        else:
            boxes.append(clean_single_box(raw_box, home_roster))
    return boxes


def clean_single_box(raw_box, roster):
    """Take a single raw stat line in a box score and transform it into a dict with usably clean
    values."""
    box = {
        'is away': raw_box[1]
    }

    # since teams do not have player IDs, positions, or playtime, collect those only for actual
    # players and merely record that the team stats come from "Team"
    if raw_box[2].strip().lower() == "team":
        box['name'] = "Team"
    else:
        player = identify_player(raw_box[0], clean_name(raw_box[2]), roster)
        box['player ID'] = player['player ID']
        box['name'] = player['name']
        position = clean_position(raw_box[3])
        if position is not None:
            raw_box['position'] = position
        box['time played'] = clean_time(raw_box[5])

    box['FGM'] = clean_stat(raw_box[6])
    box['FGA'] = clean_stat(raw_box[7])
    box['3PM'] = clean_stat(raw_box[8])
    box['3PA'] = clean_stat(raw_box[9])
    box['FTM'] = clean_stat(raw_box[10])
    box['FTA'] = clean_stat(raw_box[11])
    box['ORB'] = clean_stat(raw_box[13])
    box['DRB'] = clean_stat(raw_box[14])
    box['AST'] = clean_stat(raw_box[16])
    box['TOV'] = clean_stat(raw_box[17])
    box['STL'] = clean_stat(raw_box[18])
    box['BLK'] = clean_stat(raw_box[19])
    box['PF'] = clean_stat(raw_box[20])

    return raw_box


def clean_name(name):
    """Converts a raw player name into a clean 'First Last' format.

    Rearranges the name string such that the first name comes first, followed
    by the last name, and then any suffixes. Also removes any double spaces,
    makes all caps names into title case, and removes nicknames in quotes.

    Args:
        name: A name as written in a stats.ncaa.org box score or play-by-play
         log, usually in the format 'Last, First' (but not consistent).

    Returns:
        A clean name in the format 'First Last'."""
    name = re.sub('[^\x00-\x7f]', '', name)     # remove any non-ASCII characters
    while "  " in name:                         # remove double spaces
        name = name.replace("  ", " ")

    # rearrange the name around the comma so the first name comes first
    index_last_comma = name.rfind(',')
    if (index_last_comma > 0) and (index_last_comma < len(name) - 1):
        if name[index_last_comma + 1] == " ":
            name = name[index_last_comma + 2:] + " " + name[:index_last_comma]
        else:
            name = name[index_last_comma + 1:] + " " + name[:index_last_comma]

    # remove any all caps words 3 letters or longer
    all_caps_segments = re.findall("[A-Z]{3,}", name)
    for segment in all_caps_segments:
        name = name.replace(segment, segment.title())

    # remove any nicknames in quotes
    name = re.sub(" \".+\" ", " ", name)

    return name


def identify_player(player_id, name, roster):
    """Given the name and player ID of a player, and the roster of the team they play on, identify
    the player. Look for an exact match, otherwise choose the player with the most similar name. If
    no matching or even similar player can be found, return a player with the given name and a
    player ID of None.

    Args:
        player_id: The NCAA player ID of the player to be matched.
        name: The name of the player to be matched.
        roster: A list of dicts containing the player ID and name of each
            player on the team of the player to be identified.

    Returns:
        If player with an exactly matching player ID or name could be found,
        returns that player's dict with keys 'player ID' and 'name'. If there
        is no exact match, returns the most similar player. If no likely
        matches are found, returns a dict with the player's name unchanged and
        a player ID of None."""
    highest_similarity = 3  # discard any matches with a similarity less than 3
    most_similar = {
        'player ID': None,
        'name': name
    }

    # if there is an exactly matching name or player ID, return that player, or otherwise
    # choose the player with the most similar name
    for player in roster:
        if (player_id == player['player_id']) or (name == player['name']):
            most_similar = player
            break
        else:
            similarity = score_name_similarity(name, player[1])
            if similarity > highest_similarity:
                highest_similarity = similarity
                most_similar = player

    return most_similar


def clean_position(raw_position):
    """Given a string (or other value) possibly representing a player's position, return 'G' if
    they are a guard, 'F' if they are a forward, 'C' if they are a center, and None otherwise."""
    if isinstance(raw_position, str):
        raw_position = raw_position.lower()
        if "g" in raw_position:
            return "G"
        elif "f" in raw_position:
            return "F"
        elif "c" in raw_position:
            return "C"

    return None


def clean_time(raw_time):
    """Converts a string representing a duration in minutes and seconds to the number of total
    seconds. For example, clean_time('1:42') would return 102. If the value passed in is not a
    string or is not in M:SS format, returns 0."""
    if isinstance(raw_time, str):
        index_first_colon = raw_time.find(':')
        if index_first_colon > 0:
            try:
                str_minutes = raw_time[:index_first_colon]
                str_seconds = raw_time[index_first_colon + 1:]
                return 60 * int(str_minutes) + int(str_seconds)
            except ValueError:
                return 0  # this is dangerous, might change later

    return 0


def clean_stat(raw_stat):
    """Convert the given stat from str to int. If an int value cannot be determined, return 0
    instead."""
    try:
        return int(raw_stat)
    except ValueError:
        return 0


def score_name_similarity(name1, name2):
    """Evaluate the similarity of two names. Similarity is measured as twice number of matching
    3-character sequences (ignoring punctuation and case) minus absolute difference in length."""
    name1 = name1.lower().replace('.', '')
    name2 = name2.lower().replace('.', '')

    similarity = -abs(len(name1) - len(name2))
    for i in range(len(name1) - 2):
        if name1[i:i + 3] in name2:
            similarity += 2

    # if either name is only initials, assign a score of 8 if the names have the same initials
    if (len(name1) == 3) or (len(name2) == 3):
        index_space1 = name1.index(" ")
        index_space2 = name2.index(" ")
        if (name1[0] == name2[0]) and (name1[index_space1 + 1] == name2[index_space2 + 1]):
            return 8

    return similarity


# Below are functions for interacting with the database.


def connect_to_db():
    """Open and return a connection to the database as specified in a txt file."""
    with open('../src/db_info.txt', 'r') as db_info_file:
        host = db_info_file.readline()[:-1]
        user = db_info_file.readline()[:-1]
        password = db_info_file.readline()[:-1]
        db = db_info_file.readline()
    return pymysql.connect(host, user, password, db)


def fetch_division_code(cursor, year):
    """Get the player ID and name of each player on the team with the given team season ID or the
    given school ID and year."""
    cursor.execute(FETCH_DIVISION_CODE_QUERY, (year,))
    return cursor.fetchone()[0]


def fetch_roster(cursor, team_season_id, school_id, year):
    """Get the player ID and name of each player on the team with the given team season ID or the
    given school ID and year."""
    if team_season_id is None:
        if school_id is None:
            return []   # return an empty list if the team is has no ID of either type
        else:
            cursor.execute(FETCH_TEAM_SEASON_ID_QUERY, (school_id, year))
            team_season_id = cursor.fetchone()['team_season_id']
    cursor.execute(FETCH_ROSTER_QUERY, (team_season_id,))
    raw_roster = cursor.fetchall()
    return [{'player ID': player[0], 'name': player[1]} for player in raw_roster]


def upload_game(cursor, game_id, h_team_season_id, a_team_season_id, h_name,
                a_name, start_time, location, attendance, referees,
                is_exhibition):
    """Uploads the given game metadata to the database.

    Args:
        cursor: The pymysql cursor object of the database connection.
        game_id: The PBP ID of the game.
        h_team_season_id: The home team's team season ID.
        a_team_season_id: The away team's team season ID.
        h_name: The home team's name.
        a_name: The away team's name.
        start_time: The start time of the game, as a string in ISO-8601 format.
        location: The location of the game.
        attendance: The attendance of the game.
        referees: A list of the referees of the game.
        is_exhibition: Whether the game was an exhibition."""
    if game_id is None:
        raise ValueError('Game ID not found.')
    if h_name is None:
        raise ValueError('Home team name not found.')
    if a_name is None:
        raise ValueError('Away team name not found.')
    if len(referees) != 3:
        raise ValueError(f'Expected 3 referees. Received {len(referees)} instead.')
    game_tuple = (game_id, h_team_season_id, a_team_season_id, h_name, a_name,
                  start_time, location, attendance, referees[0], referees[1],
                  referees[2], is_exhibition)
    cursor.execute(UPLOAD_GAME_QUERY, game_tuple)


def upload_boxes(cursor, boxes):
    """Uploads the given box scores to the database.

    Args:
        cursor: The pymysql cursor object of the database connection.
        boxes: The boxes in the game as a list of dicts."""
    i = 0   # tracks which box it is in the game
    for box in boxes:
        if 'PBP id' not in box:
            raise KeyError('Game ID not found.')
        box_tuple = (box['PBP id'], i)
        for field in NULLABLE_BOX_FIELDS:
            if field not in box:
                box[field] = None   # replace nullable fields with None
            box_tuple += (box[field],)

        cursor.execute(UPLOAD_BOX_QUERY, box_tuple)
        i += 1


def upload_plays(cursor, game_id, plays):
    """Uploads the given plays to the database.

    Args:
        cursor: The pymysql cursor object of the database connection.
        game_id: The PBP ID of the game.
        plays: The plays in the game as a list of dicts."""
    i = 0   # tracks which play it is in the game
    for play in plays:
        play_tuple = (game_id, i)
        for field in NULLABLE_PLAY_FIELDS:
            if field not in play:
                play[field] = None
            play_tuple += (play[field],)

        play_tuple += (play['agent']['player ID'], play['agent']['name'])
        for player in play['h partic']:
            play_tuple += (player['player ID'], player['name'])
        for player in play['a partic']:
            play_tuple += (player['player ID'], player['name'])

        cursor.execute(UPLOAD_BOX_QUERY, play_tuple)
        i += 1


# Below are functions for parsing a play from the play-by-play logs.


def parse_all_plays(raw_plays):
    """Parses all plays in the game that can be parsed.

    Args:
        raw_plays: A list of the raw play rows of the paly-by-play log.

    Returns:
        All the plays that could be parsed, as a list of dicts of parsed
        plays."""
    plays = []
    for play_row in raw_plays:
        try:
            play = parse_play_row(play_row)
            if play is not None:
                plays.append(play)
        except ValueError:
            pass
    return plays


def parse_play_row(play_row):
    """Put all the information in the row into a dict that contains parsed play information."""
    play_1 = play_row[2]
    play_2 = play_row[4]
    score = play_row[3]

    # if there was an actual play:
    if (score != "Score") and ((len(play_1) > 0) or (len(play_2) > 0)):
        # check which team did the play
        if len(play_1) > 0:
            parsed_play = parse_play(play_1)
            is_away = True
        else:
            parsed_play = parse_play(play_2)
            is_away = False

        if parsed_play is None:
            return None

        scores = clean_score(score)
        parsed_play['home score'] = scores[1]
        parsed_play['away score'] = scores[0]
        parsed_play['time'] = clean_centi_time(play_row[1])

        # get other information from the row
        parsed_play['game ID'] = play_row[0]
        parsed_play['period'] = int(play_row[0])
        parsed_play['is away'] = is_away

        return parsed_play


def parse_play(play):
    """Parse the text of a play and return the information about it.

    Args:
        play: The text of the play, as written."""
    notation_info = get_notation_style(play)
    if notation_info is None:
        return None
    elif notation_info['is caps']:
        return parse_caps_play(notation_info['play'], notation_info['player'])
    else:
        return parse_semicolon_play(play, notation_info['player'])


def get_notation_style(play):
    """Detects the notation style and separates the name of the player from the
    rest of the play.

    Args:
        play: The text of the play, as written.

    Returns:
        A dict with keys and values:
        'is caps': True if the play is in caps format, False if it is in
            semicolon format. These are the two notation styles used by the
            NCAA in scorekeeping and must be parsed differently.
        'play': The text of the play, excluding the name of the player.
        'player': The player mentioned in the play. Alternatively, 'Team' if
            a team did the action in the play."""
    play = play.replace("UNKNOWN", "")
    while "  " in play:  # trim consecutive spaces to single spaces
        play = play.replace("  ", " ")
    index_comma1 = play.find(",")
    index_comma2 = play.find(", ")

    # find the index of the first lowercase letter or number. if there are no
    # lowercase letters or numbers in the play, set the index to just over the
    # threshold to be detected as a caps play.
    try:
        index_first_lower = play.index(re.findall("[a-z]|[0-9]", play)[0])
    except IndexError:
        index_first_lower = 7

    # if the first lowercase letter is more than 6 characters into the play, it
    # is a caps play, and the play begins just after at the first space before
    # that lowercase letter.
    if index_first_lower > 6:
        is_caps = "caps"
        index_name_end = play[:index_first_lower].rfind(" ") + 1
        player = (play[index_comma1 + 1:index_name_end] + play[0:index_comma1]).title()
        play = play[index_name_end:].lower()

    # if the play is not identified as caps but starts with "TEAM", "null", or
    # "team", it is a caps play and the player is "Team".
    elif (play[0:4] == "TEAM") \
            or (play[0:4] == "null") \
            or (play[0:4] == "team"):
        is_caps = "caps"
        player = "Team"
        index_name_end = 5

        # sometimes the string "Team" appears later in the play also. if so,
        # set the start of the play to just after that word.
        if "Team" in play:
            index_name_end = play.index("Team") + 5
        play = play[index_name_end:].lower()

    # if the play starts with "TM" or starts with 2 numbers, it is a caps play.
    # if "TM", the player is "Team", and if it starts with 2 numbers, it is
    # probably a player but they can't be identified so set them to "Team" as
    # well.
    elif (play[0:3] == "TM ") \
            or ((play[0] in "0123456789") and (play[1] in "0123456789")):
        is_caps = True
        player = "Team"
        while "  " in play:
            play = play.replace("  ", " ")
        play = play[3:].lower()

    # if it has two commas or no commas, it is a semicolon play, and the
    # play part starts after the second comma.
    elif (index_comma2 > 0) or (index_comma1 < 0):
        is_caps = False
        player = play[0:index_comma2]
        play = play[index_comma2:]

    # if no play could be identified, return None
    else:
        return None

    return {
        'is caps': is_caps,
        'play': play,
        'player': player
    }


def clean_centi_time(raw_time):
    """Clean times in the format MM:SS.cc or MM:SS to a number of seconds."""
    minutes = int(raw_time[0:2])
    seconds = int(raw_time[3:5])
    if len(raw_time) > 5:
        centiseconds = int(raw_time[6:9])
    else:
        centiseconds = 0
    return 60 * minutes + seconds + 0.01 * centiseconds


def clean_score(score):
    """Given the entry of a score cell in a play-by-play, return each team's
    score.

    Args:
        score: The raw text of the score cell, in a format like '41-39', with
            the away team's score listed first.

    Returns:
        Each team's score as an int in a dict with keys 'home' and 'away'.
        If the score can't be identified, returns a dict with the same keys and
        None for both values."""
    if (type(score) == str) and ("-" in score):
        return {
            'home': int(score[score.index("-") + 1:]),
            'away': int(score[:score.index("-")])
        }
    else:
        return {'home': None, 'away': None}


# Below are functions dedicated to parsing plays in the 'caps' notation format.


def parse_caps_play(play, player):
    """Parses a play in caps format.

    Args:
        play: The text of a play in caps format, as written, excluding the name
            of the player in the play.
        player: The player named in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who did the action in the play.
        'action': The action performed in the play. (e.g., "shot")
        'flag 1': The type of that action, if applicable. (e.g., "layup")
        'flag 2': The length of the shot, if the play was a shot. Otherwise
            this field is not included
        'flag 3': Whether the shot went in, the player was subbed in, or the
            rebound was offensive, depending on the action. Not included if it
            does not apply to the action.
        'flag 4': Whether the shot was a second-chance shot, or whether the
            rebound was a deadball rebound, if applicable.
        'flag 5': Whether the shot was in in transition, if applicable.
        'flag 6': Whether the shot was blocked, if applicable."""
    if "blocked shot" in play:  # blocks
        return {
            'player': player,
            'action': "block"
        }
    elif " rebound" in play:    # rebounds
        return parse_caps_rebound(play, player)
    elif "turnover" in play:    # turnovers
        return {
            'player': player,
            'action': "turnover"
        }
    elif "steal" in play:   # steals
        return {
            'player': player,
            'action': "steal"
        }
    elif "timeout" in play:     # timeouts
        return parse_caps_timeout(play)
    elif "assist" in play:  # assists
        return {
            'player': player,
            'action': "assist"
        }
    elif "commits foul" in play:    # fouls committed
        return {
            'player': player,
            'action': "foul committed"
        }
    elif " game" in play:   # substitutions
        return {
            'player': player,
            'action': "substitution",
            'flag 3': "enters" in play
        }
    elif "free throw" in play:  # free throws
        return {
            'player': player,
            'action': "free throw",
            'flag 3': "made" in play
        }
    elif ("missed " in play) or ("made " in play):  # shots
        return parse_caps_shot(play, player)
    else:   # if no play type found, try parsing as a semicolon play
        return parse_semicolon_play(play, player)


def parse_caps_rebound(play, player):
    """Parses a play in caps format involving a rebound.

    Args:
        play: The text of a play in caps format, as written, without the
            player's name.
        player: The player who rebounded the ball in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who rebounded the ball in the play.
        'action': 'rebound'
        'flag 3': Whether the rebound was offensive (None if it could not be
            determined).
        'flag 4': Whether the rebound was a deadball rebound."""
    if "offensive" in play:
        was_offensive = True
        was_deadball = False
    elif "defensive" in play:
        was_offensive = False
        was_deadball = False
    elif "deadball" in play:
        was_offensive = None
        was_deadball = True
    else:
        was_deadball = None
        was_offensive = None

    return {
        'player': player,
        'action': "rebound",
        'flag 3': was_offensive,
        'flag 4': was_deadball
    }


def parse_caps_timeout(play):
    """Parses a play in caps format involving a timeout.

    Args:
        play: The text of a play in caps format, as written.

    Returns:
        A dict with the following keys and values:
        'player': 'Team' if a team called the timeout, 'Floor' for media
            timeouts, and None if the caller could not be identified.
        'action': 'timeout'
        'flag 1': The type of timeout. Possible values are "short", "full",
            "media", and None (if the type of timeout could not be
            identified)."""
    if "media" in play:
        caller = "Floor"
        timeout_type = "media"
    elif "20" in play:
        caller = "Team"
        timeout_type = "short"
    elif "30" in play:
        caller = "Team"
        timeout_type = "full"
    elif "short" in play:
        caller = "Team"
        timeout_type = "short"
    elif "full" in play:
        caller = "Team"
        timeout_type = "full"
    elif (play == "timeout") or (play == "team timeout"):
        caller = "Team"
        timeout_type = None
    else:
        caller = None
        timeout_type = None

    return {
        'player': caller,
        'action': "timeout",
        'flag 1': timeout_type
    }


def parse_caps_shot(play, player):
    """Parses a play in caps format involving a field goal attempt.

    Args:
        play: The text of a play in caps format, as written, not including the
            player's name.
        player: The player who shot the ball in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who shot the ball in the play.
        'action': 'shot'
        'flag 1': The type of shot. Possible values are "jump shot", "layup",
            "dunk", and None (if the type of shot could not be identified),
            though "pull-up jump shot", "step-back jump shot", "turn-around
            jump shot", "hook shot", "driving layup", and "alley-oop" an be
            returned from semicolon notation plays.
        'flag 2': The length of the shot. Possible values are "short 2", "long
            2", and "3".
        'flag 3': Whether the shot was made.
        'flag 4': Whether the points were second-chance points.
        'flag 5': Whether the points were scored in transition.
        'flag 6': Whether the shot was blocked."""
    if " three point" in play:
        shot_length = "3"
    elif " jumper" in play:
        shot_length = "long 2"
    else:
        shot_length = "short 2"

    second_chance = None
    if " made " in play:
        blocked = False
    else:
        blocked = None

    if " layup" in play:
        shot_type = "layup"
    elif " jumper" in play:
        shot_type = "jump shot"
    elif " tip in" in play:
        shot_type = "layup"
        second_chance = True
    elif " dunk" in play:
        shot_type = "dunk"
    else:
        shot_type = None

    return {
        'player': player,
        'action': "shot",
        'flag 1': shot_type,
        'flag 2': shot_length,
        'flag 3': " made " in play,
        'flag 4': second_chance,
        'flag 5': None,
        'flag 6': blocked
    }


# Below are functions dedicated to parsing plays in the 'semicolon' notation format.


def parse_semicolon_play(play, player):
    """Parses a play in semicolon format.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player named in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who did the action in the play.
        'action': The action performed in the play. (e.g., "shot")
        'flag 1': The type of that action, if applicable. (e.g., "layup")
        'flag 2': The length of the shot, if the play was a shot. Otherwise
            this field is not included
        'flag 3': Whether the shot went in, the player was subbed in, or the
            rebound was offensive, depending on the action. Not included if it
            does not apply to the action.
        'flag 4': Whether the shot was a second-chance shot, or whether the
            rebound was a deadball rebound, if applicable.
        'flag 5': Whether the shot was in in transition, if applicable.
        'flag 6': Whether the shot was blocked, if applicable."""
    if ("period start" in play) \
            or ("game start" in play) \
            or "jumpball startperiod" in play \
            or ("period end" in play) or ("game end" in play):
        return None     # ignore the starts and ends of periods
    elif (", jumpball" in play) and ((" won" in play) or (" lost" in play)):    # jump balls
        return {
            'player': player,
            'action': "jump ball",
            'flag 3': " won" in play
        }
    elif ", substitution" in play:                                        # substitutions
        return {
            'player': player,
            'action': "substitution",
            'flag 3': " in" in play
        }
    elif "Team, jumpball" in play:  # possession arrow events
        return parse_semicolon_possession_arrow(play)
    elif "timeout " in play:    # timeouts
        return parse_semicolon_timeout(play)
    elif ", foulon" in play:                                              # fouls received
        return {
            'player': player,
            'action': "foul received"
        }
    elif ", foul" in play:  # fouls committed
        return parse_semicolon_foul_committed(play, player)
    elif ", block" in play:                                               # blocks
        return {
            'player': player,
            'action': "block"
        }
    elif ", assist" in play:                                              # assists
        return {
            'player': player,
            'action': "assist"
        }
    elif ", steal" in play:                                               # steals
        return {
            'player': player,
            'action': "steal"
        }
    elif ", turnover" in play:  # turnovers
        return parse_semicolon_turnover(play, player)
    elif " rebound" in play:    # rebounds
        return parse_semicolon_rebound(play, player)
    elif ", 2pt" in play:   # 2-pointers
        return parse_semicolon_two_pointer(play, player)
    elif ", 3pt" in play:   # 3-pointers
        return parse_semicolon_three_pointer(play, player)
    elif ", freethrow" in play:     # free throw attempts
        return {
            'player': player,
            'action': "free throw",
            'flag 3': " made" in play
        }
    else:   # raise ValueError if the play type could not be identified
        return None


def parse_semicolon_possession_arrow(play):
    """Parses a play in semicolon format involving a possession arrow event.

    Args:
        play: The text of a play in semicolon format, as written.

    Returns:
        A dict with the following keys and values:
        'player': 'Team'
        'action': 'possession arrow'
        'type': The incident that caused the possession arrow event. Possible
            values are "held ball", "block tie-up", "lodged ball", "out of
            bounds", and None (if the type of possession arrow incident could
            not be identified)."""
    if " heldball" in play:
        jumpball_type = "held ball"
    elif " blocktieup" in play:
        jumpball_type = "block tie-up"
    elif " lodgedball" in play:
        jumpball_type = "lodged ball"
    elif " outofbounds" in play:
        jumpball_type = "out of bounds"
    else:
        jumpball_type = None

    return {
        'player': "Team",
        'action': "possession arrow",
        'type': jumpball_type
    }


def parse_semicolon_timeout(play):
    """Parses a play in semicolon format involving a timeout.

    Args:
        play: The text of a play in semicolon format, as written.

    Returns:
        A dict with the following keys and values:
        'player': 'Team' if a team called the timeout, otherwise 'Floor'.
        'action': 'timeout'
        'flag 1': The type of timeout. Possible values are "short", "full",
            "media", and None (if the type of timeout could not be
            identified)."""
    if " commercial" in play:
        caller = "Floor"
        timeout_type = "media"
    elif " full" in play:
        caller = "Team"
        timeout_type = "full"
    elif " short" in play:
        caller = "Team"
        timeout_type = "short"
    else:
        caller = None
        timeout_type = None

    return {
        'player': caller,
        'action': "timeout",
        'flag 1': timeout_type
    }


def parse_semicolon_foul_committed(play, player):
    """Parses a play in semicolon format involving a foul committed.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player who committed the foul in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who committed the foul in the play.
        'action': 'foul committed'
        'flag 1': The type of foul. Possible values are "personal",
            "offensive", "bench technical, class A", "bench admin technical,
            class B", "technical, class A", "technical, flagrant 2", "double
            technical", "double coach technical", "coach technical, class A",
            "coach admin technical, class B", "deadball contact technical",
            "administrative admin technical", "admin technical, class B", and
            None (if the type of foul committed could not be identified)."""
    if " personal" in play:
        foul_type = "personal"
    elif " offensive" in play:
        foul_type = "offensive"
    elif "benchTechnical classa" in play:
        foul_type = "bench technical, class A"
    elif "adminTechnical classb" in play:
        foul_type = "admin technical, class B"
    elif "technical classa" in play:
        foul_type = "technical, class A"
    elif "technical flagrant2" in play:
        foul_type = "technical, flagrant 2"
    elif "coachTechnical classa" in play:
        foul_type = "coach technical, class A"
    elif "technical double" in play:
        foul_type = "double technical"
    elif "adminTechnical coachclassb" in play:
        foul_type = "coach admin technical, class B"
    elif "adminTechnical administrative" in play:
        foul_type = "administrative admin technical"
    elif "technical contactdeadball" in play:
        foul_type = "deadball contact technical"
    elif "adminTechnical benchclassb" in play:
        foul_type = "bench admin technical, class B"
    elif "coachTechnical double" in play:
        foul_type = "double coach technical"
    else:
        raise ValueError(f"unrecognized foul type: '{play}'")

    return {
        'player': player,
        'action': "foul committed",
        'flag 1': foul_type
    }


def parse_semicolon_turnover(play, player):
    """Parses a play in semicolon format involving a turnover.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player who turned the ball over in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who turned the ball over in the play.
        'action': 'turnover'
        'flag 1': The type of turnover. Possible values are "travel", "bad
            pass", "lost ball", "offensive foul", "3-second violation", "shot
            clock violation", "double dribble", "5-second violation",
            "10-second violation", "lane violation", "other" (an actual type
            notated by scorekeepers), and None (if the type of turnover could
            not be identified)."""
    if " travel" in play:
        turnover_type = "travel"
    elif " badpass" in play:
        turnover_type = "bad pass"
    elif " lostball" in play:
        turnover_type = "lost ball"
    elif " offensive" in play:
        turnover_type = "offensive foul"
    elif " 3sec" in play:
        turnover_type = "3-second violation"
    elif " shotclock" in play:
        turnover_type = "shot clock violation"
    elif " dribbling" in play:
        turnover_type = "double dribble"
    elif " 5sec" in play:
        turnover_type = "5-second violation"
    elif " 10sec" in play:
        turnover_type = "10-second violation"
    elif " laneviolation" in play:
        turnover_type = "lane violation"
    elif " other" in play:
        turnover_type = "other"
    else:
        raise ValueError(f"unrecognized turnover type: '{play}'")

    return {
        'player': player,
        'action': "turnover",
        'flag 1': turnover_type
    }


def parse_semicolon_rebound(play, player):
    """Parses a play in semicolon format involving a rebound.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player who rebounded the ball in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who rebounded the ball in the play.
        'action': 'rebound'
        'flag 3': Whether the rebound was offensive (None if it could not be
            determined).
        'flag 4': Whether the rebound was a deadball rebound."""
    if " offensive" in play:
        was_offensive = True
    elif " defensive" in play:
        was_offensive = False
    else:
        was_offensive = None

    return {
        'player': player,
        'action': "rebound",
        'flag 3': was_offensive,
        'flag 4': "deadball" in play
    }


def parse_semicolon_two_pointer(play, player):
    """Parses a play in semicolon format involving a two-point attempt.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player who shot the ball in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who shot the ball in the play.
        'action': 'shot'
        'flag 1': The type of shot. Possible values are "jump shot", "pull-up
            jump shot", "step-back jump shot", "turn-around jump shot", "hook
            shot", "layup", "dunk", "driving layup", "alley-oop", and None
            (if the type of shot could not be identified).
        'flag 2': The length of the shot. Possible values are "short 2" and
            "long 2", though three-pointers are labeled as "shot" and use the
            flag 2 to indicate "3".
        'flag 3': Whether the shot was made.
        'flag 4': Whether the points were second-chance points.
        'flag 5': Whether the points were scored in transition.
        'flag 6': Whether the shot was blocked."""
    if "pointsinthepaint" in play:
        shot_length = "short 2"
    else:
        shot_length = "long 2"

    if " jumpshot " in play:
        shot_type = "jump shot"
    elif " pullupjumpshot " in play:
        shot_type = "pull-up jump shot"
    elif " stepbackjumpshot " in play:
        shot_type = "step-back jump shot"
    elif " turnaroundjumpshot " in play:
        shot_type = "turn-around jump shot"
    elif " hookshot " in play:
        shot_type = "hook shot"
    elif " layup " in play:
        shot_type = "layup"
    elif " dunk " in play:
        shot_type = "dunk"
    elif " drivinglayup " in play:
        shot_type = "driving layup"
    elif " alleyoop " in play:
        shot_type = "alley-oop"
    else:
        shot_type = None

    return {
        'player': player,
        'action': "shot",
        'flag 1': shot_type,
        'flag 2': shot_length,
        'flag 3': " made" in play,
        'flag 4': "2ndchance" in play,
        'flag 5': "fastbreak" in play,
        'flag 6': "blocked" in play
    }


def parse_semicolon_three_pointer(play, player):
    """Parses a play in semicolon format involving a three-point attempt.

    Args:
        play: The text of a play in semicolon format, as written.
        player: The player who shot the ball in the play.

    Returns:
        A dict with the following keys and values:
        'player': The player who shot the ball in the play.
        'action': 'shot'
        'flag 1': The type of shot. Possible values are "jump shot", "pull-up
            jump shot", "step-back jump shot", "turn-around jump shot", and
            None (if the type of shot could not be identified).
        'flag 2': "3". Two-pointers are also labeled as "shot" and use the
            flag 2 to indicate "short 2" or "long 2".
        'flag 3': Whether the shot was made.
        'flag 4': Whether the points were second-chance points.
        'flag 5': Whether the points were scored in transition.
        'flag 6': Whether the shot was blocked."""
    if " jumpshot " in play:
        shot_type = "jump shot"
    elif " pullupjumpshot " in play:
        shot_type = "pull-up jump shot"
    elif " turnaroundjumpshot " in play:
        shot_type = "turn-around jump shot"
    elif " stepbackjumpshot " in play:
        shot_type = "step-back jump shot"
    else:
        shot_type = None

    return {
        'player': player,
        'action': "shot",
        'success': " made" in play,
        'length': "3",
        'type': shot_type,
        'second chance': "2ndchance" in play,
        'fast break': "fastbreak" in play,
        'blocked': "blocked" in play
    }



# Below are functions dedicated to cleaning and preprocessing parsed
# play-by-play logs from a game and fixing irregularities.


def track_shot_clock(plays, max_shot_clock=30, orb_to_20=True):
    """Tracks the number of seconds on the shot clock when each event happened
    and adds the shot clock to the dict of each play at the index 'shot clock'.

    Args:
        plays: The parsed list of plays in the game, as a list of dicts.
        max_shot_clock: The number of seconds the shot clock resets to
            normally. Was formerly 35.
        orb_to_20: Whether the shot clock resets to 20 seconds after an
            offensive rebound. Before the 2018–19 season the shot clock always
            reset to the maximum after offensive rebounds, so this should be
            set to false for seasons before 2019."""
    shot_clock = max_shot_clock
    last_play_time = 1200
    shot_clock_end = 1200 - shot_clock

    for play in plays:
        if play['time'] != last_play_time:
            shot_clock = max(play['time'] - shot_clock_end, 0)
        play['shot clock'] = shot_clock
        last_play_time = play['time']

        if play['action'] in CLOCK_RESETTING_ACTIONS:
            shot_clock_end = max(play['time'] - max_shot_clock, 0)
        elif play['action'] == "rebound":
            if play['offensive'] and orb_to_20:
                shot_clock_end = max(play['time'] - 20, 0)
            else:
                shot_clock_end = max(play['time'] - max_shot_clock, 0)


def track_partic(plays):
    """Tracks which players were on the court during each play and records
    participation in each play's dict. Constructs lists of the players (as
    dicts of 'player ID' and 'name') and adds the list of home players on the
    court for a play to the play at the index 'home partic' and away players
    at 'away partic'. Outputs may not have exactly 5 players per team if there
    are errors in the scorekeeping; this is corrected by correct_minutes.

    Args:
        plays: The parsed list of plays in the game, as a list of dicts."""
    h_partic = []
    a_partic = []
    backfilled = []
    last_period = 0
    last_time = 1200
    last_h_partic = []
    last_a_partic = []

    # go for the front and make a list of players known so far
    for play in plays:
        player = play['player']

        # reset everything at the start of each period
        if play['period'] != last_period:
            a_partic = []
            h_partic = []
            backfilled = []
            last_period = play['period']
            last_time = 1200
            last_a_partic = []
            last_h_partic = []

        # don't update subs until the clock changes
        if play['time'] != last_time:
            last_time = play['time']
            last_a_partic = a_partic.copy()
            last_h_partic = h_partic.copy()

        play['home partic'] = last_h_partic
        play['away partic'] = last_a_partic

        # no need to change participation if no player did this action
        if (player != "Floor") and (player != "Team"):
            if not play['is away']:
                subbed_in = (play['action'] == "substitution") and play['in']
                if ((player not in last_h_partic) or subbed_in) and (player not in h_partic):
                    h_partic.append(player)

                if (player not in backfilled) and not subbed_in:
                    for prev_play in plays:
                        if 'home partic' in prev_play:
                            if (prev_play['period'] == play['period']) \
                                    and (prev_play['time'] >= play['time']) \
                                    and (player not in prev_play['home partic']):
                                prev_play['home partic'].append(player)

                    # update subs
                    last_a_partic = a_partic.copy()
                    last_h_partic = h_partic.copy()

                if (player not in backfilled) or subbed_in:
                    backfilled.append(player)

                # remove them from the list if they were substituted out
                if (play['action'] == "substitution") and not play['in'] and (player in h_partic):
                    h_partic.remove(player)
            else:
                subbed_in = (play['action'] == "substitution") and play['in']
                if ((player not in last_a_partic) or subbed_in) and (player not in a_partic):
                    a_partic.append(player)

                if (player not in backfilled) and not subbed_in:
                    for prev_play in plays:
                        if 'away partic' in prev_play:
                            if (prev_play['period'] == play['period']) \
                                    and (prev_play['time'] >= play['time']) \
                                    and (player not in prev_play['away partic']):
                                prev_play['away partic'].append(player)

                    # update subs
                    last_a_partic = a_partic.copy()
                    last_h_partic = h_partic.copy()

                if (player not in backfilled) or subbed_in:
                    backfilled.append(player)

                # remove them from the list if they were substituted out
                if (play['action'] == "substitution") and not play['in'] and (player in a_partic):
                    a_partic.remove(player)


def correct_minutes(boxes, plays):
    """Fixes plays that don't have exactly 5 players listed as on the court for
    each team by checking who is listed as playing more or fewer minutes in the
    box score than is reflected in the play-by-play. Adds or removes players
    from to the list of participating players on their teams for the plays it
    is suspected they were misrecorded on. Each player is a dict.

    Args:
        plays: The parsed list of plays in the game, as a list of dicts.
        boxes: The parsed list of boxes in the game, as a list of dicts."""
    # make dicts of player name -> time played for each team
    h_minutes = {player['name']: player['time played'] for player in boxes
                 if (player['name'] != "Team") and not player['is away']}
    a_minutes = {player['name']: player['time played'] for player in boxes
                 if (player['name'] != "Team") and player['is away']}

    # calculate the playing time inferred from play-by-play compared to the box score
    last_time = 1200
    last_period = 0
    for play in plays:
        if play['period'] == last_period:
            time_diff = last_time - int(play['time'])
        else:
            time_diff = last_time

        last_time = int(play['time'])
        last_period = play['period']

        try:
            for player in play['home partic']:
                h_minutes[player] -= time_diff
            for player in play['away partic']:
                a_minutes[player] -= time_diff
        except KeyError:
            pass

    last_time = 1200
    last_period = 0

    # add or remove players who are logged as playing too many or too few minutes if more or fewer
    # than 5 people are on the court for each team
    for play in plays:
        h_partic = play['home partic']
        a_partic = play['away partic']
        if play['period'] == last_period:
            time_diff = last_time - int(play['time'])
        else:
            time_diff = last_time

        last_time = int(play['time'])
        last_period = play['period']

        # add the players with the most extra minutes to partic1 if there are less than 5
        while len(h_partic) < 5:
            max_player = None
            max_minutes = None

            for player in h_minutes:
                if (player not in h_partic) \
                        and ((h_minutes[player] > max_minutes) or max_minutes is None):
                    max_player = player
                    max_minutes = h_minutes[player]

            h_partic.append(max_player)
            h_minutes[max_player] -= time_diff

        # add the players with the most extra minutes to partic2 if there are less than 5
        while len(a_partic) < 5:
            max_player = None
            max_minutes = None

            for player in a_minutes:
                if (player not in a_partic) \
                        and ((a_minutes[player] > max_minutes) or max_minutes is None):
                    max_player = player
                    max_minutes = a_minutes[player]

            a_partic.append(max_player)
            a_minutes[max_player] -= time_diff

        # remove the players with the most extra plays from partic1 if there are more than 5
        while len(h_partic) < 5:
            min_player = None
            min_minutes = None

            for player in h_minutes:
                if (player in h_partic) \
                        and ((h_minutes[player] < min_minutes) or min_minutes is None):
                    min_player = player
                    min_minutes = h_minutes[player]

            h_partic.remove(min_player)
            h_minutes[min_player] += time_diff

        # remove the players with the most extra plays from partic2 if there are more than 5
        while len(a_partic) < 5:
            min_player = None
            min_minutes = None

            for player in a_minutes:
                if (player in a_partic) \
                        and ((a_minutes[player] < min_minutes) or min_minutes is None):
                    min_player = player
                    min_minutes = a_minutes[player]

            a_partic.remove(min_player)
            a_minutes[min_player] += time_diff


# Main method. Going to be entirely rewritten eventually.


def main(argv):
    if len(argv) == 6:
        scrape_range(int(argv[0]), int(argv[1]), int(argv[2]), int(argv[3]), int(argv[4]), int(argv[5]))
    else:
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(1)
        scrape_range(yesterday.year, yesterday.month, yesterday.day, today.year, today.month, today.day)


if __name__ == '__main__':
    # main([2017, 10, 30, 2018, 4, 7])
    # main(sys.argv[1:])
    pass
