from src.scrape_util import Scraper
from time import sleep
import datetime
import re

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


# Functions for scraping game information from stats.ncaa.org. Going to be entirely rewritten.


class RawGame:
    """A RawGame is an object designed to keep track of all of the information for a single game at the given box ID
    with the given scraper. This is to prevent feeding a list in every time we want to scrape a page, and it keeps
    the sets of information scraped for each game together.
    """

    def __init__(self, scraper, box_id):
        self.scraper = scraper
        self.box_id = box_id
        self.metadata = []
        self.boxes = []
        self.pbp = []

    def get_box_score(self, retries_left=MAX_RETRIES, by_pbp=False):
        """Gets box score information for the game, then calls for the other thing to get the play-by-play."""
        while retries_left > 0:
            # open the page
            if by_pbp:
                url = f"http://stats.ncaa.org/game/box_score/{self.box_id}"
            else:
                url = f"http://stats.ncaa.org/contests/{self.box_id}/box_score"
            soup = self.scraper.open_page(url=url)

            # inexplicably, sometimes the soup will not return anything despite existing
            if soup is None:
                self.scraper.log(f"Soup did not return. Box ID: {self.box_id}", 4)
                soup = self.scraper.last_soup

            try:
                # get the play-by-play ID for the game
                el_pbp = soup.find('ul', class_='level1').find_all('li')[-5].find('a')
                pbp_id = int(el_pbp.attrs['href'][-7:])

                # get the location of the game
                el_metadata_1 = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]
                metadata_1 = el_metadata_1.get_text().strip().replace("\n", " ")

                # get the list of referees for the game
                el_referees = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[3].find_all('td')[1]
                referees = el_referees.get_text().strip().replace("\n", " ")

                team_names = []
                game_stat_lines = []

                for el_box in soup.find_all('table', class_='mytable')[-2:]:
                    el_heading = el_box.find('tr', class_='heading')
                    team_name = el_heading.get_text().strip()
                    team_names.append(team_name)

                    for el_player in el_box.find_all('tr', class_='smtext'):
                        el_player_id = el_player.find('a')
                        if el_player_id is None:
                            player_id = ""
                        else:
                            player_id = int(el_player_id.attrs['href'][-7:])
                        player_stats = [self.box_id, pbp_id, team_name, player_id]

                        for el_stat in el_player.find_all('td'):
                            player_stats.append(el_stat.get_text().strip())

                        game_stat_lines.append(player_stats)

                self.scraper.log(f"Finished parsing box score. (Box ID: {self.box_id})", 2)
                for box_score in game_stat_lines:
                    self.boxes.append(box_score)
                self.metadata = [self.box_id, pbp_id, team_names[0], team_names[1], metadata_1, referees]
                retries_left = 0
                self.get_pbp()
            except AttributeError as e:
                self.scraper.log(f"Error parsing box score: '{e}' (Box ID: {self.box_id})")
                self.metadata = []
                self.boxes = []
                if retries_left <= 0:
                    self.scraper.log(f"Done retrying. (Box ID: {self.box_id})", 1)
            retries_left -= 1
            sleep(CRAWL_DELAY)

    def parse_box_score(self, soup):
        pbp_id = find_pbp_id(soup)
        game_time = find_game_time(soup)
        location = find_location(soup)
        attendance = find_attendance(soup)
        referees = find_referees(soup)
        self.boxes = find_raw_boxes(soup)

    def get_pbp(self, retries_left=MAX_RETRIES):
        """Gets play information for the game."""
        pbp_id = self.metadata[1]

        while retries_left > 0:
            # open the page
            url = f"http://stats.ncaa.org/game/play_by_play/{pbp_id}"
            soup = self.scraper.open_page(url=url)

            # inexplicably, sometimes the soup will not return anything despite existing
            if soup is None:
                self.scraper.log(f"Soup did not return. PBP ID: {pbp_id}")
                soup = self.scraper.last_soup

            try:
                period = 0
                for el_table in soup.find_all('table', class_='mytable')[1:]:
                    for el_tr in el_table.find_all('tr'):
                        pbp_row = [pbp_id, period]
                        for el_td in el_tr.find_all('td'):
                            pbp_row.append(el_td.get_text().strip())
                        while len(pbp_row) < 5:
                            pbp_row.append("")
                        self.pbp.append(pbp_row)
                    period += 1

                self.scraper.log(f"Finished parsing play-by-play. (PBP ID: {pbp_id})", 2)
                retries_left = 0
            except AttributeError as e:
                self.scraper.log(f"Error parsing play-by-play: '{e}' (PBP ID: {pbp_id})")
                self.pbp = []
                retries_left -= 1
                if retries_left <= 0:
                    self.scraper.log(f"Done retrying. (PBP ID: {pbp_id})", 1)
            sleep(CRAWL_DELAY)


def get_range(start_year, start_month, start_day, end_year, end_month, end_day):
    """Get each day in the range [start_date, end_date) and write all the results to file."""
    scraper = Scraper(thread_count=DEFAULT_THREAD_COUNT, verbose=VERBOSE)

    # make the dates into datetime objects
    start_date = datetime.datetime(start_year, start_month, start_day)
    end_date = datetime.datetime(end_year, end_month, end_day)

    # get the season code
    season = start_year
    if start_month > 6:
        season += 1
    season_code = [division['code'] for division in YEAR_DIVISIONS if division['year'] == season][0]

    # iterate through each day in the date range
    while start_date < end_date:

        # get the current day and increment
        year = start_date.year
        month = start_date.month
        day = start_date.day
        start_date += datetime.timedelta(1)

        # get all the box IDs from the day
        scraper.log(f"Started parsing day. (Date: {month}/{day}/{year})", 0)
        box_ids = get_day(scraper, month, day, year, season_code)
        raw_games = [RawGame(scraper, box_id) for box_id in box_ids]
        sleep(CRAWL_DELAY)

        # get the box and write to file for each game
        for raw_game in raw_games:
            raw_game.get_box_score()
            sleep(CRAWL_DELAY)

        scraper.log(f"Finished writing to file. (Date: {month}/{day}/{year})", 0)

    scraper.log("Finished scraping all days in range.", 0)


def get_day(scraper, month, day, year, code, retries_left=MAX_RETRIES):
    """Gets all box score IDs from games on the given date."""
    while retries_left > 0:
        # open the page
        url = f"http://stats.ncaa.org/season_divisions/{code}/scoreboards?game_date={month}%2F{day}%2F{year}"
        soup = scraper.open_page(url=url)
        box_ids = []

        # inexplicably, sometimes the soup will exist but not return anything
        if soup is None:
            scraper.log(f"Soup did not return. (URL: {url})")
            soup = scraper.last_soup

        try:
            el_table = soup.find('table', attrs={'style': 'border-collapse: collapse'})
            for el_box_cell in el_table.find_all('tr', attrs={'style': 'border-bottom: 1px solid #cccccc'}):
                el_link = el_box_cell.find('a', class_='skipMask')

                # some games do not have box score links, this prevents those from breaking everything
                if el_link is not None:
                    box_ids.append(int(el_link.attrs['href'][10:-10]))

            scraper.log(f"Finished parsing day. {len(box_ids)} games found. (Date: {month}/{day}/{year})", 0)
            retries_left = 0
        except AttributeError as e:
            scraper.log(f"Error parsing day: '{e}' (Date: {month}/{day}/{year})")
            retries_left -= 1
            if retries_left <= 0:
                scraper.log(f"Done retrying. (Date: {month}/{day}/{year})", 0)
        sleep(CRAWL_DELAY)

    return box_ids


def get_specifics(box_ids, by_pbp=False):
    scraper = Scraper(thread_count=DEFAULT_THREAD_COUNT, verbose=VERBOSE)

    # get all the box IDs from the day
    scraper.log(f"Started parsing.", 0)
    raw_games = [RawGame(scraper, box_id) for box_id in box_ids]
    sleep(CRAWL_DELAY)

    # get the box and write to file for each game
    for raw_game in raw_games:
        raw_game.get_box_score(by_pbp=by_pbp)
        sleep(CRAWL_DELAY)

    scraper.log(f"Finished writing to file.", 0)
    scraper.log("Finished scraping all days in range.", 0)


# Below are functions dedicated to extracting information from BeautifulSoup representations of box
# score webpages scraped from stats.ncaa.org. These functions do little or no pre-processing of the
# values extracted.


def find_pbp_id(soup):
    """Given a box score page, find the PBP ID of the game."""
    el_pbp = soup.find('ul', class_='level1').find_all('li')[-5].find('a')
    return int(el_pbp.attrs['href'][-7:])


def find_game_time(soup):
    """Given a box score page, find the start time of the game."""
    el_metadata = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]
    el_game_date = el_metadata.find('tr').find_all('td')[1]
    raw_date_text = el_game_date.get_text().strip()

    # some dates have TBA instead of a specific time. does not return a time for those dates
    if 'M' in raw_date_text:
        as_date = datetime.datetime.strptime(raw_date_text, '%m/%d/%Y %I:%M %p')
        return as_date.strftime('%Y/%m/%d %H:%M')
    else:
        index_space = raw_date_text.index(' ')
        as_date = datetime.datetime.strptime(raw_date_text[:index_space], '%m/%d/%Y')
        return as_date.strftime('%Y/%m/%d')


def find_location(soup):
    """Given a box score page, find the location of the game, if it is listed. If it is not
    listed, return None."""
    el_metadata = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]

    # location is not listed for all games
    if 'Location:' in el_metadata.get_text():
        el_location = el_metadata.find_all('tr')[1].find_all('td')[1]
        return el_location.get_text().strip()
    else:
        return None


def find_attendance(soup):
    """Given a box score page, find the attendance of the game."""
    el_metadata = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[2]

    # location appears above attendance in games where it is listed, but it is not listed for all
    # games
    if 'Location:' in el_metadata.get_text():
        el_attendance = el_metadata.find_all('tr')[2].find_all('td')[1]
    else:
        el_attendance = el_metadata.find_all('tr')[1].find_all('td')[1]
    return int(el_attendance.get_text().strip().replace(',', ''))


def find_referees(soup):
    """Given a box score page, find the referees if they are listed."""
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


def find_raw_boxes(soup):
    """Given a box score page, find the box scores and return them raw. Returns them in a list in the format:
    [NCAA player id (as int),
    True if away or False if home,
    name in the format 'Last, First' (suffixes are inconsistent),
    position (e.g. 'G') -- sometimes not listed,
    games played (nearly always '1' -- can safely be discarded),
    duration played (e.g. '6:04'),
    'FGM', 'FGA', '3PM', '3PA', 'FTM', 'FTA', 'PTS', 'ORB', 'DRB', 'TRB', 'AST', 'TOV', 'STL', 'BLK', 'PF', 'DQ']"""
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
    """Rearrange the given name from 'Last, First' to 'First Last'. Also accepts names without the
    space after the comma. If there is no comma, returns the name as is. Names with extra commas
    like 'Last, Jr., First' rearrange to 'First Last, Jr.'"""
    index_last_comma = name.rfind(',')
    if (index_last_comma > 0) and (index_last_comma < len(name) - 1):
        if name[index_last_comma + 1] == " ":
            return name[index_last_comma + 2:] + " " + name[:index_last_comma]
        else:
            return name[index_last_comma + 1:] + " " + name[:index_last_comma]
    else:
        return name


def identify_player(player_id, name, roster):
    """Given the name and player ID of a player, and the roster of the team they play on, identify
    the player. Look for an exact match, otherwise choose the player with the most similar name. If
    no matching or even similar player can be found, return a player with the given name and a
    player ID of None."""
    # if the player plays on a non-D1 team (thus having no associated roster), do not try to
    # identify them
    if roster is None:
        return {
            'player ID': None,
            'name': name
        }
    else:
        highest_similarity = 3  # discard any matches with a lower similarity than 3
        most_similar = {
            'player ID': None,
            'name': name
        }

        # if there is an exactly matching name or player ID, return that player, or otherwise
        # choose the player with the most similar name
        for player in roster:
            if (player_id == player[0]) or (name == player[1]):
                most_similar = player
                break
            else:
                similarity = score_name_similarity(name, player[1])
                if similarity > highest_similarity:
                    highest_similarity = similarity
                    most_similar = player

        return {
            'player ID': most_similar[0],
            'name': most_similar[1]
        }


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
    for i in range(len(name1) - 3):
        if name1[i:i + 3] in name2:
            similarity += 2

    return similarity


# Below are functions for uploading files to the database.


def upload_game(cursor, game_id, h_team_season_id, a_team_season_id, h_name, a_name, start_time,
                location, attendance, referees, is_exhibition):
    """Assert that fields that are required not to be null in the database are not null, and
    replace any other null fields with the string 'NULL'."""
    assert (game_id is not None) and (h_name is not None) and (a_name is not None)
    if h_team_season_id is None:
        h_team_season_id = "NULL"
    if a_team_season_id is None:
        a_team_season_id = "NULL"
    if start_time is None:
        start_time = "NULL"
    if location is None:
        location = "NULL"
    if attendance is None:
        attendance = "NULL"
    if referees is None:
        referees = ["NULL"] * 3
    referees = [referee for referee in referees if referee is not None]
    while len(referees) < 3:  # in case there fewer than 3 non-null referees
        referees.append("NULL")
    if is_exhibition is None:
        is_exhibition = "NULL"

    cursor.execute(
        f"""INSERT INTO games (game_id, h_team_season_id, a_team_season_id, h_name, a_name,
                               start_time, location, attendance, referees, is_exhibition)
            VALUES (`{game_id}`, `{h_team_season_id}`, `{a_team_season_id}`, `{h_name}`,
                    `{a_name}`, `{start_time}`, `{location}`, `{attendance}`, `{referees[0]}`,
                    `{referees[1]}`, `{referees[2]}`, `{is_exhibition}`);"""
    )


def upload_box(cursor, game_id, box_in_game, player_id, player_name, is_away, seconds_played,
               fgm, fga, tpm, tpa, ftm, fta, orb, drb, ast, tov, stl, blk, pf):
    """Assert that fields that are required not to be null in the database are not null, and
    replace any other null fields with the string 'NULL'. Except I'm not bothering with the ints
    yet."""
    assert (game_id is not None) and (box_in_game is not None)
    if player_id is None:
        player_id = "NULL"
    if player_name is None:
        player_name = "NULL"
    if is_away is None:
        is_away = "NULL"

    cursor.execute(
        f"""INSERT INTO boxes (game_id, box_in_game, player_id, player_name, is_away,
                               start_time, location, attendance, referees, is_exhibition)
            VALUES (`{game_id}`, `{box_in_game}`, `{player_id}`, `{player_name}`, `{is_away}`,
                    `{seconds_played}`, `{fgm}`, `{fga}`, `{tpm}`, `{tpa}`,  `{ftm}`, `{fta}`,
                    `{orb}`, `{drb}`, `{ast}`, `{tov}`, `{stl}`, `{blk}`, `{pf}`);"""
    )


def upload_play(cursor, game_id, play_in_game, period, time_remaining, shot_clock, h_score, a_score,
                agent_is_away, action, flags, agent_id, agent_name, players):
    """Assert that fields that are required not to be null in the database are not null, and
    replace any other null fields with the string 'NULL'. Except I'm not bothering with the ints
    yet."""
    assert (game_id is not None) and (play_in_game is not None)
    assert (agent_is_away is not None) and (action is not None)

    if period is None:
        period = "NULL"
    if time_remaining is None:
        period = "NULL"
    if shot_clock is None:
        period = "NULL"
    if h_score is None:
        period = "NULL"
    if a_score is None:
        period = "NULL"
    if agent_is_away is None:
        period = "NULL"
    if action is None:
        period = "NULL"
    if agent_name is None:
        period = "NULL"
    if agent_id is None:
        period = "NULL"
    if flags is None:
        flags = ["NULL"] * 6
    null_flags = [i for i in range(len(flags)) if flags[i] is None]
    for i in null_flags:
        flags[i] = "NULL"
    if players is None:
        players = ["NULL"] * 10
    null_players = [i for i in range(len(players)) if players[i] is None]
    for i in null_players:
        players[i] = "NULL"

    cursor.execute(
        f"""INSERT INTO plays (game_id, play_in_game, period, time_remaining, shot_clock, h_score,
                               a_score, agent_is_away, action, flag1, flag2, flag3, flag4, flag5,
                               flag6, agent_id, agent_name, h_p1, h_p2, h_p3, h_p4, h_p5, a_p1,
                               a_p2, a_p3, a_p4, a_p5)
            VALUES (`{game_id}`, `{play_in_game}`, `{period}`, `{time_remaining}`, `{shot_clock}`,
                    `{h_score}`, `{a_score}`, `{agent_is_away}`, `{action}`, `{flags[0]}`,
                    `{flags[1]}`, `{flags[2]}`, `{flags[3]}`, `{flags[4]}`, `{flags[5]}`,
                    `{agent_id}`, `{agent_name}`, `{players[0]}`, `{players[1]}`, `{players[2]}`,
                    `{players[3]}`, `{players[4]}`, `{players[5]}`, `{players[6]}`, `{players[7]}`,
                    `{players[8]}`, `{players[9]}`);"""
    )


# Below are functions for parsing a play from the play-by-play logs.


def parse_play_row(play_row):
    """Put all the information in the row into a dict that contains parsed play information."""
    play_1 = play_row[3]
    play_2 = play_row[5]
    score = play_row[4]

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
        parsed_play['away score'] = scores[0]
        parsed_play['home score'] = scores[1]
        parsed_play['time'] = clean_centi_time(play_row[2])

        # get other information from the row
        parsed_play['game ID'] = play_row[0]
        parsed_play['period'] = int(play_row[1])
        parsed_play['is away'] = is_away

        return parsed_play
    

def parse_play(play):
    """Parse only the text of the play and return as a dict."""
    notation_info = get_notation_style(play)
    if notation_info['style'] == 'semicolon':
        return parse_semicolon_play(play, notation_info['player'])
    elif notation_info['style'] == 'caps':
        return parse_caps_play(notation_info['player'], notation_info['rest'])


def get_notation_style(play):
    """Detect the notation style and separates the name of the player from the rest of the play."""
    play = play.replace("UNKNOWN", "")
    index_comma1 = play.find(",")
    index_comma2 = play.find(", ")
    try:
        index_first_lower = play.index(re.findall("[a-z]|[0-9]", play)[0])
    except IndexError:
        index_first_lower = 7
    if index_first_lower > 6:
        notation_style = "caps"
        index_name_end = play[:index_first_lower].rfind(" ") + 1
        player = (play[index_comma1 + 1:index_name_end] + play[0:index_comma1]).title()
        rest = play[index_name_end:].lower()
    elif (play[0:4] == "TEAM") or (play[0:4] == "null") or (play[0:4] == "team"):
        notation_style = "caps"
        player = "Team"
        while "  " in play:
            play = play.replace("  ", " ")
        index_name_end = 5
        if "Team" in play:
            index_name_end = play.index("Team") + 5
        rest = play[index_name_end:].lower()
    elif (play[0:3] == "TM ") \
            or ((play[0] >= "0") and (play[0] <= "9") and (play[1] >= "0") and (play[2] <= "9")):
        notation_style = "caps"
        player = "Team"
        while "  " in play:
            play = play.replace("  ", " ")
        rest = play[3:].lower()
    elif (index_comma2 > 0) or (index_comma1 < 0):
        notation_style = "semicolon"
        player = play[0:index_comma2]
        rest = play[index_comma2:]
    else:
        raise ValueError(f"Unrecognized notation style: '{play}'")

    return {
        'style': notation_style,
        'player': player,
        'rest': rest
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
    """Given the entry of a score in a play-by-play in a format like '41-39', return the scores in
    a format like (41, 39)."""
    if (type(score) == str) and ("-" in score):
        return int(score[:score.index("-")]), int(score[score.index("-") + 1:])
    else:
        return [None] * 2


# Below are functions dedicated to parsing plays in the 'caps' notation format.


def parse_caps_play(player, rest):
    """Parses a play in the 'caps' notation style."""
    if "blocked shot" in rest:                      # blocks
        return {
            'player': player,
            'action': "block"
        }
    elif " rebound" in rest:                        # rebounds
        return parse_caps_rebound(player, rest)
    elif "turnover" in rest:                        # turnovers
        return {
            'player': player,
            'action': "turnover"
        }
    elif "steal" in rest:                           # steals
        return {
            'player': player,
            'action': "steal"
        }
    elif "timeout" in rest:                         # timeouts
        return parse_caps_timeout(rest)
    elif "assist" in rest:                          # assists
        return {
            'player': player,
            'action': "assist"
        }
    elif "commits foul" in rest:                    # fouls committed
        return {
            'player': player,
            'action': "foul committed"
        }
    elif " game" in rest:                           # substitutions
        return {
            'player': player,
            'action': "substitution",
            'in': "enters" in rest
        }
    elif "free throw" in rest:                      # free throws
        return {
            'player': player,
            'action': "free throw",
            'success': "made" in rest,
            'number': None,
            'out of': None
        }
    elif ("missed " in rest) or ("made " in rest):  # shots
        return parse_caps_shot(player, rest)
    else:   # if no play type found, try parsing as a semicolon play
        return parse_semicolon_play(rest, player)


def parse_caps_rebound(player, rest):
    """Parses a play in caps format involving a rebound."""
    if "offensive" in rest:
        was_offensive = True
        rebound_type = "live"
    elif "defensive" in rest:
        was_offensive = False
        rebound_type = "live"
    elif "deadball" in rest:
        was_offensive = None
        rebound_type = "dead ball"
    else:
        raise ValueError(f"Unrecognized rebound type: '{rest}'")

    return {
            'player': player,
            'action': "rebound",
            'offensive': was_offensive,
            'type': rebound_type
        }


def parse_caps_timeout(rest):
    """Parses a play in caps format involving a timeout."""
    if "media" in rest:
        caller = "Floor"
        timeout_type = "media"
    elif "20" in rest:
        caller = "Team"
        timeout_type = "short"
    elif "30" in rest:
        caller = "Team"
        timeout_type = "full"
    elif "short" in rest:
        caller = "Team"
        timeout_type = "short"
    elif "full" in rest:
        caller = "Team"
        timeout_type = "full"
    elif (rest == "timeout") or (rest == "team timeout"):
        caller = "Team"
        timeout_type = "unknown"
    else:
        raise ValueError(f"Unrecognized timeout type: '{rest}'")

    return {
        'player': caller,
        'action': "timeout",
        'type': timeout_type
    }


def parse_caps_shot(player, rest):
    """Parses a play in caps format involving a field goal attempt."""
    if " three point" in rest:
        shot_length = "3"
    elif " jumper" in rest:
        shot_length = "long 2"
    else:
        shot_length = "short 2"

    second_chance = None

    if " layup" in rest:
        shot_type = "layup"
    elif " jumper" in rest:
        shot_type = "jump shot"
    elif " tip in" in rest:
        shot_type = "layup"
        second_chance = True
    elif " dunk" in rest:
        shot_type = "dunk"
    else:
        raise ValueError(f"Unrecognized shot type '{rest}'.")

    return {
        'player': player,
        'action': "shot",
        'success': " made " in rest,
        'length': shot_length,
        'type': shot_type,
        'second chance': second_chance,
        'fast break': None,
        'blocked': None
    }


# Below are functions dedicated to parsing plays in the 'semicolon' notation format.


def parse_semicolon_play(play, player):
    """Parses a play in the 'semicolon' notation style."""
    if ("period start" in play) or ("game start" in play) or "jumpball startperiod" in play \
            or ("period end" in play) or ("game end" in play):
        return None     # ignore the starts and ends of periods
    elif (", jumpball" in play) and ((" won" in play) or (" lost" in play)):    # jump balls
        return {
            'player': player,
            'action': "jump ball",
            'success': " won" in play
        }
    elif ", substitution" in play:                                        # substitutions
        return {
            'player': player,
            'action': "substitution",
            'in': " in" in play
        }
    elif "Team, jumpball" in play:                                        # possession arrow events
        return parse_semicolon_possession_arrow(play, player)
    elif "timeout " in play:                                              # timeouts
        return parse_semicolon_timeout(play, player)
    elif ", foulon" in play:                                              # fouls received
        return {
            'player': player,
            'action': "foul received"
        }
    elif ", foul" in play:                                                # fouls committed
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
    elif ", turnover" in play:                                            # turnovers
        return parse_semicolon_turnover(play, player)
    elif " rebound" in play:                                              # rebounds
        return parse_semicolon_rebound(play, player)
    elif ", 2pt" in play:                                                 # 2-pointers
        return parse_semicolon_three_pointer(play, player)
    elif ", 3pt" in play:                                                 # 3-pointers
        return parse_semicolon_two_pointer(play, player)
    elif ", freethrow" in play:                                           # free throw attempts
        return parse_semicolon_free_throw(play, player)
    else:   # raise ValueError if the play type could not be identified
        raise ValueError(f"Unrecognized play type: '{play}'")


def parse_semicolon_possession_arrow(play, player):
    """Parses a play in semicolon format involving a possession arrow event."""
    if " heldball" in play:
        jumpball_type = "held ball"
    elif " blocktieup" in play:
        jumpball_type = "block tie-up"
    elif " lodgedball" in play:
        jumpball_type = "lodged ball"
    elif " outofbounds" in play:
        jumpball_type = "out of bounds"
    else:
        raise ValueError(f"Unknown jump ball type: '{play}'")

    return {
        'player': "Team",
        'action': "possession arrow",
        'type': jumpball_type
    }


def parse_semicolon_timeout(play, player):
    """Parses a play in semicolon format involving a timeout."""
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
        raise ValueError(f"Unrecognized timeout type: '{play}'")

    return {
        'player': caller,
        'action': "timeout",
        'type': timeout_type
    }


def parse_semicolon_foul_committed(play, player):
    """Parses a play in semicolon format involving a foul committed."""
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
        'type': foul_type
    }


def parse_semicolon_turnover(play, player):
    """Parses a play in semicolon format involving a turnover."""
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
        'type': turnover_type
    }


def parse_semicolon_rebound(play, player):
    """Parses a play in semicolon format involving a rebound."""
    if " offensive" in play:
        was_offensive = True
    elif " defensive" in play:
        was_offensive = False
    else:
        raise ValueError(f"unrecognized rebound type: '{play}'")

    if "deadball" in play:
        rebound_type = "deadball"
    else:
        rebound_type = "live"

    return {
        'player': player,
        'action': "rebound",
        'offensive': was_offensive,
        'type': rebound_type
    }


def parse_semicolon_two_pointer(play, player):
    """Parses a play in semicolon format involving a two-point attempt."""
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
        raise ValueError(f"unrecognized shot type: '{play}'")

    return {
        'player': player,
        'action': "shot",
        'success': " made" in play,
        'length': shot_length,
        'type': shot_type,
        'second chance': "2ndchance" in play,
        'fast break': "fastbreak" in play,
        'blocked': "blocked" in play
    }


def parse_semicolon_three_pointer(play, player):
    """Parses a play in semicolon format involving a three-point attempt."""
    if " jumpshot " in play:
        shot_type = "jump shot"
    elif " pullupjumpshot " in play:
        shot_type = "pull-up jump shot"
    elif " turnaroundjumpshot " in play:
        shot_type = "turn-around jump shot"
    elif " stepbackjumpshot " in play:
        shot_type = "step-back jump shot"
    else:
        raise ValueError(f"unrecognized shot type: '{play}'")

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


def parse_semicolon_free_throw(play, player):
    """Parses a play in semicolon format involving a free throw attempt."""
    index_ft = play.index(", freethrow")

    return {
        'player': player,
        'action': "free throw",
        'success': " made" in play,
        'number': int(play[index_ft + 12]),
        'out of': int(play[index_ft + 15])
    }


# Main method. Going to be entirely rewritten eventually.


def main(argv):
    if len(argv) == 6:
        get_range(int(argv[0]), int(argv[1]), int(argv[2]), int(argv[3]), int(argv[4]), int(argv[5]))
    else:
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(1)
        get_range(yesterday.year, yesterday.month, yesterday.day, today.year, today.month, today.day)


if __name__ == '__main__':
    # main([2017, 10, 30, 2018, 4, 7])
    # main(sys.argv[1:])
    get_specifics([4655893, 4657243, 4662221, 4669299, 4669980, 4675929, 4748501], by_pbp=True)
