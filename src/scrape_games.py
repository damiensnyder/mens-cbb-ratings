### IMPORTS ###


from src.scrape_util import Scraper
from time import sleep
import datetime


### CONSTANTS ###


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


### CLASSES ###


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

        for box_score in game_stat_lines:
            self.boxes.append(box_score)

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


### FUNCTIONS ###


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


def main(argv):
    if len(argv) == 6:
        get_range(int(argv[0]), int(argv[1]), int(argv[2]), int(argv[3]), int(argv[4]), int(argv[5]))
    else:
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(1)
        get_range(yesterday.year, yesterday.month, yesterday.day, today.year, today.month, today.day)


### ACTUAL STUFF ###


if __name__ == '__main__':
    # main([2017, 10, 30, 2018, 4, 7])
    # main(sys.argv[1:])
    get_specifics([4655893, 4657243, 4662221, 4669299, 4669980, 4675929, 4748501], by_pbp=True)
