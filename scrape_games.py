### IMPORTS ###


from scrape_util import Scraper
from concurrent import futures
from time import sleep
import datetime
import sys


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

PATH_METADATA = "out/raw_game_metadata.csv"
PATH_BOXES = "out/raw_box_scores.csv"
PATH_PBP = "out/raw_plays.csv"


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

                # get the list of officials for the game
                el_officials = soup.find_all('table', attrs={'width': '50%', 'align': 'center'})[3].find_all('td')[1]
                officials = el_officials.get_text().strip().replace("\n", " ")

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
                self.metadata = [self.box_id, pbp_id, team_names[0], team_names[1], metadata_1, officials]
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

    def write_to_file(self, file_metadata, file_boxes, file_pbp):
        """Write all the info from this game to the given files."""
        # write the metadata to file
        while len(self.metadata) < 6:
            self.metadata.append("")
        str_metadata = f"{self.metadata[0]}," \
                       f"{self.metadata[1]}," \
                       f"\"{self.metadata[2]}\"," \
                       f"\"{self.metadata[3]}\"," \
                       f"\"{self.metadata[4]}\"," \
                       f"\"{self.metadata[5]}\"\n"
        file_metadata.write(str_metadata)
        self.scraper.log(str_metadata[:-1])

        # write the box score to file
        for box in self.boxes:
            while len(box) < 7:
                box.append("")

            str_box = f"{box[0]}," \
                      f"{box[1]}," \
                      f"\"{box[2]}\"," \
                      f"{box[3]}," \
                      f"\"{box[4]}\"," \
                      f"\"{box[5]}\"," \
                      f"\"{box[6]}\""

            # every item after the first 7 is the same
            for box_item in box[7:]:
                str_box += f",{box_item}"

            str_box += "\n"
            file_boxes.write(str_box)
            self.scraper.log(str_box[:-1], 5)

        # write the play-by-play to file
        for play in self.pbp:
            while len(play) < 6:
                play.append("")
            str_play = f"{play[0]}," \
                       f"{play[1]}," \
                       f"\"{play[2]}\"," \
                       f"\"{play[3]}\"," \
                       f"\"{play[4]}\"," \
                       f"\"{play[5]}\"\n"
            file_pbp.write(str_play)
            self.scraper.log(str_play[:-1], 5)

        file_metadata.flush()
        file_boxes.flush()
        file_pbp.flush()


### FUNCTIONS ###


def main(argv):
    if len(argv) == 6:
        get_range(int(argv[0]), int(argv[1]), int(argv[2]), int(argv[3]), int(argv[4]), int(argv[5]))
    else:
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(1)
        get_range(yesterday.year, yesterday.month, yesterday.day, today.year, today.month, today.day)


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

    # open the files we're going to write to
    file_metadata = open(PATH_METADATA, 'w')
    file_boxes = open(PATH_BOXES, 'w')
    file_pbp = open(PATH_PBP, 'w')

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
            raw_game.write_to_file(file_metadata, file_boxes, file_pbp)
            sleep(CRAWL_DELAY)

        scraper.log(f"Finished writing to file. (Date: {month}/{day}/{year})", 0)

    # close the files
    file_metadata.close()
    file_boxes.close()
    file_pbp.close()
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

    # open the files we're going to write to
    file_metadata = open(PATH_METADATA, 'w')
    file_boxes = open(PATH_BOXES, 'w')
    file_pbp = open(PATH_PBP, 'w')

    # get all the box IDs from the day
    scraper.log(f"Started parsing.", 0)
    raw_games = [RawGame(scraper, box_id) for box_id in box_ids]
    sleep(CRAWL_DELAY)

    # get the box and write to file for each game
    for raw_game in raw_games:
        raw_game.get_box_score(by_pbp=by_pbp)
        raw_game.write_to_file(file_metadata, file_boxes, file_pbp)
        sleep(CRAWL_DELAY)

    scraper.log(f"Finished writing to file.", 0)

    # close the files
    file_metadata.close()
    file_boxes.close()
    file_pbp.close()
    scraper.log("Finished scraping all days in range.", 0)


### ACTUAL STUFF ###


if __name__ == '__main__':
    # main([2017, 10, 30, 2018, 4, 7])
    # main(sys.argv[1:])
    get_specifics([4655893, 4657243, 4662221, 4669299, 4669980, 4675929, 4748501], by_pbp=True)
