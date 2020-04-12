### IMPORTS ###


from src.scrape_util import Scraper
from time import sleep

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
    {'year': 2019, 'code': 14300, 'attendance': 17900},
    {'year': 2020, 'code': 17060}
]

ALL_TEAMS_2019 = [450588, 450603, 450633, 450674, 450627, 450467, 450496, 450546, 450583, 450635, 450675, 450729,
                  450738, 450521, 450587, 450535, 450480, 450726, 450905, 450466, 450768, 450565, 450618, 450482,
                  450647, 450491, 450537, 450575, 450786, 450579, 450708, 450585, 450794, 450515, 450777, 450664,
                  450552, 450481, 450573, 450775, 450713, 450807, 450720, 450598, 450624, 450693, 450468, 450709,
                  450724, 450750, 450628, 450547, 450584, 450746, 450595, 450776, 450478, 450484, 450545, 450673,
                  450790, 450742, 450661, 450662, 450623, 450671, 450499, 450650, 450516, 450520, 450692, 450652,
                  450756, 450477, 450601, 450498, 450533, 450554, 450569, 450645, 450805, 450683, 450549, 450653,
                  450625, 450810, 450694, 450561, 450553, 450643, 450743, 450770, 450723, 450630, 450698, 450542,
                  450765, 450774, 450660, 450562, 450604, 450567, 450530, 450540, 450558, 450747, 450646, 450787,
                  450473, 450782, 450599, 450682, 450706, 450470, 450471, 450534, 450600, 450617, 450669, 450479,
                  450622, 450659, 450528, 450725, 450570, 450749, 450716, 450717, 450649, 450518, 450519, 450677,
                  450539, 450804, 450506, 450489, 450791, 450631, 450474, 450582, 450797, 450801, 450811, 450613,
                  450462, 450707, 450510, 450687, 450710, 450764, 450735, 450656, 450666, 450793, 450572, 450696,
                  450655, 450681, 450789, 450508, 450541, 450719, 450543, 450560, 450798, 450463, 450483, 450571,
                  450632, 450745, 450739, 450672, 450501, 450509, 450536, 450802, 450785, 450808, 450700, 450704,
                  450714, 450602, 450594, 450610, 450754, 450685, 450758, 450736, 450761, 450620, 450705, 450715,
                  450680, 450679, 450517, 450657, 450475, 450476, 450568, 450614, 450636, 450605, 450494, 450616,
                  450722, 450733, 450771, 450651, 450512, 450640, 450488, 450504, 450527, 450721, 450486, 450529,
                  450639, 450757, 450712, 450522, 450507, 450524, 450563, 450606, 450763, 450795, 450487, 450654,
                  450762, 450514, 450590, 450596, 450597, 450781, 450699, 450809, 450493, 450637, 450748, 450523,
                  450531, 450532, 450550, 450688, 450753, 450574, 450642, 450593, 450502, 450525, 450702, 450559,
                  450576, 450670, 450686, 450690, 450607, 450779, 450783, 450612, 450581, 450538, 450773, 450695,
                  450668, 450665, 450730, 450796, 450772, 450769, 450784, 450691, 450703, 450803, 451095, 450634,
                  450469, 450577, 450727, 450751, 450760, 450732, 450557, 450621, 450586, 450592, 450638, 450780,
                  450608, 450472, 450697, 450737, 450752, 450492, 450629, 450505, 450759, 450648, 450806, 450755,
                  450812, 450526, 450578, 450766, 450718, 450551, 450615, 450464, 450495, 450658, 450644, 450711,
                  450619, 450778, 450744, 450555, 450589, 450800, 450678, 450741, 450513, 450641, 450731, 450663,
                  450465, 450490, 450609, 450611, 450684, 450667, 450701, 450564, 450497, 450580, 450728, 450734,
                  450792, 450626, 450485, 450500, 450511, 450566, 450689, 450591, 450788, 450548, 450740, 450544,
                  450676, 450503, 450556, 450767, 450799]

PATH_METADATA = "out/raw_team_metadata.csv"
PATH_PLAYERS = "out/raw_players.csv"


### CLASSES ###


class Team:
    """A RawGame is an object designed to keep track of all of the information for a single game at the given box ID
    with the given scraper. This is to prevent feeding a list in every time we want to scrape a page, and it keeps
    the sets of information scraped for each game together.
    """

    def __init__(self, scraper, team_id, season_code):
        self.scraper = scraper
        self.team_id = team_id
        self.season_code = season_code
        self.players = []
        self.metadata = []

    def get_school_id(self, retries_left=MAX_RETRIES):
        while retries_left > 0:
            url = f"http://stats.ncaa.org/teams/{self.team_id}"
            soup = self.scraper.open_page(url=url)

            # inexplicably, sometimes the soup will not return anything despite existing
            if soup is None:
                self.scraper.log(f"Soup did not return. Box ID: {self.team_id}", 4)
                soup = self.scraper.last_soup

            try:
                # get the school ID for the team
                el_image = soup.find('img', attrs={'width': '30px', 'height': '20px'})
                img_src = el_image.attrs['src']
                index_last_slash = img_src.rfind("/")
                self.metadata.append(int(img_src[index_last_slash + 1:-4]))

                # get some metadata
                school_name = soup.find_all('img')[2].attrs['alt']
                team_name_full = soup.find_all('legend')[0].find('a').get_text()
                coach_info = soup.find('div', id='head_coaches_div').get_text().strip()
                coach_link = soup.find('div', id='head_coaches_div').find('a').attrs['href']
                arena_info = soup.find('div', id=f"team_venues_{self.team_id}").get_text().strip()

                self.metadata.append(school_name)

                team_name = team_name_full[len(school_name) + 1:]
                self.metadata.append(team_name)

                index_coach_name_start = coach_info.index("Name:") + 6
                index_coach_name_end = coach_info.index("\n", index_coach_name_start)
                self.metadata.append(coach_info[index_coach_name_start:index_coach_name_end])

                index_coach_id_start = coach_link.index("/", 1) + 1
                index_coach_id_end = coach_link.index("?")
                self.metadata.append(coach_link[index_coach_id_start:index_coach_id_end])

                index_arena_name_start = arena_info.index("Name ") + 5
                index_arena_name_end = arena_info.index("\n", index_arena_name_start)
                self.metadata.append(arena_info[index_arena_name_start:index_arena_name_end])

                index_arena_capacity_start = arena_info.index("Capacity ") + 9
                index_arena_capacity_end = arena_info.index("\n", index_arena_capacity_start)
                str_arena_capacity = arena_info[index_arena_capacity_start:index_arena_capacity_end]
                self.metadata.append(int(str_arena_capacity.replace(",", "")))

                self.scraper.log(f"Finished parsing school ID. (Team ID: {self.team_id})", 2)
                retries_left = 0
                self.get_roster()
            except AttributeError as e:
                self.scraper.log(f"Error parsing box score: '{e}' (Team ID: {self.team_id})")
                if retries_left <= 0:
                    self.scraper.log(f"Done retrying. (Team ID: {self.team_id})", 1)
            retries_left -= 1
            sleep(CRAWL_DELAY)

    def get_roster(self, retries_left=MAX_RETRIES):
        """Gets player information for the game."""
        while retries_left > 0:
            # open the page
            url = f"http://stats.ncaa.org/team/{self.metadata[0]}/roster/{self.season_code}"
            soup = self.scraper.open_page(url=url)

            # inexplicably, sometimes the soup will not return anything despite existing
            if soup is None:
                self.scraper.log(f"Soup did not return. Team ID: {self.team_id}")
                soup = self.scraper.last_soup

            try:
                el_rows = [row for row in soup.find_all('tr') if 'class' not in row.attrs]
                for el_row in el_rows:
                    player = []
                    el_items = el_row.find_all('td')
                    for el_item in el_items[:5]:
                        player.append(el_item.get_text())
                    el_player_link = el_items[1].find('a')
                    if el_player_link is None:
                        player.append(-1)
                    else:
                        player.append(int(el_player_link.attrs['href'][-7:]))
                    self.players.append(player)

                self.scraper.log(f"Finished parsing players. (Team ID: {self.team_id})", 2)
                retries_left = 0
            except AttributeError as e:
                self.scraper.log(f"Error parsing players: '{e}' (School ID: {self.metadata})")
                self.players = []
                retries_left -= 1
                if retries_left <= 0:
                    self.scraper.log(f"Done retrying. (School ID: {self.metadata})", 1)
            sleep(CRAWL_DELAY)

    def write_to_file(self, file_metadata, file_players):
        """Write all the info from this team to the given files."""
        str_metadata = f"{self.team_id},{self.metadata[0]},\"{self.metadata[1]}\",\"{self.metadata[2]}\"," \
                       + f"\"{self.metadata[3]}\",{self.metadata[4]},\"{self.metadata[5]}\",{self.metadata[6]}\n"
        file_metadata.write(str_metadata)

        for player in self.players:
            str_players = f"{self.team_id},{self.metadata[0]},\"{self.metadata[1]}\",{player[5]},\"{player[1]}\"," \
                          + f"\"{player[0]}\",\"{player[2]}\",\"{player[3]}\",\"{player[4]}\"\n"
            file_players.write(str_players)

        file_metadata.flush()
        file_players.flush()


### FUNCTIONS ###


def main(argv):
    get_all_teams(argv)


def get_all_teams(season):
    """Get each day in the range [start_date, end_date) and write all the results to file."""
    scraper = Scraper(thread_count=DEFAULT_THREAD_COUNT, verbose=VERBOSE)

    # open the files we're going to write to
    file_metadata = open(PATH_METADATA, 'w')
    file_players = open(PATH_PLAYERS, 'w')

    # get numbers relating to the current year
    season_code = [division['code'] for division in YEAR_DIVISIONS if division['year'] == season][0]
    attendance_code = [division['attendance'] for division in YEAR_DIVISIONS if division['year'] == season][0]

    # get all the box IDs from the day
    scraper.log(f"Started parsing team IDs.", 0)
    team_ids = get_team_ids(scraper, attendance_code)
    teams = [Team(scraper, team_id, season_code) for team_id in team_ids]
    sleep(CRAWL_DELAY)

    # get the box and write to file for each game
    for team in teams:
        team.get_school_id()
        team.write_to_file(file_metadata, file_players)
        sleep(CRAWL_DELAY)

        scraper.log(f"Finished writing to file. (Team ID: {team.team_id})", 0)

    # close the files
    file_metadata.close()
    file_players.close()
    scraper.log("Finished scraping all days in range.", 0)


def get_team_ids(scraper, code, retries_left=MAX_RETRIES):
    """Gets all box score IDs from games on the given date."""
    while retries_left > 0:
        # open the page
        url = f"http://stats.ncaa.org/reports/attendance?id={code}"
        soup = scraper.open_page(url=url)
        team_ids = []

        # inexplicably, sometimes the soup will exist but not return anything
        if soup is None:
            scraper.log(f"Soup did not return. (URL: {url})")
            soup = scraper.last_soup

        try:
            for el_link in soup.find_all('a', class_='skipMask'):
                team_ids.append(int(el_link.attrs['href'][-6:]))

            scraper.log(f"Finished parsing team IDs. {len(team_ids)} teams found.", 0)
            retries_left = 0
        except AttributeError as e:
            scraper.log(f"Error parsing team IDs: '{e}'")
            retries_left -= 1
            if retries_left <= 0:
                scraper.log(f"Done retrying. (Code: {code})", 0)
        sleep(CRAWL_DELAY)

    return team_ids


def get_specifics(team_ids, season, by_school_id=False):
    scraper = Scraper(thread_count=DEFAULT_THREAD_COUNT, verbose=VERBOSE)

    # open the files we're going to write to
    file_metadata = open(PATH_METADATA, 'w')
    file_players = open(PATH_PLAYERS, 'w')

    # get number relating to the current year
    season_code = [division['code'] for division in YEAR_DIVISIONS if division['year'] == season][0]

    # create teams out of all the team IDs
    teams = [Team(scraper, team_id, season_code) for team_id in team_ids]

    # get the box and write to file for each game
    for team in teams:
        team.get_school_id()
        team.write_to_file(file_metadata, file_players)
        sleep(CRAWL_DELAY)

    scraper.log(f"Finished writing to file.", 0)

    # close the files
    file_metadata.close()
    file_players.close()
    scraper.log("Finished scraping all teams.", 0)


### ACTUAL STUFF ###


if __name__ == '__main__':
    # main(2019)
    # main(sys.argv[1:])
    get_specifics(ALL_TEAMS_2019[273:274], 2019)
