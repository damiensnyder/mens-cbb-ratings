### IMPORTS ###


import csv
import re
import MySQLdb
import datetime


### CONSTANTS ###


PATH_OPEN_METADATA = "out/metadata 2018-19.csv"
PATH_OPEN_BOXES = "out/boxes 2018-19.csv"
PATH_OPEN_PLAYS = "out/plays 2018-19.csv"

PATH_WRITE_METADATA = "out/games 19.csv"
PATH_WRITE_BOXES = "out/boxes 19.csv"
PATH_WRITE_PLAYS = "out/plays 19.csv"

DUMMY_PLAY_STRING = "0,1200,30,0,0,0,\"jump ball\",\"\",\"\",1,0,0,0,\"unrated\",-1,\"unrated\",\"unrated\"," \
                    "\"unrated\",\"unrated\",\"unrated\",\"unrated\",\"unrated\",\"unrated\",\"unrated\",\"unrated\"," \
                    "\"unrated\""

DB_HOST = "localhost"
DB_USERNAME = "damiensn_mcbbP"
DB_PASSWORD = "A6HbdpQgcIze3KPX"
DB_DATABASE = "damiensn_mcbb"

SQL_METADATA = "INSERT INTO parsed_metadata (box_id, pbp_id, team_1, team_2, timestamp, location, attendance, ref_1," \
               "ref_2, ref_3, exhibition) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE" \
               "box_id=box_id"
SQL_PLAYS = "INSERT INTO parsed_plays (play_id, box_id, pbp_id, team_1, team_2, timestamp, period, time, shot_clock," \
            "score_1, score_2, is_team_1, action, flag_1, flag_2, flag_3, flag_4, flag_5, flag_6, player, player_id," \
            "partic_11, partic_12, partic_13, partic_14, partic_15, partic_21, partic_22, partic_23, partic_24," \
            "partic_25) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE play_id=play_id"

VERBOSE = 2


### CLASSES ###


class Game:
    """A game accepts metadata, box scores, and play-by-play and parses it into database-uploadable metadata and
    play events.
    """

    def __init__(self, plays, boxes, metadata):
        self.plays = plays
        self.boxes = boxes
        self.metadata = metadata
        self.parsed_plays = []

    def parse_game(self):
        """Calls all the other methods, pretty much."""
        self.remove_non_ascii()
        self.parse_all_plays()
        self.track_shot_clock()
        self.track_partic()
        self.correct_minutes()

    def remove_non_ascii(self):
        """Remove non-ASCII characters from each player's name."""
        for box in self.boxes:
            box[4] = re.sub('[^\x00-\x7f]', '', box[4])

    def parse_all_plays(self):
        """Parse all the plays and adds them to self.parsed_plays."""
        for play in self.plays:
            try:
                parsed = parse_play_row(play)
            except ValueError as e:
                log(f"WARNING: {e}")
                parsed = None

            if parsed is not None:
                # if a player does match, get their identity. otherwise, create a new one
                try:
                    player_identity = self.identify_player(parsed['player'], parsed['is team 1'])
                except KeyError:
                    player_identity = self.add_fake_player(parsed['player'], parsed['is team 1'])

                parsed['player'] = player_identity['name']
                if player_identity['player ID'] is not None:
                    parsed['player ID'] = player_identity['player ID']

                self.parsed_plays.append(parsed)

    def track_shot_clock(self):
        """Track how many seconds were on the shot clock when each event happened."""
        shot_clock_end = 1170
        shot_clock = 30
        last_play_time = 1200

        for play in self.parsed_plays:
            if play['time'] != last_play_time:
                shot_clock = max(play['time'] - shot_clock_end, 0)
            play['shot clock'] = shot_clock
            last_play_time = play['time']

            if play['action'] == "jump ball":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "possession arrow":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "shot":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "rebound":
                if play['offensive']:
                    shot_clock_end = max(play['time'] - 20, 0)
                else:
                    shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "turnover":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "steal":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "foul committed":
                shot_clock_end = max(play['time'] - 30, 0)
            elif play['action'] == "free throw":
                shot_clock_end = max(play['time'] - 30, 0)

    def track_partic(self):
        """Track which players were on the court during each play."""
        partic1 = []
        partic2 = []
        backfilled = []
        last_period = 0
        last_time = 1200
        last_partic1 = []
        last_partic2 = []
        jr = False

        # go for the front and make a list of players known so far
        for play in self.parsed_plays:
            player = play['player']

            # reset everything at the start of each period
            if play['period'] != last_period:
                partic1 = []
                partic2 = []
                backfilled = []
                last_period = play['period']
                last_time = 1200
                last_partic1 = []
                last_partic2 = []

            # don't update subs until the clock changes
            if play['time'] != last_time:
                last_time = play['time']
                last_partic1 = partic1.copy()
                last_partic2 = partic2.copy()

            play['partic1'] = last_partic1
            play['partic2'] = last_partic2

            # no need to change participation if no player did this action
            if (player != "Floor") and (player != "Team"):
                if play['is team 1']:
                    subbed_in = (play['action'] == "substitution") and play['in']
                    if ((player not in last_partic1) or subbed_in) and (player not in partic1):
                        partic1.append(player)

                    if (player not in backfilled) and not subbed_in:
                        for prev_play in self.parsed_plays:
                            if 'partic1' in prev_play:
                                if (prev_play['period'] == play['period']) \
                                        and (prev_play['time'] >= play['time']) \
                                        and (player not in prev_play['partic1']):
                                    prev_play['partic1'].append(player)

                        # update subs
                        last_partic1 = partic1.copy()
                        last_partic2 = partic2.copy()

                    if (player not in backfilled) or subbed_in:
                        backfilled.append(player)

                    # remove them from the list if they were substituted out
                    if (play['action'] == "substitution") and not play['in'] and (player in partic1):
                        partic1.remove(player)
                else:
                    subbed_in = (play['action'] == "substitution") and play['in']
                    if ((player not in last_partic2) or subbed_in) and (player not in partic2):
                        partic2.append(player)

                    if (player not in backfilled) and not subbed_in:
                        for prev_play in self.parsed_plays:
                            if 'partic2' in prev_play:
                                if (prev_play['period'] == play['period']) \
                                        and (prev_play['time'] >= play['time']) \
                                        and (player not in prev_play['partic2']):
                                    prev_play['partic2'].append(player)

                        # update subs
                        last_partic1 = partic1.copy()
                        last_partic2 = partic2.copy()

                    if (player not in backfilled) or subbed_in:
                        backfilled.append(player)

                    # remove them from the list if they were substituted out
                    if (play['action'] == "substitution") and not play['in'] and (player in partic2):
                        partic2.remove(player)

    def correct_minutes(self):
        """Fix instances with not exactly 5 players per team by checking who is listed as playing too many minutes or
        too few."""
        # some hideous list comprehension that makes dicts of player name -> box minutes for each team
        first_team = self.boxes[0][2]
        if minutes_to_seconds(self.boxes[0][7]) > minutes_to_seconds(self.boxes[0][6]):
            player_list = [[rearrange_comma(player[4]),
                            minutes_to_seconds(player[7]),
                            player[2]] for player in self.boxes]
        else:
            player_list = [[rearrange_comma(player[4]),
                            minutes_to_seconds(player[6]),
                            player[2]] for player in self.boxes]
        player_minutes1 = dict([player[0:2] for player in player_list
                                if (player[0] is not None) and (player[2] == first_team)])
        player_minutes2 = dict([player[0:2] for player in player_list
                                if (player[0] is not None) and (player[2] != first_team)])

        # calculate the number of minutes inferred from play-by-play compared to the box score listing
        last_time = 1200
        last_period = 0
        for play in self.parsed_plays:
            if play['period'] == last_period:
                time_diff = last_time - int(play['time'])
            else:
                time_diff = last_time

            last_time = int(play['time'])
            last_period = play['period']

            try:
                for player in play['partic1']:
                    player_minutes1[player] -= time_diff
                for player in play['partic2']:
                    player_minutes2[player] -= time_diff
            except KeyError:
                pass

        last_time = 1200
        last_period = 0

        # add and remove players who are logged as playing too many or too few minutes if more or fewer than 5 people
        #       are on the court for each team
        for play in self.parsed_plays:
            partic1 = play['partic1']
            partic2 = play['partic2']
            if play['period'] == last_period:
                time_diff = last_time - int(play['time'])
            else:
                time_diff = last_time

            last_time = int(play['time'])
            last_period = play['period']

            # add the players with the most unaccounted minutes to partic1 if there are less than 5
            while len(partic1) < 5:
                max_player = None
                max_minutes = -10000  # large number

                for player in player_minutes1:
                    if (player not in partic1) and (player_minutes1[player] > max_minutes):
                        max_player = player
                        max_minutes = player_minutes1[player]

                try:
                    partic1.append(max_player)
                    player_minutes1[max_player] -= time_diff
                except KeyError:
                    log(self.boxes, 0)
                    log(player_minutes1, 0)
                    exit(2)

            # add the players with the most unaccounted minutes to partic2 if there are less than 5
            while len(partic2) < 5:
                max_player = None
                max_minutes = -10000  # large number

                for player in player_minutes2:
                    if (player not in partic2) and (player_minutes2[player] > max_minutes):
                        max_player = player
                        max_minutes = player_minutes2[player]

                partic2.append(max_player)
                player_minutes2[max_player] -= time_diff

            # remove the players with the most over-accounted minutes from partic1 if there are more than 5
            while len(partic1) > 5:
                min_player = None
                min_minutes = 10000  # large number

                for player in player_minutes1:
                    if (player in partic1) and (player_minutes1[player] < min_minutes):
                        min_player = player
                        min_minutes = player_minutes1[player]

                partic1.remove(min_player)
                player_minutes1[min_player] += time_diff

            # remove the players with the most over-accounted minutes from partic2 if there are more than 5
            while len(partic2) > 5:
                min_player = None
                min_minutes = 10000  # large number

                for player in player_minutes2:
                    if (player in partic2) and (player_minutes2[player] < min_minutes):
                        min_player = player
                        min_minutes = player_minutes2[player]

                partic2.remove(min_player)
                player_minutes2[min_player] += time_diff

    def upload_to_db(self, cursor):
        """Upload the parsed plays and metadata to the database."""
        cursor.execute(SQL_METADATA, self.metadata_to_tuple())

        i = 0
        for play in self.parsed_plays:
            cursor.execute(SQL_PLAYS, self.play_to_tuple(play, i))
            i += 1

    def write_to_csv(self, file_metadata, file_boxes, file_plays):
        """Write the parsed plays and metadata to CSV files."""
        string_metadata = ""
        for column in self.metadata_to_tuple():
            if isinstance(column, int):
                string_metadata += f"{column},"
            else:
                string_metadata += f"\"{column}\","
        file_metadata.write(string_metadata[:-1] + "\n")

        i = 0
        for box in self.boxes:
            string_box = ""
            for column in self.box_to_tuple(box, i):
                if isinstance(column, int):
                    string_box += f"{column},"
                else:
                    string_box += f"\"{column}\","
            file_boxes.write(string_box[:-1] + "\n")
            i += 1

        i = 0
        for play in self.parsed_plays:
            if play['action'] not in ["timeout", "substitution"]:
                string_play = ""
                for column in self.play_to_tuple(play, i):
                    if isinstance(column, int):
                        string_play += f"{column},"
                    else:
                        string_play += f"\"{column}\","
                file_plays.write(string_play[:-1] + "\n")
                i += 1

        # if there were no plays at all, write two dummy plays to file so things don't break
        if i == 0:
            file_plays.write(f"{self.metadata[1]}000,{self.metadata[1]},{DUMMY_PLAY_STRING}\n"
                             f"{self.metadata[1]}001,{self.metadata[1]},{DUMMY_PLAY_STRING}\n")

    def metadata_to_tuple(self):
        """Convert the metadata to something uploadable to the database."""
        index_attendance = self.metadata[4].find("Attendance: ")
        if index_attendance == -1:
            attendance = 0
        else:
            str_attendance = self.metadata[4][index_attendance + 12:]
            attendance = int(str_attendance.replace(",", ""))

        index_timestamp = re.search("[0-1]", self.metadata[4]).start()
        timestamp = self.metadata[4][index_timestamp:index_timestamp + 19].replace(" TBA     ", "")

        # get the location of the game, if it exists, and replace any quotes in the name with apostrophes
        index_location = self.metadata[4].find("Location: ")
        if index_location == -1:
            location = ""
        else:
            location = self.metadata[4][index_location + 10:index_attendance].strip().replace("\"", "'")

        index_ref2 = self.metadata[5].find("   ")
        index_ref3 = self.metadata[5].rfind("   ")
        ref1 = self.metadata[5][:index_ref2].replace("  ", " ")
        ref2 = self.metadata[5][index_ref2:index_ref3].strip().replace("  ", " ")
        ref3 = self.metadata[5][index_ref3 + 2:].strip().replace("  ", " ")

        # check whether the game was an exhibition game
        exhibition = 0
        if " <i>" in self.metadata[2]:
            exhibition = 1
            self.metadata[2] = self.metadata[2][:self.metadata[2].index(" <i>")]
        if " <i>" in self.metadata[3]:
            exhibition = 1
            self.metadata[3] = self.metadata[3][:self.metadata[3].index(" <i>")]

        return int(self.metadata[0]), int(self.metadata[1]), self.metadata[2], self.metadata[3], \
               timestamp, location, attendance, ref1, ref2, ref3, exhibition

    def box_to_tuple(self, box, i):
        """Convert a row of box score into something uploadable from the database."""
        # create a unique box ID for the row
        box_id = int(self.metadata[1]) * 100 + i

        # assign the team a player ID of -2
        if box[3] == "":
            box[3] = -2

        # checks if the player plays for the first team listed in the metadata
        is_team_1 = (box[2] != self.metadata[3]) and (self.metadata[2] in box[2])
        if not is_team_1 and not (self.metadata[3] in box[2]):
            log(str(self.metadata), 1)  # log if the team in the box cannot be matched to either team

        # collects all the counting stats into a single tuple
        box_counting_stats = ()
        for item in box[8:23]:
            try:
                item = int(item)
            except ValueError:
                item = 0
            box_counting_stats += tuple([item])

        return (box_id, int(self.metadata[1]), int(box[3]), box[4], int(is_team_1), minutes_to_seconds(box[7])) + \
               box_counting_stats

    def play_to_tuple(self, play, i):
        """Convert a parsed play into something uploadable to the database."""
        play_id = int(self.metadata[1]) * 1000 + i

        score_1 = play['score 1']
        score_2 = play['score 2']
        if score_1 is None:
            score_1 = 0
        if score_2 is None:
            score_2 = 0

        flag_1 = ""
        flag_2 = ""
        flag_3 = 0
        flag_4 = 0
        flag_5 = 0
        flag_6 = 0

        if "success" in play:
            flag_3 = play['success']
        if ("type" in play) and (play['type'] is not None):
            flag_1 = play['type']

        if play['action'] == "jump ball":
            flag_3 = int(play['success'])
        elif play['action'] == "shot":
            flag_2 = str(play['length'])
            flag_4 = int(play['second chance'] is True)
            flag_5 = int(play['fast break'] is True)
            flag_6 = int(play['blocked'] is True)
        elif play['action'] == "rebound":
            flag_3 = int(play['offensive'] is True)
        elif play['action'] == "substitution":
            flag_3 = int(play['in'] is True)

        player_id = 0
        if "player ID" in play:
            player_id = play['player ID']

        return play_id, int(self.metadata[1]), play['period'], int(play['time']), int(play['shot clock']), \
               score_1, score_2, int(play['is team 1']), play['action'], flag_1, flag_2, int(flag_3), int(flag_4), \
               int(flag_5), int(flag_6), play['player'], player_id, play['partic1'][0], play['partic1'][1], \
               play['partic1'][2], play['partic1'][3], play['partic1'][4], play['partic2'][0], \
               play['partic2'][1], play['partic2'][2], play['partic2'][3], play['partic2'][4]

    def identify_player(self, player, is_team_1):
        """Given a player name, identifies which player in the box score matches and gets their ID. If no one matches
        well, throws an error so a new "fake" player can be created.
        """
        if (player == "Team") | (player == "Floor"):
            return {
                'name': player,
                'player ID': None
            }
        else:
            players = []
            player_ids = []
            for box in self.boxes:
                rearranged = rearrange_comma(box[4])
                # if it is a real name and they are on the right team
                if (rearranged is not None) and ((box[2] == self.boxes[0][2]) == is_team_1):
                    players.append(rearranged)
                    if box[3] == "":
                        player_ids.append(-1)
                    else:
                        player_ids.append(int(box[3]))
            if player in players:
                index_player = players.index(player)
                return {
                    'name': players[index_player],
                    'player ID': player_ids[index_player]
                }
            else:
                similarities = [similarity_score(player, box_player) for box_player in players]
                if max(similarities) < 5:
                    raise KeyError(f"Unidentified player: '{player}'")
                index_player = similarities.index(max(similarities))
                return {
                    'name': players[index_player],
                    'player ID': player_ids[index_player]
                }

    def add_fake_player(self, name, is_team_1):
        """Adds a player who should be in the box scores but isn't."""
        # un-rearrange the comma
        index_space = name.find(" ")
        fake_name = name[index_space + 1:] + ", " + name[0:index_space]

        # create a fake row the size of real rows. (not necessary to add all the extras for now)
        fake_row = [""] * 25

        # put the right team in
        fake_row[0:2] = self.boxes[0][0:2]
        if is_team_1:
            fake_row[2] = self.boxes[0][2]
        else:
            fake_row[2] = [row[2] for row in self.boxes if row[2] != self.boxes[0][2]][0]

        fake_row[3] = -1
        fake_row[4] = fake_name
        fake_row[6] = "0:00"
        self.boxes.append(fake_row)
        return {
            'name': name,
            'player ID': -1
        }


### FUNCTIONS ###


def main():
    with open(PATH_OPEN_PLAYS, 'r') as file_raw_plays, \
            open(PATH_OPEN_BOXES, 'r') as file_raw_boxes, \
            open(PATH_OPEN_METADATA, 'r') as file_raw_metadata:
        reader_plays = csv.reader(file_raw_plays)
        reader_boxes = csv.reader(file_raw_boxes)
        reader_metadata = csv.reader(file_raw_metadata)
        log("Separating games.", 1)
        games = separate_games(reader_metadata, reader_boxes, reader_plays)
        log("Writing all to CSV.", 1)
        write_all_to_csv(games)
        log("Finished writing to CSV.", 1)


def separate_games(reader_metadata, reader_boxes, reader_plays):
    """For each game in reader_metadata, find the corresponding box scores and plays and separate them from the rest.
    Then upload it to the database.
    """
    queue_boxes = []
    queue_plays = []
    games = []
    for row_metadata in reader_metadata:
        box_id = int(row_metadata[0])
        pbp_id = int(row_metadata[1])
        next_pbp_game_found = False
        next_box_game_found = False

        # add rows of box scores to a list until one has the wrong box ID, then add that row to the queue for the next
        while not next_box_game_found:
            was_excepted = False
            try:
                row_box = next(reader_boxes)
                if int(row_box[0]) == box_id:
                    queue_boxes.append(row_box)
            except StopIteration:
                was_excepted = True
            if (int(row_box[0]) != box_id) | was_excepted:
                next_box_game_found = True
                current_boxes = queue_boxes
                queue_boxes = [row_box]

        # add rows of plays to a list until one has the wrong PBP ID, then add that row to the queue for the next
        while not next_pbp_game_found:
            was_excepted = False
            try:
                row_pbp = next(reader_plays)
                if int(row_pbp[0]) == pbp_id:
                    queue_plays.append(row_pbp)
            except StopIteration:
                was_excepted = True
            if (int(row_pbp[0]) != pbp_id) | was_excepted:
                next_pbp_game_found = True
                current_plays = queue_plays
                queue_plays = [row_pbp]

        game = Game(current_plays, current_boxes, row_metadata)
        game.parse_game()
        games.append(game)

    return games


def upload_all_to_db(games):
    db = MySQLdb.connect(DB_HOST, DB_USERNAME, DB_PASSWORD, DB_DATABASE)
    cursor = db.cursor()
    for game in games:
        game.upload_to_db(cursor)
    db.commit()
    db.close()


def write_all_to_csv(games):
    file_metadata = open(PATH_WRITE_METADATA, 'w')
    file_boxes = open(PATH_WRITE_BOXES, 'w')
    file_plays = open(PATH_WRITE_PLAYS, 'w')
    for game in games:
        game.write_to_csv(file_metadata, file_boxes, file_plays)
    file_metadata.close()
    file_boxes.close()
    file_plays.close()


def rearrange_comma(name):
    """Take a name in the form 'Last, First' and return 'First Last'."""
    index_comma = name.rfind(",")
    if index_comma > 0:
        return name[index_comma + 2:] + " " + name[:index_comma]
    else:
        return None


def minutes_to_seconds(minutes):
    """Converts a string like 1:42 to the int 102."""
    index_semicolon = minutes.find(":")
    try:
        if index_semicolon < 1:
            return 0
        else:
            str_minutes = minutes[:index_semicolon]
            str_seconds = minutes[index_semicolon + 1:]
            return 60 * int(str_minutes) + int(str_seconds)
    except ValueError:
        return 0


def similarity_score(str1, str2):
    """Returns the number of 3-character substrings in str1 that occur in str2, and subtract the difference in
            length."""
    str1 = str1.lower().replace(".", "")
    str2 = str2.lower().replace(".", "")
    matching_3s = sum(1 for i in range(len(str1) - 2) if str1[i:i + 3] in str2)
    diff_length = abs(len(str1) - len(str2))
    return 2 * matching_3s - diff_length


def parse_play_row(row_play):
    """Put all the information in the row into a dict that contains parsed play information."""
    play_1 = row_play[3]
    play_2 = row_play[5]
    score = row_play[4]

    if (score != "Score") & ((len(play_1) > 0) | (len(play_2) > 0)):
        # check which team did the play
        if len(play_1) > 0:
            play = play_1
        else:
            play = play_2
        is_team_1 = play_1 != ""

        # get the notation style
        notation_info = get_notation_style(play)
        if notation_info['style'] == "semicolon":
            parsed_play = parse_semicolon_play(play, notation_info['player'])
        elif notation_info['style'] == "caps":
            parsed_play = parse_caps_play(notation_info['player'], notation_info['rest'])

        if parsed_play is None:
            return None

        # get the score
        if (type(score) == str) & ("-" in score):
            score_1 = int(score[:score.index("-")])
            score_2 = int(score[score.index("-") + 1:])
        else:
            score_1 = None
            score_2 = None

        # get the time remaining in the period, in seconds
        time_str = row_play[2]
        minutes = int(time_str[0:2])
        seconds = int(time_str[3:5])
        if len(time_str) > 5:
            centiseconds = int(time_str[6:9])
        else:
            centiseconds = 0
        seconds_remaining = 60 * minutes + seconds + 0.01 * centiseconds

        # get other information from the row
        parsed_play['game ID'] = row_play[0]
        parsed_play['period'] = int(row_play[1])
        parsed_play['time'] = seconds_remaining
        parsed_play['score 1'] = score_1
        parsed_play['score 2'] = score_2
        parsed_play['is team 1'] = is_team_1

        return parsed_play


def parse_caps_play(player, rest):
    """Parses a play in the 'caps' notation style."""
    # blocks
    if "blocked shot" in rest:
        return {
            'player': player,
            'action': "block"
        }

    # rebounds
    elif " rebound" in rest:
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

    # turnovers
    elif "turnover" in rest:
        return {
            'player': player,
            'action': "turnover"
        }

    # steals
    elif "steal" in rest:
        return {
            'player': player,
            'action': "steal"
        }

    # timeouts
    elif "timeout" in rest:
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

    # assists
    elif "assist" in rest:
        return {
            'player': player,
            'action': "assist"
        }

    # fouls committed
    elif "commits foul" in rest:
        return {
            'player': player,
            'action': "foul committed"
        }

    # substitutions
    elif " game" in rest:
        if "enters" in rest:
            subbed_in = True
        else:
            subbed_in = False

        return {
            'player': player,
            'action': "substitution",
            'in': subbed_in
        }

    # free throws
    elif "free throw" in rest:
        if "made" in rest:
            shot_made = True
        else:
            shot_made = False

        return {
            'player': player,
            'action': "free throw",
            'success': shot_made,
            'number': None,
            'out of': None
        }

    # shots
    elif (("missed " in rest) | ("made " in rest)) & ("free throw" not in rest):
        shot_made = " made " in rest

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
            'success': shot_made,
            'length': shot_length,
            'type': shot_type,
            'second chance': second_chance,
            'fast break': None,
            'blocked': None
        }

    # if it can't be solved, try parsing it as a semicolon play
    else:
        return parse_semicolon_play(rest, player)


def parse_semicolon_play(play, player):
    """Parses a play in the 'semicolon' notation style."""
    # starts of periods
    if ("period start" in play) | ("game start" in play):
        return None
        # return {
        #     'player': "Floor",
        #     'action': "period start"
        # }

    # jump ball thrown
    elif "jumpball startperiod" in play:
        return None
        # return {
        #     'player': "Floor",
        #     'action': "jump ball thrown"
        # }

    # ends of periods
    elif ("period end" in play) | ("game end" in play):
        return None
        # return {
        #     'player': "Floor",
        #     'action': "period end"
        # }

    # jump balls
    elif (", jumpball" in play) & ((" won" in play) | (" lost" in play)):
        jumpball_result = " won" in play

        return {
            'player': player,
            'action': "jump ball",
            'success': jumpball_result
        }

    # substitutions
    elif ", substitution" in play:
        subbed_in = " in" in play

        return {
            'player': player,
            'action': "substitution",
            'in': subbed_in
        }

    # possession arrow events
    elif "Team, jumpball" in play:
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

    # timeouts
    elif "timeout " in play:
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

    # fouls received
    elif ", foulon" in play:
        return {
            'player': player,
            'action': "foul received"
        }

    # fouls committed
    elif ", foul" in play:
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

    # blocks
    elif ", block" in play:
        return {
            'player': player,
            'action': "block"
        }

    # assists
    elif ", assist" in play:
        return {
            'player': player,
            'action': "assist"
        }

    # steals
    elif ", steal" in play:
        return {
            'player': player,
            'action': "steal"
        }

    # turnovers
    elif ", turnover" in play:
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

    # rebounds
    elif " rebound" in play:
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

    # 2-point field goal attempts
    elif ", 2pt" in play:
        if "pointsinthepaint" in play:
            shot_length = "short 2"
        else:
            shot_length = "long 2"

        shot_made = " made" in play
        second_chance = "2ndchance" in play
        fast_break = "fastbreak" in play
        blocked = "blocked" in play

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
            'success': shot_made,
            'length': shot_length,
            'type': shot_type,
            'second chance': second_chance,
            'fast break': fast_break,
            'blocked': blocked
        }

    # 3-point field goal attempts
    elif ", 3pt" in play:
        shot_made = " made" in play
        second_chance = "2ndchance" in play
        fast_break = "fastbreak" in play
        blocked = "blocked" in play

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
            'success': shot_made,
            'length': "3",
            'type': shot_type,
            'second chance': second_chance,
            'fast break': fast_break,
            'blocked': blocked
        }

    # free throw attempts
    elif ", freethrow" in play:
        shot_made = " made" in play

        index_ft = play.index(", freethrow")
        shot_number = int(play[index_ft + 12])
        shot_out_of = int(play[index_ft + 15])

        return {
            'player': player,
            'action': "free throw",
            'success': shot_made,
            'number': shot_number,
            'out of': shot_out_of
        }

    # errors
    else:
        raise ValueError(f"Unrecognized play type: '{play}'")


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
    elif (play[0:3] == "TM ") or ((play[0] >= "0") and (play[0] <= "9") and (play[1] >= "0") and (play[2] <= "9")):
        notation_style = "caps"
        player = "Team"
        while "  " in play:
            play = play.replace("  ", " ")
        rest = play[3:].lower()
    elif (index_comma2 > 0) | (index_comma1 < 0):
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


def log(message, verbosity=2):
    if verbosity < VERBOSE:
        print(datetime.datetime.strftime(datetime.datetime.now(), "%H:%M:%S: ") + message)


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
