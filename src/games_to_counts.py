### IMPORTS ###


import csv
import numpy as np
import datetime
import math


### CONSTANTS ###


PATH_OPEN_GAMES = "out/games 19.csv"
PATH_OPEN_BOXES = "out/boxes 19.csv"
PATH_OPEN_PLAYS = "out/plays 19.csv"
PATH_OPEN_TEAMS = "out/teams 19.csv"
PATH_OPEN_PLAYERS = "out/players 19.csv"

POSSESSION_INDICATORS = ["turnover", "shot", "free throw", "assist"]
ELIGIBILITY_YEARS = ["Fr", "So", "Jr", "Sr"]
POSITIONS = ["G", "F", "C"]

DAY_ZERO = datetime.datetime(2018, 10, 1)


### CLASSES ###


class Game:
    def __init__(self, metadata, boxes, plays):
        self.metadata = metadata
        self.boxes = boxes
        self.plays = plays
        self.league = None
        self.box_totals = [[0] * 15, [0] * 15]

    def __str__(self):
        return f"{self.metadata[1]}: {self.metadata[2]} vs. {self.metadata[3]}, {self.metadata[4]}"

    def preprocess(self):
        self.metadata[0] = int(self.metadata[0])
        self.metadata[1] = int(self.metadata[1])
        self.metadata[6] = int(self.metadata[6])
        self.metadata[10] = int(self.metadata[10])

        # convert the timestamp to an actual datetime object. if no time is listed, say it happened at 6pm
        if ":" not in self.metadata[4]:
            self.metadata[4] += " 06:00 PM"
        self.metadata[4] = datetime.datetime.strptime(self.metadata[4], "%m/%d/%Y %I:%M %p")

        # identify which teams are in the metadata
        self.metadata[2] = self.league.teams[self.league.identify_team(self.metadata[2])]
        self.metadata[3] = self.league.teams[self.league.identify_team(self.metadata[3])]

        for box in self.boxes:
            for i in range(0, 3):
                box[i] = int(box[i])
            for i in range(4, 21):
                box[i] = int(box[i])
            for i in range(6, 21):
                if box[4]:
                    self.box_totals[1][i - 6] += box[i]
                else:
                    self.box_totals[0][i - 6] += box[i]

            # rearrange players' names so their first names go before their last, then identify them on the team
            index_comma = box[3].rfind(", ")
            if index_comma > 0:
                box[3] = f"{box[3][index_comma + 2:]} {box[3][:index_comma]}".strip()
            if box[4]:
                box[2] = self.metadata[2].identify_player(box[3], box[2])
            else:
                box[2] = self.metadata[3].identify_player(box[3], box[2])

        for play in self.plays:
            for i in range(0, 8):
                play[i] = int(play[i])
            for i in range(11, 15):
                play[i] = int(play[i])
            play[16] = int(play[16])

            # identify players
            if play[7]:
                play[16] = self.metadata[2].identify_player(play[15], play[16])
            else:
                play[16] = self.metadata[3].identify_player(play[15], play[16])
            for i in range(17, 22):
                play[i] = self.metadata[2].identify_player(play[i], None)
            for i in range(22, 27):
                play[i] = self.metadata[3].identify_player(play[i], None)

    def count_possessions(self):
        """Counts the number of possessions in the game and time of possession for each team."""
        last_t1_has_ball = None
        last_time = 1200
        possessions = 0
        previous = None
        changes_allowed = True

        top_t1 = 0
        top_t2 = 0

        for play in self.plays:
            # re-allow changes if time has passed since the last play
            if play[3] != last_time:
                changes_allowed = True

            # if a new period begins:
            if play[3] > last_time:
                # check if the previous period had a ghost possession
                if had_possession_after(previous, last_t1_has_ball):
                    possessions += 1

                # increment those last several seconds
                if last_t1_has_ball == had_possession_after(previous, last_t1_has_ball):
                    top_t2 += last_time
                else:
                    top_t1 += last_time

                # reset the possessing team
                last_t1_has_ball = None

                # set the last time 1200, or 300 if it is an overtime period
                if play[3] <= 300:
                    last_time = 300
                else:
                    last_time = 1200

            # if the play indicates who has possession:
            if t1_has_ball(play) is not None:
                # add to possession length for the appropriate team
                if t1_has_ball(play):
                    top_t1 += last_time - play[3]
                else:
                    top_t2 += last_time - play[3]
                last_time = play[3]

                # if the team with the ball has changed, increment possessions
                if (t1_has_ball(play) != last_t1_has_ball) and changes_allowed:
                    last_t1_has_ball = t1_has_ball(play)
                    possessions += 1
                    changes_allowed = False

            previous = play

        # increment one more time if the final period had a ghost possession
        if had_possession_after(previous, last_t1_has_ball):
            possessions += 1
            if last_t1_has_ball:
                top_t2 += last_time
            else:
                top_t1 += last_time

        return possessions, top_t2, top_t1

    def create_output(self):
        """Outputs summary statistics of the game that we care about."""
        output = [None] * 39
        output[0] = self.metadata[1]
        output[1] = self.metadata[3].metadata[1]
        output[2] = self.metadata[2].metadata[1]
        output[3] = (self.metadata[4] - DAY_ZERO).days  # distance from october 1st, 2018, aka 'day zero'

        # set HCA to 0 if neutral (imperfectly measured as non-standard arena) or 1 if home
        if self.metadata[5] != "":
            output[4] = 0
        else:
            output[4] = 1

        output[5] = self.box_totals[0][6]  # pts
        output[6] = self.box_totals[1][6]

        # get possessions from PBP and check it against an estimate of FGA - ORB + TOV + 0.45 * FTA. if the totals are
        #       different by more than 15 (7.5 possessions per team), go with the box score
        poss_tuple = self.count_possessions()
        possessions = poss_tuple[0]
        box_possessions = self.box_totals[0][1] + self.box_totals[1][1] - \
                          (self.box_totals[0][7] + self.box_totals[1][7]) + \
                          self.box_totals[0][11] + self.box_totals[1][11] + \
                          0.45 * (self.box_totals[0][5] + self.box_totals[1][5])
        if abs(box_possessions - possessions) > 10:
            output[7] = round(box_possessions / 2)
        else:
            output[7] = round((possessions + box_possessions) / 4)

        # calculate the number of minutes in the game
        if self.plays[-1][2] > 1:
            output[10] = 35 + self.plays[-1][2] * 5
        else:
            output[10] = 40

        # if the time of possession for either team is implausibly low, set each TOP to half the seconds in the game
        if (poss_tuple[1] < 800) | (poss_tuple[2] < 800):
            output[8] = output[10] * 30
            output[9] = output[10] * 30
        # otherwise, just make it add up to the right number
        else:
            output[8] = round(poss_tuple[1] / (poss_tuple[1] + poss_tuple[2]) * output[10] * 60)
            output[9] = round(poss_tuple[2] / (poss_tuple[1] + poss_tuple[2]) * output[10] * 60)

        # calculate the proportion of 2pm and 2pa that were short versus long
        s2m_h = sum([play[11] for play in self.plays if
                     (not play[7]) and (play[8] == "shot") and (play[10] == "short 2")])
        s2a_h = sum([1 for play in self.plays if (not play[7]) and (play[8] == "shot") and (play[10] == "short 2")])
        l2m_h = sum([play[11] for play in self.plays if
                     (not play[7]) and (play[8] == "shot") and (play[10] == "long 2")])
        l2a_h = sum([1 for play in self.plays if (not play[7]) and (play[8] == "shot") and (play[10] == "long 2")])
        s2m_a = sum([play[11] for play in self.plays if
                     play[7] and (play[8] == "shot") and (play[10] == "short 2")])
        s2a_a = sum([1 for play in self.plays if play[7] and (play[8] == "shot") and (play[10] == "short 2")])
        l2m_a = sum([play[11] for play in self.plays if
                     play[7] and (play[8] == "shot") and (play[10] == "long 2")])
        l2a_a = sum([1 for play in self.plays if play[7] and (play[8] == "shot") and (play[10] == "long 2")])
        if s2m_h + l2m_h == 0:
            s2m_h = 1
            l2m_h = 1
        if s2a_h + l2a_h == 0:
            s2a_h = 1
            l2a_h = 1
        if s2m_a + l2m_a == 0:
            s2m_a = 1
            l2m_a = 1
        if s2a_a + l2a_a == 0:
            s2a_a = 1
            l2a_a = 1

        output[11] = round((self.box_totals[0][0] - self.box_totals[0][2]) * s2m_h / (s2m_h + l2m_h))
        output[12] = round((self.box_totals[1][0] - self.box_totals[1][2]) * s2m_a / (s2m_a + l2m_a))
        output[13] = round((self.box_totals[0][0] - self.box_totals[0][2]) * l2m_h / (s2m_h + l2m_h))
        output[14] = round((self.box_totals[1][0] - self.box_totals[1][2]) * l2m_a / (s2m_a + l2m_a))
        output[19] = round((self.box_totals[0][1] - self.box_totals[0][3]) * s2a_h / (s2a_h + l2a_h))
        output[20] = round((self.box_totals[1][1] - self.box_totals[1][3]) * s2a_a / (s2a_a + l2a_a))
        output[21] = round((self.box_totals[0][1] - self.box_totals[0][3]) * l2a_h / (s2a_h + l2a_h))
        output[22] = round((self.box_totals[1][1] - self.box_totals[1][3]) * l2a_a / (s2a_a + l2a_a))

        output[15] = self.box_totals[0][2]  # 3pm
        output[16] = self.box_totals[1][2]
        output[23] = self.box_totals[0][3]  # 3pa
        output[24] = self.box_totals[1][3]
        output[17] = self.box_totals[0][4]  # ftm
        output[18] = self.box_totals[1][4]
        output[25] = self.box_totals[0][5]  # fta
        output[26] = self.box_totals[1][5]
        output[27] = self.box_totals[0][10]  # ast
        output[28] = self.box_totals[1][10]
        output[29] = self.box_totals[0][11]  # tov
        output[30] = self.box_totals[1][11]
        output[31] = self.box_totals[0][12]  # stl
        output[32] = self.box_totals[1][12]
        output[33] = self.box_totals[0][13]  # blk
        output[34] = self.box_totals[1][13]
        output[35] = self.box_totals[0][7]  # orb
        output[36] = self.box_totals[1][7]
        output[37] = self.box_totals[0][7] + self.box_totals[1][8]  # rop
        output[38] = self.box_totals[1][7] + self.box_totals[0][8]

        return output


class Team:
    def __init__(self, metadata, players):
        self.metadata = metadata
        self.players = players

        self.preprocess()

        self.player_ids = [int(player[3]) for player in players]
        self.player_names = [player[4] for player in players]

    def __str__(self):
        return f"{self.metadata[1]}: {self.metadata[2]}"

    def preprocess(self):
        """Turns stringified numbers into numbers, puts people's first names ahead of their last names, converts
        people's positions and eligibility years into numbers, and converts their heights into a number of inches."""
        for player in self.players:
            player[0] = int(player[0])
            player[1] = int(player[1])
            player[3] = int(player[3])
            player[5] = int(player[5])

            # rearrange players' names so their first names go before their last
            index_comma = player[4].rfind(", ")
            if index_comma > 0:
                player[4] = f"{player[4][index_comma + 2:]} {player[4][:index_comma]}".strip()

            # turn players' eligibility years into a number so Fr = 0, So = 1, Jr = 2, Sr = 3, error = -1
            if player[8] in ELIGIBILITY_YEARS:
                player[8] = ELIGIBILITY_YEARS.index(player[8])
            else:
                player[8] = -1

            # turn players' positions into a number so G = 0, F = 1, C = 2, error = -1
            if player[6] in POSITIONS:
                player[6] = POSITIONS.index(player[6])
            else:
                player[6] = -1

            # calculate players' heights as a number of inches. any time the height is below 5-0 or can't be
            #       calculated is an error and is replaced with -1
            try:
                index_dash = player[7].find("-")
                player[7] = int(player[7][:index_dash]) * 12 + int(player[7][index_dash + 1:])
            except ValueError:
                player[7] = -1
            if player[7] < 60:
                player[7] = -1

        self.metadata[0] = int(self.metadata[0])
        self.metadata[1] = int(self.metadata[1])
        self.metadata[5] = int(self.metadata[5])
        self.metadata[7] = int(self.metadata[7])

    def identify_player(self, name, player_id):
        """Get the index of the player in the list of players by searching for matching player ID or name. Raise a
        ValueError if it can't be found."""
        if self.metadata[1] == -1:
            return 0

        if player_id in self.player_ids:
            return self.player_ids.index(player_id)
        elif name in self.player_names:
            return self.player_names.index(name)
        else:
            return 0


class League:
    def __init__(self, games, teams):
        self.games = games
        self.teams = teams
        self.team_names = [team.metadata[2] for team in teams]

    def empty_array(self):
        """Creates an ndarray of zeros with one row and column per team."""
        return np.zeros(shape=(len(self.teams), len(self.teams)), dtype=np.uint16)

    def identify_team(self, name):
        if name in self.team_names:
            return self.team_names.index(name)
        else:
            return 0


### FUNCTIONS ###


def main():
    file_games = open(PATH_OPEN_GAMES, 'r')
    file_boxes = open(PATH_OPEN_BOXES, 'r')
    file_plays = open(PATH_OPEN_PLAYS, 'r')
    file_teams = open(PATH_OPEN_TEAMS, 'r')
    file_players = open(PATH_OPEN_PLAYERS, 'r')

    reader_games = csv.reader(file_games)
    reader_boxes = csv.reader(file_boxes)
    reader_plays = csv.reader(file_plays)
    reader_teams = csv.reader(file_teams)
    reader_players = csv.reader(file_players)

    # get the games and teams and put them into the league
    games = separate_games(reader_games, reader_boxes, reader_plays)
    teams = separate_teams(reader_teams, reader_players)
    league = League(games, teams)
    for game in games:
        game.league = league
        game.preprocess()

    file_games.close()
    file_boxes.close()
    file_plays.close()
    file_teams.close()
    file_players.close()

    with open('out/counts 19.csv', 'w') as f:
        for game in league.games:
            output = game.create_output()
            str_output = ""
            for item in output:
                str_output += str(item) + ","
            f.write(str_output[:-1] + "\n")


def separate_games(reader_games, reader_boxes, reader_plays):
    """Separate game metadata, boxes, and plays into atomic Game objects and return all the games."""
    games = []
    queue_boxes = []
    queue_plays = []

    # add each game to games as a Game object that tracks metadata, boxes, and plays
    for metadata in reader_games:
        game_id = int(metadata[1])

        # add rows of plays to a list until one has the wrong PBP ID, then add that row to the queue for the next
        next_found = False
        while not next_found:
            was_excepted = False
            try:
                box = next(reader_boxes)
                if int(box[1]) == game_id:
                    queue_boxes.append(box)
            except StopIteration:
                was_excepted = True
            if (int(box[1]) != game_id) | was_excepted:
                next_found = True
                current_boxes = queue_boxes
                queue_boxes = [box]

        # add rows of boxes to a list until one has the wrong PBP ID, then add that row to the queue for the next
        next_found = False
        while not next_found:
            was_excepted = False
            try:
                play = next(reader_plays)
                if int(play[1]) == game_id:
                    queue_plays.append(play)
            except StopIteration:
                was_excepted = True
            if (int(play[1]) != game_id) | was_excepted:
                next_found = True
                current_plays = queue_plays
                queue_plays = [play]
                games.append(Game(metadata, current_boxes, current_plays))

    return games


def separate_teams(reader_teams, reader_players):
    """Separate team metadata and players into atomic Team objects and return all the teams."""
    teams = [Team([-1, -1, "unrated", "unrated", "unrated", -1, "unrated", 0],
                  [[-1, -1, "unrated", -1, "unrated", "-1", "G", "0-0", "Fr"]])]
    queue_players = [[-1, -1, "unrated", -1, "unrated", "-1", "G", "0-0", "Fr"]]

    # add each team to teams as a Team object that tracks metadata and players
    for metadata in reader_teams:
        team_id = int(metadata[1])

        # add rows of players to a list until one has the wrong team ID, then add that row to the queue for the next
        next_found = False
        while not next_found:
            was_excepted = False
            try:
                player = next(reader_players)
                if int(player[1]) == team_id:
                    queue_players.append(player)
            except StopIteration:
                was_excepted = True
            if (int(player[1]) != team_id) | was_excepted:
                next_found = True
                current_players = queue_players
                queue_players = [[-1, -1, "unrated", -1, "unrated", "-1", "G", "0-0", "Fr"], player]
                teams.append(Team(metadata, current_players))

    return teams


def t1_has_ball(play):
    """Returns which team must have had the ball at the time the play happened."""
    if play[8] in ["turnover", "shot", "free throw", "assist"]:
        return bool(play[7])
    elif play[8] in ["steal", "block"]:
        return not bool(play[7])
    elif play[8] in ["rebound", "jump ball"]:
        return play[7] == play[11]
    else:
        return None


def had_possession_after(play, last_was_t1):
    """Returns true if there was a possession at the end of the period that did not contain a countable action."""
    if play[3] <= 5:
        return False
    elif play[8] in ["assist", "turnover", "steal", "free throw"]:
        return True
    elif play[8] == "shot":
        return bool(play[11])
    elif play[8] == "rebound":
        return not bool(play[11])
    elif play[8] in ["foul received", "possession arrow"]:
        return play[7] != last_was_t1
    elif play[8] == "foul committed":
        return play[7] == last_was_t1


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
