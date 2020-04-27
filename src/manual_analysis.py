### IMPORTS


import pandas as pd
import numpy as np

### CONSTANTS ###


PATH_COUNTS = "out/counts 19.csv"
RAW_HEADER = ["PBP ID", "team ID 1", "team ID 2", "days", "HCA", "pts 1", "pts 2", "poss", "TPL 1", "TPL 2",
              "min", "S2M 1", "S2M 2", "L2M 1", "L2M 2", "3PM 1", "3PM 2", "FTM 1", "FTM 2", "S2A 1",
              "S2A 2", "L2A 1", "L2A 2", "3PA 1", "3PA 2", "FTA 1", "FTA 2", "ast 1", "ast 2", "tov 1",
              "tov 2", "stl 1", "stl 2", "blk 1", "blk 2", "ORB 1", "ORB 2", "opp 1", "opp 2"]

PARAM_MEAN_REGRESSION = 1000    # 400
PARAM_OFFENSE_INFLUENCE = 0.99  # 0.6
PARAM_HALF_LIFE = 200   # 200
PARAM_NON_D1_WEIGHT = 1   # 0.8
PARAM_DEFENSE_MULTIPLIER = 10    # 1

INDEX_T1_OCCS = 17  # 5
INDEX_T2_OCCS = 18  # 6
INDEX_T1_OPPS = 25  # 7
INDEX_T2_OPPS = 26  # 7

COEFF_DAYS = 0.0001
COEFF_INTERCEPT = 0.71 - 104.22 * COEFF_DAYS  # 1.03 - 104.22 * COEFF_DAYS


### CLASSES ###


class League:
    def __init__(self, games, teams):
        self.opportunities = np.zeros((2 * len(teams), 2 * len(teams)))
        self.deviations = np.zeros((2 * len(teams)))
        self.games = games
        self.teams = teams
        self.num_teams = len(teams)
        self.predictions = np.zeros((len(games), 2))
        self.solution = None

        # give starting ratings / regression to mean for everyone
        np.fill_diagonal(self.opportunities, PARAM_MEAN_REGRESSION)
        self.opportunities[self.num_teams:, :self.num_teams] += PARAM_MEAN_REGRESSION / self.num_teams
        self.opportunities[:self.num_teams, self.num_teams:] += PARAM_MEAN_REGRESSION / self.num_teams

        self.opportunities[:, self.num_teams:] *= PARAM_DEFENSE_MULTIPLIER

    def backpredict(self):
        """Create each team's rating at the start of each day of games and use them to predict the outcomes."""
        prev_day = 0
        i = 0

        for game in self.games:
            if int(game[3]) != prev_day:
                self.create_ratings()
                prev_day = int(game[3])

            self.predictions[i, ] = self.predict_game(game)
            self.add_game(game)
            i += 1

    def add_game(self, game):
        """Send the right information to the matrix updater."""
        weight = 2 ** (game[3] / PARAM_HALF_LIFE)
        if (game[1] == -1) or (game[2] == -1):
            weight *= PARAM_NON_D1_WEIGHT
        index1 = self.teams.index(game[1])
        index2 = self.teams.index(game[2])
        deviation1 = game_info_to_deviation(game[INDEX_T1_OCCS], game[INDEX_T1_OPPS], game[3])
        deviation2 = game_info_to_deviation(game[INDEX_T2_OCCS], game[INDEX_T2_OPPS], game[3])
        opportunities1 = game[INDEX_T1_OPPS]
        opportunities2 = game[INDEX_T2_OPPS]
        self.update_matrix(weight, index1, index2, deviation1, deviation2, opportunities1, opportunities2)

    def predict_game(self, game):
        """Return predictions for the number of occurrences for each team, using basic game info and the teams'
        ratings."""
        prediction1 = game_info_to_expectation(self.solution[self.teams.index(game[1])],
                                               self.solution[self.teams.index(game[2]) + len(self.teams)],
                                               game[INDEX_T1_OPPS],
                                               game[3])
        prediction2 = game_info_to_expectation(self.solution[self.teams.index(game[2])],
                                               self.solution[self.teams.index(game[1]) + len(self.teams)],
                                               game[INDEX_T2_OPPS],
                                               game[3])

        return np.asarray((prediction1, prediction2))

    def update_matrix(self, weight, index1, index2, deviation1, deviation2, opportunities1, opportunities2):
        """For each team, insert the number of opportunities their offense and defense had and their opponent's
        offense and defense had, and insert the deviation in the number of occurrences. Weight all of those figures by
        the given weight."""
        self.opportunities[index1, index1] += opportunities1 * weight
        self.opportunities[index2 + len(self.teams), index2 + len(self.teams)] += opportunities1 * weight
        self.opportunities[index1, index2 + len(self.teams)] += opportunities1 * weight
        self.opportunities[index2 + len(self.teams), index1] += opportunities1 * weight
        self.deviations[index1] += deviation1 * weight
        self.deviations[index2 + len(self.teams)] += deviation1 * weight

        self.opportunities[index2, index2] += opportunities2
        self.opportunities[index1 + len(self.teams), index1 + len(self.teams)] += opportunities2 * weight
        self.opportunities[index2, index1 + len(self.teams)] += opportunities2 * weight
        self.opportunities[index1 + len(self.teams), index2] += opportunities2 * weight
        self.deviations[index2] += deviation2 * weight
        self.deviations[index1 + len(self.teams)] += deviation2 * weight

    def create_ratings(self):
        """Create ratings for each team by solving the augmented matrix corresponding to each team's number of
        opportunities against each other and the amount of deviation above expectation they've had."""
        opportunities = self.opportunities.copy()
        deviations = self.deviations.copy()

        # attribute the proportion of influence to the offense given by the fixed parameter
        opportunities[-1, :self.num_teams] = 1
        opportunities[-1, self.num_teams:] = 0
        deviations[-1] = sum(deviations[:-1]) / sum(opportunities.diagonal()[:-1]) \
                         * self.num_teams * PARAM_OFFENSE_INFLUENCE

        self.solution = np.linalg.solve(opportunities, deviations)


### FUNCTIONS ###


def main():
    df_raw = pd.read_csv(PATH_COUNTS, header=None, names=RAW_HEADER)
    df_raw.sort_values(by="days", inplace=True)
    df_raw.nunique()
    teams = list(set(df_raw["team ID 1"]).union(set(df_raw["team ID 2"])))
    teams.sort()

    league = League(df_raw.to_numpy(), teams)
    league.backpredict()
    print(measure_accuracy(league.games[:, 1], league.games[:, 2], league.games[:, [INDEX_T1_OCCS, INDEX_T2_OCCS]],
                           league.predictions))


def game_info_to_deviation(occurrences, opportunities, days):
    """Convert some info about the game to the amount of occurrences above the expected amount, given the number of
    opportunities."""
    return occurrences - (days * COEFF_DAYS + COEFF_INTERCEPT) * opportunities


def game_info_to_expectation(offense, defense, opportunities, days):
    """Convert some info about the game and the teams playing it to an expected number of occurrences, given the number
    of opportunities."""
    return (offense + defense + days * COEFF_DAYS + COEFF_INTERCEPT) * opportunities


def measure_accuracy(team1, team2, occurrences, predictions):
    """Return some error metrics about the error in of the predictions compared to actual outcomes, counting only games
    where both teams were D1."""
    # get an ndarray of all games where both teams are D1
    d1_vs_d1 = (np.asarray([team1, team2]) != -1).all(axis=0)

    # calculate error metrics
    residuals = (occurrences - predictions)[d1_vs_d1]
    absolute_error = abs(residuals)
    mean_squared_error = absolute_error ** 2

    return {
        'absolute error': round(absolute_error.mean(), 2),
        'mean squared error': round(mean_squared_error.mean(), 2)
    }


### ACTUAL STUFF ###


if __name__ == '__main__':
    main()
