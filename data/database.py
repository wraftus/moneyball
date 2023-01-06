import os
import json
import sqlite3
from dataclasses import dataclass

import statsapi

#################################################
########## FETCHING DATA FROM STATSAPI ##########

# fetch all games from a given season from statsapi
GAME_API_KEYS = {'game_type', 'away_id', 'home_id', 'away_score', 'home_score'}
def fetch_seasons_games(season : int):
    print(f"Fetching game schedule for {season} ...")

    # fetch the schedule for the given season
    start_date, end_date = f"04/01/{season}", f"12/31/{season}"
    raw_games = statsapi.schedule(start_date=start_date, end_date=end_date)

    # parse the raw games into a dict keyed by their id
    games = {}
    for raw_game in raw_games:
        if raw_game['status'] != 'Final': continue
        
        game = {'season': season}
        for key in GAME_API_KEYS: game[key] = raw_game[key]
        games[raw_game['game_id']] = game
    return games

# fetch team rosters during a season for a list of teams
def fetch_season_rosters(season : int, team_ids : set[int]):
    team_rosters = dict()
    for idx, team_id in enumerate(team_ids):
        print(f"\rFetching team roster {(idx + 1):2d}/{len(team_ids):2d} ...", end="")
        api_res = statsapi.get("team_roster", {'teamId': team_id, "season": season})
        team_rosters[team_id] = [player['person']['id'] for player in api_res['roster']]
    print()
    return team_rosters

PLAYER_DATADEF_FILE = os.path.join(os.path.dirname(__file__), "player_datadef.json")
def fetch_players_career_stats(player_ids : set[int]):
    player_stats = dict()
    # fetch the data for each player
    for idx, player_id in enumerate(player_ids):
        player_stats[player_id] = {}
        print(f"\rFetching player stats {(idx + 1):2d}/{len(player_ids):2d} ...", end="")
        player_data = statsapi.player_stat_data(player_id, type="career")

        # divide the player stats into each group
        for stats_group in player_data['stats']:
            group_name = stats_group['group']
            group_stats = stats_group['stats']

            # fielding is further divided into 10 subgroups
            if group_name.lower() == 'fielding':
                group_name = f"{group_name}_{int(group_stats['position']['code'])-1}"
                del group_stats['position']

            player_stats[player_id][group_name] = group_stats
    print()
    return player_stats

####################################################
########## CREATING AND UPDATING DATABASE ##########

# TODO this should be singleton-y (gross)
# TODO standarize when db.con.commit() gets called
@dataclass # dataclass to wrap sqlite database objects
class Database:
    con : sqlite3.Connection
    cur : sqlite3.Cursor

    DEFAULT_DB_NAME = "mlb_stats.db"
    @classmethod # spawn a Database from a db file
    def from_db_file(cls, filepath):
        con = sqlite3.connect(filepath)
        cur = con.cursor()
        return cls(con, cur)

# creates a new empty database, overwriting the previous one
def make_fresh_database():
    # delete the old db file if nesc. and create a database connection
    db_path = os.path.join(os.path.dirname(__file__), Database.DEFAULT_DB_NAME)
    if os.path.exists(db_path): os.remove(db_path)
    db = Database.from_db_file(db_path)

    # create a blank table for the game data
    games_columns = ['game_id INTEGER PRIMARY KEY', 'season INTEGER']
    games_columns.extend(f"{key} INTEGER NOT NULL" for key in GAME_API_KEYS)
    db.cur.execute(f"CREATE TABLE games({', '.join(games_columns)})")

    # create blank tables for the different player stat groups
    with open(PLAYER_DATADEF_FILE, 'r') as file:
        player_datadef = json.load(file)
    for stats_group, group_keys in player_datadef.items():
        group_columns = ['player_id INTEGER PRIMARY KEY']
        group_columns.extend(f"{key} REAL" for key in group_keys)
        db.cur.execute(f"CREATE TABLE stats_{stats_group}({', '.join(group_columns)})")

    # commit these new tables
    db.con.commit()
    return db

# creates (or overides) a roster table for a given season
MAX_TEAM_MEMBERS = 70
def create_roster_table(db : Database, season : int):
    # drop the table if it extists
    db.cur.execute(f"DROP TABLE IF EXISTS roster_{season}")

    # create the roster table
    roster_columns = ['team_id INTEGER PRIMARY KEY']
    roster_columns.extend(f'member_{i+1} INTEGER' for i in range(MAX_TEAM_MEMBERS))
    db.cur.execute(f"CREATE TABLE roster_{season}({', '.join(roster_columns)})")
    return f"roster_{season}"

# collects and overrides all game information for a given season in a database
def collect_season_data(db : Database, season : int):
    # fetch game data and insert regular season game data into the sqlite db
    season_games = fetch_seasons_games(season)
    season_games = [{'game_id': game_id} | game for game_id, game in season_games.items()
                                                if game['game_type'] == 'R']
    game_keys = ['game_id', 'season'] + [key for key in GAME_API_KEYS]
    game_data = [tuple(game[key] for key in game_keys) for game in season_games]

    question_marks = ', '.join('?' for _ in range(len(game_keys)))
    db.cur.executemany(f"INSERT OR REPLACE INTO games VALUES({question_marks})", game_data)

    # create the season's roster table and populate it
    roster_name = create_roster_table(db, season)
    team_ids = query_teams_in_season(db, season)
    team_rosters = fetch_season_rosters(season, team_ids)

    roster_data = []
    for team_id, roster in team_rosters.items():
        padded_roster = roster + [None]*(MAX_TEAM_MEMBERS - len(roster))
        roster_data.append(tuple([team_id] + padded_roster))

    question_marks = ','.join('?' for _ in range(1 + MAX_TEAM_MEMBERS))
    db.cur.executemany(
        f"INSERT OR REPLACE INTO {roster_name} VALUES({question_marks})", roster_data)
    db.con.commit()

# update all player data in the database given team ids
def update_players_data(db : Database, player_ids : set[int]):
    # get the stats schema and initialize the grouped stats data
    with open(PLAYER_DATADEF_FILE, 'r') as file:
        player_datadef = json.load(file)
    grouped_stats_data = {group: [] for group in player_datadef.keys()}

    def parse_stat(stat_str: str):
        try:    return float(stat_str)
        except: return None

    # fetch stats for each player and reshuffle the data by group
    player_stats = fetch_players_career_stats(player_ids)
    for player_id, stats_groups in player_stats.items():
        for group, stats in stats_groups.items():
            group_keys = player_datadef[group]
            stats = [parse_stat(stats[key]) for key in group_keys]
            grouped_stats_data[group].append(tuple([player_id] + stats))

    # insert the player data into the tables
    for stats_group, group_data in grouped_stats_data.items():
        question_marks = ','.join('?' for _ in range(1 + len(player_datadef[stats_group])))
        db.cur.executemany(
            f"INSERT OR REPLACE INTO stats_{stats_group} VALUES({question_marks})", group_data)
    db.con.commit()

#######################################
########## QUERYING DATABASE ##########

# query all team ids that played during a given season
def query_teams_in_season(db : Database, season : int):
    db.cur.execute(f'''
            SELECT DISTINCT away_id FROM games WHERE season=\'{season}\'
            UNION
            SELECT DISTINCT home_id FROM games WHERE season=\'{season}\'
    ''')
    return set(first for first, in db.cur.fetchall())

# query the rosters for all teams during a given season
def query_team_rosters(db : Database, season : int):
    # query team roster
    db.cur.execute(f'SELECT * FROM roster_{season}')
    roster_query = {roster_data[0]: roster_data[1:] for roster_data in db.cur.fetchall()}

    # filter out null values
    team_rosters = {}
    for team_id, roster in roster_query.items():
        filtered_roster = [player_id for player_id in roster if player_id is not None]
        team_rosters[team_id] = filtered_roster
    return team_rosters

