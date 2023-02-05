import sys, os
import json
import statsapi

RANDOM_TEAM_ID = 118
RANDOM_SEASON = 2018

# fetch random team's roster from random season, collect player ids
roster_res = statsapi.get('team_roster', {'teamId': RANDOM_TEAM_ID, 'season': RANDOM_SEASON})
players = [player['person']['id'] for player in roster_res['roster']]

# collect and organize career stats for full roster
player_stats = dict()
for player_id in players:
    # extract stat groups for each player
    player_res = statsapi.player_stat_data(player_id, type="career")
    player_stats[player_res['id']] = dict()
    for stats_group in player_res['stats']:
        # extract the group's name and stats
        group_name = stats_group['group']
        group_stats = stats_group['stats'].copy()

        # fielding is further divided by position, with a type + code
        if group_name.lower() == 'fielding':
            field_code = group_stats['position']['code']
            group_name = f"{group_name}_{int(field_code)-1}"

            del group_stats['position']

        player_stats[player_res['id']][group_name] = group_stats
player_ids = list(player_stats.keys())

# collect all stat keys for each stat group, confirm group keys are universal
stat_group_keys = dict()
for group_name, stat_group in player_stats[player_ids[0]].items():
    stat_group_keys[group_name] = set(stat_group.keys())
for player_id in player_ids:
    for group_name, stat_group in player_stats[player_id].items():
        if group_name not in stat_group_keys:
            stat_group_keys[group_name] = set(stat_group.keys())
        elif set(stat_group.keys()) != stat_group_keys[group_name]:
            print(f"Group Keys did not match {player_id}, {group_name}")
            print(set(stat_group.keys()))
            print(stat_group_keys[group_name])
            print("\n--------------------------------------------")
            sys.exit()

# create sqlite "schema" datadef for each stat group, dump to json
(stat_groups := [stat_group for stat_group in stat_group_keys.keys()]).sort()
player_datadef = dict()
for stat_group in stat_groups:
    (stat_keys := list(stat_group_keys[stat_group])).sort()
    player_datadef[stat_group] = stat_keys

with open(os.path.join(os.path.dirname(__file__), "player_datadef.json"), 'w') as file:
    json.dump(player_datadef, file, indent=4)
