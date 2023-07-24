from flask import Flask, render_template, request, jsonify, redirect, url_for, session, json
import pandas as pd, os, numpy as np, warnings, os
from unidecode import unidecode
from InitialiseMySQLServer import cursor, cnx
import tqdm
import socceraction.spadl as spadl
import socceraction.xthreat as xthreat
from socceraction.data.statsbomb import StatsBombLoader
import socceraction.vaep.labels as lab
# set display options
pd.set_option('display.max_columns', 500000)
pd.set_option('display.width', 500000)
pd.set_option('display.max_rows', 500000)
# You can then start using the SoccerAction module in your code
match_id = (3869685,)
matchid = 3869685
cursor.execute("SELECT home_team, away_team FROM matches WHERE match_id = %s", match_id)
fetch = cursor.fetchone()
hteam = fetch[0]
ateam = fetch[1]
# Fetch all events
cursor.execute("SELECT id, type, player, shot_outcome, team, period, minute, "
               "duration, pass_end_location, carry_end_location, pass_outcome, "
               "location, foul_committed_card, bad_behaviour_card FROM events "
               "WHERE period != 5 AND match_id = %s", (match_id))
# Fetch events, determine match minute
all_events = pd.DataFrame(cursor.fetchall())
all_events.columns = ['id', 'type', 'player', 'shot_outcome', 'team', 'period',
                      'minute', 'duration', 'pass_end_location', 'carry_end_location',
                      'pass_outcome', 'location', 'foul_committed_card',
                      'bad_behaviour_card']
all_events['duration'] = all_events['duration'].astype(float)
# Create match timeline
event = all_events.loc[all_events['period'] == 1]
p1 = pd.DataFrame({'minute': range(0, event['minute'].max() + 1), 'period': 1})
event = all_events.loc[all_events['period'] == 2]
p2 = pd.DataFrame({'minute': range(45, event['minute'].max() + 1), 'period': 2})
event = all_events.loc[all_events['period'] == 3]
p3 = pd.DataFrame({'minute': range(event['minute'].min(), event['minute'].max() + 1), 'period': 3})
event = all_events.loc[all_events['period'] == 4]
p4 = pd.DataFrame({'minute': range(event['minute'].min(), event['minute'].max() + 1), 'period': 4})
# combine dfs
events = pd.concat([p1, p2, p3, p4]).reset_index(drop=True)
events['match_minute_v1'] = events['minute'] + 1
events['match_minute'] = (events['match_minute_v1'].diff() != 0).cumsum()
xt_events = events.copy()
# compute match scoreboard minute
events['timeline'] = np.where(
    (events['period'] == 1) & (events['match_minute_v1'] < 46),
    events['match_minute_v1'].astype(str) + "'",
    np.where(
        (events['period'] == 1) & (events['match_minute_v1'] > 45),
        "45+" + (events['match_minute_v1'] - 45).astype(str) + "'",
        np.where(
            (events['period'] == 2) & (events['match_minute_v1'] < 91),
            events['match_minute_v1'].astype(str) + "'",
            np.where(
                (events['period'] == 2) & (events['match_minute_v1'] > 90),
                "90+" + (events['match_minute_v1'] - 90).astype(str) + "'",
                np.where(
                    (events['period'] == 3) & (events['match_minute_v1'] < 106),
                    events['match_minute_v1'].astype(str) + "'",
                    np.where(
                        (events['period'] == 3) & (events['match_minute_v1'] > 105),
                        "105+" + (events['match_minute_v1'] - 105).astype(str) + "'",
                        np.where(
                            (events['period'] == 4) & (events['match_minute_v1'] < 121),
                            events['match_minute_v1'].astype(str) + "'",
                            np.where(
                                (events['period'] == 4) & (events['match_minute_v1'] > 120),
                                "120+" + (events['match_minute_v1'] - 120).astype(str) + "'",
                                ""))))))))
# count home team cumulative goals
goals = all_events[((all_events['type'] == 'Own Goal For') | (all_events['shot_outcome'] == 'Goal'))]
htgoals = goals.loc[goals['team'] == hteam].groupby('minute').size().reset_index(name='HTCumGoals')
# count away team cumulative goals
atgoals = goals.loc[goals['team'] == ateam].groupby('minute').size().reset_index(name='ATCumGoals')
# join goals
events = pd.merge(htgoals, events, on=['minute'], how='right')
events = pd.merge(atgoals, events, on=['minute'], how='right')
# add goal scorers
htgoalscorers = goals[['minute', 'player', 'team']]
htgoalscorers = pd.merge(events[['minute', 'timeline']], htgoalscorers, on='minute')
htgoalscorers['Scorer'] = htgoalscorers['player'] + ' ' + htgoalscorers['timeline']
grouped_players = htgoalscorers.groupby(['minute', 'team'])['Scorer'].apply(lambda x: ', '.join(x)).reset_index()
grouped_players = grouped_players[['minute', 'Scorer', 'team']]
htgrouped_players = grouped_players.loc[grouped_players['team'] == hteam]
del htgrouped_players['team']
htgrouped_players.columns = ['minute', 'HTScorer']
atgrouped_players = grouped_players.loc[grouped_players['team'] == ateam]
del atgrouped_players['team']
atgrouped_players.columns = ['minute', 'ATScorer']
events = pd.merge(htgrouped_players[['minute', 'HTScorer']], events, on=['minute'], how='right')
events = pd.merge(atgrouped_players[['minute', 'ATScorer']], events, on=['minute'], how='right')


class ScorerProcessor:
    def __init__(self):
        self.last_valid = ''

    def process_row(self, row):
        if pd.isna(row):
            return self.last_valid
        else:
            if self.last_valid != '':
                self.last_valid += ', '
            self.last_valid += row
            return self.last_valid


events['HTScorer'] = events['HTScorer'].apply(ScorerProcessor().process_row)
events['ATScorer'] = events['ATScorer'].apply(ScorerProcessor().process_row)
# shots dataframe, join shot data
cursor.execute(
    "SELECT events.id, events.location, events.match_id, events.shot_outcome, "
    "minute, team, player, shot_one_on_one, shot_statsbomb_xg, shot_type "
    "FROM u496378222_Football.events inner join shots on events.id = shots.id "
    "WHERE period != 5 AND events.match_id = %s", (match_id))
shots = pd.DataFrame(
    cursor.fetchall(), columns=["id", "shot_location", "match_id", "shot_outcome",
                                "minute", "team", "player", "shot_one_on_one",
                                "shot_statsbomb_xg", "shot_type"])
shots['shot_statsbomb_xg'] = shots['shot_statsbomb_xg'].astype(float)
match_dict = events.set_index('minute')['match_minute'].to_dict()
shots['match_minute'] = shots['minute'].map(match_dict)
shots[['x', 'y']] = shots['shot_location'].str.strip('[]').str.split(',', expand=True).astype(float)
xgmap = shots[["id", "match_id", "x", "y", "shot_outcome", "match_minute", "team", "player",
                "shot_statsbomb_xg"]].rename(columns={'shot_outcome': 'shotType',
                                                      'shot_statsbomb_xg': 'xG'})
# Scale the 'x' column
xgmap['y_scaled'] = xgmap['x'].apply(lambda x: (((((0 - x) + 120) * 0.4075) * 2) + 5.1))
xgmap['x_scaled'] = xgmap['y'].apply(lambda y: (((y / 80) * (36.6 - 3.46)) + 3.46))
del xgmap['x']
del xgmap['y']
xgmap = xgmap[["id", "match_id", "x_scaled", "y_scaled", "shotType",
               "match_minute", "team", "player", "xG"]].rename(
    columns={'x_scaled': 'x', 'y_scaled': 'y'})
# counts team cumulative shots on target
htshots = shots.loc[shots['team'] == hteam]
htshotstotal = htshots.groupby('minute').size().reset_index(name='HTCumShots')
# count away team cumulative shots
atshots = shots.loc[shots['team'] == ateam]
atshotstotal = atshots.groupby('minute').size().reset_index(name='ATCumShots')
# home team shots on target
htshotsot = htshots.loc[htshots['shot_outcome'].isin(['Goal', 'Saved', 'Saved To Post'])]
htshotsot = htshotsot.groupby('minute').size().reset_index(name='HTCumShotsOT')
# away team shots on target
atshotsot = atshots.loc[atshots['shot_outcome'].isin(['Goal', 'Saved', 'Saved To Post'])]
atshotsot = atshotsot.groupby('minute').size().reset_index(name='ATCumShotsOT')
# home team xg
htxg = htshots.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='HTxG')
# away team xg
atxg = atshots.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='ATxG')
# home team xg "Open Play"
htxgopenplay = htshots.loc[htshots['shot_type'] == 'Open Play']
htxgopenplay = htxgopenplay.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='HTOpenPlayxG')
# away team xg "Open Play"
atxgopenplay = atshots.loc[atshots['shot_type'] == 'Open Play']
atxgopenplay = atxgopenplay.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='ATOpenPlayxG')
# home team xg "Set Piece"
htxgsetpiece = htshots.loc[htshots['shot_type'] != 'Open Play']
htxgsetpiece = htxgsetpiece.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='HTSetPiecexG')
# away team xg "Set Piece"
atxgsetpiece = atshots.loc[atshots['shot_type'] != 'Open Play']
atxgsetpiece = atxgsetpiece.groupby('minute')['shot_statsbomb_xg'].sum().reset_index(name='ATSetPiecexG')
# home team big chances
htbigchance = htshots.loc[htshots['shot_one_on_one'] == 'True']
htbigchance = htbigchance.groupby('minute').size().reset_index(name='HTBigChance')
# away team big chances
atbigchance = atshots.loc[atshots['shot_one_on_one'] == 'True']
atbigchance = atbigchance.groupby('minute').size().reset_index(name='ATBigChance')
# construct passes dataframe
passes = all_events[["id", "minute", "type", "team", "player", "duration",
                     "pass_outcome", "pass_end_location", "carry_end_location",
                     "location"]]
# possession time
possession = passes.loc[passes['type'].isin(['Pass', 'Carry'])]
possessiontime = possession.groupby(['team', 'minute'])['duration'].sum().reset_index()
htpossessiontime = possessiontime[['minute', 'duration']].loc[possessiontime['team'] == hteam]
htpossessiontime.columns = ['minute', 'HTPossession']
atpossessiontime = possessiontime[['minute', 'duration']].loc[possessiontime['team'] == ateam]
atpossessiontime.columns = ['minute', 'ATPossession']
# total passes
passestotal = possession.loc[(possession['type'] == 'Pass')]
passtotal = passestotal.groupby(['team', 'minute']).size().reset_index(name='Passes')
htpassestotal = passtotal[['minute', 'Passes']].loc[passtotal['team'] == hteam]
htpassestotal.columns = ['minute', 'HTPasses']
atpassestotal = passtotal[['minute', 'Passes']].loc[passtotal['team'] == ateam]
atpassestotal.columns = ['minute', 'ATPasses']
# accurate passes
accuratepass = passestotal[['minute', 'team']].loc[(passestotal['pass_outcome'].isnull())]
accuratepasses = accuratepass.groupby(['team', 'minute']).size().reset_index(name='AccuratePasses')
htaccuratepasses = accuratepasses[['minute', 'AccuratePasses']].loc[accuratepasses['team'] == hteam]
htaccuratepasses.columns = ['minute', 'HTAccuratePasses']
ataccuratepasses = accuratepasses[['minute', 'AccuratePasses']].loc[accuratepasses['team'] == ateam]
ataccuratepasses.columns = ['minute', 'ATAccuratePasses']
# final third entries
entries = possession.loc[((possession['pass_outcome'].isnull()) & (possession['pass_end_location'].notnull())) | (possession['carry_end_location'].notnull())]
entries = entries.copy()
entries[['pass_end_X', 'pass_end_Y']] = entries['pass_end_location'].str.strip('[]').str.split(',', expand=True).astype(float)
entries[['carry_end_X', 'carry_end_Y']] = entries['carry_end_location'].str.strip('[]').str.split(',', expand=True).astype(float)
entries[['location_X', 'location_Y']] = entries['location'].str.strip('[]').str.split(',', expand=True).astype(float)
# final third entries
finalthird = entries[((entries['pass_end_X'] > (120 * 0.666)) & (entries['location_X'] < (120 * 0.666)) |
                        (entries['carry_end_X'] > (120 * 0.666)) & (entries['location_X'] < (120 * 0.666)))]
htfinalthird = finalthird.loc[finalthird['team'] == hteam].groupby('minute', group_keys=True).size().reset_index(name='HTFinalThirdEntries')
atfinalthird = finalthird.loc[finalthird['team'] == ateam].groupby('minute', group_keys=True).size().reset_index(name='ATFinalThirdEntries')
# penalty box entries
penaltyboxentries = \
    entries[(((entries['pass_end_X'] > 101.99) & (entries['pass_end_Y'] > 17.99)
              & (entries['pass_end_Y'] < 62.01)) & (entries['location_X'] < 102) |
             ((entries['location_X'] > 101.99) & (entries['location_Y'] < 18)) |
             ((entries['location_X'] > 101.99) & (entries['location_Y'] > 62)))]
htpenaltyboxentries = penaltyboxentries[penaltyboxentries['team'] == hteam].\
    groupby('minute', group_keys=True).size().reset_index(name='HTPenaltyBoxEntries')
atpenaltyboxentries = penaltyboxentries[penaltyboxentries['team'] == ateam].\
    groupby('minute', group_keys=True).size().reset_index(name='ATPenaltyBoxEntries')
# red cards
redcards = all_events[['team', 'minute', 'foul_committed_card', 'bad_behaviour_card']].loc[
    (all_events['foul_committed_card'] == 'Red Card') | (all_events['bad_behaviour_card'] == 'Red Card')]
htredcards = redcards.loc[redcards['team'] == hteam].groupby('minute', group_keys=True).size().reset_index(name='HTRedCards')
atredcards = redcards.loc[redcards['team'] == ateam].groupby('minute', group_keys=True).size().reset_index(name='ATRedCards')
# merge dataframes
events = pd.merge(htredcards[['minute', 'HTRedCards']], events, on=['minute'], how='right')
events = pd.merge(atredcards[['minute', 'ATRedCards']], events, on=['minute'], how='right')
events = pd.merge(htpossessiontime[['minute', 'HTPossession']], events, on=['minute'], how='right')
events = pd.merge(atpossessiontime[['minute', 'ATPossession']], events, on=['minute'], how='right')
events = pd.merge(htshotstotal[['minute', 'HTCumShots']], events, on=['minute'], how='right')
events = pd.merge(atshotstotal[['minute', 'ATCumShots']], events, on=['minute'], how='right')
events = pd.merge(htshotsot[['minute', 'HTCumShotsOT']], events, on=['minute'], how='right')
events = pd.merge(atshotsot[['minute', 'ATCumShotsOT']], events, on=['minute'], how='right')
events = pd.merge(htxg[['minute', 'HTxG']], events, on=['minute'], how='right')
events = pd.merge(atxg[['minute', 'ATxG']], events, on=['minute'], how='right')
events = pd.merge(htxgopenplay[['minute', 'HTOpenPlayxG']], events, on=['minute'], how='right')
events = pd.merge(atxgopenplay[['minute', 'ATOpenPlayxG']], events, on=['minute'], how='right')
events = pd.merge(htxgsetpiece[['minute', 'HTSetPiecexG']], events, on=['minute'], how='right')
events = pd.merge(atxgsetpiece[['minute', 'ATSetPiecexG']], events, on=['minute'], how='right')
events = pd.merge(htfinalthird[['minute', 'HTFinalThirdEntries']], events, on=['minute'], how='right')
events = pd.merge(atfinalthird[['minute', 'ATFinalThirdEntries']], events, on=['minute'], how='right')
events = pd.merge(htpenaltyboxentries[['minute', 'HTPenaltyBoxEntries']], events, on=['minute'], how='right')
events = pd.merge(atpenaltyboxentries[['minute', 'ATPenaltyBoxEntries']], events, on=['minute'], how='right')
events = pd.merge(htpassestotal[['minute', 'HTPasses']], events, on=['minute'], how='right')
events = pd.merge(atpassestotal[['minute', 'ATPasses']], events, on=['minute'], how='right')
events = pd.merge(htaccuratepasses[['minute', 'HTAccuratePasses']], events, on=['minute'], how='right')
events = pd.merge(ataccuratepasses[['minute', 'ATAccuratePasses']], events, on=['minute'], how='right')
# fill empty rows with 0
events[['ATCumGoals', 'HTCumGoals', 'HTCumShots', 'ATCumShots',
        'HTCumShotsOT', 'ATCumShotsOT', 'HTxG', 'ATxG', 'HTOpenPlayxG',
        'ATOpenPlayxG', 'HTSetPiecexG', 'ATSetPiecexG', 'HTPossession',
        'ATPossession', 'HTFinalThirdEntries', 'ATFinalThirdEntries',
        'HTPenaltyBoxEntries', 'ATPenaltyBoxEntries', 'HTAccuratePasses',
        'ATAccuratePasses', 'HTRedCards', 'ATRedCards', 'HTPasses', 'ATPasses']] = \
    events[['ATCumGoals', 'HTCumGoals', 'HTCumShots', 'ATCumShots',
            'HTCumShotsOT', 'ATCumShotsOT', 'HTxG', 'ATxG', 'HTOpenPlayxG',
            'ATOpenPlayxG',  'HTSetPiecexG', 'ATSetPiecexG', 'HTPossession',
            'ATPossession', 'HTFinalThirdEntries', 'ATFinalThirdEntries',
            'HTPenaltyBoxEntries', 'ATPenaltyBoxEntries', 'HTAccuratePasses',
            'ATAccuratePasses', 'HTRedCards', 'ATRedCards', 'HTPasses', 'ATPasses']].fillna(0)
events['matchid'] = matchid
e = events[['matchid', 'period', 'match_minute_v1']]
del events['match_minute_v1']

import math
# load data from statsbomb
SBL = StatsBombLoader()
df_games = SBL.games(competition_id=43, season_id=106)
game_id = 3869685
df_games = df_games.loc[df_games['game_id'] == game_id]
home_team_id = df_games['home_team_id'].iloc[0]
dataset = [
    {
        **game,
        'actions': spadl.statsbomb.convert_to_actions(
            events=SBL.events(game['game_id']),
            home_team_id=game['home_team_id']
        )
    }
    for game in df_games.to_dict(orient='records')
]
# 2. Convert direction of play + add names
df_actions_ltr = pd.concat([
  spadl.play_left_to_right(game['actions'], game['home_team_id'])
  for game in dataset
])
df_actions_ltr = spadl.add_names(df_actions_ltr)
# load pre-trained model
url_grid = "https://karun.in/blog/data/open_xt_12x8_v1.json"
xTModel = xthreat.load_model(url_grid)
xt_df = xthreat.get_successful_move_actions(df_actions_ltr)
xt_df["xT_value"] = xTModel.rate(xt_df)
vaep_df = pd.read_parquet("xt_vaep.parquet")
# Filter xT columns
team_names = SBL.teams(game_id=3869685)
player_names = SBL.players(game_id=3869685)
xt_df = pd.merge(team_names, xt_df, on='team_id')
xt_df = pd.merge(player_names, xt_df, on='player_id')
xt_df = xt_df[['period_id', 'time_seconds', 'team_name', 'player_name', 'start_x',
               'start_y', 'end_x', 'end_y', 'type_name', 'result_name', 'xT_value']].sort_values(
    by=['period_id', 'time_seconds'])
xt_df['min'] = (xt_df['time_seconds'] / 60) + 0.0000001
xt_df['min'] = xt_df['min'].apply(lambda x: math.ceil(x))
xt_df[['start_x', 'end_x']] = ((xt_df[['start_x', 'end_x']] / 105) * 17.9) + 1.07
xt_df[['start_y', 'end_y']] = (((80 - ((xt_df[['start_y', 'end_y']] / 68) * 80))/80)*9.57) + 1.22
xt_events['min'] = (xt_events['period'] != xt_events['period'].shift()).cumsum()
xt_events['min'] = xt_events.groupby('min').cumcount() + 1
xt_df = xt_df.rename(columns={'period_id': 'period'})
xt_df = pd.merge(xt_events[['match_minute', 'min', 'period']], xt_df, on=['min', 'period'], how='left')
cols = ['min', 'period', 'time_seconds']
xt_df = xt_df.drop(columns=cols)
xt_df['matchid'] = matchid
xt_df = xt_df.replace(np.NaN, None)
xt_df = xt_df.loc[xt_df['xT_value'] > 0]

gridSections = [
    {'x': 1.07, 'y': 1.22, 'width': 2.71, 'height': 2.16, 'zone': 1},  # Top Wing Z1
    {'x': 3.78, 'y': 1.22, 'width': 3.12, 'height': 2.16, 'zone': 2},  # Top Wing Z2
    {'x': 6.9, 'y': 1.22, 'width': 3.12, 'height': 2.16, 'zone': 3},  # Top Wing Z3
    {'x': 10.02, 'y': 1.22, 'width': 3.12, 'height': 2.16, 'zone': 4},  # Top Wing Z4
    {'x': 13.14, 'y': 1.22, 'width': 3.12, 'height': 2.16, 'zone': 5},  # Top Wing Z5
    {'x': 16.26, 'y': 1.22, 'width': 2.71, 'height': 2.16, 'zone': 6},  # Top Wing Z6
    {'x': 3.78, 'y': 3.38, 'width': 6.24, 'height': 1.43, 'zone': 7},  # Top Wing Defensive Half Space Z8
    {'x': 3.78, 'y': 4.81, 'width': 6.24, 'height': 2.39, 'zone': 8},  # Centre Defensive Midfield Z9
    {'x': 3.78, 'y': 7.2, 'width': 6.24, 'height': 1.43, 'zone': 9},  # Bottom Wing Defensive Half Space Z10
    {'x': 10.02, 'y': 3.38, 'width': 6.24, 'height': 1.43, 'zone': 10},  # Top Wing Attacking Half Space Z11
    {'x': 10.02, 'y': 4.81, 'width': 6.24, 'height': 2.39, 'zone': 11},  # Centre Attacking Midfield Z12
    {'x': 10.02, 'y': 7.2, 'width': 6.24, 'height': 1.43, 'zone': 12},  # Bottom Attacking Wing Half Space Z13
    {'x': 1.07, 'y': 3.38, 'width': 2.71, 'height': 5.25, 'zone': 13},  # Defending Team Box Z7
    {'x': 1.07, 'y': 8.63, 'width': 2.71, 'height': 2.16, 'zone': 14},  # Bottom Wing Z14
    {'x': 3.78, 'y': 8.63, 'width': 3.12, 'height': 2.16, 'zone': 15},  # Bottom Wing Z15
    {'x': 6.9, 'y': 8.63, 'width': 3.12, 'height': 2.16, 'zone': 16},  # Bottom Wing Z16
    {'x': 10.02, 'y': 8.63, 'width': 3.12, 'height': 2.16, 'zone': 17},  # Bottom Wing Z17
    {'x': 13.14, 'y': 8.63, 'width': 3.12, 'height': 2.16, 'zone': 18},  # Bottom Wing Z18
    {'x': 16.26, 'y': 8.63, 'width': 2.71, 'height': 2.16, 'zone': 19},  # Bottom Wing Z19
    {'x': 16.26, 'y': 3.38, 'width': 2.71, 'height': 5.25, 'zone': 20},  # Attacking Team Box Z20
]

def assign_zone(start_x, start_y):
    for section in gridSections:
        if (
            section['x'] <= start_x <= (section['x'] + section['width']) and
            section['y'] <= start_y <= (section['y'] + section['height'])
        ):
            return section['zone']  # Return the zone value
    return None  # No zone found for the given coordinates


# Assign zone value to xt_df['zone'] column
xt_df['start_zone'] = xt_df.apply(lambda row: assign_zone(row['start_x'], row['start_y']), axis=1)
xt_df['end_zone'] = xt_df.apply(lambda row: assign_zone(row['end_x'], row['end_y']), axis=1)
sum_value = sum(xt_df['xT_value'].loc[(xt_df['start_zone'] == 3) & (xt_df['end_zone'] == 3)])
rounded_sum = round(sum_value, 4)
print(vaep_df)
pd.mde(0)
# vaep data
vaep_df = vaep_df[['period_id', 'time_seconds', 'team_name', 'player_name', 'start_x',
               'start_y', 'end_x', 'end_y', 'type_name', 'vaep_value']].sort_values(
    by=['period_id', 'time_seconds'])
vaep_df['min'] = (vaep_df['time_seconds'] / 60) + 0.0000001
vaep_df['min'] = vaep_df['min'].apply(lambda x: math.ceil(x))
xt_events['min'] = (xt_events['period'] != xt_events['period'].shift()).cumsum()
xt_events['min'] = xt_events.groupby('min').cumcount() + 1
vaep_df = vaep_df.rename(columns={'period_id': 'period'})
vaep_df = pd.merge(xt_events[['match_minute', 'min', 'period']], vaep_df, on=['min', 'period'], how='left')
cols = ['min', 'period', 'time_seconds']
vaep_df = vaep_df.drop(columns=cols)
vaep_df['matchid'] = matchid
vaep_df = vaep_df.replace(np.NaN, None)

print(vaep_df)

#goalkeepers_insert_query = "INSERT INTO xt (match_minute, team_name, player_name, start_x, start_y, end_x, end_y, type_name, result_name, xT_value, matchid, start_zone, end_zone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
#cursor.executemany(goalkeepers_insert_query, xt_df.values.tolist())
#cnx.commit()

#cursor.execute("delete from xgmap;")
#cnx.commit()
#shots_insert_query = "INSERT INTO xgmap (id, match_id, x, y, shotType, match_minute, team, player, xG) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
#cursor.executemany(shots_insert_query, xgmap.values.tolist())
#cnx.commit()
#query = """INSERT INTO matchstats (
 #       minute, ATAccuratePasses, HTAccuratePasses,  ATPasses, HTPasses, ATPenaltyBoxEntries, HTPenaltyBoxEntries, ATFinalThirdEntries, HTFinalThirdEntries, ATSetPiecexG, HTSetPiecexG,
  #      ATOpenPlayxG, HTOpenPlayxG, ATxG, HTxG, ATCumShotsOT, HTCumShotsOT, ATCumShots, HTCumShots, ATPossession, HTPossession,
   #     ATRedCards, HTRedCards, ATScorer, HTScorer, ATCumGoals, HTCumGoals, period,
    #    match_minute, timeline, matchid) VALUES (
     #   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s); """
#cursor.executemany(query, events.values.tolist())
#cnx.commit()