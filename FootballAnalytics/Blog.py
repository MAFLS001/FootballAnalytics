from flask import Flask, render_template, request, jsonify, redirect, url_for, session, json
import pandas as pd, os, numpy as np, statsbombpy as sb
from fuzzywuzzy import fuzz
from InitialiseMySQLServer import cursor, cnx

app = Flask(__name__)


@app.route("/")
def Home():
    return render_template("Home.html")


@app.route("/Acknowledgement")
def Acknowledgement():
    return render_template("Acknowledgement.html")


@app.route("/LetsGetStarted")
def lets_get_started():
    # Retrieve distinct competitions from the Competitions table
    cursor.execute("SELECT DISTINCT competition_name FROM competitions")
    competitions = cursor.fetchall()
    return render_template("LetsGetStarted.html", competitions=competitions)


@app.route("/get_season", methods=["GET"])
def get_season():
    competition = request.args.get("competition")
    # Retrieve distinct seasons for the selected competition from the Competitions table
    cursor.execute("SELECT DISTINCT season_name FROM competitions WHERE competition_name = %s", (competition,))
    season = cursor.fetchall()
    return jsonify(season)


@app.route("/getHomeTeams", methods=["GET"])
def get_home_teams():
    season = request.args.get("season")
    competition = request.args.get("competition")
    # Retrieve distinct home teams for the selected season and competition from the Matches table
    cursor.execute("SELECT DISTINCT home_team FROM matches WHERE season = %s AND competition = %s", (season, competition,))
    hteam = cursor.fetchall()
    return jsonify(hteam)


@app.route("/getAwayTeams", methods=["GET"])
def get_away_teams():
    season = request.args.get("season")
    competition = request.args.get("competition")
    hteam = request.args.get("hteam")
    # Retrieve distinct away teams for the selected season, competition, and home team from the Matches table
    cursor.execute("SELECT DISTINCT away_team FROM matches WHERE season = %s AND competition = %s AND home_team = %s",(season, competition, hteam,))
    ateam = cursor.fetchall()
    return jsonify(ateam)


@app.route("/Analysis", methods=["POST"])
def analysis():
    season = request.form.get("season")
    competition = request.form.get("competition")
    hteam = request.form.get("hteam")
    ateam = request.form.get("ateam")
    # Query to find match_id and max_minutes
    cursor.execute("SELECT match_id FROM matches WHERE season = %s AND competition = %s AND home_team = %s AND away_team = %s", (season, competition, hteam, ateam))
    match_id = cursor.fetchall()[0][0]
    cursor.execute("SELECT * FROM matchstats WHERE matchid = %s", (match_id,))
    matchstats = cursor.fetchall()
    # construct a dataframe from the matchstats data
    columns = ['minute', 'ATAccuratePasses', 'HTAccuratePasses', 'ATPasses',
               'HTPasses', 'ATPenaltyBoxEntries', 'HTPenaltyBoxEntries',
               'ATFinalThirdEntries', 'HTFinalThirdEntries', 'ATSetPiecexG',
               'HTSetPiecexG', 'ATOpenPlayxG', 'HTOpenPlayxG',
               'ATxG', 'HTxG', 'ATCumShotsOT', 'HTCumShotsOT', 'ATCumShots',
               'HTCumShots', 'ATPossession', 'HTPossession', 'ATRedCards',
               'HTRedCards', 'ATScorer', 'HTScorer', 'ATCumGoals',
               'HTCumGoals', 'period', 'match_minute', 'timeline', 'matchid']
    # add other column names to the 'columns' list
    matchstats = pd.DataFrame(matchstats, columns=columns)
    max_minutes_played = max(matchstats['match_minute'].astype(float))
    matchstats_list = matchstats.to_dict('records')
    matchstats_json = matchstats_list
    matchstats['HTGoal'] = matchstats['HTCumGoals'].apply(lambda x: 1 if x > 0 else 0)
    matchstats['ATGoal'] = matchstats['ATCumGoals'].apply(lambda x: 1 if x > 0 else 0)
    # collate xg timeline data
    xgtl = matchstats[['match_minute', 'timeline', 'HTxG', 'ATxG', 'HTGoal', 'ATGoal']]
    xgtimeline = xgtl.copy()
    xgtimeline['CumHTxG'] = xgtimeline['HTxG'].cumsum()
    xgtimeline['CumATxG'] = xgtimeline['ATxG'].cumsum()
    xgtimeline_list = xgtimeline.to_dict('records')
    xgtimeline_json = xgtimeline_list
    # collate xg shot map data
    cursor.execute("SELECT x, y, shotType, match_minute, team, player, xG "
                   "FROM xgmap WHERE match_id = %s", (match_id,))
    xgmap = cursor.fetchall()
    columns = ["x", "y", "shotType", "match_minute", "team", "player", "xG"]
    xgmap = pd.DataFrame(xgmap, columns=columns)
    # Convert xg shot map data to JSON
    xgmap_list = xgmap.to_dict('records')
    xgmap_json = xgmap_list
    # collate xt map data
    cursor.execute("SELECT match_minute, team_name, player_name, type_name,"
                   " xT_value, start_zone, end_zone FROM xt WHERE matchid = %s", (match_id,))
    xtmap = cursor.fetchall()
    columns = ["match_minute", "team_name", "player_name", "type", "xT", "start_zone", "end_zone"]
    xtmap = pd.DataFrame(xtmap, columns=columns)
    xtmap = xtmap.replace(np.NaN, None)
    # Convert xt map data to JSON
    xtmap_list = xtmap.to_dict('records')
    xtmap_json = xtmap_list
    return render_template("Analysis.html", season=season,
                           max_playedmins=max_minutes_played,
                           min_playedmins=1, competition=competition,
                           hteam=hteam, ateam=ateam, match_id=match_id,
                           matchstats=matchstats_json, xgmap=xgmap_json,
                           xgtimeline=xgtimeline_json, xtmap=xtmap_json)


@app.route('/getFilteredEvents', methods=['POST'])
def get_filtered_events():
    # retrieve match stats
    data = request.get_json()
    minute_start = data.get('minute_start')
    minute_end = data.get('minute_end')
    matchstats = data.get('matchstats')
    xgmap = data.get('xgmap')
    xgtimeline = data.get('xgtimeline')
    xtmap = data.get('xtmap')
    # filter matchstats data
    filtered_events = [event for event in matchstats if minute_start <= event['match_minute'] <= minute_end]
    # filter xtmap
    filtered_xtmap = [event for event in xtmap if minute_start <= event['match_minute'] <= minute_end]
    # filter xgmap data
    filtered_xgmap = [event for event in xgmap if minute_start <= event['match_minute'] <= minute_end]
    # filter xgtimeline data
    filtered_xgtimeline = [event for event in xgtimeline if minute_start <= event['match_minute'] <= minute_end]
    # find last event
    latest_event = filtered_events[-1]
    # calculate scores and scorers
    hteam_score = sum(event['HTCumGoals'] for event in filtered_events)
    ateam_score = sum(event['ATCumGoals'] for event in filtered_events)
    hteam_scorers = latest_event['HTScorer']
    ateam_scorers = latest_event['ATScorer']
    hteam_xg = round(sum(event['HTxG'] for event in filtered_events), 2)
    ateam_xg = round(sum(event['ATxG'] for event in filtered_events), 2)
    hteam_shot = sum(event['HTCumShots'] for event in filtered_events)
    ateam_shot = sum(event['ATCumShots'] for event in filtered_events)
    hteam_shotot = sum(event['HTCumShotsOT'] for event in filtered_events)
    ateam_shotot = sum(event['ATCumShotsOT'] for event in filtered_events)
    hteam_opxg = round(sum(event['HTOpenPlayxG'] for event in filtered_events), 2)
    ateam_opxg = round(sum(event['ATOpenPlayxG'] for event in filtered_events), 2)
    hteam_spxg = round(sum(event['HTSetPiecexG'] for event in filtered_events), 2)
    ateam_spxg = round(sum(event['ATSetPiecexG'] for event in filtered_events), 2)
    hteam_possession = sum(event['HTPossession'] for event in filtered_events)
    ateam_possession = sum(event['ATPossession'] for event in filtered_events)
    hteam_pass = sum(event['HTPasses'] for event in filtered_events)
    ateam_pass = sum(event['ATPasses'] for event in filtered_events)
    hteam_accpass = sum(event['HTAccuratePasses'] for event in filtered_events)
    ateam_accpass = sum(event['ATAccuratePasses'] for event in filtered_events)
    hteam_ftentries = sum(event['HTFinalThirdEntries'] for event in filtered_events)
    ateam_ftentries = sum(event['ATFinalThirdEntries'] for event in filtered_events)
    hteam_pbentries = sum(event['HTPenaltyBoxEntries'] for event in filtered_events)
    ateam_pbentries = sum(event['ATPenaltyBoxEntries'] for event in filtered_events)
    hteam_redcard = sum(event['HTRedCards'] for event in filtered_events)
    ateam_redcard = sum(event['ATRedCards'] for event in filtered_events)

    response_data = {
        'hteam_score': hteam_score,
        'ateam_score': ateam_score,
        'hteam_scorers': hteam_scorers,
        'ateam_scorers': ateam_scorers,
        'hteam_xg': hteam_xg,
        'ateam_xg': ateam_xg,
        'hteam_opxg': hteam_opxg,
        'ateam_opxg': ateam_opxg,
        'hteam_spxg': hteam_spxg,
        'ateam_spxg': ateam_spxg,
        'hteam_shot': hteam_shot,
        'ateam_shot': ateam_shot,
        'hteam_shotot': hteam_shotot,
        'ateam_shotot': ateam_shotot,
        'hteam_redcard': hteam_redcard,
        'ateam_redcard': ateam_redcard,
        'hteam_poss': hteam_possession,
        'ateam_poss': ateam_possession,
        'hteam_pass': hteam_pass,
        'ateam_pass': ateam_pass,
        'hteam_accpass': hteam_accpass,
        'ateam_accpass': ateam_accpass,
        'hteam_ftentries': hteam_ftentries,
        'ateam_ftentries': ateam_ftentries,
        'hteam_pbentries': hteam_pbentries,
        'ateam_pbentries': ateam_pbentries,
        'filtered_xgmap': filtered_xgmap,
        'filtered_xgtimeline': filtered_xgtimeline,
        'filtered_xtmap': filtered_xtmap}
    return jsonify(response_data)


@app.route("/Test")
def Test():
    return render_template("Test.html")


if __name__ == "__main__":
    app.run(debug=True)