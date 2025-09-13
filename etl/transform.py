import re
import pandas as pd


def transform_data():
    transform_wiki_data()
    transform_sports_reference_data()


def clean_column(name):
    return re.sub(r'\s*\(.*?\).*', '', name)


def transform_wiki_data():
    wiki_data = pd.read_csv('data/teams_data.csv', encoding='utf-8')
    wiki_data.sort_values(by='Team', inplace=True)
    wiki_data.rename(columns={'Head coach': 'Coach'}, inplace=True)

    wiki_data['Coach'] = wiki_data['Coach'].apply(clean_column)
    
    wiki_data['Conference'] = wiki_data['Conference'].apply(clean_column)
    wiki_data['Conference'] = wiki_data['Conference'].str.replace(r'\bConference\b', '', regex=True).str.strip()

    wiki_data['Nickname'] = wiki_data['Nickname'].apply(clean_column)

    wiki_data['Record'] = wiki_data['Record'].str.replace('\u2013', '-').str.strip()
    wiki_data[['Wins', 'Losses', 'Conference Wins', 'Conference Losses']] = wiki_data['Record'].str.extract(r'(\d+)-(\d+)\s+\((\d+)-(\d+)[^)]*\)').astype(int)
    wiki_data = wiki_data[[
        "Team", "Record", "Wins", "Losses", "Conference Wins", "Conference Losses",
        "University", "Coach", "Conference", "Location", "Nickname"
    ]]
    
    wiki_data.to_csv('data/cleaned_teams_data.csv', index=False)


def transform_sports_reference_data():
    roster_data = pd.read_csv('data/team_rosters.csv', encoding='utf-8')
    stats_data = pd.read_csv('data/team_stats.csv', encoding='utf-8')

    roster_data = roster_data[~roster_data['Summary'].isnull()]
    roster_data.drop(axis=1, columns=['RSCI Top 100', 'Summary'], inplace=True)
    roster_data.loc[roster_data['Team'].str.contains('McNeese State', case=False, na=False), 'Team'] = 'McNeese'
    roster_data.loc[roster_data['Team'].str.contains('UNC', case=False, na=False), 'Team'] = 'North Carolina'
    roster_data['Team'] = roster_data['Team'].str.replace('-', ' ')
    roster_data['Team'] = roster_data['Team'].apply(clean_column)

    stats_data = stats_data[stats_data['Player'] != 'Team Totals']
    stats_data.drop(axis=1, columns=['Rk', 'Pos', 'Awards'], inplace=True)
    stats_data.loc[stats_data['Team'].str.contains('McNeese State', case=False, na=False), 'Team'] = 'McNeese'
    stats_data.loc[stats_data['Team'].str.contains('UNC', case=False, na=False), 'Team'] = 'North Carolina'
    stats_data['Team'] = stats_data['Team'].str.replace('-', ' ')
    stats_data['Team'] = stats_data['Team'].apply(clean_column)

    stats_name_map = {}
    for _, row in stats_data.iterrows():
        player_name = row['Player']
        team = row['Team']

        base_name = player_name
        for suffix in [' Jr.', ' Sr.', ' II', ' III', ' IV', ' V']:
            if player_name.endswith(suffix):
                base_name = player_name.replace(suffix, '')
                break
        stats_name_map[(base_name, team)] = player_name

    def update_name(row):
        key = (row['Player'], row['Team'])
        return stats_name_map.get(key, row['Player'])

    roster_data['Player'] = roster_data.apply(update_name, axis=1)

    def fix_encoding(s):
        if isinstance(s, str):
            return s.encode("latin1").decode("utf-8")
        return s

    roster_data['Player'] = roster_data['Player'].apply(fix_encoding)
    roster_data['High School'] = roster_data['High School'].apply(fix_encoding)
    roster_data['Hometown'] = roster_data['Hometown'].apply(fix_encoding)
    roster_data.sort_values(by=['Team', 'Player'], inplace=True)
    

    stats_data['Player'] = stats_data['Player'].apply(fix_encoding)
    stats_data.sort_values(by=['Team', 'Player'], inplace=True)

    stats_data.reset_index(drop=True, inplace=True)
    stats_data.index.name = 'Player ID'
    stats_data.index += 1
    stats_data.drop(columns=['Player', 'Team'], inplace=True)

    # roster_data.to_csv('data/cleaned_roster_data.csv', index=False)
    # stats_data.to_csv('data/cleaned_stats_data.csv')

    roster_data.to_csv('data/test_roster_data.csv', index=False)
    stats_data.to_csv('data/test_stats_data.csv')