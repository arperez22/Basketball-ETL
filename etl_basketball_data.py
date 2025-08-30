import requests
import psycopg2
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
import regex as re


def extract_data():
    extract_sports_reference_data()
    extract_wiki_data()


def extract_sports_reference_data():
    team_roster_info = []
    team_stats_info = []

    url = 'https://www.sports-reference.com/cbb/postseason/men/2025-ncaa.html'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    divs = soup.find_all('div', {'class': 'team16'})
    divs = ''.join(str(div) for div in divs)
    div_soup = BeautifulSoup(divs, 'lxml')

    tags = div_soup.find_all('a')

    team_links_to_names = {tag.get('href'): tag.text.strip() for tag in tags if '/cbb/schools/' in tag.get('href')}
    team_links_to_names = {f"https://www.sports-reference.com/{link}": name for link, name in team_links_to_names.items()}

    team_links_to_names = dict(sorted(team_links_to_names.items(), key=lambda item: item[1]))

    for team_url, team_name in team_links_to_names.items():
        html = requests.get(team_url).text
        soup = BeautifulSoup(html, 'lxml')

        tables = soup.find_all('table')

        roster_table = tables[0]
        stats_table = tables[5]

        roster_info = pd.read_html(str(roster_table))[0]
        roster_info["Team"] = team_name
        team_roster_info.append(roster_info)

        stats_info = pd.read_html(str(stats_table))[0]
        stats_info["Team"] = team_name
        team_stats_info.append(stats_info)

        print(f"Finished processing {team_name}!")
        sleep(5)

    roster_df = pd.concat(team_roster_info, ignore_index=True)
    roster_df.to_csv('data/team_rosters.csv', index=False)

    stats_df = pd.concat(team_stats_info, ignore_index=True)
    stats_df.to_csv('data/team_stats.csv', index=False)


def extract_wiki_data():
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MyBasketballETLBot/1.0; +https://github.com/arperez22/Basketball-ETL)"}
    first_four_losers = set(['Texas', 'San Diego State', 'Saint Francis', 'American'])
    teams_data = []

    wiki_url = 'https://en.wikipedia.org/wiki/2025_NCAA_Division_I_men%27s_basketball_tournament'
    wiki_html = requests.get(wiki_url, headers=headers).text
    wiki_soup = BeautifulSoup(wiki_html, 'lxml')
    tables = wiki_soup.find_all('table')

    tags = [tag for table in tables[6:10] for tag in table.find_all('a')]
    team_links_to_names = {tag.get('href'): tag.text.strip() for tag in tags if 'basketball_team' in tag.get('href') and tag.text.strip() not in first_four_losers}
    team_links_to_names = {f"https://en.wikipedia.org/api/rest_v1/page/html/{href.lstrip('./wiki')}": text for href, text in team_links_to_names.items()}

    for team_season_url, team_name in team_links_to_names.items():
        try:
            team_season_html = requests.get(team_season_url, headers=headers).text
            team_season_soup = BeautifulSoup(team_season_html, 'lxml')
            record_th = team_season_soup.find('th', text='Record')
            record_td = record_th.find_next_sibling('td')
            span = team_season_soup.find('span', class_='vcard attendee fn org')
            tag = span.find_next('a')
            team_page_url = f'https://en.wikipedia.org/api/rest_v1/page/html{tag.get("href").lstrip(".")}'

            team_html = requests.get(team_page_url, headers=headers).text
            team_soup = BeautifulSoup(team_html, 'lxml')
            table = team_soup.find('table', class_='infobox vcard')
            image_tag = table.find('a', class_='mw-file-description')
            image_src = image_tag.find('img').get('src')
            image_link = 'https:' + image_src[0 : image_src.find('.svg') + 4].replace('/thumb', '')
            image_response = requests.get(image_link, headers=headers)

            with open(f'data/images/{team_name}_logo.svg', 'wb') as f:
                f.write(image_response.content)

            file_headers = ['Team', 'Record', 'University', 'Head coach', 'Conference', 'Location', 'Nickname']
            team_data = [team_name, record_td.text.strip()]
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                if th and th.text.strip() in file_headers:
                    print(th.text.strip())
                    td = th.find_next_sibling('td')
                    if td:
                        team_data.append(td.text.strip())

            teams_data.append(team_data)
            sleep(10)

        except Exception as e:
            print(f"Error processing {team_name}: {e}")


    teams_df = pd.DataFrame(teams_data, columns=file_headers)
    teams_df.to_csv('data/teams_data.csv', index=False)


def transform_data():
    transform_sports_reference_data()
    transform_wiki_data()


def transform_sports_reference_data():
    roster_data = pd.read_csv('data/team_rosters.csv')
    stats_data = pd.read_csv('data/team_stats.csv')

    stats_data = stats_data[stats_data['Player'] != 'Team Totals']
    stats_data.drop(axis=1, columns=['Rk', 'Pos', 'Awards'], inplace=True)

    roster_data = roster_data[~roster_data['Summary'].isnull()]
    roster_data.drop(axis=1, columns=['RSCI Top 100', 'Summary'], inplace=True)

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

    roster_data.to_csv('data/cleaned_roster_data.csv', index=False)
    stats_data.to_csv('data/cleaned_stats_data.csv', index=False)


def transform_wiki_data():
    wiki_data = pd.read_csv('data/teams_data.csv')
    wiki_data.sort_values(by='Team', inplace=True)
    wiki_data.rename(columns={'Head coach': 'Coach'}, inplace=True)

    def clean_column(name):
        return re.sub(r'\s*\(.*?\).*', '', name)

    wiki_data['Coach'] = wiki_data['Coach'].apply(clean_column)
    
    wiki_data['Conference'] = wiki_data['Conference'].apply(clean_column)
    wiki_data['Conference'] = wiki_data['Conference'].str.replace(r'\bConference\b', '',regex=True).str.strip()

    wiki_data['Nickname'] = wiki_data['Nickname'].apply(clean_column)

    wiki_data['Record'] = wiki_data['Record'].str.replace('\u2013', '-').str.strip()
    wiki_data[['Wins', 'Losses', 'Conference Wins', 'Conference Losses']] = wiki_data['Record'].str.extract(r'(\d+)-(\d+)\s+\((\d+)-(\d+)[^)]*\)').astype(int)
    wiki_data = wiki_data[[
        "Team", "Record", "Wins", "Losses", "Conference Wins", "Conference Losses",
        "University", "Coach", "Conference", "Location", "Nickname"
    ]]
    
    wiki_data.to_csv('data/cleaned_teams_data.csv', index=False)


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""  CREATE TABLE IF NOT EXISTS teams (
                        team_id             INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name                VARCHAR(50) NOT NULL UNIQUE,
                        record              VARCHAR(30) NOT NULL,
                        wins                INT NOT NULL,
                        losses              INT NOT NULL,
                        conference_wins     INT NOT NULL,
                        conference_losses   INT NOT NULL,
                        university          VARCHAR(50) NOT NULL,
                        coach               VARCHAR(50) NOT NULL,
                        conference          VARCHAR(50) NOT NULL,
                        location            VARCHAR(50) NOT NULL,
                        nickname            VARCHAR(50) NOT NULL
                    ); """)
    
    cursor.execute("""  CREATE TABLE IF NOT EXISTS players (
                        player_id       INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name            VARCHAR(50) NOT NULL,
                        jersey_number   INT NOT NULL,
                        class           VARCHAR(2) NOT NULL,
                        position        VARCHAR(1) NOT NULL,
                        height          VARCHAR(10) NULL,
                        weight          REAL NULL,
                        hometown        VARCHAR(30) NULL,
                        high_school     VARCHAR(30) NULL,
                        team            VARCHAR(50),

                        CONSTRAINT fk_team FOREIGN KEY (team) REFERENCES teams(name) ON DELETE CASCADE
                    ); """)

    cursor.execute("""  CREATE TABLE IF NOT EXISTS player_stats (
                        stats_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        player_id INT,
                        games_played INT,
                        games_started INT,
                        minutes_played NUMERIC(3, 1),
                        field_goals NUMERIC(3, 1),
                        field_goals_attempted NUMERIC(3, 1),
                        field_goal_percentage NUMERIC(3, 3) DEFAULT 0,
                        three_points NUMERIC(3, 1),
                        three_points_attempted NUMERIC(3, 1),
                        three_points_percentage NUMERIC(3, 3) DEFAULT 0,
                        two_points NUMERIC(3, 1),
                        two_points_attempted NUMERIC(3, 1),
                        two_points_percentage NUMERIC(3, 3) DEFAULT 0,
                        effective_field_goal_percentage NUMERIC (3, 3) DEFAULT 0,
                        free_throws NUMERIC(3, 1),
                        free_throws_attempted NUMERIC(3, 1),
                        free_throws_percentage NUMERIC(3, 3) DEFAULT 0,
                        offensive_rebounds NUMERIC(3, 1),
                        defensive_rebounds NUMERIC(3, 1),
                        total_rebounds NUMERIC(3, 1),
                        assists NUMERIC(3, 1),
                        steals NUMERIC(3, 1),
                        blocks NUMERIC(3, 1),
                        turnovers NUMERIC(3, 1),
                        personal_fouls NUMERIC(3, 1),
                        points_per_game NUMERIC(3, 1),
                   
                        CONSTRAINT fk_player FOREIGN KEY (player_id) REFERENCES players(player_id)
                    ); """)    

    conn.commit()
    cursor.close()