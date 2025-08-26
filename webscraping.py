import requests
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd


def extract_data():
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


def transform_data():
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

    roster_data.to_csv('data/updated_roster_data.csv', index=False)
    stats_data.to_csv('data/updated_stats_data.csv', index=False)