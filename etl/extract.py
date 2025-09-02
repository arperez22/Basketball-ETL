import requests
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd


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