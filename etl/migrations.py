def link_players_to_teams(conn):
    cursor = conn.cursor()

    cursor.execute("""  ALTER TABLE players
                        ADD COLUMN IF NOT EXISTS
                        team_id INT;
                   """)
    
    cursor.execute("""  UPDATE players
                        SET team_id = teams.id
                        FROM teams
                        WHERE players.team = teams.name;
                   """)
    
    cursor.execute("""  ALTER TABLE players
                        ADD CONSTRAINT fk_team_id
                        FOREIGN KEY (team_id) REFERENCES teams(id);
                   """)
    
    cursor.execute("""  ALTER TABLE players
                        DROP COLUMN team;
                   """)
    
    cursor.execute("""  ALTER TABLE players
                        DROP CONSTRAINT IF EXISTS fk_team;
                   """)
    
    conn.commit()
    cursor.close()