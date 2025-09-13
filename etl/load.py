def load_data(conn):
    create_tables(conn)
    load_teams(conn)
    load_players(conn)
    load_player_stats(conn)


def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""  CREATE TABLE IF NOT EXISTS teams (
                        id                  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name                VARCHAR(50) NOT NULL UNIQUE,
                        record              VARCHAR(50) NOT NULL,
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
                        id                  INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        name                VARCHAR(50) NOT NULL,
                        jersey_number       INT NOT NULL,
                        class               VARCHAR(2) NULL,
                        position            VARCHAR(1) NOT NULL,
                        height              VARCHAR(10) NULL,
                        weight              REAL NULL,
                        hometown            VARCHAR(150) NULL,
                        high_school         VARCHAR(150) NULL,
                        team                VARCHAR(50),

                        CONSTRAINT fk_team FOREIGN KEY (team) REFERENCES teams(name) ON DELETE CASCADE
                    ); """)

    cursor.execute("""  CREATE TABLE IF NOT EXISTS player_stats (
                        id                              INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        player_id                       INT NOT NULL,
                        games_played                    INT,
                        games_started                   INT,
                        minutes_played                  NUMERIC(3, 1),
                        field_goals                     NUMERIC(3, 1),
                        field_goals_attempted           NUMERIC(3, 1),
                        field_goal_percentage           NUMERIC(4, 3) DEFAULT 0,
                        three_points                    NUMERIC(3, 1),
                        three_points_attempted          NUMERIC(3, 1),
                        three_point_percentage          NUMERIC(4, 3) DEFAULT 0,
                        two_points                      NUMERIC(3, 1),
                        two_points_attempted            NUMERIC(3, 1),
                        two_point_percentage            NUMERIC(4, 3) DEFAULT 0,
                        effective_field_goal_percentage NUMERIC (4, 3) DEFAULT 0,
                        free_throws                     NUMERIC(3, 1),
                        free_throws_attempted           NUMERIC(3, 1),
                        free_throws_percentage          NUMERIC(4, 3) DEFAULT 0,
                        offensive_rebounds              NUMERIC(3, 1),
                        defensive_rebounds              NUMERIC(3, 1),
                        total_rebounds                  NUMERIC(3, 1),
                        assists                         NUMERIC(3, 1),
                        steals                          NUMERIC(3, 1),
                        blocks                          NUMERIC(3, 1),
                        turnovers                       NUMERIC(3, 1),
                        personal_fouls                  NUMERIC(3, 1),
                        points_per_game                 NUMERIC(3, 1),
                   
                        CONSTRAINT fk_player FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
                    ); """)    

    conn.commit()
    cursor.close()


def load_teams(conn):
    cursor = conn.cursor()
    with open('data/cleaned_teams_data.csv', 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
                            COPY teams(name, record, wins, losses, conference_wins, conference_losses,
                                       university, coach, conference, location, nickname)
                            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                            """,
                            f
                            )
    
    conn.commit()
    cursor.close()
        

def load_players(conn):
    cursor = conn.cursor()
    with open('data/cleaned_roster_data.csv', 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
                            COPY players(name, jersey_number, class, position, height,
                                         weight, hometown, high_school, team)
                            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                            """,
                            f
                            )
        
    conn.commit()
    cursor.close()


def load_player_stats(conn):
    cursor = conn.cursor()
    with open('data/cleaned_stats_data.csv', 'r', encoding='utf-8') as f:
        cursor.copy_expert("""
                            COPY player_stats(player_id, games_played, games_started, minutes_played, field_goals, field_goals_attempted,
                                              field_goal_percentage, three_points, three_points_attempted, three_point_percentage,
                                              two_points, two_points_attempted, two_point_percentage, effective_field_goal_percentage,
                                              free_throws, free_throws_attempted, free_throws_percentage, offensive_rebounds,
                                              defensive_rebounds, total_rebounds, assists, steals, blocks, turnovers, personal_fouls, points_per_game)
                            FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                            """,
                            f
                            )
        
    conn.commit()
    cursor.close()