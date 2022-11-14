# CSC325 - Milestone 2
# 
# Authors: Josh Reed, Jake Conrad
from google.cloud.sql.connector import Connector
import pymysql
import pymysql.cursors
import pandas as pd
import numpy as np
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= "C:/Users/jrcon/OneDrive/Desktop/Junior/S1/CSC325/esoteric-quanta-361217-ae5bc1926cd5.json"

# make the connection to the db
def make_connection():
    return connector.connect(
    "esoteric-quanta-361217:us-central1:db325-instance",
    "pymysql",
    user="root",
    password="24=L%/F;kGc}.AN~",
    database=None
)

# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = connector.connect(
        "esoteric-quanta-361217:us-central1:db325-instance",
        "pymysql",
         user="root",
        password="24=L%/F;kGc}.AN~",
        db= None
    )
    return conn

# initialize dataframe object
df = pd.read_csv('C:/Users/jrcon/CSC325-Term_Project/Video_Games_Sales_as_at_22_Dec_2016.csv')

# converting NaN to None for MySQL compatibility
df = df.replace({np.nan: None})
#df = df.astype(object).where(pd.notnull(df),None) # other solution?

# sets up database
def setup_db(cursor):
    cursor.execute('CREATE DATABASE IF NOT EXISTS video_game_sales_db')
    cursor.execute('USE video_game_sales_db')

    cursor.execute('DROP TABLE IF EXISTS Project')
    cursor.execute('DROP TABLE IF EXISTS SaleScore')
    cursor.execute('DROP TABLE IF EXISTS Version')
    cursor.execute('DROP TABLE IF EXISTS Platform')
    cursor.execute('DROP TABLE IF EXISTS Game')
    cursor.execute('DROP TABLE IF EXISTS Publisher')
    cursor.execute('DROP TABLE IF EXISTS Developer')

    cursor.execute('''
    CREATE TABLE Publisher (
        name    VARCHAR(80) PRIMARY KEY NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE Developer (
        name    VARCHAR(80) PRIMARY KEY NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE Game (
        name            VARCHAR(80) NOT NULL,
        year_of_release VARCHAR(10) NOT NULL,
        genre           VARCHAR(20) NOT NULL,
        esrb_rating     VARCHAR(10),
        publisher_name  VARCHAR(80),
        FOREIGN KEY(publisher_name) REFERENCES Publisher(name),
        PRIMARY KEY(name, year_of_release)
    );
    ''')

    cursor.execute('''
    CREATE TABLE Project (
        developer_name      VARCHAR(80) NOT NULL,
        game_name           VARCHAR(80) NOT NULL,
        year_of_release     VARCHAR(10) NOT NULL,
        FOREIGN KEY(developer_name) REFERENCES Developer(name),
        FOREIGN KEY(game_name, year_of_release) REFERENCES Game(name, year_of_release),
        PRIMARY KEY(developer_name, game_name, year_of_release)
    );
    ''')

    cursor.execute('''
    CREATE TABLE Platform (
        name    VARCHAR(30) PRIMARY KEY NOT NULL
    );
    ''')

    cursor.execute('''
    CREATE TABLE Version (
        platform_name       VARCHAR(30) NOT NULL,
        game_name           VARCHAR(80) NOT NULL,
        year_of_release     VARCHAR(10) NOT NULL,
        FOREIGN KEY(platform_name) REFERENCES Platform(name),
        FOREIGN KEY(game_name, year_of_release) REFERENCES Game(name, year_of_release),
        PRIMARY KEY(platform_name, game_name, year_of_release)
    );
    ''')

    cursor.execute('''
    CREATE TABLE SaleScore (
        id              INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
        critic_score    INT,
        critic_count    INT,
        user_score      VARCHAR(20),
        user_count      INT,
        na_sales        INT NOT NULL,
        eu_sales        INT NOT NULL,
        jp_sales        INT NOT NULL,
        other_sales     INT NOT NULL,
        global_sales    INT NOT NULL,
        platform_name   VARCHAR(30),
        game_name       VARCHAR(80),
        year_of_release VARCHAR(10),
        FOREIGN KEY(platform_name, game_name, year_of_release) REFERENCES Version(platform_name, game_name, year_of_release)
    );
    ''')

# inserts data from csv file
def insert_data(cursor):
    count = 1
    for tuple in df.itertuples(name=None):
        # print(tuple)
        game_name = tuple[1]
        platform = tuple[2]
        year_of_release = tuple[3]
        genre = tuple[4]
        publisher = tuple[5]
        na_sales = tuple[6]
        eu_sales = tuple[7]
        jp_sales = tuple[8]
        other_sales = tuple[9]
        global_sales = tuple[10]
        critic_score = tuple[11]
        critic_count = tuple[12]
        user_score = tuple[13]
        user_count = tuple[14]
        developer = tuple[15]
        if developer != None:
            developer = developer.split(', ')
        rating = tuple[16]

        print(str(count))
        count += 1
        # insert into Publisher
        if publisher != None:
            cursor.execute('''INSERT IGNORE INTO Publisher (name) 
                VALUES ( %s ) ''', (publisher))
    
        # insert into Developer
        if developer != None:
            for entry in developer:
                cursor.execute('''INSERT IGNORE INTO Developer (name) 
                    VALUES ( %s ) ''', (entry))
        
        # insert into Game
        # gets foreign key - Publisher Id
        # cursor.execute('SELECT id FROM Publisher WHERE name = %s ', (publisher))
        # publisher_id = cursor.fetchone().get('id')
        cursor.execute('''INSERT IGNORE into Game (name, year_of_release, genre, esrb_rating, publisher_name) 
            VALUES ( %s, %s, %s, %s, %s ) ''', (game_name, year_of_release, genre, rating, publisher))

        # insert into project
        if developer != None:
            for entry in developer:
                cursor.execute('''INSERT IGNORE into Project (developer_name, game_name, year_of_release) 
                    VALUES ( %s, %s, %s ) ''', (entry, game_name, year_of_release))

        # insert into Platform
        cursor.execute('''INSERT IGNORE INTO Platform (name) 
            VALUES ( %s ) ''', (platform))

        # insert into Version
        # # gets foreign key - Platform id
        # cursor.execute('SELECT id FROM Platform WHERE name = %s ', (platform))
        # platform_id = cursor.fetchone().get('id')
        cursor.execute(''' INSERT IGNORE INTO Version (platform_name, game_name, year_of_release) 
            VALUES ( %s, %s, %s) ''', (platform, game_name, year_of_release))
        
        # insert into SaleScore
        cursor.execute(''' INSERT IGNORE INTO SaleScore (critic_score, critic_count, user_score, user_count, na_sales, eu_sales, jp_sales,
            other_sales, global_sales, platform_name, game_name, year_of_release) 
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) ''', (critic_score, critic_count, user_score, user_count, na_sales,
            eu_sales, jp_sales, other_sales, global_sales, platform, game_name, year_of_release))

cnx = getconn() 
cur = cnx.cursor()
print("Starting Setup...")
setup_db(cur)
print("Finished Setup.")
print("Starting Insert...")
insert_data(cur)
print("Finished Insert.")
cur.close()
cnx.commit()
cnx.close()
print("FINISHED")