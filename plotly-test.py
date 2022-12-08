import pymysql
import pymysql.cursors
import warnings
import pandas as pd
import plotly.express as px
import os

# ignore warning
# getting warning about connection using local DB
warnings.simplefilter("ignore")

# Color theme used by the visualizations
color = px.colors.sequential.Agsunset

# Creates a pie chart for the genre distribution
def genre_distribution(connection, cursor):
    cursor.execute('USE video_game_sales_db;')
    # Queries database
    df = pd.read_sql('''SELECT genre, COUNT(genre) as genre_count 
                        FROM game
                        WHERE genre != ''
                        GROUP BY genre;''', connection)
    # Creates figure
    fig = px.pie(df, values = 'genre_count', names = 'genre', 
                labels = {
                    'genre_count': 'Number of Games',
                    'genre': 'Genre'
                },
                title = 'Game Genre Distribution', color_discrete_sequence = color)
    fig.show()

# Creates a pie chart for the platform distribution
def platform_distribution(connection, cursor):
    cursor.execute('USE video_game_sales_db;')
    # Queries database
    df_main = pd.read_sql('''SELECT platform_name, COUNT(platform_name) as platform_count 
                        FROM version 
                        GROUP BY platform_name
                        HAVING platform_count > 400;''', connection)
    # Gets the combined value for all the miscellaneous categories, platforms with less than 400 games
    df_misc = pd.read_sql('''SELECT COUNT(platform_name) as platform_count
                            FROM version
                            WHERE platform_name IN (SELECT platform_name
                                                    FROM version
                                                    GROUP BY platform_name
                                                    HAVING COUNT(platform_name) <= 400);''', connection)
    count = df_misc['platform_count'].iloc[0]
    df_temp = pd.DataFrame({'platform_name': ['Other'], 'platform_count': [count]})
    # Adds new "other" category to the dataframe
    df = pd.concat([df_main, df_temp])

    # Creates main figure
    fig = px.pie(df, values = 'platform_count', names = 'platform_name', 
                labels = {
                    'platform_count': 'Number of Games',
                    'platform_name': 'Platform Name'
                },
                title = 'Game Platform Distribution', color_discrete_sequence = color)
    fig.show()

    # Queries database
    df_misc = pd.read_sql('''SELECT platform_name, COUNT(platform_name) as platform_count 
                        FROM version 
                        GROUP BY platform_name
                        HAVING platform_count <= 400 AND platform_count >= 15;''', connection)
    # Creates "other" figure
    fig = px.pie(df_misc, values = 'platform_count', names = 'platform_name',
                labels = {
                    'platform_count': 'Number of Games',
                    'platform_name': 'Platform Name'
                },
                title = 'Game Platform Distribution', color_discrete_sequence = color)
    fig.show()

# Creates a bar graph for the sales per platform
def sales_by_platform(connection, cursor):
    cursor.execute('USE video_game_sales_db;')
    # Queries database
    # Gets all platforms with over 100 millions game sales
    df = pd.read_sql('''SELECT platform_name, SUM(global_sales) as total_sales 
                        FROM salescore 
                        GROUP BY platform_name 
                        HAVING total_sales > 100;''', connection)
    # Creates figure
    fig = px.bar(df, x = 'platform_name', y = 'total_sales', 
                labels = {
                    'platform_name': 'Platform Name',
                    'total_sales': 'Total Sales (million)'
                },
                title = 'Global Sales By Platform', color_discrete_sequence = color)
    fig.show()

# Creates a line graph for the sales by year, showing the trend in different regions, as well as globally
def sales_by_year(connection, cursor):
    cursor.execute('USE video_game_sales_db;')
    # Queries database
    df = pd.read_sql('''SELECT SUM(na_sales) as 'North America', SUM(eu_sales) as Europe, SUM(jp_sales) as Japan, 
                            SUM(global_sales) as Global, SUM(other_sales) as Other, year_of_release 
                        FROM salescore 
                        WHERE global_sales > 0
                        GROUP BY year_of_release 
                        ORDER BY year_of_release ASC;''', connection)
    # Creates figure
    fig = px.line(df, x = 'year_of_release', y = ['Global', 'North America', 'Europe', 'Japan'],
                labels = {
                    'year_of_release': 'Year of Release',
                    'value': 'Global Sales (million)',
                    'variable': 'Region',
                    'global_total': 'Global',
                    'na_total': 'North America',
                    'eu_total': 'Europe',
                    'jp_total': 'Japan'
                },
                title = 'Game Sales Per Year', markers = True, color_discrete_sequence = color)
    fig.show()

def ratings_by_year(connection, cursor):
    cursor.execute('USE video_game_sales_db;')
    # Queries database
    df = pd.read_sql('''SELECT ROUND(AVG(user_score),2) as User, ROUND(AVG(critic_score)/10,2) as Critic, year_of_release
                    FROM salescore
                    WHERE user_score != '' AND critic_score != '' AND year_of_release != '' AND year_of_release >= 2000
                    GROUP BY year_of_release
                    ORDER BY year_of_release ASC;''', connection)
    # Creates figure
    fig = px.bar(df, x = 'year_of_release', y = ['User', 'Critic'], barmode = 'group',
                labels = {
                    'year_of_release': 'Year of Release',
                    'value': 'Average Score',
                    'variable': 'Average Score'
                },
                title = 'Game Scores Per Year', color_discrete_sequence = color)
    fig.show()

genre_distribution(connection, cursor)
platform_distribution(connection, cursor)
sales_by_platform(connection, cursor)
sales_by_year(connection, cursor)
ratings_by_year(connection, cursor)
