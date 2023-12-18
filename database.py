CREATE_POLLS = """CREATE TABLE IF NOT EXISTS polls
(id SERIAL PRIMARY KEY, title TEXT, owner_username TEXT);"""
CREATE_OPTIONS = """CREATE TABLE IF NOT EXISTS options
(id SERIAL PRIMARY KEY, option_text TEXT, poll_id INTEGER, FOREIGN KEY(poll_id) REFERENCES polls (id));"""
CREATE_VOTES = """CREATE TABLE IF NOT EXISTS votes
(username TEXT, option_id INTEGER, FOREIGN KEY(option_id) REFERENCES options (id));"""


SELECT_ALL_POLLS = "SELECT * FROM polls;"
SELECT_POLL_WITH_OPTIONS = """SELECT * FROM polls
JOIN options ON polls.id = options.poll_id
WHERE polls.id = %s;"""
SELECT_LATEST_POLL = """SELECT * FROM polls JOIN options 
    ON polls.id = options.poll_id 
    WHERE polls.id = (SELECT id FROM polls ORDER BY DESC LIMIT 1);"""
SELECT_RANDOM_CORRECT_VOTER = "SELECT * FROM votes WHERE option_id = %s ORDER BY RANDOM() LIMIT 1;"

# in this below query. We are getting the options.id, options.options_text, and COUNT(votes.option_id) from
# the group by options_id. The vote_percent is calculated using the count and a sum over a window, which is our whole table
SELECT_POLL_VOTE_DETAILS = """SELECT options.id, options.option_text, COUNT(votes.option_id) AS vote_count,
COUNT(votes.option_id)/SUM(COUNT(votes.option_id)) OVER() * 100.0 AS vote_percent
FROM options LEFT JOIN votes ON options.id = votes.option_id
WHERE options.poll_id = %s
GROUP BY options.id;"""
SELECT_RANKED_POLLS = """SELECT polls.title, COUNT(votes) as vote_count, 
    RANK() OVER(ORDER BY COUNT(votes) DESC)
    FROM polls
    LEFT JOIN options ON options.poll_id = polls.id
    LEFT JOIN votes ON votes.option_id = options.id
    GROUP BY polls.title;"""
SELECT_POLLS_WITH_VOTE_COUNTS = """SELECT polls.title, COUNT(votes) as vote_count
    FROM polls
    LEFT JOIN options ON options.poll_id = polls.id
    LEFT JOIN votes ON votes.option_id = options.id
    GROUP BY polls.title;"""

INSERT_POLL_RETURN_ID = "INSERT INTO polls (title, owner_username) VALUES (%s, %s) RETURNING id;"
INSERT_OPTION = "INSERT INTO options (option_text, poll_id) VALUES %s;"
INSERT_VOTE = "INSERT INTO votes (username, option_id) VALUES (%s, %s);"

from psycopg2.extras import execute_values # optimized for loop


def create_tables(connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_POLLS)
            cursor.execute(CREATE_OPTIONS)
            cursor.execute(CREATE_VOTES)


def get_polls(connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_ALL_POLLS)
            return cursor.fetchall()


def get_latest_poll(connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_LATEST_POLL)
            return cursor.fetchone()


def get_poll_details(connection, poll_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_POLL_WITH_OPTIONS, (poll_id,))
            return cursor.fetchall()


def get_poll_and_vote_results(connection, poll_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_POLL_VOTE_DETAILS, (poll_id, ))
            return cursor.fetchall()


def get_random_poll_vote(connection, option_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_RANDOM_CORRECT_VOTER, (option_id, ))
            return cursor.fetchone()

            # import random
            # cursor.execute("SELECT * FROM votes WHERE option_id = %s", (option_id,))
            # choice = random.choice(list(cursor.fetchall()))
            # return choice


def create_poll(connection, title, owner, options):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_POLL_RETURN_ID, (title, owner))  # please note the RETURNING keyword used in the query

            poll_id = cursor.fetchone()[0]  # this gives us the id that was given back using the RETURNING keyword in sql
            option_values = [(option_text, poll_id) for option_text in options]  # list comp to pair the option text with the poll id

            execute_values(cursor, INSERT_OPTION, option_values) # cursor, query, and list of tuples

            # execute_values is looping through the option_values,
            # running the INSERT_OPTION query with each option value as a parameter, using the cursor for our db.
            # we can also do this with our own loop, but this is faster.
            # Note it is imported from psycopg2.extras at the top


def add_poll_vote(connection, username, option_id):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_VOTE, (username, option_id))


def select_rank_polls(connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_RANKED_POLLS)
            return cursor.fetchall()


def get_ranked_polls_python(connection):
    # Function not used. If we decided to rank in Python rather than SQL.
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SELECT_POLLS_WITH_VOTE_COUNTS)
            ranked_polls = sorted(list(cursor.fetchall()), key=lambda x: x[1], reverse=True)
            return ranked_polls
