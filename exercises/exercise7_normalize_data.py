'''
Assume that all tweets have been imported into database table tweet_sample_raw.
'''

# imports
import datetime
import sqlite3
import sys
import traceback

# Twitter variables
TWITTER_DATE_FORMAT = '%a %b %d %H:%M:%S +0000 %Y'

#==============================================================================#
# define functions
#==============================================================================#


def create_tweet_row( raw_row_IN, cursor_IN, connection_IN ):

    '''
    accepts a row from the raw tweet data table, a cursor, and a connection.
       Checks to see if row exists for the tweet.  If so, just returns its ID.
       If not, uses data in raw tweet row to create and populate a normalized
       tweet row for the tweet and returns ID of newly created row.
    '''

    # return reference
    row_id_OUT = -1

    # declare variables
    
    # variables for checking to see if already exists.
    twitter_tweet_id = ""
    lookup_sql_string = ""
    lookup_rs = None
    lookup_result_row = None
    lookup_result_count = -1

    # values for inclusion in new tweet.
    twitter_tweet_id = -1
    tweet_user_id = -1
    tweet_twitter_user_id = -1
    tweet_timestamp = ""
    tweet_timestamp_dt = None
    tweet_text = ""
    tweet_language = ""
    tweet_retweet_count = -1
    tweet_user_mention_count = -1
    tweet_hashtag_mention_count = -1
    tweet_url_count = -1
    tweet_place = ""
    tweet_geo = ""
    insert_sql_string = ""
    value_tuple = ()

    # get twitter_tweet_id.
    twitter_tweet_id = raw_row_IN[ "twitter_tweet_id" ]

    # check to see if row already exists.
    lookup_sql_string = "SELECT COUNT( * ) AS tweet_count FROM tweet WHERE twitter_tweet_id = ?;"
    lookup_rs = cursor_IN.execute( lookup_sql_string, [ twitter_tweet_id ] )

    # get count
    lookup_result_row = cursor_IN.fetchone()
    lookup_result_count = lookup_result_row[ "tweet_count" ]

    # count = 0?
    if lookup_result_count == 0:

        # no existing tweet.

        # get values prepped for new record, adding each to list as you go.

        # twitter_tweet_id
        twitter_tweet_id = raw_row_IN[ "twitter_tweet_id" ]
        
        # tweet_user_id - for now, just set "foreign key" to 0.
        tweet_user_id = 0

        # tweet_twitter_user_id
        tweet_twitter_user_id = raw_row_IN[ "twitter_user_twitter_id" ]

        # tweet_timestamp
        tweet_timestamp = raw_row_IN[ "tweet_timestamp" ]

        # tweet_timestamp_dt - parse date into datetime
        tweet_timestamp_dt = datetime.datetime.strptime( tweet_timestamp, TWITTER_DATE_FORMAT )

        # tweet_text
        tweet_text = raw_row_IN[ "tweet_text" ]

        # tweet_language
        tweet_language = raw_row_IN[ "tweet_language" ]

        # tweet_retweet_count
        tweet_retweet_count = raw_row_IN[ "tweet_retweet_count" ]

        # tweet_user_mention_count
        tweet_user_mention_count = raw_row_IN[ "tweet_user_mention_count" ]

        # tweet_hashtag_mention_count
        tweet_hashtag_mention_count = raw_row_IN[ "tweet_hashtag_mention_count" ]

        # tweet_url_count
        tweet_url_count = raw_row_IN[ "tweet_url_count" ]

        # tweet_place
        tweet_place = raw_row_IN[ "tweet_place" ]

        # tweet_geo - not present in file if loaded with pandas, so leaving out.
        #tweet_geo = raw_row_IN[ "tweet_geo" ]

        # create INSERT statement
        insert_sql_string = '''
            INSERT INTO tweet
            (
                twitter_tweet_id,
                tweet_user_id,
                tweet_twitter_user_id,
                tweet_timestamp,
                tweet_timestamp_dt,
                tweet_text,
                tweet_language,
                tweet_retweet_count,
                tweet_user_mention_count,
                tweet_hashtag_mention_count,
                tweet_url_count,
                tweet_place,
                tweet_geo
            )
            VALUES
            (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?
            );
        '''

        # value tuple
        value_tuple = ( twitter_tweet_id, tweet_user_id, tweet_twitter_user_id, tweet_timestamp, tweet_timestamp_dt, tweet_text, tweet_language, tweet_retweet_count, tweet_user_mention_count, tweet_hashtag_mention_count, tweet_url_count, tweet_place, tweet_geo )

        # run INSERT statement
        cursor_IN.execute( insert_sql_string, value_tuple )

        # get ID of newly created row.
        row_id_OUT = cursor_IN.lastrowid

        # commit
        connection_IN.commit()

        print( "Created new tweet for " + str( twitter_tweet_id ) + ": " + str( row_id_OUT ) )

    else:

        # if no, do select to get matching row, return its ID.
        lookup_sql_string = "SELECT * FROM tweet WHERE twitter_tweet_id = ?;"
        lookup_rs = cursor_IN.execute( lookup_sql_string, [ twitter_tweet_id ] )
        
        # if you are worried about corruption, you can do a loop here, count
        #    matching rows.  If more than 1, then you have problems.

        # assuming only 1 match - get matching row.
        lookup_result_row = cursor_IN.fetchone()

        # get and return ID
        row_id_OUT = lookup_result_row[ "id" ]

        print( "Found existing tweet for " + str( twitter_tweet_id ) + ": " + str( row_id_OUT ) )


    #-- END check to see if existing rows. --#

    return row_id_OUT

#-- END function create_tweet_row --#


def create_user_row( raw_row_IN, cursor_IN, connection_IN ):

    '''
    accepts a row from the raw tweet data table, a cursor, and a connection.
       Checks to see if row exists for the tweet's user.  If so, just returns
       its ID.  If not, uses data in raw tweet row to create and populate a
       normalized user row for the tweet's user and returns ID of newly created
       row.
    '''

    # return reference
    row_id_OUT = -1

    # declare variables
    
    # variables for checking to see if already exists.
    twitter_user_twitter_id = ""
    lookup_sql_string = ""
    lookup_rs = None
    lookup_result_row = None
    lookup_result_count = -1

    # values for inclusion in new tweet.
    tweet_twitter_user_id = -1
    twitter_user_screenname = ""
    user_created = ""
    user_created_dt = None
    user_followers_count = -1
    user_favorites_count = -1
    user_location = ""
    user_description = ""
    user_friends_count = -1

    insert_sql_string = ""
    value_tuple = ()

    # get twitter_user_twitter_id.
    twitter_user_twitter_id = raw_row_IN[ "twitter_user_twitter_id" ]

    # check to see if row already exists.
    lookup_sql_string = "SELECT COUNT( * ) AS user_count FROM user WHERE twitter_user_twitter_id = ?;"
    lookup_rs = cursor_IN.execute( lookup_sql_string, [ twitter_user_twitter_id ] )

    # get count
    lookup_result_row = cursor_IN.fetchone()
    lookup_result_count = lookup_result_row[ "user_count" ]

    # count = 0?
    if lookup_result_count == 0:

        # no existing tweet.

        # get values prepped for new record, adding each to list as you go.

        # twitter_user_twitter_id
        twitter_user_twitter_id = raw_row_IN[ "twitter_user_twitter_id" ]
        
        # twitter_user_screenname
        twitter_user_screenname = raw_row_IN[ "twitter_user_screenname" ]

        # user_created
        user_created = raw_row_IN[ "user_created" ]

        # user_created_dt - parse date into datetime
        user_created_dt = datetime.datetime.strptime( user_created, TWITTER_DATE_FORMAT )

        # user_followers_count
        user_followers_count = raw_row_IN[ "user_followers_count" ]

        # user_favorites_count
        user_favorites_count = raw_row_IN[ "user_favorites_count" ]

        # user_location
        user_location = raw_row_IN[ "user_location" ]

        # user_description
        user_description = raw_row_IN[ "user_description" ]

        # user_friends_count
        user_friends_count = raw_row_IN[ "user_friends_count" ]

        # user_friends_count
        user_statuses_count = raw_row_IN[ "user_friends_count" ]

        # create INSERT statement
        insert_sql_string = '''
            INSERT INTO user
            (
                twitter_user_twitter_id,
                twitter_user_screenname,
                user_created,
                user_created_dt,
                user_followers_count,
                user_favorites_count,
                user_location,
                user_description,
                user_friends_count,
                user_statuses_count
            )
            VALUES
            (
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?,
                ?
            );
        '''

        # value tuple
        value_tuple = ( twitter_user_twitter_id, twitter_user_screenname, user_created, user_created_dt, user_followers_count, user_favorites_count, user_location, user_description, user_friends_count, user_statuses_count )

        # run INSERT statement
        cursor_IN.execute( insert_sql_string, value_tuple )

        # get ID of newly created row.
        row_id_OUT = cursor_IN.lastrowid

        # commit
        connection_IN.commit()

        print( "Created new user for " + str( twitter_user_twitter_id ) + ": " + str( row_id_OUT ) )

    else:

        # if no, do select to get matching row, return its ID.
        lookup_sql_string = "SELECT * FROM user WHERE twitter_user_twitter_id = ?;"
        lookup_rs = cursor_IN.execute( lookup_sql_string, [ twitter_user_twitter_id ] )
        
        # if you are worried about corruption, you can do a loop here, count
        #    matching rows.  If more than 1, then you have problems.

        # assuming only 1 match - get matching row.
        lookup_result_row = cursor_IN.fetchone()

        # get and return ID
        row_id_OUT = lookup_result_row[ "id" ]

        print( "Found existing user for " + str( twitter_user_twitter_id ) + ": " + str( row_id_OUT ) )

    #-- END check to see if existing rows. --#

    return row_id_OUT

#-- END function create_user_row --#


def relate_user_to_tweet( tweet_id_IN, user_id_IN, cursor_IN, connection_IN ):

    '''
    accepts a tweet ID and a user ID, a cursor, and a connection.  Updates the
       tweet so that it contains user ID passed in, returns user ID.  If error,
       returns -1.

    To test:
    SELECT COUNT( * )
    FROM tweet t, user u
    WHERE t.tweet_user_id = u.id
        AND t.tweet_twitter_user_id != u.twitter_user_twitter_id;
    /* should be 0 */

    SELECT COUNT( * )
    FROM tweet t, user u
    WHERE t.tweet_user_id = u.id
        AND t.tweet_twitter_user_id = u.twitter_user_twitter_id;
    /* should be same number as number of rows in tweet. */
    '''

    # return reference
    row_id_OUT = -1

    # declare variables
    me = "relate_user_to_tweet"
    update_sql_string = ""
    value_tuple = ()

    # make sure you have a tweet ID
    if ( ( tweet_id_IN != None ) and ( tweet_id_IN > 0 ) ): 

        # and make sure you have a user ID.
        if ( ( user_id_IN != None ) and ( user_id_IN >= 0 ) ):

            # create update string
            update_sql_string = "UPDATE tweet SET tweet_user_id = ? WHERE id = ?;"

            # prepare UPDATE values - user ID (SET), then tweet ID (WHERE).
            value_tuple = ( user_id_IN, tweet_id_IN )

            # execute the update.
            cursor_IN.execute( update_sql_string, value_tuple )

            # commit.
            connection_IN.commit()

            # return ID
            row_id_OUT = user_id_IN

        else:

            # tweet ID, but no user ID.  error.
            row_id_OUT = -1

        #-- END check to see if user ID --#

    else:

        # no tweet ID.  Error.
        row_id_OUT = -1

    #-- END check to see if tweet ID --#

    return row_id_OUT

#-- END function relate_user_to_tweet() --#


def create_hashtag_rows( raw_row_IN, cursor_IN, connection_IN ):

    '''
    Accepts raw tweet row, cursor and connection.  Uses tweet_hashtags_mentioned
       column, a comma-delimited list of hashtags.  Splits on comma, then for
       each hashtag, checks to see if it is in the database already.  If yes,
       gets ID and adds it to output list.  If no, creates row, gets ID of new
       record, then add ID to output list.  Returns list of hashtags associated
       with the tweet.
    '''

    # return reference
    hashtag_list_OUT = []

    # declare variables
    string_hashtag_list = ""
    hashtag_list = []
    hashtag_count = -1
    current_hashtag = ""
    select_sql_string = ""
    select_result = None
    lookup_result_row = None
    lookup_result_count = -1
    insert_sql_string = ""
    hashtag_id = -1

    # get string hashtag list
    string_hashtag_list = raw_row_IN[ "tweet_hashtags_mentioned" ]

    # got anything at all?
    if ( ( string_hashtag_list is not None ) and ( string_hashtag_list != "" ) ):

        # convert to formal Python list
        hashtag_list = string_hashtag_list.split( "," )

        # got anything?
        hashtag_count = len( hashtag_list )
        if hashtag_count > 0:

            # got more than one.  Loop!
            for current_hashtag in hashtag_list:

                # check to see if hashtag is already in table.
                select_sql_string = "SELECT COUNT( * ) AS hashtag_count FROM hashtag WHERE hashtag_value = ?;"

                # run SQL
                select_result = cursor_IN.execute( select_sql_string, [ current_hashtag ] )

                # get count
                lookup_result_row = cursor_IN.fetchone()
                lookup_result_count = lookup_result_row[ "hashtag_count" ]

                # any matches?
                if ( lookup_result_count == 0 ):

                    # no - make new row, then add newly created ID to list.
                    insert_sql_string = "INSERT INTO hashtag ( hashtag_value ) VALUES ( ? );"

                    # perform INSERT
                    cursor_IN.execute( insert_sql_string, [ current_hashtag ] )

                    # get ID of new row
                    hashtag_id = cursor_IN.lastrowid

                    # commit.
                    connection_IN.commit()

                else:

                    # yes - get ID of matching row.
                    select_sql_string = "SELECT id FROM hashtag WHERE hashtag_value = ?;"

                    # run SQL
                    select_result = cursor_IN.execute( select_sql_string, [ current_hashtag ] )

                    # get ID
                    lookup_result_row = cursor_IN.fetchone()
                    hashtag_id = lookup_result_row[ "id" ]

                #-- END check to see if matches. --#

                # regardless of how we found the ID, add it to the list.
                hashtag_list_OUT.append( hashtag_id )

            #-- END loop over hashtags. --#

        #-- END check to see if list length greater than 0 --#

    #-- END check to see if any hashtags --#

    return hashtag_list_OUT

#-- END function create_hashtag_rows() --#


def join_tweet_to_hashtags( tweet_id_IN, hashtag_id_list_IN, cursor_IN, connection_IN ):

    '''
    Accepts tweet ID, hashtag ID list, cursor and connection.  For each hashtag
       ID, checks to see if join row exists for tweet.  If not, creates join row
       then adds ID of new row to list.  If yes, gets ID of row, adds it to the
       list.  Returns list of join rows.
    '''

    # return reference
    join_row_id_list = []

    # declare variables
    hashtag_id_count = -1
    current_hashtag_id = -1
    select_sql_string = ""
    select_result = ""
    join_row_id = -1
    insert_sql_string = ""
    values_list = []

    # make sure we have a tweet ID.
    if ( ( tweet_id_IN is not None ) and ( tweet_id_IN > 0 ) ):

        # yes.  Anything in hashtag list?
        hashtag_id_count = len( hashtag_id_list_IN )
        if ( hashtag_id_count > 0 ):

            # yes.  Loop!
            for current_hashtag_id in hashtag_id_list_IN:

                # check if row is present.
                select_sql_string = "SELECT * FROM tweet_hashtag WHERE tweet_id = ? AND hashtag_id = ?;"
                values_list = [ tweet_id_IN, current_hashtag_id ]

                # execute SELECT
                cursor_IN.execute( select_sql_string, values_list )

                # get result row.
                select_result = cursor_IN.fetchone()

                # Got one?
                if ( select_result is not None ):

                    # yes.  Get ID to add to list.
                    join_row_id = select_result[ "id" ]

                else:

                    # no. INSERT it, then get new row's ID.
                    insert_sql_string = "INSERT INTO tweet_hashtag ( tweet_id, hashtag_id ) VALUES ( ?, ? )"
                    values_list = [ tweet_id_IN, current_hashtag_id ]

                    # INSERT
                    cursor_IN.execute( insert_sql_string, values_list )

                    # get ID of newly-created row.
                    join_row_id = cursor_IN.lastrowid

                    # commit.
                    connection_IN.commit()

                #-- END check to see if got result. --#

                # Add ID to list.
                join_row_id_list.append( join_row_id )

            #-- END loop over hashtag IDs. --#

        #-- END check to see if hashtag IDs --#

    #-- END check to see if tweet ID --#

    return join_row_id_list

#-- END function join_tweet_to_hashtags() --#


def join_tweet_to_user_mentions( raw_row_IN, tweet_id_IN, cursor_IN, connection_IN ):

    '''
    Accepts current raw tweet row, ID of current tweet, cursor and connection.
       From raw row, gets the list of IDs of users mentioned, comma-delimited.
       for each, looks up user.  If user exists, gets ID.  If not, creates user
       row with ID and screenname, and retrieves ID of newly created user.
       Then, creates join row table to join tweet and mentioned user.  Returns
       list of IDs of join rows.
    '''

    # return reference
    join_row_id_list = []

    # declare variables
    mentioned_user_id_list_string = ""
    mentioned_user_screenname_list_string = ""
    mentioned_user_id_list = []
    mentioned_user_screenname_list = []
    user_list_index = -1
    current_twitter_user_id = -1
    select_sql_string = ""
    value_list = []
    select_result = None
    current_user_id = -1
    current_screenname = ""
    join_row_id = -1

    # make sure we have a tweet ID
    if ( ( tweet_id_IN is not None ) and ( tweet_id_IN > 0 ) ):

        # get mentioned name lists
        mentioned_user_id_list_string = raw_row_IN[ "tweet_users_mentioned_ids" ]
        mentioned_user_screenname_list_string = raw_row_IN[ "tweet_users_mentioned_screennames" ]

        # anything in the list?
        if ( ( mentioned_user_id_list_string is not None ) and ( mentioned_user_id_list_string != "" ) ):

            # convert to actual Python lists.
            mentioned_user_id_list = mentioned_user_id_list_string.split( "," )
            mentioned_user_screenname_list = mentioned_user_screenname_list_string.split( "," )

            # loop over the list of twitter user IDs.
            user_list_index = -1
            for current_twitter_user_id in mentioned_user_id_list:

                # increment list index
                user_list_index = user_list_index + 1

                # look for user.
                select_sql_string = "SELECT * FROM user WHERE twitter_user_twitter_id = ?;"
                value_list = [ current_twitter_user_id ]

                # execute SELECT
                cursor_IN.execute( select_sql_string, value_list )
                select_result = cursor_IN.fetchone()

                # Got one?
                if ( select_result is not None ):

                    # yes.  Get ID to add to list.
                    current_user_id = select_result[ "id" ]

                else:

                    # no. INSERT into user table, then get new row's ID.

                    # get screen name
                    current_screenname = mentioned_user_screenname_list[ user_list_index ]

                    # create INSERT SQL
                    insert_sql_string = "INSERT INTO user ( twitter_user_twitter_id, twitter_user_screenname ) VALUES ( ?, ? )"
                    values_list = [ current_twitter_user_id, current_screenname ]

                    # INSERT
                    cursor_IN.execute( insert_sql_string, values_list )

                    # get ID of newly-created row.
                    current_user_id = cursor_IN.lastrowid

                    # commit.
                    connection_IN.commit()

                #-- END check to see if got result. --#

                # now, need to see if there is a join row for the combination
                #    of tweet ID and user ID.
                select_sql_string = "SELECT * FROM tweet_user_mentions WHERE tweet_id = ? AND mentioned_user_id = ?;"
                values_list = [ tweet_id_IN, current_user_id ]

                # execute SELECT
                cursor_IN.execute( select_sql_string, values_list )

                # get result row.
                select_result = cursor_IN.fetchone()

                # Got one?
                if ( select_result is not None ):

                    # yes.  Get ID to add to list.
                    join_row_id = select_result[ "id" ]

                else:

                    # no. INSERT it, then get new row's ID.
                    insert_sql_string = "INSERT INTO tweet_user_mentions ( tweet_id, mentioned_user_id ) VALUES ( ?, ? )"
                    values_list = [ tweet_id_IN, current_user_id ]

                    # INSERT
                    cursor_IN.execute( insert_sql_string, values_list )

                    # get ID of newly-created row.
                    join_row_id = cursor_IN.lastrowid

                    # commit.
                    connection_IN.commit()

                #-- END check to see if got result. --#

                # Add ID to list.
                join_row_id_list.append( join_row_id )

            #-- END loop over mentioned user IDs. --#

        #-- END check to see if id list string --#

    #-- END check to see if tweet ID present. --#

    return join_row_id_list

#-- END function join_tweet_to_user_mentions() --#


#==============================================================================#
# main program code
#==============================================================================#

# declare variables

# database connection
my_db_connection = None
cursor_outer_loop = None
cursor_processing = None

# processing variables
raw_tweet_rs = None
raw_tweet_counter = -1
current_raw_tweet_row = None
new_tweet_id = -1
new_user_id = -1
hashtag_list = None
mentioned_user_id_list = []

# exception processing
exception_type = ""
exception_value = ""
exception_traceback = None

# build SQL statements
sql_raw_select_string = "SELECT * FROM tweet_sample_raw ORDER BY twitter_tweet_id ASC;"

# if you want to test, use LIMIT
#sql_raw_select_string = "SELECT * FROM tweet_sample_raw ORDER BY twitter_tweet_id ASC LIMIT 10;"

# IF YOU USED pandas TO LOAD INSTEAD OF SQL FROM CSV:
# take out the ORDER BY in the SQL above.

try:

    # connect to database and make a cursor for outer loop.
    my_db_connection = sqlite3.connect( "twitter_api.sqlite" )

    # set row_factory that returns values mapped to column names
    #   as well as in an ordered list
    my_db_connection.row_factory = sqlite3.Row

    # make a cursor for the loop over raw tweets
    cursor_outer_loop = my_db_connection.cursor()

    # make a cursor for use in processing each raw tweet.
    cursor_processing = my_db_connection.cursor()

    # execute SQL to pull in all raw tweets.
    raw_tweet_rs = cursor_outer_loop.execute( sql_raw_select_string )

    # loop over raw tweets
    raw_tweet_counter = 0
    for current_raw_tweet_row in raw_tweet_rs:

        # increment counter
        raw_tweet_counter += 1

        #print( "====> raw row " + str( raw_tweet_counter ) )

        # call function(s) for processing we want to do.
        
        # create tweet
        new_tweet_id = create_tweet_row( current_raw_tweet_row, cursor_processing, my_db_connection )

        # create user
        new_user_id = create_user_row( current_raw_tweet_row, cursor_processing, my_db_connection )

        # relate user to tweet - must call create_tweet_row and create_user_row
        #    so that we have IDs, even if they just look IDs up in existing
        #    records.
        relate_user_to_tweet( new_tweet_id, new_user_id, cursor_processing, my_db_connection )

        # create hashtag rows.
        hashtag_list = create_hashtag_rows( current_raw_tweet_row, cursor_processing, my_db_connection )

        # make join table rows for hashtags.
        join_tweet_to_hashtags( new_tweet_id, hashtag_list, cursor_processing, my_db_connection )

        # make join table rows for user mentions.
        join_tweet_to_user_mentions( current_raw_tweet_row, new_tweet_id, cursor_processing, my_db_connection )

        if ( ( raw_tweet_counter % 1000 ) == 0 ):

            print( "====> tweets loaded: " + str( raw_tweet_counter ) )

        #-- print status every hundred. --#

    #-- END loop over raw tweet rows --#
    
except Exception as e:
    
    # get exception details
    exception_type, exception_value, exception_traceback = sys.exc_info()
    print( "Exception caught: " )
    print( "- args = " + str( e.args ) )
    print( "- type = " + str( exception_type ) )
    print( "- value = " + str( exception_value ) )
    print( "- traceback = " + str( traceback.format_exc() ) )
    
finally:

    # close cursors
    cursor_processing.close()
    cursor_outer_loop.close()

    # close connection
    my_db_connection.close()

#-- END try-->except-->finally around database access. --#
