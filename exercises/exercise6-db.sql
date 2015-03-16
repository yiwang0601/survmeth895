CREATE TABLE "tweet_sample_raw" (
     "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
     "tweet_timestamp" TEXT(255,0),
     "twitter_tweet_id" TEXT(255,0),
     "tweet_text" BLOB,
     "tweet_language" TEXT(255,0),
     "tweet_retweet_count" INTEGER,
     "tweet_place" TEXT(255,0),
     "tweet_user_mention_count" TEXT(255,0),
     "tweet_users_mentioned_screennames" TEXT(255,0),
     "tweet_users_mentioned_ids" TEXT(255,0),
     "tweet_hashtag_mention_count" TEXT(255,0),
     "tweet_hashtags_mentioned" TEXT(255,0),
     "tweet_url_count" TEXT(255,0),
     "tweet_shortened_urls_mentioned" TEXT(255,0),
     "tweet_full_urls_mentioned" TEXT(255,0),
     "tweet_display_urls_mentioned" TEXT(255,0),
     "timestamp_ms" TEXT(255,0),
     "tweet_geo" TEXT(255,0),
     "twitter_user_twitter_id" INTEGER,
     "twitter_user_screenname" TEXT(255,0),
     "user_followers_count" INTEGER,
     "user_favorites_count" INTEGER,
     "user_created" TEXT(255,0),
     "user_location" TEXT(255,0),
     "user_description" TEXT(255,0),
     "user_friends_count" INTEGER,
     "user_statuses_count" INTEGER
)


CREATE TABLE "tweet" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "twitter_tweet_id" INTEGER,
    "tweet_user_id" INTEGER,
    "tweet_twitter_user_id" INTEGER,
    "tweet_timestamp" TEXT(255,0),
    "tweet_timestamp_dt" DATETIME,
    "tweet_text" TEXT,
    "tweet_language" TEXT(255,0),
    "tweet_retweet_count" INTEGER,
    "tweet_user_mention_count" INTEGER,
    "tweet_hashtag_mention_count" INTEGER,
    "tweet_url_count" INTEGER,
    "tweet_place" TEXT(255,0),
    "tweet_geo" TEXT(255,0)
)

CREATE TABLE "user" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "twitter_user_twitter_id" INTEGER,                  "twitter_user_screenname" TEXT(255,0),
    "user_followers_count" INTEGER,
    "user_favorites_count" INTEGER,
    "user_created" TEXT(255,0),
    "user_created_dt" DATETIME,
    "user_location" TEXT,
    "user_description" TEXT,
    "user_friends_count" INTEGER,
    "user_statuses_count" INTEGER
)

CREATE TABLE "hashtag" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "hashtag_value" TEXT
)

CREATE TABLE "tweet_hashtag" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "tweet_id" INTEGER,
    "hashtag_id" INTEGER
)

CREATE TABLE "tweet_user_mentions" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "tweet_id" INTEGER,
    "mentioned_user_id" INTEGER
)

