#!/usr/bin/env python3

import requests
import time
import os
import sys
import operator
from operator import truediv
import logging
logger = logging.getLogger('bot')
import random
import sqlite3
import statistics
import string
from psaw import PushshiftAPI
 
import codecs
import nltk
from nltk.corpus import stopwords
default_stopwords = set(nltk.corpus.stopwords.words('english'))
default_stopwords.update([ 'http', 'https' ])

from readability import Readability

from better_profanity import profanity
profanity.load_censor_words()

import pprint
pp = pprint.PrettyPrinter(indent=4)

## Functions to count total comments and comment karma for a user in particular
## subreddit

database = "%s/github/bots/userdata/usersdata.db" % os.getenv("HOME")
 
sql_create_userdata_table = """ CREATE TABLE IF NOT EXISTS userdata ( 
                                user TEXT, epoch INTEGER, sub TEXT, 
                                comment_karma INTEGER, comment_count INTEGER, comment_median_length REAL, 
                                sub_karma INTEGER, sub_count INTEGER, top_words TEXT, grade_level TEXT, comment_profanity_pct TEXT, last_activity INTEGER
                            ); """

def get_User_Data(reddit, Search_User, Search_Subs_List, Expiration=2, Source='reddit', Request_Type='FULL', dbfile=database):
    User_Data = {}
    Needed_List = []
    logger.debug("get_User_Data: %s" % Search_User)
    logger.debug("Search_Subs_List: %s" % Search_Subs_List)
    logger.debug("Expiration: %s" % Expiration)
    logger.debug("Source: %s" % Source)
    logger.debug("Request Type: %s" % Request_Type)
    logger.debug("DBFILE: %s" % dbfile)

    # first get the data we already have in the cache DB
    for sreddit in Search_Subs_List:
        logger.debug('get_User_Data: User=%s Sub=%s' % (Search_User, sreddit))
        SQLDATA = get_user_sub_data_sql(Search_User, sreddit, dbfile)
        User_Data.update(SQLDATA)
        if not sreddit in User_Data:
            logger.debug('# %s Not found in DB, GO FISH', sreddit)
            Needed_List.append(sreddit)

    # for the data we do not have, go fish
    if len(Needed_List) > 0:
        logger.debug("# Sub Needed_List > 0 (%s)" % len(Needed_List))
        logger.debug("# %s" % Needed_List)
        if Source == 'reddit':
            Fetch_Data = fetch_Data_reddit(reddit,Search_User,Search_Subs_List,Expiration,Request_Type)
        else:
            Fetch_Data = fetch_Data_pushshift(Search_User,Search_Subs_List,Request_Type)
        # for the data we just collected, save the records to the DB
        update_user_sub_data_sql(Search_User, Fetch_Data, dbfile)
        # append Fetch_Data onto User_Data before returning
        User_Data.update(Fetch_Data)
    return User_Data
    

# DATABASE STUFF
def get_user_sub_data_sql(Search_User, Search_Sub, dbfile):
    # update cache db
    comment_karma=-1
    comment_count=-1
    sub_karma=-1
    sub_count=-1
    SQLDATA = {}

    MaxDaysOld = 14
    MaxAge_Epoch = int(time.time()) - (MaxDaysOld * 86400)

    try:
        con = sqlite3.connect(dbfile, timeout=300)
        #con.set_trace_callback(print)
        qcur = con.cursor()
        qcur.execute(sql_create_userdata_table)
        username=str(Search_User)
        searchsub=str(Search_Sub)
        execute = qcur.execute('SELECT comment_karma, comment_count, comment_median_length, sub_karma, sub_count, top_words, grade_level, comment_profanity_pct, last_activity FROM userdata WHERE user=? and sub=? and epoch>? order by epoch DESC LIMIT 1', (username,searchsub,MaxAge_Epoch))
        row = qcur.fetchone()
        if row:
            SQLDATA[Search_Sub] = {}
            SQLDATA[Search_Sub]['c_karma'] = row[0]
            SQLDATA[Search_Sub]['c_count'] = row[1]
            SQLDATA[Search_Sub]['c_median_length'] = row[2]
            SQLDATA[Search_Sub]['s_karma'] = row[3]
            SQLDATA[Search_Sub]['s_count'] = row[4]
            SQLDATA[Search_Sub]['top_words'] = row[5]
            SQLDATA[Search_Sub]['grade_level'] = row[6]
            SQLDATA[Search_Sub]['p_pct'] = row[7]
            SQLDATA[Search_Sub]['last_activity'] = row[8]

            logger.debug("FOUND: %s" % SQLDATA[Search_Sub]) 

    except sqlite3.Error as e:
        logger.error( "Error2 {}:".format(e.args[0]))
        logger.error( "User=%s Sub=%s" % (Search_User, Search_Sub))
        sys.exit(1)
        logger.error( "User=%s Sub=%s" % (Search_User, Search_Sub))
        sys.exit(1)
    finally:
        if con:
            con.close()
    
    return SQLDATA

def update_user_sub_data_sql(Search_User, Fetch_Data, dbfile):
    # update cache db
    try:
        logger.debug("Insert Data into DB")
        insert_user=str(Search_User)
        insert_time=int(time.time())
        con = sqlite3.connect(dbfile, timeout=30)
        insert_sql = ''' INSERT INTO userdata(user,epoch,sub,comment_karma,comment_count,comment_median_length,sub_karma,sub_count,top_words,grade_level,comment_profanity_pct,last_activity) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''
        icur = con.cursor()
        # create table if not exist
        icur.execute(sql_create_userdata_table)
        # insert new data
        for sreddit in Fetch_Data:
            icur.execute (insert_sql, [ Search_User, insert_time, sreddit, Fetch_Data[sreddit]['c_karma'], Fetch_Data[sreddit]['c_count'], Fetch_Data[sreddit]['c_median_length'], Fetch_Data[sreddit]['s_karma'], Fetch_Data[sreddit]['s_count'], Fetch_Data[sreddit]['top_words'], Fetch_Data[sreddit]['grade_level'], Fetch_Data[sreddit]['p_pct'], Fetch_Data[sreddit]['last_activity'] ])
            con.commit()
            # delete old data if exist
            dcur = con.cursor()
            dquery = 'DELETE FROM userdata WHERE user=? and sub=? and epoch<?'
            dcur.execute(dquery, [ Search_User, sreddit, insert_time ])
    except sqlite3.Error as e:
        logger.error("Error {}:".format(e.args[0]))
        sys.exit(1)
    finally:
        if con:
            con.close()
    return 1


# REDDIT STUFF
def fetch_Data_reddit(reddit, Search_User, Search_Subs_List, Expiration=14, Request_Type='FULL'):
    Fetch_Data = {}
    _c_comment_lengths = {}
    _c_comment_texts = {}
    c_count = 0

    logger.debug("fetch_Data_reddit user=%s Request_Type=%s" % (Search_User, Request_Type))
    for comment in reddit.redditor(Search_User).comments.new(limit=1000):
        commentsub=comment.subreddit.display_name.lower()
        if commentsub in Search_Subs_List:
            if commentsub not in Fetch_Data:
                Fetch_Data[commentsub] = {}
                Fetch_Data[commentsub]['c_karma'] = 0
                Fetch_Data[commentsub]['c_count'] = 0
                Fetch_Data[commentsub]['s_karma'] = 0
                Fetch_Data[commentsub]['s_count'] = 0
                Fetch_Data[commentsub]['p_count'] = 0
                Fetch_Data[commentsub]['p_pct'] = ''
                Fetch_Data[commentsub]['last_activity'] = 0
                Fetch_Data[commentsub]['c_median_length'] = 0
                Fetch_Data[commentsub]['top_words'] = ''
                Fetch_Data[commentsub]['grade_level'] = ''
                _c_comment_texts[commentsub] = ""
                _c_comment_lengths[commentsub] = []

            Fetch_Data[commentsub]['c_karma'] += comment.score
            Fetch_Data[commentsub]['c_count'] += 1
            _c_comment_texts[commentsub] += comment.body
            _c_length = len(comment.body.split())
            _c_comment_lengths[commentsub].append(_c_length)
            if profanity.contains_profanity(comment.body):
               Fetch_Data[commentsub]['p_count'] += 1
            if comment.created_utc > Fetch_Data[commentsub]['last_activity']:
                Fetch_Data[commentsub]['last_activity'] = comment.created_utc
                  
    s_count = 0
    for submit in reddit.redditor(Search_User).submissions.new(limit=1000):
            submitsub=submit.subreddit.display_name.lower()
            if submitsub in Search_Subs_List:
                if submitsub not in Fetch_Data:
                    Fetch_Data[submitsub] = {}
                    Fetch_Data[submitsub]['c_karma'] = 0
                    Fetch_Data[submitsub]['c_count'] = 0
                    Fetch_Data[submitsub]['s_karma'] = 0
                    Fetch_Data[submitsub]['s_count'] = 0
                    Fetch_Data[submitsub]['p_count'] = 0
                    Fetch_Data[submitsub]['p_pct'] = ''
                    Fetch_Data[submitsub]['last_activity'] = 0
                    Fetch_Data[submitsub]['c_median_length'] = 0
                    Fetch_Data[submitsub]['top_words'] = ''
                    Fetch_Data[submitsub]['grade_level'] = ''
                Fetch_Data[submitsub]['s_karma'] += submit.score
                Fetch_Data[submitsub]['s_count'] += 1

                if submit.created_utc > Fetch_Data[submitsub]['last_activity']:
                    Fetch_Data[submitsub]['last_activity'] = submit.created_utc

    # Process comment data
    if 'FULL' in Request_Type:
        logger.debug("Request Mode: FULL")
        for sreddit in Fetch_Data:
            if sreddit in _c_comment_texts:
                words = nltk.word_tokenize(_c_comment_texts[sreddit])
                words = [ word for word in words if len(word) > 3]
                words = [ word.lower() for word in words ]
                words = [ word for word in words if word not in default_stopwords ]
                words = [ word for word in words if word not in string.punctuation ]
            else:
                words = nltk.word_tokenize('')
            fdist = nltk.FreqDist(words)
            wordlist = []
            for topword, frequency in fdist.most_common(3):
                wordlist.append(topword)
            topwords = ', '.join(wordlist)
            Fetch_Data[sreddit]['top_words'] = topwords 
            if sreddit in _c_comment_lengths:
                Fetch_Data[sreddit]['c_median_length'] = statistics.median(_c_comment_lengths[sreddit])
                if len(words) > 100:
                    r = Readability(_c_comment_texts[sreddit])
                    Fetch_Data[sreddit]['grade_level'] = r.ari().grade_levels[0]
                else:
                    Fetch_Data[sreddit]['grade_level'] = ''
            else:
                Fetch_Data[sreddit]['c_median_length'] = 0
                Fetch_Data[sreddit]['grade_level'] = ''

            if Fetch_Data[sreddit]['p_count'] > 0 and Fetch_Data[sreddit]['c_count'] > 0:
                p_percent = Fetch_Data[sreddit]['p_count'] / Fetch_Data[sreddit]['c_count'] * 100
                Fetch_Data[sreddit]['p_pct'] = '{0:.1f}%'.format(p_percent)
            else:
                Fetch_Data[sreddit]['p_pct'] = ''

    # mark other subs searched as empty
    for sreddit in Search_Subs_List:
        if sreddit not in Fetch_Data:
            Fetch_Data[sreddit] = {}
            Fetch_Data[sreddit]['c_karma'] = 0
            Fetch_Data[sreddit]['c_count'] = 0
            Fetch_Data[sreddit]['s_karma'] = 0
            Fetch_Data[sreddit]['s_count'] = 0
            Fetch_Data[sreddit]['p_count'] = 0
            Fetch_Data[sreddit]['p_pct'] = ''
            Fetch_Data[sreddit]['last_activity'] = 0
            Fetch_Data[sreddit]['c_median_length'] = 0
            Fetch_Data[sreddit]['top_words'] = ''
            Fetch_Data[sreddit]['grade_level'] = ''

    logger.debug("FETCH: %s" % Fetch_Data)
    return Fetch_Data


# PUSHSHIFT STUFF
def get_author_comments_pushshift(**kwargs):
    r = requests.get("https://api.pushshift.io/reddit/comment/search/",params=kwargs)
    data = r.json()
    return data['data']

def get_author_submissions_pushshift(**swargs):
    r = requests.get("https://api.pushshift.io/reddit/submission/search/",params=swargs)
    data = r.json()
    return data['data']

def fetch_Data_pushshift(Search_User,Search_Subs_List):
    Fetch_Data = {}
    _c_comment_lengths = {}
    _c_comment_texts = {}
    logger.debug("fetch_Data_pushshift user=%s" % Search_User)

    c_count = 0
    #comments = get_author_comments_pushshift(author=Search_User,size=5000,sort='desc',sort_type='created_utc')
    api = PushshiftAPI()
    comments = api.search_comments(author=Search_User,limit=2000,sort='desc',sort_type='created_utc',filter=['subreddit', 'author', 'score', 'body'])
    for comment in comments:
        #pp.pprint(comment)
        commentsub=comment.subreddit.lower()
        if commentsub in Search_Subs_List:
            #if 'the_donald' in commentsub:
            #    pp.pprint(comment)
            if commentsub not in Fetch_Data:
                Fetch_Data[commentsub] = {}
                Fetch_Data[commentsub]['c_karma'] = 0
                Fetch_Data[commentsub]['c_count'] = 0
                Fetch_Data[commentsub]['s_karma'] = 0
                Fetch_Data[commentsub]['s_count'] = 0
                Fetch_Data[commentsub]['p_count'] = 0
                Fetch_Data[commentsub]['last_activity'] = 0
                Fetch_Data[commentsub]['p_pct'] = ''
                _c_comment_texts[commentsub] = ""
                _c_comment_lengths[commentsub] = []

            Fetch_Data[commentsub]['c_karma'] += comment.score
            Fetch_Data[commentsub]['c_count'] += 1
            if comment.created_utc > Fetch_Data[commentsub]['last_activity']:
                Fetch_Data[commentsub]['last_activity'] = comment.created_utc
            if comment.body:
                _c_comment_texts[commentsub] += comment.body
                _c_length = len(comment.body.split())
                _c_comment_lengths[commentsub].append(_c_length)

    s_count = 0
    #submissions = get_author_submissions_pushshift(author=Search_User,size=5000,sort='desc',sort_type='created_utc')
    submissions = api.search_submissions(author=Search_User,size=2000,sort='desc',sort_type='created_utc', filter=['subreddit','score','author'])
    for submit in submissions:
        #pp.pprint(submit)
        try:
            if submit.subreddit:
                submitsub=submit.subreddit.lower()
                if submitsub in Search_Subs_List:
                    if submitsub not in Fetch_Data:
                        Fetch_Data[submitsub] = {}
                        Fetch_Data[submitsub]['c_karma'] = 0
                        Fetch_Data[submitsub]['c_count'] = 0
                        Fetch_Data[submitsub]['s_karma'] = 0
                        Fetch_Data[submitsub]['s_count'] = 0
                        Fetch_Data[submitsub]['p_count'] = 0
                        Fetch_Data[submitsub]['last_activity'] = 0
                        Fetch_Data[submitsub]['p_pct'] = ''
                        Fetch_Data[submitsub]['s_karma'] += submit.score
                        Fetch_Data[submitsub]['s_count'] += 1
                        if submit.created_utc > Fetch_Data[commentsub]['last_activity']:
                            Fetch_Data[commentsub]['last_activity'] = submit.created_utc
        except:
            continue

    # Process comment data
    for sreddit in Fetch_Data:
        if sreddit in _c_comment_texts:
            words = nltk.word_tokenize(_c_comment_texts[sreddit])
            words = [ word for word in words if len(word) > 3]
            words = [ word.lower() for word in words ]
            words = [ word for word in words if word not in default_stopwords ]
            words = [ word for word in words if word not in string.punctuation ]
        else:
            words = nltk.word_tokenize('')
        fdist = nltk.FreqDist(words)
        wordlist = []
        for topword, frequency in fdist.most_common(3):
            wordlist.append(topword)
        topwords = ', '.join(wordlist)
        Fetch_Data[sreddit]['top_words'] = topwords
        if sreddit in _c_comment_lengths:
            Fetch_Data[sreddit]['c_median_length'] = statistics.median(_c_comment_lengths[sreddit])
            if len(words) > 100:
                r = Readability(_c_comment_texts[sreddit])
                Fetch_Data[sreddit]['grade_level'] = r.ari().grade_levels[0]
            else:
                Fetch_Data[sreddit]['grade_level'] = ''
        else:
            Fetch_Data[sreddit]['c_median_length'] = 0
            Fetch_Data[sreddit]['grade_level'] = ''


    # mark other subs searched as empty
    for sreddit in Search_Subs_List:
        if sreddit not in Fetch_Data:
            Fetch_Data[sreddit] = {}
            Fetch_Data[sreddit]['c_karma'] = 0
            Fetch_Data[sreddit]['c_count'] = 0
            Fetch_Data[sreddit]['s_karma'] = 0
            Fetch_Data[sreddit]['s_count'] = 0
            Fetch_Data[sreddit]['p_count'] = 0
            Fetch_Data[sreddit]['p_pct'] = ''
            Fetch_Data[sreddit]['c_median_length'] = 0
            Fetch_Data[sreddit]['top_words'] = ''
            Fetch_Data[sreddit]['grade_level'] = ''
            Fetch_Data[sreddit]['last_activity'] = 0

    logger.debug("FETCH: %s" % Fetch_Data)

    return Fetch_Data


# MAIN

#mytestdata = fetch_Data_pushshift('nixfu',['libertarian'])
