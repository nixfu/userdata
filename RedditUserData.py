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

import codecs
import nltk
from nltk.corpus import stopwords
default_stopwords = set(nltk.corpus.stopwords.words('english'))
default_stopwords.update([ 'http', 'https' ])

from readability import Readability

from better_profanity import profanity
profanity.load_censor_words()

## Functions to count total comments and comment karma for a user in particular
## subreddit

database = "%s/github/bots/userdata/userdata.db" % os.getenv("HOME")
 
sql_create_userdata_table = """ CREATE TABLE IF NOT EXISTS userdata ( 
                                user TEXT, epoch INTEGER, sub TEXT, 
                                comment_karma INTEGER, comment_count INTEGER, comment_median_length REAL, 
                                sub_karma INTEGER, sub_count INTEGER, top_words TEXT, grade_level TEXT, comment_profanity_pct TEXT
                            ); """

def get_User_Data(reddit, Search_User, Search_Subs_List, Expiration=14, Source='reddit'):
    User_Data = {}
    Needed_List = []

    # first get the data we already have in the cache DB
    for sreddit in Search_Subs_List:
        #logger.debug('get_User_Data: User=%s Sub=%s' % (Search_User, sreddit))
        SQLDATA = get_user_sub_data_sql(Search_User, sreddit)
        User_Data.update(SQLDATA)
        if not sreddit in User_Data:
            #logger.debug('# Not found in DB, GO FISH')
            Needed_List.append('sreddit')

    # for the data we do not have, go fish
    if len(Needed_List) > 0:
        logger.debug("Sub Needed_List > 0 (%s)" % len(Needed_List))
        if Source == 'reddit':
            Fetch_Data = fetch_Data_reddit(reddit,Search_User,Search_Subs_List)
        else:
            Fetch_Data = fetch_Data_pushshift(Search_User,Search_Subs_List)
        # for the data we just collected, save the records to the DB
        update_user_sub_data_sql(Search_User, Fetch_Data)
        # append Fetch_Data onto User_Data before returning
        User_Data.update(Fetch_Data)
    return User_Data
    

# DATABASE STUFF
def get_user_sub_data_sql(Search_User, Search_Sub):
    # update cache db
    comment_karma=-1
    comment_count=-1
    sub_karma=-1
    sub_count=-1
    SQLDATA = {}

    try:
        con = sqlite3.connect(database)
        qcur = con.cursor()
        qcur.execute(sql_create_userdata_table)
        qcur.execute('''SELECT ifnull(comment_karma,0), ifnull(comment_count,0), ifnull(comment_median_length,0), ifnull(sub_karma,0), ifnull(sub_count,0), ifnull(top_words,''), ifnull(grade_level,''), ifnull(comment_profanity_pct,'') FROM userdata WHERE user=? and sub=? and epoch > strftime('%s','now', '-7 day') order by epoch DESC LIMIT 1''', (str(Search_User),Search_Sub))
        row = qcur.fetchone()
        #keys = row.keys()
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
            
    except sqlite3.Error as e:
        logger.error( "Error2 {}:".format(e.args[0]))
        logger.error( "User=%s Sub=%s" % (Search_User, Search_Sub))
        sys.exit(1)
    finally:
        if con:
            con.close()
    
    return SQLDATA

def update_user_sub_data_sql(Search_User, Fetch_Data):
    # update cache db
    try:
        logger.debug("Insert Data into DB")
        insert_user=str(Search_User)
        insert_time=int(round(time.time())-(86400 * 7))
        con = sqlite3.connect(database)
        insert_sql = ''' INSERT INTO userdata(user,epoch,sub,comment_karma,comment_count,comment_median_length,sub_karma,sub_count,top_words,grade_level,comment_profanity_pct) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '''
        icur = con.cursor()
        # create table if not exist
        icur.execute(sql_create_userdata_table)
        # insert new data
        for sreddit in Fetch_Data:
            icur.execute (insert_sql, [ Search_User, insert_time, sreddit, Fetch_Data[sreddit]['c_karma'], Fetch_Data[sreddit]['c_count'], Fetch_Data[sreddit]['c_median_length'], Fetch_Data[sreddit]['s_karma'], Fetch_Data[sreddit]['s_count'], Fetch_Data[sreddit]['top_words'], Fetch_Data[sreddit]['grade_level'], Fetch_Data[sreddit]['p_pct'] ])
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
def fetch_Data_reddit(reddit, Search_User, Search_Subs_List, Expiration=14):
    Fetch_Data = {}
    _c_comment_lengths = {}
    _c_comment_texts = {}
    c_count = 0

    logger.debug("fetch_Data_reddit user=%s" % Search_User)
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
                _c_comment_texts[commentsub] = ""
                _c_comment_lengths[commentsub] = []

            Fetch_Data[commentsub]['c_karma'] += comment.score
            Fetch_Data[commentsub]['c_count'] += 1
            _c_comment_texts[commentsub] += comment.body
            _c_length = len(comment.body.split())
            _c_comment_lengths[commentsub].append(_c_length)
            if profanity.contains_profanity(comment.body):
               Fetch_Data[commentsub]['p_count'] += 1
                  
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
                Fetch_Data[submitsub]['s_karma'] += submit.score
                Fetch_Data[submitsub]['s_count'] += 1

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
            Fetch_Data[sreddit]['c_median_length'] = 0
            Fetch_Data[sreddit]['top_words'] = ''
            Fetch_Data[sreddit]['grade_level'] = ''

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
    comments = get_author_comments_pushshift(author=Search_User,size=1000,sort='desc',sort_type='created_utc')
    for comment in comments:
        commentsub=comment['subreddit'].lower()
        if commentsub in Search_Subs_List:
            if commentsub not in Fetch_Data:
                Fetch_Data[commentsub] = {}
                Fetch_Data[commentsub]['c_karma'] = 0
                Fetch_Data[commentsub]['c_count'] = 0
                Fetch_Data[commentsub]['s_karma'] = 0
                Fetch_Data[commentsub]['s_count'] = 0
                _c_comment_texts[commentsub] = ""
                _c_comment_lengths[commentsub] = []

            Fetch_Data[commentsub]['c_karma'] += comment['score']
            Fetch_Data[commentsub]['c_count'] += 1
            _c_comment_texts[commentsub] += comment.body
            _c_length = len(comment.body.split())
            _c_comment_lengths[commentsub].append(_c_length)

    s_count = 0
    submissions = get_author_submissions_pushshift(author=Search_User,size=1000,sort='desc',sort_type='created_utc')
    for submit in submissions:
        if 'subreddit' in submit:
            submitsub=submit['subreddit'].lower()
            if submitsub in Search_Subs_List:
                if submitsub not in Fetch_Data:
                    Fetch_Data[submitsub] = {}
                    Fetch_Data[submitsub]['c_karma'] = 0
                    Fetch_Data[submitsub]['c_count'] = 0
                    Fetch_Data[submitsub]['s_karma'] = 0
                    Fetch_Data[submitsub]['s_count'] = 0
                Fetch_Data[submitsub]['s_karma'] += submit['score']
                Fetch_Data[submitsub]['s_count'] += 1

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
            Fetch_Data[sreddit]['c_median_length'] = 0
            Fetch_Data[sreddit]['top_words'] = ''
            Fetch_Data[sreddit]['grade_level'] = ''

    return Fetch_Data
