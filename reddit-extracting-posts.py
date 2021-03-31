
import psycopg2
import os
import datetime
import pandas as pd
import ast
from textblob import TextBlob


# add to scrapper the last update method

class redditpostliteralextraction():

    def __init__(self, tickers=None, update_from_date=None):
        # list of tickers to search for in text
        # if no tickers provided this will search all tickers
        self.tickers = tickers
        # Optional inject formated date as string "2021-03-21"
        self.update_from_date = update_from_date
        # theses are used to track the progress and print when the int percent complete changes
        self.counter = 0
        self.percent_tracker = 0
        self.time_started_class = datetime.datetime.now() # never changes
        self.time_temp = datetime.datetime.now() # used to track time for each post
        self.first_hundred_post_time = list() 
        self.percentage_processed = 0 

    def __enter__(self, ticker=None):
        # connect to DB and create required tables if needed
        self.conn = psycopg2.connect(
                host= os.getenv('postgreshost'),
                database="postgres",
                user="postgres",
                password=os.getenv('postgrespassword'))
        self.conn.autocommit=True
        self.curr = self.conn.cursor()
        # Create Tables 
    
        self._create_literal_post_extraction_table()
        # create ticker list
        self.ticker = ticker
        self._find_tickers_to_search()
        print('searching test for ', self.tickers)

        self._get_last_post_update()
        print("updating posts starting from ",self.update_from_date)
        
        for df in self._post_generator():
            # self._extract_noun_from_chunk(df)
            # must run _extract_literal_from_chunk for estimate to be accurate
            self._extract_literal_from_chunk(df)
            
            del(df)

    def _extract_literal_from_chunk(self, df):
        for index, post in df.iterrows():
            #used to store metions for each post
            literal_mentions_title = dict()
            literal_mentions_text = dict()
            title = self._format_text(post['title'])
            text = self._format_text(post['posttext'])
            #cycle through tickers and update mention dicts
            for ticker in self.tickers:
                # #PROCESS POST TITLE 
                found_ticker_title = self._search_text_for_matches(title, ticker)
                if found_ticker_title:
                    literal_mentions_title[ticker] = len(found_ticker_title)
                #PROCESS POST TEXT
                found_ticker_text = self._search_text_for_matches(text, ticker)
                if found_ticker_text:
                    literal_mentions_text[ticker] = len(found_ticker_text)

            text_polarity, text_subjectivity = self._sentiment(post['posttext'])
            title_polarity, title_subjectivity = self._sentiment(post['title'])

            temp = [ post['id'], 
                    str(literal_mentions_title), 
                    str(literal_mentions_text) ,
                    title_polarity,
                    title_subjectivity,
                    text_polarity,
                    text_subjectivity
                   ]
            
            self._save_literal_extraction(temp)
            
            # SECTION USED FOR TRACKING ESTIMATE RUN TIME
            self.counter += 1
            self.percentage_processed =  (self.counter /self.post_count) * 100
            self.temp_percent = int(self.percentage_processed)
            if self.counter < 100: # create average of first 100 transactions
                time_elapsed = datetime.datetime.now() - self.time_temp
                self.first_hundred_post_time.append(time_elapsed)
                self.time_temp = datetime.datetime.now()
            if self.counter == 100:
                self.average_time = sum(self.first_hundred_post_time ,datetime.timedelta())
                self.estimate_time_to_run = (self.post_count / 100) * self.average_time
                print('\nfrom the first 100 processed post\nthe estimate run time is ',self.estimate_time_to_run)
            # END TRACKING ESTIMATE RUN TIME

    def _search_text_for_matches(self, text, ticker):
        matching_list = [word for word in text.split() if word == ticker]    
        return matching_list

    def _sentiment(self,text):
        text = str(text)
        blob = TextBlob(text)
        text_polarity, self.text_subjectivity = blob.sentiment
        return text_polarity, self.text_subjectivity
    
    def _save_literal_extraction(self, temp):
        # try to qury extraction
        self.curr.execute("""SELECT * FROM redditpostliteralextraction WHERE postid=(%s)""",(temp[0],))
        post = self.curr.fetchone()
        if post:# if post exists update values
            # tickers may have been added and posts could have been updated changing setiment
            # add the dicts together

            qury_title_tickers = ast.literal_eval(post[1])
            qury_text_tickers = ast.literal_eval(post[2])
            new_title_tickers  = ast.literal_eval(temp[1])
            new_text_tickers = ast.literal_eval(temp[2])

            updated_title_tickers = self._add_dicts(dict1=qury_title_tickers, dict2=new_title_tickers)
            updated_text_tickers = self._add_dicts(dict1=qury_text_tickers, dict2=new_text_tickers)

            self.curr.execute("""UPDATE redditpostliteralextraction SET
                        postid=%s,
                        title_tickers_used=%s,
                        text_tickers_used=%s,
                        post_title_polarity=%s,
                        post_title_subjectivity=%s,
                        post_text_polarity=%s,
                        post_text_subjectivity=%s
                        WHERE postid=%s
                        """,
                        (temp[0],str(updated_title_tickers),str(updated_text_tickers),temp[3],temp[4],temp[5],temp[6],temp[0]))
        else: # save the data for the first time
            self.curr.execute("""INSERT INTO redditpostliteralextraction
                            (postid,
                            title_tickers_used,
                            text_tickers_used,
                            post_title_polarity,
                            post_title_subjectivity,
                            post_text_polarity,
                            post_text_subjectivity)
                            VALUES (%s , %s, %s, %s, %s, %s, %s)
                            """,(temp[0],str(temp[1]),str(temp[2]),temp[3],temp[4],temp[5],temp[6],))

    def _add_dicts(self, dict1, dict2):
        out_dict = dict()
        for ticker in dict1:
            out_dict[ticker] = dict1[ticker]
        for ticker in dict2:
            try:
                out_dict[ticker]
                out_dict[ticker] += dict2[ticker]
            except KeyError:
                out_dict[ticker] = dict2[ticker]
        return out_dict
    
    def _create_literal_post_extraction_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS redditpostliteralextraction(
                postid int references redditpost(id) UNIQUE,
                title_tickers_used varchar(100000) NOT NULL,
                text_tickers_used varchar(100000) NOT NULL,
                post_title_polarity float NOT NULL,
                post_title_subjectivity float NOT NULL,
                post_text_polarity float NOT NULL,
                post_text_subjectivity float NOT NULL,
                CONSTRAINT fk_redditpost
                    FOREIGN KEY(postid)
                        REFERENCES redditpost(id)
            );""")
            
    def _format_text(self, text):
        text = str(text)
        remove_elements = ['$','.','"',"'","!","?","*","/",'(',')']
        for element in remove_elements:
            text = text.replace(element,' ')
        text = text.encode('ascii', 'ignore').decode('ascii')
        text = text.upper()
        return text

    def _ectract_nouns_from_chunk(self):
        pass

    def _extract_names_from_chunk(self):
        pass

    def _find_tickers_to_search(self):
        # senario1=  get all tickers from database
        # senario2= set self.tickers to the user input value
        if self.tickers == None:
            self.curr.execute("""SELECT ticker FROM nasdaqtickers """)
            nasdaq_tickers = self.curr.fetchmany(100000)
            nasdaq_tickers = [qury[0].upper() for qury in nasdaq_tickers]
            self.curr.execute("""SELECT ticker FROM nysetickers """)
            nyse_tickers = self.curr.fetchmany(100000)
            nyse_tickers = [qury[0].upper() for qury in nyse_tickers]
            self.tickers = nyse_tickers + nasdaq_tickers
        else:
            self.tickers = ast.literal_eval(str(self.tickers))
        
    def _post_generator(self):
        # get how many posts to update
        self.curr.execute("""
                SELECT count(id) 
                FROM redditpost
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.update_from_date, datetime.datetime.now()])
        self.post_count = self.curr.fetchone()[0]
        # generates a pandas DF for each day going foward until current
        # each dataframe is one days worth of posts
        # starts at the self.update_from_date which keeps adding a day for each interation
        while self.update_from_date <= datetime.datetime.now():
            # calculate self.end_chunk_date one day from the self.update_from_date
            self.end_chunk_date = datetime.timedelta(days=1) + self.update_from_date
            
            # PRGOGRESS report
            # used to calculate the estimate time and percentage complete
            print("\nChunk request between {} and {}".format(self.update_from_date, self.end_chunk_date))     
            print(self.percentage_processed,'% processed')
            if self.counter > 100: # after hundred post crate estimate run time
                time_elapsed =  datetime.datetime.now() - self.time_started_class 
                time_per_post = time_elapsed / self.counter
                new_estimate = time_per_post *  (self.post_count - self.counter)
                print("Estimated remaining run time ", new_estimate)
                original_estimate_acuracy = ( self.estimate_time_to_run - new_estimate) - time_elapsed
                # formate for negative to make more human readable
                print("add ",original_estimate_acuracy,' to original estimate of ',self.estimate_time_to_run)

            # qury the posts
            self.curr.execute("""
                SELECT * 
                FROM redditpost
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.update_from_date, self.end_chunk_date])
            chunk = self.curr.fetchmany(1000000)
        
            # after qurying posts update self.update_from_date by adding a day
            self.update_from_date += datetime.timedelta(days=1)
            if chunk:
                columns = [
                    'id','title','username','upvotes','percentupvotes',
                    'commentquantity','posttext','datetime','subreddit']
                df = pd.DataFrame(chunk, columns=columns).fillna(value=0)
                yield df
        
    def _get_last_post_update(self):
        #  To preserve resouces calcualte as little as possible
        # senerio 1= qury redditlastpostupdate in DB for date to update FROM
        # senerio 2= if user provides start date set to update_from_date provided
        # senerio 3= if no qury or provided datetime is found set to oldest post datetime
        if self.update_from_date:
            string_date = self.update_from_date
            format = "%Y-%m-%d"
            self.update_from_date = datetime.datetime.strptime(string_date, format)
        else: # no provided date try to set to redditlastpostupdate value from DB
            self.curr.execute("""SELECT * FROM redditlastpostupdate""")
            redditlastpostupdate = self.curr.fetchall()
            if redditlastpostupdate:
                # go through all dates to find oldest date
                oldestdate = redditlastpostupdate[0][0]
                for date in redditlastpostupdate:
                    if date[0] < oldestdate:
                        oldestdate = date
                self.update_from_date = oldestdate

            else: # set to oldest date post
                self.curr.execute("""
                SELECT min(datetime) 
                FROM redditpost
                ;""")
                self.update_from_date = self.curr.fetchone()[0]
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.curr.close()

with redditpostliteralextraction(tickers=os.getenv('tickers'),
                        update_from_date=os.getenv('update_from_date'),
                        ) as runner:
    runner
