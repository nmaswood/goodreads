import csv
from pymongo import MongoClient
from collections import defaultdict

class filter_reviews():

    def __init__(self):

        client  = MongoClient('localhost', 27017)
        db = client['GOODREADS']

        self.incoming_l = db['L_REVIEWS']
        self.incoming_r = db['C_REVIEWS']

        self.outgoing_l = db['L_REVIEWS_PROCESSED']
        self.outgoing_r = db['C_REVIEWS_PROCESSED']

    def main(self):

        print ("Hello World!")
        for db_in, db_out in [(self.incoming_r, self.outgoing_r)]:

            for item in db_in.find(no_cursor_timeout = True):

                status = item.get('status')
                review = item.get('review')

                if status:
                    continue
                elif review is None:
                    continue
                else:
                    db_out.insert(item)

run = filter_reviews()
run.main()

class i_o():

    def get_from_csv(self, the_type):

        books = {
        'C': 'Distinctive_CON_Top500_updated.csv',
        'L': 'Distinctive_LIB_Top500_updated.csv', 
        'N' : 'Distinctive_Neither_Top100_updated.csv'
        }

        with open(books[the_type], 'r') as csv_infile:
            reader = csv.reader(csv_infile)
            return [l for l in reader if l]

    def get_column(self, matrix, i):

        return [el[i] for el in matrix]

    def get_name(self,the_type):

        return self.get_column(self.get_from_csv(the_type),1)[1:]

class process():

    def __init__(self):

        client = MongoClient('localhost', 27017)
        db = client['GOODREADS']

        self.incoming_l = db['L_REVIEWS_PROCESSED']
        self.incoming_r = db['R_REVIEWS_PROCESSED']

    def reviews_per_book(self, category):

        db = {'L' : self.incoming_l, 'R' : self.incoming_r}

        #for review_item in db.find(no_cursor_timeout = True):

        i_o_instance = i_o()

        books_names  = None









#run = i_o()

#print (run.get_name('con'))