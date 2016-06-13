import csv
from pymongo import MongoClient
from collections import defaultdict
import os

class filter_reviews():

    def __init__(self):

        client  = MongoClient('localhost', 27017)
        db = client['GOODREADS']

        self.incoming_l = db['L_REVIEWS']
        self.incoming_c = db['C_REVIEWS']

        self.outgoing_l = db['L_REVIEWS_PROCESSED_PRIME']
        self.outgoing_c = db['C_REVIEWS_PROCESSED_PRIME']

    def main(self):

        print ("Hello World!")
        for db_in, db_out in [(self.incoming_c, self.outgoing_c)]:

            for item in db_in.find(no_cursor_timeout = True):

                status = item.get('status')
                review = item.get('review')

                if status:
                    continue
                elif review is None:
                    continue
                else:


                    db_out.insert(item)

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
        self.incoming_c = db['C_REVIEWS_PROCESSED']

    def reviews_per_book(self, category, neither = False):

        db = {'L' : self.incoming_l, 'C' : self.incoming_c}[category]

        i_o_instance = i_o()

        if neither:
            category = 'N'

        books_names  = i_o_instance.get_name(category)

        d = defaultdict(list)

        for review_item in db.find(no_cursor_timeout = True):

            book_name, review = review_item.get('book_name'), review_item.get('review')

            if review is None or type(review) == list:
                continue

            d[book_name].append(review)

        return dict(d)

    def write_to_file(self, d, folder):

        i = 0

        if not os.path.exists(folder): os.mkdir(folder)

        id_book = {}

        for k,v in d.items():

            id_book[i] = k

            k_prime = k.replace("/", "_")

            with open(folder + '/{}'.format(i), 'w') as out:

                try:
                    out.write(' '.join(v))
                except:
                    print (v)

            i += 1 

        with open(folder + '_KEYS', 'w') as out:

            for k,v in id_book:
                outfile.write("{}:{}\n".format(k,v))

    def main(self):

        d = self.reviews_per_book('C')
        self.write_to_file(d, 'DISTINCTIVE_CONSVERATIVE')

run = process()
run.main()


# By book and then as invidual txt
# Distinctive Neither by Liberals
# Distinctive Neither by Conservative
# Distinctive Liberal
# Distinctive Conservative