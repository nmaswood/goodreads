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

    def get_top_50_l(self):

        # Sorted by third value indexing at 0

        all_liberals = self.get_from_csv('L')[1:]

        top_fifty_books = sorted([(x[1], int(x[3])) for x in all_liberals], key = lambda x: x[1], reverse = True)[:50]

        return [x[0].replace(";", ",") for x in top_fifty_books]

    def get_top_50_c(self):

        # Sorted by second value indexing at 2

        all_c = self.get_from_csv('C')[1:]

        top_fifty_books = sorted([(x[1], int(x[2])) for x in all_c], key = lambda x: x[1], reverse = True)[:50]

        return [x[0].replace(";", ',') for x in top_fifty_books]

    def get_neutrals(self):


        # Skipped 648,Miss Peregrine_Ñés Home for Peculiar Children (Miss Peregrine_Ñés Peculiar Children; #1),426,678,1.110940843,0.09238193,

        with open('Distinctive_Neither_Top100_updated.csv', 'rb') as infile:
            next(infile)

            l = [x.split(b',')[1] for x in infile]

            words = []

            for x in l:

                try:
                    words.append(x.decode('utf-8').replace(";", ','))
                except:
                    pass

            return words

run = i_o()

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
            book_names = i_o_instance.get_neutrals()
        elif category == 'L':
            book_names = i_o_instance.get_top_50_l()
        else:
            book_names = i_o_instance.get_top_50_c()

        d = defaultdict(list)

        for book_name in book_names:

            from_db = db.find({"book_name" : book_name})

            d[book_name] += [x.get('review') for x in from_db if x.get('review') != 'None' and type(x) == str]

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
                except Exception as e:
                    print (e)
                    print ("------")
                    print (v)
                    print ('-------')

            i += 1 

        with open(folder + '_KEYS', 'w') as out:

            for k,v in id_book.items():
                out.write("{}:{}\n".format(k,v))

    def main(self):

        d = self.reviews_per_book('C')
        self.write_to_file(d, '50_DISTINCTIVE_CONSERVATIVE')

        d = self.reviews_per_book('L')
        self.write_to_file(d, '50_DISTINCTIVE_LIBERAL')

        d = self.reviews_per_book('C', neither = True )
        self.write_to_file(d, 'NEITHER_CONSERVATIVE')

        d = self.reviews_per_book('L', neither = True)
        self.write_to_file(d, 'NEITHER_LIBERAL')

run = process()
run.main()


# By book and then as invidual txt
# Distinctive Neither by Liberals
# Distinctive Neither by Conservative
# Distinctive Liberal
# Distinctive Conservative