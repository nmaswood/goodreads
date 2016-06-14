from time import sleep
from pymongo import MongoClient
import datetime

from UFunc import make_request, go_to_sleep
from dateutil.parser import parse
import pickle

#http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date=19990103&begin_date=19990101&facet=true

class CollectLinks():

    def __init__(self):

        client = MongoClient('localhost', 27017)
        db = client['NYTIMES']
        self.outgoing = db['linkUrlsNew_TEST']

    def get_links(self):

        def format_date (x): return ''.join(str(x).split(" ")[0].split("-"))

        def resume():

            with open("leftoff.pkl", 'rb') as infile:
                from_date, to_date, iteration = pickle.load(infile)
                return from_date, to_date, iteration


        def write_progress(from_date, to_date, iteration):

            with open("leftoff.pkl", 'wb') as f:
                pickle.dump([from_date, to_date, iteration], f)

        def fetch_json_from_url(url):

            return make_request(url,{
                    'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
                    'Accept-Encoding' : 'gzip, deflate, sdch',
                    'Accept-Language' : 'en-US,en;q=0.8',
                    'Connection': 'keep-alive',
                    'X-Requested-With' : 'XMLHttpRequest',
                    'Accept' : "application/json, text/javascript, */*; q=0.01",
                    'Referer': 'http://query.nytimes.com/search/sitesearch/'
             }, 1.5,1800, session = False, return_json = True)

        def update_dates(f, t):

            two_days = datetime.timedelta(days = 2)
            f,t =  [x + two_days  for x in [f, t]]

            return f,t,2 

        def empty_json(json):

            docs = json.get("response").get("docs")
            return docs == [] or docs is None

        def format_url(_from, to, i):
            return "http://query.nytimes.com/svc/add/v1/sitesearch.json?end_date={}&begin_date={}&page={}&facet=true".format(to, _from, i)

        def main():

            from_date, to_date, iteration = resume()
            print ("hello")

            while True:

                if from_date == datetime.date.today():
                    go_to_sleep(86400 * 2,  "Going to sleep for two days")

                from_formatted, to_formatted = [format_date(x) for x in [from_date, to_date]]
                new_url = format_url(from_formatted,to_formatted, iteration)

                json = fetch_json_from_url(new_url)

                if empty_json(json) or iteration >= 100:
                    from_date,to_date, iteration = update_days(from_date, to_date)
                    continue

                self.outgoing.insert({
                     "json": json,
                     "FROM": from_formatted,
                     "TO"  : to_formatted,
                     "iter": iteration,
                     "link": new_url
                    })

                iteration +=1 

                write_progress(from_date, to_date, iteration)

        main()

if __name__ == '__main__':
    run = CollectLinks()
    run.get_links()
