import requests
from selenium import webdriver
from pymongo import MongoClient
from UFunc import go_to_sleep, login, make_request

class CollectArticles():

    def __init__(self):

        client  = MongoClient('localhost', 27017)
        db = client['NYTIMES']
        self.incoming = db["linkUrlsNew"]
        self.outgoing =  db["html"]
        self.login = login()

    def check_for_existence_of_url(self,url):
        return self.out_collection.find_one({"url": url}, no_cursor_timeout=True) is  None

    def json_to_urls(self, json):

        response = json.get("response")

        if response is None:
            print ("EMPTY RESPONSE")
            return

        docs = response["docs"]
        if docs is None:
            print ("EMPTY DOCS")
            return

        for item in docs:
            yield (item["web_url"], url)

    def get_html(self):

        def get_page(url, referring_url,session):

            return make_request(url,{
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate, sdch',
            'Accept-Language':'en-US,en;q=0.8',
            'Connection':'keep-alive',
            'Host':'www.nytimes.com',
            'Referer':'http://www.nytimes.com/',
            'Upgrade-Insecure-Requests':1,
            'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2711.0 Safari/537.36'
            }, 1.5, 1800, session = self.login, return_json = False, refresh_session = login)


        def main():

            for mongo_obj in self.outgoing.find(no_cursor_timeout = True):

                json = item.get('json')
                referring_url = item.get('link')

                urls = self.json_to_urls(json)

                for url, original_json_item in urls:

                    if self.check_for_existence_of_url(url):

                        data = get_page(url, referring_url,session)
                        if data: 
                            data['referring_url'] = item['link']
                            self.outgoing.insert(data)

        main()

if __name__ == '__main__':
    run = CollectHtml()
    run.get_html()
