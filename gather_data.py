import csv
from pymongo import MongoClient
import requests
from time import sleep
import xml.etree.ElementTree as ET

class GoodReads():

	def __init__(self):

		self.key = 'uUFRZBYoKnyQcFbi0v5CMA'
		self.fp1 = {"path" :'c.csv', "db" : "CONSERVATIVE"}
		self.fp2 = {"path" : 'l.csv', "db" : "LIBERAL"}
		self.client = MongoClient('localhost', 27017)
		self.db = self.client['GOODREADS']
		self.url = 'https://www.goodreads.com/book/isbn/{isbn}?key=uUFRZBYoKnyQcFbi0v5CMA'
		self.headers = {
		'If-None-Match' :  'W/"af4160f05643efe60ba4718e6f2c03ec',
		'Accept-Encoding': 'gzip, deflate, sdch',
		'Accept-Language' : 'en-US,en;q=0.8',
		'Upgrade-Insecure-Requests' : 1,
		'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36',
		'Referer' : 'Referer: https://www.goodreads.com/api/index',
		'Connection': 'keep-alive',
		'Cache-Control': 'max-age=0'
		}

	def csv_to_mongo(self):

		def read_csv(info):

			path = info['path']
			c   = info['db']

			with open(path,'r') as infile:

				fields = ["date", "author", "book", "isbn", "politician"]

				reader = csv.reader(infile)
				next(reader)

				for row in reader:
					self.db[c].insert({k:v for k,v in zip(fields, row)})

		read_csv(self.fp1)
		read_csv(self.fp2)

	def get_book_urls(self):

		def get_urls(collection):

			isbn_mongo = self.db[collection].find()

			for unit in isbn_mongo:

				init_isbn = unit["isbn"]

				isbn = "0" + init_isbn if len(init_isbn) in [9,12] else init_isbn

				url = self.url.format(isbn=isbn)
				print (url)

				result = requests.get(url, headers = self.headers)
				sleep(1)
				root   = ET.fromstring(result.text).find('book')
				categories = ['description', 'url', 'id', 'title', 'isbn', 'isbn13','average_rating','publication_year']

				authors = root.find('authors')

				author_list = []

				for k in authors:
					name = k.find('name').text
					_id  = k.find('id').text
					author_list.append({"name": name, "id" : _id})


				data = {k : root.find(k).text for k in categories}
				data["authors"] = author_list
				self.db[collection].insert(data)
				print(data['title'])

		get_urls("LIBERAL")
		get_ursl("CONSERVATIVE")

if __name__ == "__main__":
	g = GoodReads()
	g.csv_to_mongo()
	g.get_book_urls()
