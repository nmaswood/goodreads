import csv
from pymongo import MongoClient
import requests
from time import sleep
import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from bs4 import BeautifulSoup


class GoodReads():

	def __init__(self):

		self.key = 'uUFRZBYoKnyQcFbi0v5CMA'
		self.fp1 = {"path" :'c.csv', "db" : "C_SRC"}
		self.fp2 = {"path" : 'l.csv', "db" : "L_SRC"}
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
		self.AJAX_LIMIT    = 15
		self.REQUEST_LIMIT = 1.5
		self.ITER_LIMIT    = 100

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

		def get_urls(_input):

			_from, to = _input

			for unit in self.db[_from].find():

				isbn = unit["isbn"]
				l = len(isbn)

				if l not in [10,13]:

					if l < 10: 
						while len(isbn) < 10 :
							isbn = "0" + isbn
					else:
						while len(isbn) != 13:
							isbn = "0" + isbn

				url = self.url.format(isbn=isbn)
				print (url)

				result = requests.get(url, headers = self.headers)
				try:
					if result.status_code == 200:

						root   = ET.fromstring(result.text).find('book')
						categories = ['description', 'url', 'id', 'title', 'isbn', 'isbn13','average_rating','publication_year']

						data = {k : root.find(k).text for k in categories}
						data['authors'] = [{"name":k.find("name").text, "id": k.find("id").text} for k in root.find('authors')]
						self.db[to].insert(data)

						print(data['title'])
					else:
						print (result.status_code, url)
					sleep(1)
				except:
					print ("broke at", url)
		get_urls(["L_SRC", "L_BOOKS"])
		get_urls(["C_SRC", "C_BOOKS"])

	def get_users(self):

		def get_next_page(driver):

			next_page = None
			status    = "DNE"

			try:	
				print ("1")
				next_selector = '#reviews > div.uitext > div > a.next_page'
				next_page = driver.find_element_by_css_selector(next_selector)
				classes   = next_page.get_attribute("class")
				status = "END" if 'disabled' in classes else "OK"
			except:
				print("2")

			return (status, next_page)


		def extract_reviews(html,db_name):


			def extract(review_body, obj,db_name):

				selector_url   = obj['selector_url']
				selector_rating = obj['selector_rating']

				url    = review_body.select(selector_url)
				rating = review_body.select(selector_rating)

				url    = None if not url else url[0].get("href")
				rating = None if not rating else rating[0].get("title")

				print (url, rating, db_name)


				data = {
				"user_url" : url,
				"rating"   : rating
				}

				self.db[db_name].insert(data)

				return data

			def reviewed(review_body, db_name):

				obj = {

				"selector_url" : "a.user" ,
				"selector_rating" : "span.staticStars > span"
				}

				return extract(review_body, obj, db_name)

			def main(html, db_name):

				bs_obj  = BeautifulSoup(html, 'lxml')
				review_bodies = bs_obj.select("#bookReviews > div.friendReviews.elementListBrown > div.section > div.review > div.left.bodycol > div.reviewHeader.uitext.stacked")
				if not review_bodies:
					return {"status" : "BAD", "data" : None}
				else:

					return {
					"status" : "OK",
					"data"   : list(map(lambda x: reviewed(x, db_name), review_bodies))

					}

			return main(html,db_name)

		def get_page(url,db_name_books):

			db_name = db_name_books + "_RATINGS"

			driver = webdriver.Chrome()

			driver.get(url)
			status, next_page = get_next_page(driver)
			i = 0
			source  = driver.page_source
			reviews = extract_reviews(source, db_name)

			while status == "OK" and i < self.ITER_LIMIT:

				next_page.click()
				wait = WebDriverWait(driver, self.AJAX_LIMIT)
				sleep(4)
				source  = driver.page_source
				reviews = extract_reviews(source,db_name)

				status, next_page = get_next_page(driver)
				i +=1
				print ("3")
				print (status)

			driver.close()

		for db_name_books in ["L_BOOKS", "C_BOOKS"]:
			for link in self.db[db_name_books].find(no_cursor_timeout=True):
				get_page(link["url"], db_name_books)

if __name__ == "__main__":
	g = GoodReads()
	#g.csv_to_mongo()
	#g.get_book_urls()
	g.get_users()