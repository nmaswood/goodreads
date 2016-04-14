import csv
from pymongo import MongoClient
import requests
from time import sleep
import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from bs4 import BeautifulSoup
import pprint
from sys import exit
from math import ceil


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

		self.headers2 = {
		'Accept' : 'text/javascript, text/html, application/xml, text/xml, */*',
		'Connection': 'keep-alive',
		'Cache-Control' : "max-age=0",
		'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537.36',
		}

		self.AJAX_LIMIT    = 15
		self.REQUEST_LIMIT = 1.5
		self.ITER_LIMIT    = 100
		self.GOODNIGHT = 1800
		self.READ_BOOKS_DB_DICT = {
		"L_BOOKS_RATINGS" : "L_BOOKS_FINAL_PRIME",
		"C_BOOKS_RATINGS" : "C_BOOKS_FINAL_PRIME"
		}

	def go_to_sleep(self, msg, time):

		msg and print (msg)
		print ("Sleeping for {time}".format(time=time))
		sleep(time)

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
				except Exception as e:
					print (e)
					print ("fucked up")
					print ("broke at", url)
		get_urls(["L_SRC", "L_BOOKS"])
		get_urls(["C_SRC", "C_BOOKS"])

	def get_users(self):

		def get_next_page(driver):

			next_page = None
			status    = "DNE"

			try:	
				print ("going to next page")
				next_selector = '#reviews > div.uitext > div > a.next_page'
				next_page = driver.find_element_by_css_selector(next_selector)
				classes   = next_page.get_attribute("class")
				status = "END" if 'disabled' in classes else "OK"
			except Exception as e:
				print (e)
				print ("could not progress to next page")

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

			try:
				driver.get(url)
				status, next_page = get_next_page(driver)
				i = 0
				source  = driver.page_source
				reviews = extract_reviews(source, db_name)
			except Exception as e:
				print (e)
				print ("fuck")
				print ("get_page error")
				print ("page not accessible broke asap")
				driver.close()
				return

			while status == "OK" and i < self.ITER_LIMIT:
				try:
					next_page.click()
					wait = WebDriverWait(driver, self.AJAX_LIMIT)
					sleep(self.REQUEST_LIMIT)
					source  = driver.page_source
					reviews = extract_reviews(source,db_name)

					status, next_page = get_next_page(driver)
					i +=1
					print ("3")
					print (status)
				except:
					print ("page not accessible")
					break

			driver.close()

		for db_name_books in ["L_BOOKS", "C_BOOKS"]:
			for link in self.db[db_name_books].find(no_cursor_timeout=True):
				get_page(link["url"], db_name_books)

	def get_read_books(self):

		def log_object(obj):
			self.db[self.READ_BOOKS_DB].insert(obj)

		def print_object(obj):

			pprint.PrettyPrinter(indent=4).pprint(obj)

		def positive_rating(rating):
			## No assumptions made about input, CHECK FOR FALSY

			if rating:
				return rating in ["it was amazing", "really liked it", "liked it"]

			return False

		def infinite_urls(url,iter_num):
			## Assumption made that url and iter_num exist

			generic = "https://www.goodreads.com/review/list/{id}?page={iter_num}&per_page=infinite&shelf=read&utf8=%E2%9C%93"
			unique_id = url.split("/show/")[-1]
			unique_id_p = unique_id.split("-")[0] if "-" in unique_id else unique_id

			return generic.format(id = unique_id_p, iter_num = iter_num)

		def iter_limit(html):

			bs_obj = BeautifulSoup(html, "lxml")
			no_content_selector = "#rightCol > div.greyText.nocontent.stacked"
			no_content = bs_obj.select(no_content_selector)

			if no_content != [] and 'No matching items' in no_content:
				print ("FUCK")
				return 0
			else:
				try:
					infinite_status = bs_obj.select("#infiniteStatus")[0].contents[0]
				except Exception as e:
					print (e)
					print ("SHIT")
					return 0
				remove_first_number = infinite_status.split("of ")[-1]
				limit = remove_first_number.split(" loaded")[0]

			num_pages = lambda x: ceil(int(x)/30)

			print ("num_pages", num_pages)

			return num_pages(limit)

		def parse_page(html, referring_url, user_url):

			def extract_one(book, type_name):

				query = book.select("td.field.{type_name} > div.value".format(type_name = type_name))

				if query != []:
					c = query[0].contents
					if c != []:
						return c[0].strip()

				return None

			bs_obj = BeautifulSoup(html, "lxml")
			books  = bs_obj.select("#booksBody > tr")

			for book in books:

				author_unit = book.select('td.field.author > div.value > a')
				title_unit  = book.select("td.field.title > div.value > a")

				group_one = [
				"avg_rating",
				"num_ratings",
				"isbn",
				"isbn13",
				"date_pub_edition",
				"date_pub",
				"date_started",
				"purchase_location",
				"date_purchased"]

				partial = lambda key : extract_one(book,key)

				data = {k : partial(k) for k in group_one}

				functions = {

				"author"     : (author_unit, lambda x: x[0].contents),
				"author_url" : (author_unit, lambda x: x[0].get("href")),
				"book_name"  : (title_unit, lambda  x: x[0].get("title")),
				"book_url"   : (title_unit, lambda  x: x[0].get("href")),
				"rating"     : (book.select("td.field.rating > div.value > span.staticStars > span.staticStar"), lambda x: x[0].get("title")),
				"num_pages"  : (book.select("td.field.num_pages > div.value > nobr"), lambda x: x[0].contents[0].strip()),
				"votes"      : (book.select("td.field.votes > div.value > a"), lambda x: x[0].contents[0]),
				"date_read"  : (book.select("td.date_read > div.value > span"), lambda x : x[0].contents[0].strip())

				}

				for key, value in functions.items():

					_input, func = value
					if _input:
						data[key] = func(_input)

				data["referring_url"] = referring_url
				data["user_url"]      = user_url

				print_object(data)
				log_object(data)

		def process_user_url(rating, user_url):

			if positive_rating(rating):

				i    = 1
				url  = infinite_urls(user_url, i)

				res = requests.get(url, headers = self.headers2)
				self.go_to_sleep("initial request", self.REQUEST_LIMIT)

				code = res.status_code

				if int(code) in [504, 404]:
					print (code, "page not found")
					return

				limit = iter_limit(res.text)

				print (user_url,url,  rating, limit)

				if limit:
					parse_page(res.text,url,user_url)
					limit-=1; i+=1;
					while limit:

						url  = infinite_urls(user_url, i)
						res = requests.get(url, headers = self.headers2)
						self.go_to_sleep("request {i} made".format(i=i), self.REQUEST_LIMIT)
						parse_page(res.text,url, user_url)
						limit -= 1; i+=1
			else:

				print (user_url, rating)

		def run():

			for db_rating_name in ["L_BOOKS_RATINGS", "C_BOOKS_RATINGS"]:

				self.READ_BOOKS_DB = self.READ_BOOKS_DB_DICT[db_rating_name]

				for x in self.db[db_rating_name].find(no_cursor_timeout=True):

					rating   = x["rating"]
					user_url = x["user_url"]

					entry = self.db[self.READ_BOOKS_DB].find_one({"user_url": user_url}, no_cursor_timeout=True)

					if entry == None:

						try:
							process_user_url(rating, user_url)
						except Exception as e:
							print (e)
							self.go_to_sleep(e, self.GOODNIGHT)
					else:
						print ("NON-UNIQUE ENTRY", user_url)
		run()

if __name__ == "__main__":

	g = GoodReads()

	try:
		g.get_read_books()
	except Exception as e:
		print (e)
		sleep(1800)
		g.get_read_books()
