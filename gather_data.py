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
import datetime
from time import time
import json

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
		self.REQUEST_LIMIT = 2.0
		self.ITER_LIMIT    = 100
		self.GOODNIGHT = 1800
		self.READ_BOOKS_DB_DICT = {
		"L_BOOKS_RATINGS" : "L_BOOKS_FINAL_PRIME",
		"C_BOOKS_RATINGS" : "C_BOOKS_FINAL_PRIME"
		}

	def go_to_sleep(self, msg, time_in_sec, verbose = True):

		#msg : Msg you would like to display before sleep
		#time: Duration of sleep
		#verbose : Print statements?

		def t_stamp():
			return str(datetime.datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S'))

		if verbose and msg: print (msg)

		time_str = "Sleeping for {sec}"
		if time_in_sec > 30:
			segment = time_in_sec / 3
			for stage in range(3):
				ts = t_stamp()
				print ("{time_stamp} : {stage}/2".format(time_stamp = ts, stage = stage), time_str.format(sec=segment))
				sleep(segment)
		else:
			print (time_str.format(sec=time_in_sec), t_stamp())
			sleep(time_in_sec)

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

		for x in ["L_SRC", "L_BOOKS"]:
			get_urls(x)

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
				except Exception as e:
					print (e)
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
					print ("Limit automatically set to 0")
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
					print ("Logged user as zero limit")
					log_object({
						"user_url" : user_url,
						"rating"   :  rating,
						"status"   : "limit was zero"
						})
			else:
				print ("Logged user as low rating")
				log_object({
					"user_url" : user_url,
					"rating"   :  rating,
					"status"   : "rating_is_too_low"
					})

				print (user_url, rating)

		def run(db_rating_name):

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

		#run("L_BOOKS_RATINGS")
		#run("C_BOOKS_RATINGS")
	def get_book_tuples(self):

		data = {}
		filenames = ["liberal_4000_urls", "conservative_4000_urls"]


		for filename in filenames:
			with open (filename + ".json", 'r') as infile:
				data[filename] = json.load(infile)

		foo = [v for k,v in data.items()]

		bar = foo[0]  + foo[1]

		books = [v["_id"] for v in bar]

		return [
		(v.split(".")[-1],
		 v.split("/show/")[1].split(".")[0]
		)
		for v in books if v is not None
		]

	def get_book_shelves(self):

		def get_page(book_id):

			link_url = "https://www.goodreads.com/book/show/{}.xml?key=uUFRZBYoKnyQcFbi0v5CMA".format(book_id)

			headers = {
				"Host": "www.goodreads.com",
				"Connection": "keep-alive",
				"Cache-Control": "max-age=0",
				"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
				"Upgrade-Insecure-Requests": 1,
				"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.86 Safari/537.36",
				"Accept-Encoding": "gzip, deflate, sdch",
				"Accept-Language": "en-US,en;q=0.8",
			}

			result = requests.get(link_url, headers= headers, timeout= 200)

			status_code = int(result.status_code)

			return result.text if status_code == 200 else None



		def parse_xml(html):

			root = list(ET.fromstring(html).find('book').find("popular_shelves"))

			return [(element.get("name"), element.get("count")) for element in root]


		def main():

			print ("hello")

			for book, book_id in self.get_book_tuples():

				if self.db["BOOK_SHELVES"].find_one({"book": book}) == None:

					print ("Unique Entry")

					try:
						html = get_page(book_id)
					except Exception as e:
						self.go_to_sleep("ERROR due to " + e, self.GOODNIGHT)
					else:
						shelves = parse_xml(html)
						self.db["BOOK_SHELVES"].insert({
							"book" : book,
							"book_id": book_id,
							"shelves": shelves
							})
						self.go_to_sleep("Logged" + book, self.REQUEST_LIMIT)
				else:
					print (book, "Non-unique entry")
		main()
	def process_book_shelves(self):

		def book_url_to_book_name(book):

			return "-".join(book.split("/")[-1].split("-")[1:])

		def book_id_to_book_name(book):
			return book.split("/")[-1].split("-")[0]

		def from_mongo():

			data = self.db["BOOK_SHELVES"].find()

			def process_shelf(d):
				return [x[0].replace("'", '"') for x in d["shelves"]]

			my_json = dict({d["book"] : process_shelf(d) for d in data})
			print (my_json)

			with open("shelves_new.json", 'w') as outfile:
				json.dump(outfile, data)

		from_mongo()


		def process_json():

			my_dict = {}
			with open("shelves.json", 'r') as infile:

				for line in infile:
					x +=1
					parsed_json = json.loads(line)
					book = book_url_to_book_name(parsed_json["book"])
					shelves = parsed_json["shelves"]
					print (book)
					my_dict[book] = [x[0] for x in shelves]
				print (x)
			print (len(my_dict))
			return my_dict

		def  asssign_genre():

			return_dict = {}

			fiction = ['fiction', 'fantasy', 'sci-fi', 'science-fiction', 'sciencefiction','contemporary-fiction', 'suspense-fiction', 'crime-fiction']
			non_fiction = ['non-fiction', 'nonfiction', 'history', 'social-science', 'political','philosophy','business', 'science', 'psychology','biography','physics']
			data = process_json()

			print (len(data))

			for book, shelf in data.items():

				lower_book = book.lower()

				for word in fiction:

					if word in shelf:

						return_dict[lower_book] = 'fiction'
						break

				for word in non_fiction:

					if word in shelf:

						return_dict[lower_book] = 'non-fiction'
						break

				if return_dict.get(lower_book) is None:
					print (lower_book)
					print (shelf)

			return return_dict

		def get_data(let):

			letter = {"l" : "liberal", "c" : "conservative"}

			with open(letter[let] + "_4000_books.json", 'r') as infile:
				return json.load(infile)

		def process_data(let):

			data = get_data(let)
			genre_dict = asssign_genre()

			def format_book_name(_str):

				if "(" in _str:
					_str = _str.split("(")[0]

				return "_".join(_str.lower().strip().split())

			new_dict = {
			format_book_name(k["_id"]):
			{"count": k["count"]}
			for k in data
			if k["_id"] is not None
			}


			for k,v in new_dict.items():


				pass
			print (len(genre_dict))




		#process_data("c")




if __name__ == "__main__":

	g = GoodReads()
	g.process_book_shelves()

	#g.get_book_shelves()
