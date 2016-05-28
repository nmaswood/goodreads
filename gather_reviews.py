import csv
from pymongo import MongoClient
import requests
from time import sleep
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import pprint
from sys import exit
from math import ceil
import datetime
from time import time
import json
from UFunc import login, go_to_sleep
from math import ceil

class GatherReviews():

	def __init__(self):

		client  = MongoClient('localhost', 27017)
		self.db = client['GOODREADS']
		self.session = login()
		self.REQUEST_TIME_OUT = 1
		self.ERROR_TIMEOUT = 1800

	def scrape_user_page(self,mongo_object):

		def positive_rating(rating):
			if rating:
				return rating in ["it was amazing", "really liked it", "liked it"]

			return False

		def create_user_url(second_half):
			return 'https://www.goodreads.com' + second_half

		def make_request(user_url):

			session = self.session

			headers = {
				'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding':'gzip, deflate, sdch, br',
				'Accept-Language':'en-US,en;q=0.8',
				'Cache-Control':'max-age=0',
				'Connection':'keep-alive',
				'Host':'www.goodreads.com',
				'Upgrade-Insecure-Requests':1,
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2745.0 Safari/537.36'
			}

			r = session.get(user_url,
					  headers = headers,
					  timeout = 200
			 )

			return r.text

		def extract_data(html):

			bs_obj = BeautifulSoup(html, 'lxml')
			review_select = 'body > div.content > div.mainContentContainer > div.mainContent > div.mainContentFloat > div.leftContainer > div.leftAlignedImage > a'

			reviews = bs_obj.select(review_select)

			as_strings  = [x.string.strip() for x in reviews if x.string is not None]

			d = {x: None for x in ['num_ratings', 'avg', 'num_reviews']}

			for x in as_strings:
				if 'ratings' in x:
					d['num_ratings'] = int(x.split(" ratings")[0])
					break

			for x in as_strings:
				if 'avg' in x:
					d['avg'] = float(x.split(" ")[0].strip("()"))
					break

			for x in as_strings:
				if 'reviews' in x:
					d['num_reviews'] = int(x.split(" reviews")[0])

			return d

		user_url = mongo_object.get('user_url')
		rating   = mongo_object.get('rating')

		if positive_rating(rating) and user_url is not None:

			request_me = create_user_url(user_url)

			try:
				html = make_request(request_me)
				go_to_sleep('Succesfully reached {} from scrape_user_page'.format(user_url), self.REQUEST_TIME_OUT)
				return extract_data(html)

			except Exception as e:
				go_to_sleep("Failed during scrape_user_page due to {}".format(e), self.ERROR_TIMEOUT)

			try:
				html = make_request(request_me)
				go_to_sleep('Succesfully reached {} from scrape_user_page'.format(user_url), self.REQUEST_TIME_OUT)
				return extract_data(html)

			except Exception as e:
				go_to_sleep("Failed during scrape_user_page due to {}".format(e), self.ERROR_TIMEOUT)
				return False

		else:
			print ("Non-positive rating of {} from {}".format(rating, user_url))
			return False

	def scrape_review_page(self,user_url, page_number):


		def new_url(old_url, page_number):

			def to_user_id(old_url):
				return old_url.split("/show/")[-1]

			user_id = to_user_id(old_url)

			if page_number == 1:

				return 'https://www.goodreads.com/review/list/{user_id}?utf8=%E2%9C%93&utf8=%E2%9C%93&order=d&sort=review&view=reviews&per_page=100'.format (user_id = user_id)

			else:

				return 'https://www.goodreads.com/review/list/{user_id}?order=d&page={page_number}&per_page=100&sort=review&utf8=%E2%9C%93&view=reviews'.format(user_id = user_id, page_number = page_number)


		def make_request(user_url):

			headers = {

				'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding':'gzip, deflate, sdch, br',
				'Accept-Language':'en-US,en;q=0.8',
				'Cache-Control':'max-age=0',
				'Connection':'keep-alive',
				'Host':'www.goodreads.com',
				'Upgrade-Insecure-Requests':1,
				'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2745.0 Safari/537.36'
			}

			return self.session.get(user_url,
					  headers = headers,
					  timeout = 200).text

		def parse_page(html):

			def extract_one(book, type_name):

				query = book.select("td.field.{type_name} > div.value".format(type_name = type_name))

				if query != []:
					c = query[0].contents
					if c != []:
						return c[0].strip()

				return None

			def process_book(book):

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
					"date_read"  : (book.select("td.date_read > div.value > span"), lambda x : x[0].contents[0].strip()),

				}

				reviews = [x.get_text() for x in book.select('td.field.review > div.value > span')]

				try:
					data["review"] = reviews[0] if len(reviews) == 1 else reviews[1]
				except Exception as e:
					
					print ("REVIEWS was {}".format(reviews))
					print ("WARNING JUST GOT THIS EXCEPTION WHILE TRYING TO GET REVIEW {}".format(e))

					reviews.append("!@#ERROR!@#")
					data['review'] = reviews

				for key, value in functions.items():

					_input, func = value
					if _input:
						data[key] = func(_input)

				return data

			books = BeautifulSoup(html, "lxml").select('#booksBody > tr')
			return [process_book(book) for book in books ]

		#########


		url = new_url(user_url, page_number)

		try:

			html = make_request(url)
			go_to_sleep('Succesfully reached {} from scrape_review_page'.format(user_url), self.REQUEST_TIME_OUT)
			return parse_page(html)

		except Exception as e:

			go_to_sleep("Failed during scrape_user_page due to {}".format(e), self.ERROR_TIMEOUT)

		try:

			html = make_request(url)
			go_to_sleep('Succesfully reached {} from scrape_review_page'.format(user_url), self.REQUEST_TIME_OUT)
			return parse_page(html)

		except Exception as e:

			go_to_sleep("Failed during scrape_user_page due to {}".format(e), self.ERROR_TIMEOUT)
			return False




	def main(self, obj,outgoing):
		print ("----------------------------------------\n")

		review_info = self.scrape_user_page(obj)

		user_url = obj.get('user_url')
		no_accidental_insert = obj.get("book_name") is None

		unique = self.db[outgoing].find_one({"user_url" : user_url}) is None

		if review_info and unique and no_accidental_insert and user_url:

			page_number = review_info.get('num_reviews')

			if page_number is not None:

				user_url = obj['user_url']
				total_pages = ceil(page_number / 100)

				for idx in range(1,total_pages + 1):
					print ("{} / {} pages".format(idx,total_pages))
					book_reviews = self.scrape_review_page(user_url, idx)
					if book_reviews:
						for book_review in book_reviews:
							book_review['user_url'] = user_url
							self.db[outgoing].insert(book_review)
							print ("Logged valid item to {}".format(outgoing))
			else:
				print ("Misc error in grabbing value of page_number is: {}".format(page_number))
				self.db[outgoing].insert({
				'user_url' : user_url,
				'status' : 'fucked up',
				'unique' : unique,
				'review_info' : review_info, 
				'rating' : obj.get('rating'),
				'no_accidental_insert' : no_accidental_insert,
				'page_number': page_number
				})
				print ("Error: non-unqiue, bad rating or shitty entry logged to {}".format(outgoing))

		else:
			print ("No Reviews/ Bad Rating: {} || Is-not-unique : {} || is-not-valid : {} || no_url : {}".format(not bool(review_info),  not unique,  not no_accidental_insert, bool(user_url)))
			self.db[outgoing].insert({
				'user_url' : user_url,
				'status' : 'fucked up',
				'unique' : unique,
				'review_info' : review_info, 
				'rating' : obj.get('rating'),
				'no_accidental_insert' : no_accidental_insert,
			})
			print ("Error: non-unqiue, bad rating or shitty entry logged to {}".format(outgoing))
		print ("-----------------------------------------------\n")

	def run(self):

		for incoming, outgoing in [("C_BOOKS_RATINGS", "C_REVIEWS"), ("L_BOOKS_RATINGS","L_REVIEWS")]:

			for obj in self.db[incoming].find(no_cursor_timeout=True):
				self.main(obj,outgoing)


run = GatherReviews()


run.run()
