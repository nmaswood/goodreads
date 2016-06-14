from time import sleep, time
import datetime
from selenium import webdriver
import requests
import os
import csv

def login():

    d = webdriver.PhantomJS()

    login = [
        ('richardjeanso@gmail.com','//*[@id="userid"]' ),
        ('minerva2','//*[@id="password"]')
    ]

    d.get("https://myaccount.nytimes.com/auth/login")

    for text, xpath in login:
        element = d.find_element_by_xpath(xpath)
        element.clear()
        element.send_keys(text)

    d.find_element_by_xpath('//*[@id="js-login-submit-button"]').click()

    s = requests.Session()

    for c in d.get_cookies(): s.cookies.set(c['name'], c['value'])

    d.close()
    return s

def make_request(url, headers, request, error, session = False, return_json = False, refresh_session = None):

	get_function = session.get if session else requests.get


	try:
		go_to_sleep("About to make request {}".format(url), request)

		if return_json:
			return get_function(url, headers = headers, timeout = 200).json()
		else:
			return get_function(url, headers = headers, timeout = 200).text()

	except Exception as e:
		go_to_sleep(e, error)

		if refresh_session is not None:
			session = refresh_session()

	try:
		go_to_sleep("Again lets try {}".format(url), request)	

		if return_json:
			return get_function(url, headers = headers, timeout = 200).json
		else:
			return get_function(url, headers = headers, timeout = 200).text

	except Exception as e:
		print ("Given up on link due to :{}:".format(e))
		return None

def go_to_sleep(msg, time_in_sec, verbose = True):

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

def write_to_data_to_csv(schema, cursor):

	if os.path.exists('data'): os.mkdir('data')

	with open('data.csv', 'w') as outfile:

		writer = csv.writer(outfile)
		writer.writerow(schema)

		for idx, item in enumerate(cursor):

			item.insert(0,idx)

			write.write_row(*item[:-1])

			with open ("data/{}".format(idx), 'w') as outfile_prime:
				outfile_prime.write(item[-1])

def sample_f(f):
	with open ("sample.html", 'r') as infile:
		f(infile.read())


