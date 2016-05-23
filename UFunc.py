from time import sleep, time
import datetime
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
import requests


def login():

    d = webdriver.PhantomJS()

    login = [
        ('nmaswood@uchicago.edu','//*[@id="user_email"]' ),
        ('goodreads','//*[@id="user_password"]')
    ]
    url = 'https://www.goodreads.com/user/sign_in'

    d.get(url)

    for text, xpath in login:
        element = d.find_element_by_xpath(xpath)
        element.clear()
        element.send_keys(text)

    for e in ['//*[@id="remember_me"]', '//*[@id="emailForm"]/form/fieldset/div[5]/input']:

	    d.find_element_by_xpath(e).click()

    s = requests.Session()

    for c in d.get_cookies(): s.cookies.set(c['name'], c['value'])

    d.close()
    return s

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
		