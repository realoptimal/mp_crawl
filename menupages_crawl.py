'''	menupages_crawl: a python based web crawler tailored for extracting restaurant
	review data from www.menupages.com (mp) website.

	The basic things this this must do are: 
	
	1.	Crawl the base page and gather links to different areas of the site.
		
	2.	Partition links into categories based on the apparent pattern or template
		of those site-links and assign category tags.  Even though a cursory look
		at the site and examining the URLs visually reveals enough too be able to
		clone the hierarchical categorization already embedded in the site.  We
		choose to pick just a few of these for now based on this knowledge, but
		still rely on implicit patterns for making assignments. E.g.
		    
		a. LOCATION	: {	(City : 'NY'), (Area : 'Manhattan'), 
						(Neighborhood : 'East Village'), (ZIP : '10009') }
			
		b. CUISINE	: ['Asian-Fusion', 'French', 'Vietnamese']
		
		c. FEATURES	: { (Serves : ['Dinner', 'Lunch']), (Open24hrs : False),
						(Delivery : False), (WheelchairAccess : True), 
						(ReviewedRecently : True), (TrendingUp : True)}
						
		[N.B. - A simpler approach would be to just examine the categories and
		 and links on the page and set up the templates, but this may not work
		 in the future if the links change, either by design or because the site
		 changes to a different backend arch. which necessitates it.]
	
	3.	Visit restaurant links found at the most general level of categorization
		possible and gather store off relevant venue specific information. E.g.
		
		a. NAME		: 'Tartine'
		
		b. ADDRESS	: {	(Street : '253 W. 11th St.'), (Proximity : 'At W. 4th') 
						(City : 'NY'), (ZIP : '10004') }
		
		c. PHONE		: '(212)-229-2611'
		
		d. PRICE		: 3  ### -- $$$ = 3 dollar signs out of 5 --- ###
		
		e. CUISINE	: ['French', 'Bakeries']
		
		f. FEATURES	: {	(Serves : ['Lunch', 'Dinner', 'Brunch']), (Delivery : True),
						(CreditCards : []), ...}
		
		(). HOURS		: ------------- NOT IMPLEMENTING FOR NOW --------------------			
		
		g. RATINGS	: [(Food, Value, Service, Atmosphere), [4.0, 4.5, 3.0, 3.5]]
		
		h. REVIEWS	: {	(Total : 48),
						(UserComments : { (Handle : 'ana d.'), (Date : '2011-06-11'),
										(Text : "Enjoyed It...Amazing..."  } }
										
		i. SIMILAR	: [List of Similar Venues....]
						  
						  
	4.	Dump data to CSV files and/or store in sqlite db
'''

__author__ = 'Jeremy S. Gerstle (jgerstle@realoptimal.com)'
__date__  = '$Date: 2011-06-28 20:16:34 $'

# Import most of the libraries we need for crawling & parsing
# Primarily we rely on urllib2 and BeautifulSoup for this.
from crawl_utils import *          
from datetime import *
from csv import *
import optparse, re, sys, codecs, types

BASE_URL = "http://www.menupages.com/"
USER_AGENT = "menupages_crawl/1.0 +http://www.realoptimal.com/"

''' 
	-------------------------random helper functions ---------------------------------------- 
'''
def objinfo(object, spacing=10, collapse=1):
	"""Print methods and doc strings. Similar to __dict__ if object has this attribute
	Takes module, class, list, dictionary, or string.  
	NOTE:
	May fail if object or instance does not have __weakref__"""
	methodList = [e for e in dir(object) if callable(getattr(object, e))]
	processFunc = collapse and (lambda s: " ".join(s.split())) or (lambda s: s)
	print "\n".join(["%s %s" % 
					(method.ljust(spacing), 
					processFunc(str(getattr(object, method).__doc__)))
					for method in methodList])
	return 1


def flatten_dict(d):
	"""Produce a flattened list of key-value pairs from a nested dictionary."""
	res={}
	for k,v in d.items():
		if isinstance(v,dict):
			subdict=flatten_dict(v)
			for subk,subv in subdict.items():
				res[k+'_'+subk]=subv
		else:
			res[k]=v
	return res
	
	
def flatten_r(d):
	"""
		Produce a flattened list of key-value pairs from a nested dictionary
		with values being themselves lists (possibly of other dictionaries).
	"""
	res={}
	for k,v in d.items():
		if isinstance(v,dict):
			subdict=flatten_dict(v)
			for subk,subv in subdict.items():
				res[k+'_'+subk]=subv
		elif isinstance(v, list):
			res[k] = len(v)
			for li in v:
				if isinstance(li, unicode):
					res[k+'_%d' % v.index(li)] = li.encode('utf-8')
				elif isinstance(li, dict):
					res[k+'_%d' % v.index(li)] = flatten_r(li)
				else:
					res[k+'_%d' % v.index(li)] = len(v)
		elif isinstance(v, unicode):
			res[k]=v.encode('utf-8')
		else:
			res[k]=v
	return res
	
	
def list_uniques(olist):
	''' return a list with just unique elements non destructively '''
	import copy
	lst = copy.deepcopy(olist)
	for l in lst:
		while(lst.count(l) > 1): 
			lst.remove(l)
	return lst	



''' 
	--------------------MenuPage Crawler Class :: [MpCrawler] --------------------------------------- 
'''

class MpCrawler(MinimalSoup):
	'''	Encapsulate most of the BS features we need to gather mp listings and reviews. '''
	link_queue = {}  # key, value store of links; k = url, v = filter-type {by-name, by-area, by-cusine, by-feature}
	restaurants = {} # key, value store of restaurant profiles with k = url, v is the list of attributes
	crawled = []	  # list of links that have been crawled already
	
	def __init__(self, doc, parseOnlyThese=None):
		self.link_queue = {}
		self.restaurants = {}
		self.crawled = []
		MinimalSoup.__init__(self, doc, parseOnlyThese)

	
	def output_markup(self):
		print(self.prettify())


	def output_db(self):
		import MySQLdb

		conn = MySQLdb.connect(
			host='localhost',
			user='crawler',
			db='menupages')
	
		cursor = conn.cursor()
		data = list(self.restaurants.values())
					
		venue_tbl_sql = "INSERT INTO venue (name, url, street_addr, " \
					+ "city, zip_code, area, neighborhood) " \
					+ "VALUES (%(name)s, %(mp_url)s, %(street address)s, " \
					+ "%(city)s, %(zip-code)s, %(area)s, %(neighborhood)s);"
		
		cursor.executemany(venue_tbl_sql, data)
		
		detail_tbl_sql = "INSERT INTO detail (name, cuisine, meals, features) " \
					+ "VALUES (%(name)s, %(cuisine)s, %(meals)s, %(features)s);"

		detail_data = [{'name' : r['name']} for r in data]

		c = [{'cuisine' : r['cuisine']} for r in data]
		m = [r['meals'] for r in data]
		f = [r['features'] for r in data]
		
		# Combine values for features and meals into a flat record per venue
		
		# The 'detail' table uses the SET column type for meals and for features
		# Basically, the set of values that are contained in the meals list is
		# binary encoded so that that the binary value in the table is the sum
		# of values that are contained in a venue's meals or features lists.
		
		_details_dct = { 'cuisine' : "NULL", 'meals' : "NULL", 'features' : "NULL"} 
		update_details = lambda x, y: x.update(y) if isinstance(y, dict) else x.update(_details_dct)
		
		map(update_details, detail_data, c) # detail_data now has cuisine for each venue
		
		# gather up the list of features that we'll turn in to 'detail' table fields		
		flat_ftrs = []
		for ent in f:
			for itm in ent:
				flat_ftrs.append(itm)
				
		# only want uniques from the list; this is the total set of known features across restaurants
		uniq_ftrs = list_uniques(flat_ftrs)
		uniq_ftrs = [s.encode('utf-8') for s in uniq_ftrs] # convert from unicode to utf-8
		uniq_ftrs.sort() # sort in alphabetical asc. order
		
		# now let's add them to the table if they are not already there.
		modify_tbl_sql = "ALTER TABLE detail MODIFY features SET ('" + "','".join(uniq_ftrs) + "');"
		cursor.execute(modify_tbl_sql)
		
		# create the the string of meals and string of features to pass into the sql
		# note: there should be no space between comma seperated elements in a set when referenced
		# a sql INSERT or UPDATE statement.
		
		meals_sqlsubstr = lambda i: ','.join([mval.encode('utf-8') for mval in m[i]])
		meals_sqlstr = map(meals_sqlsubstr, range(0, len(m)))
		meals_data = [{'meals' : s} for s in meals_sqlstr]
		map(update_details, detail_data, meals_data) # detail_data now has meals for each venue
		

		ftrs_sqlsubstr = lambda i: ','.join([fval.encode('utf-8') for fval in f[i]])
		ftrs_sqlstr = map(ftrs_sqlsubstr, range(0, len(f)))
		ftrs_data = [{'features' : s} for s in ftrs_sqlstr]
		map(update_details, detail_data, ftrs_data) # detail_data now has meals for each venue
		
		# now write the table
		cursor.executemany(detail_tbl_sql, detail_data)
		
		# Now write restaurant ratings to the 'rating' table
		p = [{'name' : r['name']} for r in data]
		q = [r['ratings'] for r in data]
		
		# Combine these into a flat dictionary
		# Update with an empty dictionary rather than None as a place-holder
		_ratings_dct = { 
			'count' : 0, 'average' : 0.0, 
			'food' : 0.0, 'value' : 0.0, 
			'service' : 0.0, 'atmosphere' : 0.0
			}
		update_ratings = lambda x,y: x.update(y) if isinstance(y, dict) else x.update(_ratings_dct)
		map(update_ratings, p, q)  
		# Note: the result is actually in p
		rating_data = p
		
		rating_tbl_sql = "INSERT INTO rating (name, count, average, " \
					+ "food, value, service, atmosphere) " \
					+ "VALUES (%(name)s, %(count)s, %(average)s, " \
					+ "%(food)s, %(value)s, %(service)s, " \
					+ "%(atmosphere)s);"
		
		cursor.executemany(rating_tbl_sql, rating_data)
		
		
		w = [{'name' : r['name']} for r in data]
		v = [r['reviews'] for r in data]
		add_name_to_reviews = lambda i,j: v[i][j].update(w[i])
		
		for i in range(0, len(w)):
			if v[i]:				
				for j in range(0, len(v[i])):
					add_name_to_reviews(i, j)
				review_data = v[i]
				review_tbl_sql = "INSERT INTO reviews (name, reviewer, dtreviewed, " \
							+ "summary, comment) " \
							+ "VALUES (%(name)s, %(reviewer)s, %(dtreviewed)s, " \
							+ "%(summary)s, %(comment)s);"
				cursor.executemany(review_tbl_sql, review_data)		
		
		# Finalize Everything By Halting the cursor and closing the connection.
		cursor.close()
		conn.close()
		
		
		
	def output_csv(self, fname, header_row=True):
		''' 
			Flatten all restaurant entity attributes and in the restaurants array
			and write them to a csv file named by the string var: fname .
		'''
		flatten_func = lambda (x): flatten_dict(flatten_r(x))
		
		rsts = self.restaurants.values()  # all our attributes are in the dictionary
		rsts_flat = map(flatten_func, rsts)
		
		fp = open(fname + '.csv', 'w')
		flds = []
		for rst in rsts_flat:
			flds.extend(rst.keys())
			
		flds = list_uniques(flds)
		csv_writer = DictWriter(fp, flds)
		
		if header_row:
			headers = dict([(n,n) for n in flds])
			csv_writer.writerow(headers)
	
		csv_writer.writerows(rsts_flat)
		fp.close()
		print('done writing csv files') # DEBUG statement
	

			
	def scrape_profile(self, doc, url):
		''' Checks if doc (html data) is for a venue, gathers and saves relevant info.  '''
		pparser = BeautifulSoup(doc)
		info_tags = ['meta', 'li', 'span', 'tr', 'div']
		
		''' Cheating for now: using known structure of restaurant pages to pull out relevant info '''
		venue_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('restaurant', 2)})
		
		try:
			assert venue_tag, "No Tag Reference Found"
			venue_txt = venue_tag[0]['content'].rsplit('-')[0]

		except Exception, e:
			print("Unretrievable Info Or Non-Restaurant Page")
			print("Non-Exit Failure: " + str(e)) # DEBUG statement
			return ''
		

		self.restaurants[url] = dict.fromkeys([
										'name', 'street address', 'city', 
										'zip-code', 'area', 'neighborhood', 
										'cuisine', 'meals', 'features',
										'ratings', 'reviews'])
		
		self.restaurants[url]['mp_url'] = url
		self.restaurants[url]['name'] = venue_txt
		
		street_tag = pparser.fetch(info_tags, attrs = {'class' : re.compile('street.address')})
		self.restaurants[url]['street address'] = street_tag[0].text
		
		city_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('city')})
		self.restaurants[url]['city'] = city_tag[0]['content']
		
		postal_tag = pparser.fetch(info_tags, attrs = {'class' : re.compile('postal.code')})
		self.restaurants[url]['zip-code'] = postal_tag[0].text
		
		area_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('area')})
		self.restaurants[url]['area'] = area_tag[0]['content']
		
		hood_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('neighborhood')})
		self.restaurants[url]['neighborhood'] = hood_tag[0]['content']
		
		cuisine_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('cuisine')})
		self.restaurants[url]['cuisine'] = cuisine_tag[0]['content']
		
		meals_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('meal')})
		self.restaurants[url]['meals'] = map(lambda(x): x['content'], meals_tag)

		features_tag = pparser.fetch(info_tags, attrs = {'name' : re.compile('feature')})
		self.restaurants[url]['features'] = map(lambda(x): x['content'], features_tag)
		
		ratings_tag = pparser.fetch(info_tags, attrs = {'id' : re.compile('restaurant.ratings')})
		
		# If there are no ratings (or reviews) then we should dismiss this profile
		if not ratings_tag:
			venue_txt = ''
			return venue_txt
			
		# Store ratings items in a dictionary (not the most efficient for memory but simpler code)
		ratings_dct = { 
			'count' : 0, 'average' : 0.0, 
			'food' : 0.0, 'value' : 0.0, 
			'service' : 0.0, 'atmosphere' : 0.0
			}
			
		# Descend the tag structure to gather the necessary data items to put into dictionary
		info_tags.append('table') # a few more tags needed for combing the structure; exclude "td"
		info_tags.append('th')
		
		itm_fetch = lambda(s): ratings_tag[0].fetch(info_tags, attrs = {'class' : re.compile(s)})
		# Slightly different treatment depending on where each element resides in the table structure
		''' 
		##  In the future a few helper functions to traverse the tag structure and "find" relevant elements
		##  would allow this to be more generic; relying on known structure of mp's restaurant profile html
		##  for now.  There's no reason that these items need to be hardcoded -- but would require
		##  a lexical ontology from which parsing can take place.
		'''
		
		ratings_dct['count'] = int(itm_fetch('count')[0].text)
		if ratings_dct['count']:  # if there are no ratings, other values will be non-existent
			ratings_dct['average'] = float(itm_fetch('average')[0].text)
			ratings_dct['food'] = float(itm_fetch('food.rating')[0].first().text)
			ratings_dct['value'] = float(itm_fetch('value.rating')[0].first().text)
			ratings_dct['service'] = float(itm_fetch('service.rating')[0].first().text)
			ratings_dct['atmosphere'] = float(itm_fetch('atmosphere.rating')[0].first().text)
		
		# Assign it to the ratings key
		self.restaurants[url]['ratings'] = ratings_dct
		
		
		info_tags = ['p', 'li', 'cite', 'h6', 'span']  # Review items are nested in these tags
		reviews_tag = pparser.fetch(info_tags, attrs = {'class' : re.compile('comment.\w*')})
		
		self.restaurants[url]['reviews'] = []
		
		select_itm = lambda s,m: m.fetch(info_tags, attrs = {'class' : re.compile(s)})
		# Just the reviews listed on this page -- to get all the reviews we'd have to make AJAX calls to
		# paginate through and pull in the ones on other pages for this restaurant
		for tag in reviews_tag:
			# Store reviews in a dictionary too
			reviews_dct = {
				'reviewer' : '', 'dtreviewed' : '',
				'summary' : '', 'comment' : ''
				}
			reviews_dct['reviewer'] = select_itm('reviewer', tag)[0].text
			_dt_str = select_itm('dtreviewed', tag)[0].text
			if _dt_str != '':
				_dt_mdy = map(int, _dt_str.encode().split('/'))
				reviews_dct['dtreviewed'] = date(_dt_mdy[2], _dt_mdy[0], _dt_mdy[1]).isoformat()
			reviews_dct['summary'] = select_itm('summary', tag)[0].text
			reviews_dct['comment'] = select_itm('description', tag)[0].text
			
			# Finally -- append it to the list of reviews for this url
			self.restaurants[url]['reviews'].append(reviews_dct)
		
		print(self.restaurants[url])			# DEBUG statement
		
		return venue_txt
		
		
	def scan_restaurant_links(self, doc=''):
		''' Parse Out relative links containing the term restaurant or restaurants '''
		linkfilt = SoupStrainer('a', href = re.compile('/?restaurants?/'))
		linktags = []
		if doc != '':
			#print("Scanning Link Tags off Related")   # DEBUG statement
			## BUG / ISSUE: Links on restaurant pages may be loaded by an AJAX process
			## ------------ May be possible to recreate pythonically using spidermonkey
			linktags = [tag for tag in BeautifulSoup(doc, parseOnlyThese=linkfilt)]
		else:
			linktags = self.findAll(linkfilt)
		# We only care about the urls themselves
		links =  list_uniques([tag['href'] for tag in linktags])  # filter a unique list
		if links:
			print("Trying to add %d restaurant links to queue." % len(links))
			
			num_urls = self.__update_link_queue(links)
			print("\tadded %d successfully" % num_urls)

		else:
			print("No Restaurant Links To Add")
			

	## INTERNAL: __update_link_queue(self, urls)
		
	def __update_link_queue(self, urls):
		# import nltk # need for [future dev]
		
		''' filter out non-relevant links and add links if they were not already visited '''
		full_urls = map(lambda (x): urlparse.urljoin(BASE_URL, x.lstrip('/')), urls)
		
		# filter out the ones we've been to before
		in_crawled = lambda(x): x in self.crawled
		full_urls = [url for url in full_urls if not in_crawled(url)]		
		
		if not full_urls:
			print ("Links to Add Already Crawled")
			return 0
			
		''' OPTION 1 '''		
		
		# Break up the urls again into components recognized by urlparse
		urlparse_objs = map(urlparse.urlparse, full_urls)				
	
				
		'''		 
		##	Ignore the scheme & netloc: scheme+netloc = BASE_URL
		##	If others are blank for all urls in the list we ignore those too.
		##
		##	------------------ Highlighted Feature ----------------------------
		##	While this is written generically, we know for menupages.com that 
		##	the params, query and fragment pieces will be blank, all navigation
		##	info is encoded by the url path, but let's pretend we didn't know
		##	that a priori as this could potentially change in the future.
		'''
		
		upo_paths = [url.path for url in urlparse_objs]	

		# Let's examine the last piece of the path since we know that the other items
		# after the path are empty (all info is in the path) the shortest paths
		# that are unique are the most likely venue pages.  At the end of longer
		# paths they are higher level categorizations.  We can figure out what
		# to do with those later.
		upo_path_wrds = map(lambda (x): x.strip('/').split('/'), upo_paths)
		plen_min = min(map(len, upo_path_wrds))
		upo_venue_wrds = [wrd[len(wrd)-1] for wrd in upo_path_wrds if len(wrd) == plen_min]
		upo_venue_wrds = list_uniques(upo_venue_wrds)
		
		is_venue = lambda(wlst): wlst[len(wlst)-1] in upo_venue_wrds
		venue_path_idxs = [upo_path_wrds.index(wset) for wset in upo_path_wrds if is_venue(wset)] 
		venue_paths = map(lambda(i): upo_paths[i], venue_path_idxs)
		
		# Reconstruct the full path urls to push onto the queue
		full_urls = map(lambda (x): urlparse.urljoin(BASE_URL, x.lstrip('/')), venue_paths)
		
		''' ---------------------- OPTION 2 [FUTURE DEV] ---------------------------- 	
		##	Let's look at path structure and use a bit of nltk magic to 
		##	help us organize and select relevant links!
		'''
		
		# Let's bust everything up into words that we can hopefully use to determine 
		# pages to look at based on uniqueness.  The concept being that our 
		# categorization is organized by words in the url and restauarants are
		# usually names which are somewhat unique. i.e. build a word count or 
		# word freqz (occurrence and/or co-occurence) distribution and only crawl
		# urls containing lowest freq words or bigrams.  Higher freq words or bigrams
		# are likely to be category buckets -- figure out what to do with them later.
		
		## upo_paths = [url.path for url in urlparse_objs]	
		
		## Similarily we could get the list of other pieces to include:
		'''
		N.B. There is probably a more elegeant and more efficient
			way of doing this all in one shot. THis is just easier! 
		'''		
		## upo_qrs   = [url.query for url in urlparse_objs]
		## upo_prms  = [url.params for url in urlparse_objs]
		## upo_frags = [url.fragments for url in urlparse_objs]
		''' -------------------------------------------------------------------------- '''
		
		# The next line adds the urls as keys to the link_queue
		self.link_queue.update(dict.fromkeys(full_urls, 'by-name'))
		return len(full_urls)
		
		
	def crawl(self, n = -1):
		if n < 0:  # Loop through the whole queue if passed a negative number or default
			n = len(self.link_queue)
			self.CRAWL_MAX = 10000
		else:
			self.CRAWL_MAX = n
			
		htmldata = ''
		while n and (len(self.crawled) < self.CRAWL_MAX):
			try:
				assert n <= len(self.link_queue), "Will Only Crawl %d Links in Queue" % len(self.link_queue)
				
				iter_q = self.link_queue.iterkeys()
				mpp_url = iter_q.next()
				mpp = fetch_page(mpp_url)
				
				if mpp['status'] != 200:
					raise Exception("page fetch error: %d" % mpp['status'])
					break
				htmldata = mpp['data']
								
			except (AssertionError, Exception), e:
				print str(e)
				continue
			
			else:

				venue = self.scrape_profile(htmldata, mpp_url)
				if venue:
					print("Restaurant: %s Info Pulled" % venue) 
				self.scan_restaurant_links(htmldata)	

				
			finally:
				# Regardless of errors, pop the link off the queue
				# and add it to a list of crawled links
				qmsg = self.link_queue.pop(mpp_url,'empty')	
				if qmsg != 'empty':
					self.crawled.append(mpp_url)
					print("Adding to Already Crawled: %s" % mpp_url)
				print("Crawled %d links so far." % len(self.crawled))
				print("Crawl Q Has %d links to go" % len(self.link_queue))
				n = len(self.link_queue)
					
		print {'status': 'SUCCESS', 'errors' : None}
		return 1
				
if __name__ == "__main__":
	
	fp = fetch_page(BASE_URL)
	data = fp['data']
	crawler = MpCrawler(data)
	crawler.close()
	#crawler.output_markup()
	crawler.scan_restaurant_links()
	crawler.crawl()
	print "%d Restaurants Crawled Successfully!" % len(crawler.restaurants.keys())

