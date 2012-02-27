from __future__ import print_function

from sgmllib import SGMLParser
from HTMLParser import HTMLParser, HTMLParseError
from htmlentitydefs import entitydefs
from BeautifulSoup import BeautifulSoup, MinimalSoup, SoupStrainer  ### HTML Parsing
from StringIO import StringIO

import urllib2, urlparse, gzip
import re

__author__ = 'Jeremy S. Gerstle (jgerstle@realoptimal.com)'
__date__  = '$Date: 2011-06-28 16:25:41 $'

''' Tools for Querying URLs and Grabbing HTML Objects '''

USER_AGENT = 'Mozilla/5.0' #"crawl_utils/1.0 +http://www.realoptimal.com/"


# Subclass urllib2.HTTPRedirectHandler so that we know status code for future
class SmartRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_301(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_301(
            self, req, fp, code, msg, headers)
        result.status = code
        return result

    def http_error_302(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_302(
            self, req, fp, code, msg, headers)
        result.status = code
        return result
        

# Subclass urllib2.HTTPError so that we handle errors gracefully       
class DefaultErrorHandler(urllib2.HTTPDefaultErrorHandler):
    def http_error_default(self, req, fp, code, msg, headers):
        result = urllib2.HTTPError(
            req.get_full_url(), code, msg, headers, fp)
        result.status = code
        return result
        

def open_url(source, etag=None, lastmodified=None, agent=USER_AGENT):
	""" Function takes a source URL and builds a stream object capable of 
		handling redirects and other HTTP request errors and opens the connection.
		It also checks to see if the requested page is cached via a server ETag 
		and if the requested page has been modified since our last request.
	"""
	
	if urlparse.urlparse(source)[0] == 'http':
		# open URL with urllib2
		request = urllib2.Request(source)
		request.add_header('User-Agent', agent)
		if lastmodified:
			request.add_header('If-Modified-Since', lastmodified)
		if etag:
			request.add_header('If-None-Match', etag)
		request.add_header('Accept-encoding', 'gzip')
		opener = urllib2.build_opener(SmartRedirectHandler(), DefaultErrorHandler())
		return opener.open(request)
		


def fetch_page(source, etag=None, lastmodified=None, agent=USER_AGENT):
    '''Fetch data and metadata from a URL'''
    result = {}
    f = open_url(source, etag, lastmodified, agent)
    result['data'] = f.read()
    if hasattr(f, 'headers'):
        # save ETag, if the server sent one
        result['etag'] = f.headers.get('ETag')
        # save Last-Modified header, if the server sent one
        result['lastmodified'] = f.headers.get('Last-Modified')
        if f.headers.get('content-encoding') == 'gzip':
            # data came back gzip-compressed, decompress it
            result['data'] = gzip.GzipFile(fileobj=StringIO(result['data'])).read()
    if hasattr(f, 'url'):
        result['url'] = f.url
        result['status'] = 200
    if hasattr(f, 'status'):
        result['status'] = f.status
    f.close()
    return result
    

'''
# ---------------------------------------------------------------------------------------
# ---------------------------------SIMPLE & EFFECTIVE -----------------------------------
# ---------------------------------------------------------------------------------------
'''

''' Tools for Parsing HTML Objects ''' 
		
class URLLister(SGMLParser):
	def reset(self):
		# extend (called by SGMLParser.__init__)
		self.urls = []
		SGMLParser.reset(self)
		
	def start_a(self, attrs):
		href = [v for k,v in attrs if k=='href']
		if href:
			self.urls.extend(href)
		
'''
# ---------------------------------------------------------------------------------------
# -------------------------------------- TODO -------------------------------------------
# ---------------------------------------------------------------------------------------
'''

class ParsePageError(HTMLParseError):
	"""Exception raised for all parse errors."""

	def __init__(self, msg, position=(None, None)):
		HTMLParseError.__init__(self, msg, position)
		

	def __str__(self):
    		result = HTMLParseError.__str__(self)
		return result



class ParsePage(HTMLParser):
	def reset(self):
		# extend (called by HTMLParser.__init__)
		self.pieces = []		
		HTMLParser.reset(self)
		

	def handle_starttag(self, tag, attrs):
		strattrs = "".join([' %s="%s"' % (key, value) for key, value in attrs])
		print("Parsed: <%(tag)s%(strattrs)s>" % locals())
		if tag=='script':
			self.handle_comment(attrs)
		else:
			print('Tag start:', tag, attrs)
		

	def handle_endtag(self, tag):
		print('tag end:  ', tag)
		
	def handle_data(self, data):
		print('data......', data.rstrip())
		
	def handle_charref(self, name):
		print('charref.......', name)
		
	def handle_entityref(self, name):
		if entitydefs.has_key(name):
			print('ref.......', entitydefs[name])
	
	def handle_comment(self, data):
		print('comment.......', data)
		
	def handle_decl(self, decl):
		print('SGML Decl.....', decl)
		
	def handle_pi(self, text):
		print('Proc Instr....', text)
		
		
		
    

