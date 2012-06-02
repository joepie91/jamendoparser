import urllib, gzip, sys
from lxml import etree

xml_url = "http://img.jamendo.com/data/dbdump_artistalbumtrack.xml.gz"
xml_file = "dump.xml.gz"

def update_progress(count, blocksize, totalsize):
	percent = int(count * blocksize * 100 / totalsize)
	sys.stdout.write("\r%2d%%" % percent)
	sys.stdout.flush()

def fast_iter(context, func):
	# http://www.ibm.com/developerworks/xml/library/x-hiperfparse/
	# Author: Liza Daly
	for event, elem in context:
		func(elem)
		elem.clear()
		while elem.getprevious() is not None:
			del elem.getparent()[0]
	del context

def process_element(elem):
	print elem.xpath( 'description/text( )' )

print "Retrieving Jamendo database..."
urllib.urlretrieve(xml_url, xml_file, reporthook=update_progress)

exit(0)
context = etree.iterparse( MYFILE, tag='item' )
fast_iter(context,process_element)
