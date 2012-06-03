import urllib, gzip, sys
from lxml.etree import iterparse

xml_url = "http://img.jamendo.com/data/dbdump_artistalbumtrack.xml.gz"
xml_file = "dump.xml.gz"

def update_progress(count, blocksize, totalsize):
	percent = int(count * blocksize * 100 / totalsize)
	sys.stdout.write("\r%2d%%" % percent)
	sys.stdout.flush()
	
def get_attribute(element, tagname):
	val = element.find(tagname)
	
	if val is None:
		return ""
	else:
		if val.text == "None":
			return ""
		else:
			return val.text

database = sqlite3.connect(options['database'])
cursor = database.cursor()

#print "Retrieving Jamendo database..."
#urllib.urlretrieve(xml_url, xml_file, reporthook=update_progress)
#print ""

xml = gzip.open(xml_file)

for event, element in iterparse(xml, tag="artist"):
	# id, name, url, image, mbgid, location, Albums
	artistid = get_attribute(element, 'id')
	name = get_attribute(element, 'name')
	image = get_attribute(element, 'image')
	mbgid = get_attribute(element, 'mbgid')
	location = get_attribute(element, 'location')
	
	print "[%s] %s from %s (image: %s)" % (artistid, name, location, image)
	
	for album in element.find('Albums'):
		# id, name, url, releasedate, filename, mbgid, license_artwork, Tracks
		albumname = get_attribute(album, 'name')
		albumurl = get_attribute(album, 'url')
		albumrelease = get_attribute(album, 'releasedate')
		albumfilename = get_attribute(album, 'filename')
		albummbgid = get_attribute(album, 'mbgid')
		albumartworklicense = get_attribute(album, 'license_artwork')
		
		print "    -> Album: %s (%s) at %s" % (albumname, albumrelease, albumurl)
		
		for track in album.find('Tracks'):
			# id, name, filename, mbgid, numalbum, id3genre, license, Tags
			trackid = get_attribute(track, 'id')
			trackname = get_attribute(track, 'name')
			trackfilename = get_attribute(track, 'filename')
			trackmbgid = get_attribute(track, 'mbgid')
			tracknumber = get_attribute(track, 'numalbum')
			trackgenre = get_attribute(track, 'id3genre')
			tracklicense = get_attribute(track, 'license')
			
			print "        [%3d] %s (ID: %s)" % (int(tracknumber), trackname, trackid)
			
			alltags = []
			taglist = track.find('Tags')
			
			if taglist is not None:
				for tag in taglist:
					# idstr, weight
					tagid = get_attribute(tag, 'idstr')
					tagweight = get_attribute(tag, 'weight')
					alltags.append("%s (weight %s)" % (tagid, tagweight))
				
				print "                  %s" % '   '.join(alltags)
			else:
				print "                  No tags."
		
	print ""
	element.clear()
