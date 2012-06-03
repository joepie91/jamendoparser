import urllib, gzip, sys, argparse, sqlite3
from lxml.etree import iterparse

xml_url = "http://img.jamendo.com/data/dbdump_artistalbumtrack.xml.gz"

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

parser = argparse.ArgumentParser(description='Downloads and parses the Jamendo XML dump, and creates an SQLite database with all artist, album, track, and tag data.')

parser.add_argument('-D', dest='no_download', action='store_true',
                   help='don\'t download the XML dump and use an existing XML dump instead')
                   
parser.add_argument('-d', dest='database', action='store', default='jamendo.db',
                   help='path of the database that should be used to store the data (will be created if it does not exist yet)')
                   
parser.add_argument('-x', dest='xml_path', action='store', default='jamendo.xml.gz',
                   help='path to the Jamendo XML dump (this is the file that will be created when a new dump is downloaded)')

args = parser.parse_args()
options = vars(args)

xml_file = options['xml_path']

if options['no_download'] == False:
	print "Retrieving Jamendo database..."
	urllib.urlretrieve(xml_url, xml_file, reporthook=update_progress)
	print ""

database = sqlite3.connect(options['database'])
cursor = database.cursor()

try:
	# Try to create artists table
	cursor.execute("CREATE TABLE artists (`id`, `name`, `url`, `image`, `mbgid`, `location`)")
except sqlite3.OperationalError:
	pass
	
try:
	# Try to create albums table
	cursor.execute("CREATE TABLE albums (`id`, `artist_id`, `name`, `url`, `releasedate`, `filename`, `mbgid`, `license_artwork`)")
except sqlite3.OperationalError:
	pass
	
try:
	# Try to create tracks table
	cursor.execute("CREATE TABLE tracks (`id`, `artist_id`, `album_id`, `name`, `filename`, `mbgid`, `tracknumber`, `genre`, `license`)")
except sqlite3.OperationalError:
	pass
	
try:
	# Try to create tags table
	cursor.execute("CREATE TABLE tags (`track_id`, `name`, `weight`)")
except sqlite3.OperationalError:
	pass

xml = gzip.open(xml_file)

for event, element in iterparse(xml, tag="artist"):
	# id, name, url, image, mbgid, location, Albums
	artistid = get_attribute(element, 'id')
	name = get_attribute(element, 'name')
	url = get_attribute(element, 'url')
	image = get_attribute(element, 'image')
	mbgid = get_attribute(element, 'mbgid')
	location = get_attribute(element, 'location')
	
	cursor.execute("INSERT INTO artists VALUES (?, ?, ?, ?, ?, ?)", (artistid, name, url, image, mbgid, location))
	print "[%s] %s from %s (image: %s)" % (artistid, name, location, image)
	
	for album in element.find('Albums'):
		# id, name, url, releasedate, filename, mbgid, license_artwork, Tracks
		albumname = get_attribute(album, 'name')
		albumurl = get_attribute(album, 'url')
		albumrelease = get_attribute(album, 'releasedate')
		albumfilename = get_attribute(album, 'filename')
		albummbgid = get_attribute(album, 'mbgid')
		albumartworklicense = get_attribute(album, 'license_artwork')
		
		cursor.execute("INSERT INTO albums VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (albumid, albumname, albumurl, albumrelease, albumfilename, albummbgid, albumartworklicense))
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
