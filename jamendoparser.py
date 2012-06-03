import urllib, gzip, sys, argparse, sqlite3, datetime, time
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
total = 0

for event, element in iterparse(xml, tag="artist"):
	# id, name, url, image, mbgid, location, Albums
	artistid = get_attribute(element, 'id')
	name = get_attribute(element, 'name')
	url = get_attribute(element, 'url')
	image = get_attribute(element, 'image')
	mbgid = get_attribute(element, 'mbgid')
	
	location_element = element.find('location')
	try:
		country = get_attribute(location_element, 'country')
	except AttributeError:
		country = ""
	
	cursor.execute("INSERT INTO artists VALUES (?, ?, ?, ?, ?, ?)", (artistid, name, url, image, mbgid, country))
	
	for album in element.find('Albums'):
		# id, name, url, releasedate, filename, mbgid, license_artwork, Tracks
		albumid = get_attribute(album, 'id')
		albumname = get_attribute(album, 'name')
		albumurl = get_attribute(album, 'url')
		albumrelease = int(time.mktime(datetime.datetime.strptime(get_attribute(album, 'releasedate').split('+')[0], '%Y-%m-%dT%H:%M:%S').timetuple()))
		albumfilename = get_attribute(album, 'filename')
		albummbgid = get_attribute(album, 'mbgid')
		albumartworklicense = get_attribute(album, 'license_artwork')
		
		cursor.execute("INSERT INTO albums VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (albumid, artistid, albumname, albumurl, albumrelease, albumfilename, albummbgid, albumartworklicense))
		
		for track in album.find('Tracks'):
			# id, name, filename, mbgid, numalbum, id3genre, license, Tags
			trackid = get_attribute(track, 'id')
			trackname = get_attribute(track, 'name')
			trackfilename = get_attribute(track, 'filename')
			trackmbgid = get_attribute(track, 'mbgid')
			tracknumber = get_attribute(track, 'numalbum')
			trackgenre = get_attribute(track, 'id3genre')
			tracklicense = get_attribute(track, 'license')
			
			cursor.execute("INSERT INTO tracks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (trackid, artistid, albumid, trackname, trackfilename, trackmbgid, tracknumber, trackgenre, tracklicense))
			
			taglist = track.find('Tags')
			
			if taglist is not None:
				for tag in taglist:
					# idstr, weight
					tagid = get_attribute(tag, 'idstr')
					tagweight = get_attribute(tag, 'weight')
					cursor.execute("INSERT INTO tags VALUES (?, ?, ?)", (trackid, tagid, tagweight))
	
	print "Inserted %s into database" % (name,)
	
	total += 1
	element.clear()

print "Parsed and inserted a total of %d artists." % total
database.commit()
print "Changes committed to database."
