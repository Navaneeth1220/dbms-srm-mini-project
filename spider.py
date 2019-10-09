import sqlite3
import urllib.error
import ssl
import time
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# defines a function which returns true if the link is a link to a file 
# false otherwise 
def find_files(href):
	href = href.lower()
	# images
	if href.endswith('.png') or href.endswith('.jpg') or href.endswith('.svg') or href.endswith('.jpeg') or href.endswith('.gif'): return True
	# audio and video
	if href.endswith('.mp3') or href.endswith('.bit'): return True
	if href.endswith('ogg') or href.endswith('.webm') or href.endswith('.flac'): return True
	if href.endswith('.wav') or href.endswith('.midi') or href.endswith('.ogv'): return True

	return False

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT, 
    title TEXT, error INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER,
    PRIMARY KEY ( from_id, to_id) )''')

cur.execute('''CREATE TABLE IF NOT EXISTS Websites (url TEXT UNIQUE)''')

#Resuming Progress if some error happens
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()
if row is not None:
	print("Restarting previous session")
else:
	start_url = input('Enter URL:')
	if start_url.endswith('/'): start_url = start_url[:-1]
	parse_url = urlparse(start_url)
	website = f'{parse_url.scheme}://{parse_url.netloc}'
	if len(start_url) > 3 :
		cur.execute('INSERT OR IGNORE INTO Websites (url) VALUES ( ? )', ( website, ))
		cur.execute('INSERT OR IGNORE INTO Pages (url) VALUES ( ?)', ( start_url, ) )
		conn.commit()

cur.execute('''SELECT url FROM Websites''')
webs = list()
for row in cur:
    webs.append(str(row[0]))

print("These are the following websites the program allows currently:")
print(webs)
prev = 0
while True:

	cur.execute('''SELECT id,url FROM Pages WHERE html is NULL and error
		is NULL ORDER BY RANDOM() LIMIT 1''')
	try:
		row = cur.fetchall()[0]
		fromid = row[0]
		url = row[1]
	except:
		print('No new HTML pages found')
		break

	print(fromid, '\t', url, end='\t')
	time.sleep(5)
	cur.execute('DELETE FROM Links WHERE from_id=?', (fromid, ))
	try:
		document = urlopen(url, context=ctx)
		if document.getcode() != 200:
			print("Error on page: ", document.getcode())
			cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))
			conn.commit()
			continue
		if 'text/html' != document.info().get_content_type():
			print('Ignoring non text/html files')
			cur.execute('DELETE FROM Pages WHERE url=?', (url, ))
			conn.commit()
			continue
		html = document.read()

		soup = BeautifulSoup(html, 'html.parser')
	except KeyboardInterrupt:
		print('')
		print('Program interrupt by user...')
		break
	except Error as error:
		print('Unable to retrive or parse page')
		print(error)
		cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
		conn.commit()
		continue

	cur.execute('''INSERT OR IGNORE INTO Pages(url)
		VALUES (?)''', (url, ))
	cur.execute('UPDATE PAGES SET html=? WHERE url=?', (memoryview(html), url))
	conn.commit()

	anchor = soup('a')
	count = 0
	for tag in anchor:
		href = tag.get('href', None)
		if href is None: continue
		#ignore files
		if find_files(href): continue
		#resolve relative anchors like '/hello/hi'
		desturl = urlparse(href)
		if len(desturl.scheme) < 1: href = urljoin(url, href)
		#removes anchors to headings like '/hello#chapter1'
		if href.find('#') >= 1: href = href[:href.find('#')]
		if href.find('?') >= 1: href = href[:href.find('?')]
		if href.endswith('/'): href = href[:-1]
		#ignores if the ancor is not from the domains from Websites table
		desturl = urlparse(href)
		inone = False
		for site in webs:
			if site == f'{desturl.scheme}://{desturl.netloc}':
				inone = True
				break
		if not inone:
			continue

		cur.execute('''INSERT OR IGNORE INTO Pages (url)
			VALUES (?)''', ( href, ) )
		count += 1
		conn.commit()

		cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
		try:
			row = cur.fetchone()
			toid = row[0]
		except:
			print('Could not retrieve id')
			continue
		cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) )

	cur.execute('SELECT COUNT(*) FROM PAGES')
	after = int(cur.fetchone()[0])
	print('(',count, ',', after - prev, ')')
	prev = after