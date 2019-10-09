import sqlite3
from urllib.parse import urlparse

weights = 999999

def print_path(path):
	global weights
	for i in path:
		cur.execute('SELECT url FROM Pages WHERE id=?', (i,))
		p = urlparse(cur.fetchone()[0])
		print(p.path, end=' -> ')
	print()
	print('################')
	weights = len(path)
	print(weights)
	print('################')



def printAllPathsUtil(u, d, visited, path, inside): 
	global weights
	visited.append(u)
	path.append(u)
	if inside >= weights: 
		print('Too Deep')
		return
	if u == d: 
		print_path(path)
	elif inside < weights + 1: 
		cur.execute('''SELECT from_id
		FROM Links
		WHERE to_id = ?''', (u,))
		rows = cur.fetchall()
		for i in rows: 
			if i[0] not in visited: 
				printAllPathsUtil(i[0], d, visited, path, inside + 1) 

	path.pop()
	visited.remove(u)

def printAllPaths(s, d): 
	visited = []
	path = []
	printAllPathsUtil(s, d,visited, path, 0) 


######

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()
print('1. Search for pages')
print('2. Find connection between 2 webpages')
inp = int(input('Enter (1-2):'))
if inp == 1:
	find = input('Enter Keyword:')
	find = '%%' + find + '%%'
	cur.execute(f'''SELECT id, url
		FROM Pages
		WHERE url LIKE ?''', (find, ))
	rows = cur.fetchall()
	for i in rows:
		print(str(i[0]) + '\t' + i[1])
else:
	findWebsite = input('Enter url:')
	if findWebsite.endswith('/'): findWebsite = findWebsite[:-1]
	
	cur.execute('''SELECT id FROM Pages 
		WHERE url = ?''', (findWebsite,))
	source_id = cur.fetchone()
	if source_id is None:
		print('Website not found!')
		exit()
	else:
		source_id = source_id[0]
	
	printAllPaths(source_id, 1	)
	
	cur.execute('''SELECT from_id, a.url, to_id, b.url
		FROM Pages as a, Links, Pages as b
		ON a.id = from_id AND b.id = to_id
		WHERE b.url = ?''', (findWebsite,))