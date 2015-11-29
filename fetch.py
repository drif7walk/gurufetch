import urllib2
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker

from bs4 import BeautifulSoup



page=1
outfilename="inserts.sql"
fetchurl="http://www.guru.com/d/jobs/pg/"+str(page)

print "Fetching all jobs on page " + str(page) + " into " + outfilename


page = urllib2.urlopen(fetchurl).read()
soup = BeautifulSoup(page, "html.parser")

# Fetches all list items from specified page. List items should contain
# the needed data.
list = soup.findAll('li', attrs={'class':'serviceItem'})

# XXX: Change to whatever to log into database
engine = create_engine('sqlite:///:memory:', echo=True)
metadata = MetaData()
t_jobs = Table('jobs', metadata,

	Column('id', Integer, primary_key=True),

	Column('title', String), 

	Column('linkto', String), 

	Column('desc', String), 

	Column('postedat', String), 

	Column('expiresat', String), 

	Column('paytype', String), 

	Column('price', String), 

	Column('numquotes', String), 

	Column('username', String), 

	Column('country', String), 

	Column('spent', String), 

	Column('hasfeedback', String), 

	Column('skills', String)

)
metadata.create_all(engine)
conn = engine.connect()

f = open(outfilename, 'w')

for li in list:
    try:
        title = li.find('h2', attrs={'class':'servTitle'}).text.strip() #
        linkto = "http://www.guru.com" + li.find('h2', attrs={'class':'servTitle'}).find('a')['href']
        desc = li.find('p', attrs={'class':'desc'}).text.strip()
        postedat = li.find('div', attrs={'class':'reltime_new dt-style1'})['data-date']
        expiresat = li.find('span', attrs={'class':'dt-style6'})['data-date']
        paytype = li.find('div', attrs={'class':'projAttributes'}).text.split('|')[0]
        price = li.find('div', attrs={'class':'projAttributes'}).text.split('|')[1]
        numquotes = li.find('div', attrs={'class':'projAttributes'}).text.split('|')[4].split(' ')[0]
        username = li.find('h4').text.strip()
        country = li.find('p', attrs={'class':'countryInfo'}).text.strip().split('|')[0].strip()
        spent = li.find('p', attrs={'class':'countryInfo'}).text.strip().split('|')[1].strip().split(' ')[0]
        hasfeedback = ('True', 'False')[li.find('p', attrs={'class':'countryInfo'}).text.strip().split('|')[2].strip() == 'No Feedback'] # ~ 
        skills = li.find('ul', attrs={'class':'skills'}).findAll('a')
        
        # xxx
        skillsstr = ""
        i=0
        for sk in skills:
            skillsstr += sk.text + "|_|" # delimiter

        ins = t_jobs.insert().values(title=title, linkto=linkto, desc=desc, postedat=postedat, expiresat=expiresat, paytype=paytype, price=price, numquotes=numquotes, username=username, country=country, spent=spent, hasfeedback=hasfeedback, skills=skillsstr)
        result = conn.execute(ins)

        f.write("INSERT INTO jobs (title, linkto, desc, postedat, expiresat, paytype, price, numquotes, username, country, spent, hasfeedback, skills) VALUES (" + "\"" + title +"\", \"" + linkto + "\", \"" + desc + "\", \"" + postedat + "\", \"" + expiresat + "\", \"" + paytype + "\", \"" + price + "\", \"" + numquotes + "\", \"" + username + "\", \"" + country + "\", \"" + spent + "\", \"" + hasfeedback + "\", \"" + skillsstr + "\");\r\n")
    except:
        pass

f.close()
conn.close()
