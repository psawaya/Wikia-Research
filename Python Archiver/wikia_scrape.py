from wikia_db import WikiaDB

from BeautifulSoup import BeautifulSoup

import json #not the py2.6 'json' library

import urllib2
import os
from time import sleep

from httplib import BadStatusLine

DOWNLOAD_FILES = True
DOWNLOAD_DIR = "" # "/nas/wikia_dumps/"

EMAIL_THRESHOLD = 5 #50 * 1000 * 1000 #email me every 50 megs

SLEEP_BETWEEN_DLS = 0 #20 

PAULS_PHONE_NUMBER = 1234561234

def increaseSleepTime(sleep_time):
    return 0 #right now, don't bother with sleeping

class WikiaScraper:
    def __init__(self):
        self.json_docs = []
        
        self.db = WikiaDB()
        
        self.downloaded_already = 0
        self.downloaded_since_email = 0
    
    def scrapeIndex(self):
        self.scrapePage("http://wiki-stats.wikia.com/",0)
        
    @staticmethod
    def fetchPage(url,nth = 0):
        try:
            retval = urllib2.urlopen(url)
        except BadStatusLine:
            
            print "badStatusLine"
            
            emailPaul("Bad status, waiting two minutes", "URL = " + url)
            
            if nth == 0:
                emailPaul("Bad status, waiting two minutes", "URL = " + url, sms=True)
            
            sleep(60 * 2) #Wait two minutes
            return fetchPage(url,nth = nth+1) #Try again
        
        return retval
        
    def scrapePage(self,url,wait_time):
        
        if self.db.dirAlreadyVisited(url): return
        
        # print "hitting page %s, sleeping %ss first\n" % (url,wait_time)
        
        sleep(wait_time)
        
        page = self.fetchPage(url)#urllib2.urlopen(url)
        soup = BeautifulSoup(page)

        #Check if this is an end page (that describes a wikia wiki, and contains a .json)
        
        jsonFiles = [a.contents for a in filter(lambda x: x.contents[0][-5:] == ".json",soup("a"))]
        
        if len(jsonFiles) == 1: #we're at an end page
            (url,archive_name,timestamp,found_file) = self.scrapeJSON(url,jsonFiles[0][0].strip(" "))
            
            json_files_txt = open("json_files.txt",'a')
            
            if found_file:
                file_size = soup.find(text=archive_name).findParent().findParent().findNext().findNext().findNext().findNext(text=True)
                file_size = file_size.strip(" ")
            else:
                file_size = 0

            json_files_txt.write("\n%s\t%s\t%s" % (url+archive_name,timestamp,file_size))

            json_files_txt.close()
            
            # print "time to check db for %s! " % (url+archive_name) 
            
            if self.db.fileNotArchivedOrOld(url+archive_name,timestamp):
                # print "time to archive!"
                wiki_name = url.split("/")[-2]
                
                self.archiveFile(url+archive_name,wiki_name)
                
                self.db.recordArchiveFile(wiki_name,url+archive_name,DOWNLOAD_FILES,timestamp,file_size)
                
                self.downloaded_already += self.db.parseFilesizeString(file_size)
                self.downloaded_since_email += self.db.parseFilesizeString(file_size)
                
                if self.downloaded_since_email >= EMAIL_THRESHOLD:
                    self.downloaded_since_email = 0
                    # self.emailPaul(wiki_name)
                    self.emailPaul("downloaded %iK\n" % float(self.downloaded_already/1000), "Last downloaded: " + wiki_name)
                    self.json_docs = []
            
        else:        
            contents = []
        
            for link in soup('a'):
                contents.append(link.contents)
        
            new_sub_dirs = filter(lambda dir_str: dir_str[-1][-1] == '/',contents)

            for sub_dir in new_sub_dirs:
                self.scrapePage(url + sub_dir[0],increaseSleepTime(wait_time))
            
            #Record that we've been here in the DB
            self.db.recordDirAsScraped(url)
            
    def scrapeJSON(self,url,filename):
        # self.json_docs.append(url)

        # print ("json file = %s" % (url+filename))
        json_doc = self.fetchPage(url+filename).read()
        #urllib2.urlopen(url + filename).read()
        
        # print "json_doc = %s " % json_doc
        
        json_data = json.read(json_doc)
        
        #Try for the full archive first
        if 'pages_full.xml.gz' in json_data:
            archive_name = "pages_full.xml.gz"
        #failing that, look for a "current" one
        elif 'pages_current.xml.gz' in json_data:
            archive_name = "pages_current.xml.gz"
        #And finally, just try whichever one is first in the JSON
        else:
            if len(json_data.keys()) == 0:
                #No data in this JSON doc
                
                return (url, archive_name, "(no data found)", False)
            
            first_file = json_data.keys()[0]
            
            if 'timestamp' in json_data[first_tile]:
                archive_name = first_file
            else:
                #Badly formed JSON
                
                return (url, archive_name, "(no data found)", False)
        
        return (url, archive_name, json_data[archive_name]['timestamp'], True)

    # def emailPaul(self,last_download):
    #     SENDMAIL = "/usr/sbin/sendmail" # sendmail location
    #     p = os.popen("%s -t" % SENDMAIL, "w")
    #     p.write("To: me@paulsawaya.com\n")
    #     p.write("Subject: downloaded %iK\n" % float(self.downloaded_already/1000))
    #     p.write("Last downloaded: %s\n" % last_download) # blank line separating headers from body
    #     sts = p.close()
    #     if sts != 0:
    #         pass
    #         # print "Sendmail exit status", sts
    #     # self.json_docs = []
        
    def emailPaul(self,subject,message_body,sms=False):
        SENDMAIL = "/usr/sbin/sendmail" # sendmail location
        p = os.popen("%s -t" % SENDMAIL, "w")
        
        if sms:
            p.write("To: %s@messaging.sprintpcs.com\n" % PAULS_PHONE_NUMBER)
        else:
            p.write("To: me@paulsawaya.com\n")

        p.write("Subject: %s\n" % subject)#downloaded %iK\n" % float(self.downloaded_already/1000))
        p.write(message_body + "\n") #% last_download) # blank line separating headers from body
        sts = p.close()
        if sts != 0:
            pass
        
    def archiveFile(self,url,filename):
        if DOWNLOAD_FILES:
            # print "About to archive file %s" % url
            os.system ("wget %s -O %s.xml.gz" % (url,DOWNLOAD_DIR + filename))
            
            sleep(SLEEP_BETWEEN_DLS) #sleep 20 seconds
            
        
if __name__ == "__main__":
    scraper = WikiaScraper()
    scraper.scrapeIndex()