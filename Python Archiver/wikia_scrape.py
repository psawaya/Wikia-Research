from BeautifulSoup import BeautifulSoup

import json #not the py2.6 'json' library

import urllib2

from time import sleep

def increaseSleepTime(sleep_time):
    return 0 #right now, don't bother with sleeping

class WikiaScraper:
    def __init__(self):
        self.json_docs = []
    
    def scrapeIndex(self):
        self.scrapePage("http://wiki-stats.wikia.com/",0)
        
    def scrapePage(self,url,wait_time):
        print "hitting page %s, sleeping %ss first\n" % (url,wait_time)
        
        sleep(wait_time)
        
        page = urllib2.urlopen(url)
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
            
        else:        
            contents = []
        
            for link in soup('a'):
                contents.append(link.contents)
        
            new_sub_dirs = filter(lambda dir_str: dir_str[-1][-1] == '/',contents)

            for sub_dir in new_sub_dirs:
                self.scrapePage(url + sub_dir[0],increaseSleepTime(wait_time))
            
    def scrapeJSON(self,url,filename):
        self.json_docs.append(url)

        print ("json file = %s" % (url+filename))
        json_doc = urllib2.urlopen(url + filename).read()
        
        print "json_doc = %s " % json_doc
        
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
        
if __name__ == "__main__":
    scraper = WikiaScraper()
    scraper.scrapeIndex()