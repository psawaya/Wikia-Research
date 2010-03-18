import MySQLdb

from datetime import datetime

class WikiaDB:
    def __init__(self):
        self.db = MySQLdb.connect(user="psawaya",db="wikia")
        
        c = self.db.cursor()
        
        #Create files table, if it doesn't already exist
        c.execute("""CREATE TABLE if not exists `wikia`.`wikia_files` (
          `id` int(11) NOT NULL AUTO_INCREMENT,
          `wiki_name` tinytext,
          `url` tinytext,
          `downloaded` tinyint(1) DEFAULT NULL,
          `last_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          `filesize` bigint(20) DEFAULT NULL,
          PRIMARY KEY (`id`)
        ) ENGINE=MyISAM AUTO_INCREMENT=103 DEFAULT CHARSET=utf8;""")
        
        #Create dirs table, if it doesn't already exist
        c.execute("""CREATE TABLE if not exists `wikia`.`wikia_dirs` (
          `id` INT NOT NULL AUTO_INCREMENT,
          `url` TINYTEXT NOT NULL,
          `lastvisited` TIMESTAMP,
          PRIMARY KEY (`id`)
        ) CHARACTER SET utf8;""")

    def recordDirAsScraped(self,url):
        c = self.db.cursor()
        
        c.execute("INSERT INTO wikia_dirs (url,lastvisited) VALUES ('%s','%s')" % (url,datetime.now()))
        
    def dirAlreadyVisited(self,dir_url):
        c = self.db.cursor()
        
        c.execute("SELECT id FROM wikia_dirs WHERE url = '%s'" % dir_url)
        
        return c.fetchone() is not None
    
    def recordArchiveFile(self,wiki_name,url,downloaded,last_modified,filesize_str):
        c = self.db.cursor()
        
        print "last_modified: %i" % last_modified
        
        c.execute("""INSERT INTO wikia_files (wiki_name,url,downloaded,last_modified,filesize)
            VALUES ('%s','%s',%i,'%s',%i)"""  % (wiki_name,url,int(downloaded), datetime.fromtimestamp(last_modified),self.parseFilesizeString(filesize_str)))
    
    def fileNotArchivedOrOld(self,url,last_modified):
        c = self.db.cursor()
        
        c.execute("""SELECT id FROM wikia_files WHERE 
            url = '%s' AND (downloaded = 1 AND last_modified >= '%s')""" % (url, datetime.fromtimestamp(last_modified)))
        
        return c.fetchone() is None
    
    @staticmethod
    def parseFilesizeString(filesize_str):
        mult_factor = {'K' : 1000, 'M' : 1000000, 'G' : 1000000000}[filesize_str[-1].upper()]
        
        return float(filesize_str[:-1]) * mult_factor