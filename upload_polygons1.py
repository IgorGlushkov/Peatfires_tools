__author__ = "IG,authentification block to Google FT by JOE STORY "
__copyright__ = ""
__license__ = ""
__modified__ = "## IG"

import json
import sys
import requests
import httplib2
import csv
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import pandas as pd
import time,os
from apiclient.http import MediaFileUpload
from bs4 import BeautifulSoup
#need lxml installed
#from fastkml import kml

#set sources / names and outputs!!TODO normal parser for arguments
#FT id code
tableId = "1Wa13rTvN9sxHZozOElN-RkDd8H2KEoZIPzu8e5wm"


#dirs for tmp and result files
source_dir='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source'
result_dir='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source/Peatlands_to_upload'
#filenames for points and polygons (peatlands)
filename_peatlands = 'mask'

#working path2filenames
kmlfile = os.path.join(source_dir, '%s.kml'%(filename_peatlands))
sourcepath_shp = os.path.join(source_dir, '%s.shp'%(filename_peatlands))
outpath_rescsv = os.path.join(result_dir, '%s.csv'%(filename_peatlands))
outpath_fincsv = os.path.join(result_dir, '%s_fin.csv'%(filename_peatlands))
#remove 
def silent_remove(filename):
  if os.path.exists(filename):
    os.remove(filename)

#def authentification to FT
def auth2FT():
   json_key = json.load(open('d:\\Thematic\\Peatfires\\Python\\import_export_csv2ft\\iggkey.json'))
   scope = ['https://www.googleapis.com/auth/fusiontables']
   credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
   http = httplib2.Http()
   http = credentials.authorize(http)
   #TODOcheck what is build
   service = build("fusiontables", "v1", http=http)
   return(service)

#parce kml and append coordinates
def parce_kml(kmlfile):
    kml=file(kmlfile).read()
    soup = BeautifulSoup(kml,'xml')
    coord = []
    for c in soup.findAll('Polygon'):
      coord.append(c)
    return(coord)

#shp2csv
def shp2csv():
   if os.path.isfile(outpath_rescsv) != 1:
      command = "ogr2ogr -overwrite -f CSV %s %s" % (outpath_rescsv,sourcepath_shp)
      print(command)
      os.system(command)
   else:
      print("File already exsist") 
  

#create csv to upload with kml field
#create final csv ready to upload with the same columns as in FT

def create_csv_to_upload():
   if os.path.isfile(outpath_rescsv):
      csv = pd.read_csv(outpath_rescsv)
   else:
      print("File doesn't exsist")
   tmppath = os.path.join(result_dir, 'tmp.csv')
   tmpcsv = []
   kml = parce_kml(kmlfile)
   for index,row in enumerate(csv.iterrows()):
         id = row[1]['Name']
         reg = row[1]['FedDistric']
         obl = row[1]['SubjectNam']
         rai = row[1]['NAME_1']
         area = row[1]['AREA_1']
         #print str(area)
         kmlline = kml[index]
         line = str(id) + ";" + str(reg) + ";" + str(obl)+ ";" + str(rai)+ ";" + str(round(area,1)) + ";" + str(kmlline)
         #print(line+'\n')
         tmpcsv.append(line)
   try:
     tmpf = open(tmppath,'wb')
     for line in tmpcsv:
         tmpf.write(line + '\n')
     tmpf.close()
     df = pd.read_csv(tmppath,header=None, sep=';', names=('GlobID', 'reg', 'obl', 'rai', 'area','location'))
     df.to_csv(outpath_fincsv, sep=',', encoding='utf-8', index=False,header=False)
     silent_remove(tmppath)
   except:
      print('Err')	

	  #upload to FT
def upload_to_FT(outpath_fincsv):
   try:
     if os.path.isfile(outpath_fincsv):
        print(os.path.basename(outpath_fincsv))
        media=MediaFileUpload(outpath_fincsv,mimetype='application/octet-stream', resumable=True)
        request = service.table().importRows(tableId=tableId,media_body=media)
        nms =request.execute()
     else:
        print('File not exist')
   except:
        #time.sleep(60)
     print('An error occurs during the uploading file %s' % (os.path.basename(outpath_fincsv)))			
			
if __name__ == "__main__":
   service = auth2FT()
   shp2csv()
   create_csv_to_upload()
   upload_to_FT(outpath_fincsv)  

