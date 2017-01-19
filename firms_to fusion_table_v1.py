__name__ = "Import FIRMS data to fusion table for Indonesia peatlands "
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
import glob

#'''set by user'''
#Script for downloading, overlaying and uploading to Fusion Table(FT) and Drive, fires data (Firms) for Indonesia peatlands, FT have to be open for access and editing for e-mail from .json key file
#see http://tech.thejoestory.com/2015/05/gspread-python-module-and-more-push-ups.html and http://tech.thejoestory.com/2015/12/fusion-table-api-insert-data-python.html 
#set sources / names and outputs!!TODO normal parser for arguments
#FT id code
tableId = "1lCVZWyWQIfMfVWAEpg7h9KnTyHmujoWEoVN_kaFo"
#drive folder id code
folder_id = '0B2diDVTPYguoU09XTVhuR1I2MzA'
#fire data interval
FIRE_LASTS ='24h'
#url to MODIS data
URL_MOD_FIRE_SHAPES = 'https://firms.modaps.eosdis.nasa.gov/active_fire/c6/text/MODIS_C6_SouthEast_Asia_%s.csv' % FIRE_LASTS
#url to VIIRS data
URL_VII_FIRE_SHAPES = 'https://firms.modaps.eosdis.nasa.gov/active_fire/viirs/text/VNP14IMGTDL_NRT_SouthEast_Asia_%s.csv' % FIRE_LASTS
#dirs for temporal and result files
source_dir='d:/Thematic/Peatfires/Indonesia/Firms_source'
source_sel='d:/Thematic/Peatfires/Indonesia/Firms_source/Sel'
result_dir='d:/Thematic/Peatfires/Indonesia/Firms_source/Res'
#filenames for polygons (peatlands from GFW Indonesia_Peat_Lands.shp)
filename_peatlands = 'mask'
#''set by user''

#set working path2filenames
def set_outputs(filename):
  sourcepath = os.path.join(source_dir, '%s.csv'%(filename))
  outpath_selcsv = os.path.join(source_sel, '%s.csv'%(filename))
  outpath_selshp = os.path.join(source_sel, '%s.shp'%(filename))
  outpath_selvrt = os.path.join(source_dir, '%s.vrt'%(filename))
  outpath_tmpshp = os.path.join(source_dir, '%s_tmp.shp'%(filename))
  outpath_rescsv = os.path.join(result_dir, '%s.csv'%(filename))
  outpath_resshp = os.path.join(result_dir, '%s.shp'%(filename))
  outpath_reskml = os.path.join(result_dir, '%s.kml'%(filename))
  return sourcepath,outpath_selcsv,outpath_selshp,outpath_selvrt,outpath_tmpshp,outpath_rescsv,outpath_resshp,outpath_reskml

#remove files
def silent_remove(filename):
  if os.path.exists(filename):
    os.remove(filename)

#authentification to FT (needed .json key saved on disk)
def auth2FT():
   json_key = json.load(open('d:\\Thematic\\Peatfires\\Python\\import_export_csv2ft\\iggkey.json'))
   scope = ['https://www.googleapis.com/auth/fusiontables']
   credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
   http = httplib2.Http()
   http = credentials.authorize(http)
   #TODOcheck what is build
   service = build("fusiontables", "v1", http=http)
   return(service)

#authentification to drive
def auth2drive():
   json_key = json.load(open('d:\\Thematic\\Peatfires\\Python\\import_export_csv2ft\\iggkey.json'))
   scope = ['https://www.googleapis.com/auth/drive']
   credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)
   http = httplib2.Http()
   http = credentials.authorize(http)
   #TODOcheck what is build
   service = build("drive", "v3", http=http)
   return(service)

#def get session
def get_session(url):
  url = url
  s = requests.session()
  headers = {
       'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
       'X-Requested-With': 'XMLHttpRequest',
       'Referer': url,
       'Pragma': 'no-cache',
       'Cache-Control': 'no-cache'}
  r = s.get(url, headers=headers)
  return(r)

#read file from site and save to csv
def read_csv_from_site(url):
   r = get_session(url)
   reader = csv.reader(r.iter_lines(), delimiter=',', quotechar='"')
   outfile=open(sourcepath, 'wb')
   tmp = csv.writer(outfile)
   tmp.writerows(reader)

#intersect with polygons layer using ogr2ogr
def sp_join(filename):
   #convert to shp
   #create vrt and convert to shp
   f = open(outpath_selvrt, 'w')
   f.write("<OGRVRTDataSource>\n")
   f.write("  <OGRVRTLayer name=\"%s_tmp\">\n" % (filename))
   f.write("    <SrcDataSource relativeToVRT=\"1\">%s</SrcDataSource>\n" % (source_dir))
   f.write("    <SrcLayer>%s</SrcLayer>\n" % (filename))
   f.write("    <GeometryType>wkbPoint</GeometryType>\n")
   f.write("    <LayerSRS>WGS84</LayerSRS>\n")
   f.write("    <GeometryField encoding=\"PointFromColumns\" x=\"longitude\" y=\"latitude\"/>\n")
   f.write("  </OGRVRTLayer>\n")
   f.write("</OGRVRTDataSource>\n")
   f.close()
   #convert
   command="ogr2ogr -overwrite -skipfailures -f \"ESRI Shapefile\" %s %s && ogr2ogr -f \"ESRI Shapefile\" %s %s"  % (source_dir,sourcepath,source_dir,outpath_selvrt)
   print(command)
   os.system(command)   
   #intersect   
   command = "ogr2ogr -overwrite -sql \"SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, A.*, B.* FROM %s_tmp A, %s B WHERE ST_Intersects(A.geometry, B.geometry)\" -dialect SQLITE %s %s -nln %s_tmp1" % (filename,filename_peatlands,source_dir,source_dir,filename)
   print(command)
   os.system(command)
   #conver back to csv
   command = "ogr2ogr -overwrite -skipfailures -f CSV %s %s" % (outpath_selcsv,os.path.join(source_dir, '%s_tmp1.shp'%(filename))) 
   print(command)
   os.system(command)

#find last rowId from FT
def find_last_id_in_FT():
   reqrowid = "SELECT ROWID FROM " + tableId 		
   request = service.query().sql(sql=reqrowid)
   rsp = request.execute()
   lastid = int(max(rsp["rows"])[0])
   return(lastid)

#compose new fields and create final csv ready to upload with the same columns as in FT
def create_csv_to_upload():
   if os.path.isfile(outpath_selcsv):
      csvday = pd.read_csv(outpath_selcsv)
   else:
      print("File doesn't exsist yet")
   tmppath = os.path.join(result_dir, 'tmp.csv')
   tmpcsv = []
   lastid = find_last_id_in_FT()
   for row in csvday.iterrows():
         id = row[0]+lastid
         lat = row[1]['latitude']
         lon = row[1]['longitude']
         acq_date = row[1]['acq_date']
         acq_time = row[1]['acq_time']
         conf = row[1]['confidence']
         type = 'modis'
         reg = row[1]['objectid_1']
         note = ' '
         whouploaded = 'GProbot'
         who = ' '
         link = " "
         status_no = 1
         line = str(id) + "," + str(lat) + "," + str(lon) + "," + acq_date + "," + str(conf) + "," + type + "," + whouploaded + "," + str(reg) + "," + who + "," + str(status_no) + "," + note + "," + link
         tmpcsv.append(line)
   try:
     tmpf = open(tmppath,'wb')
     for line in tmpcsv:
         tmpf.write(line + '\n')
     tmpf.close()
     df = pd.read_csv(tmppath,header=None, names=('GlobID', 'lat', 'lon','acq_date','confidence', 'type', 'whouploaded', 'reg', 'who','status_no', 'note','link'))
     df.to_csv(outpath_rescsv, sep=',', encoding='utf-8', index=False,header=False)
     silent_remove(tmppath)
   except:
      print('Err')

#convert to kml
def convert2kml(filename):
  try:
    command = "ogr2ogr -overwrite -skipfailures -f KML %s %s" % (outpath_reskml,outpath_rescsv)
    os.system(command)
  except:
    print('An error occurs during the convertation %s' % (outpath_rescsv))

#upload kml to drive
def upload_to_drive(outpath_reskml):
   try:
     if os.path.isfile(outpath_reskml):
        print(outpath_reskml)
        file_metadata = {
           'name' : '%s'%(os.path.basename(outpath_reskml)),
           'parents': [ folder_id ]}
        media=MediaFileUpload(outpath_reskml,mimetype='text/csv', resumable=True)
        request = servicedrive.files().create(body=file_metadata, media_body=media, fields='id')
        nms =request.execute()
     else:
        print('File not exist')
   except:
        #time.sleep(60)
     print('An error occurs during the uploading file %s' % (outpath_reskml))


#upload csv to FT
def upload_to_FT(outpath_rescsv):
   try:
     if os.path.isfile(outpath_rescsv):
        print(os.path.basename(outpath_rescsv))
        media=MediaFileUpload(outpath_rescsv,mimetype='application/octet-stream', resumable=True)
        request = service.table().importRows(tableId=tableId,media_body=media)
        nms =request.execute()
     else:
        print('File not exist')
   except:
        #time.sleep(60)
     print('An error occurs during the uploading file %s' % (os.path.basename(outpath_rescsv)))


if __name__ == "__main__":
  while True:
   #current date!!TODO make normal format
   currtime = time.localtime()
   date=time.strftime('%d%m%Y',currtime)
   #set modis filename
   filename_modis = 'modis_%s' % (date)
   #set viirs filename
   filename_viirs = 'viirs_%s' % (date)
   print 'Process started at %s'%(date)
   #create dictionary 
   sat_url={filename_modis:URL_MOD_FIRE_SHAPES,filename_viirs:URL_VII_FIRE_SHAPES}
   #build servises for uploading
   service = auth2FT()
   servicedrive = auth2drive()
   #start workflow for modis and viirs 
   for filename,url in sat_url.iteritems():
     sourcepath,outpath_selcsv,outpath_selshp,outpath_selvrt,outpath_tmpshp,outpath_rescsv,outpath_resshp,outpath_reskml=set_outputs(filename)
     read_csv_from_site(url)
     sp_join(filename)
     #create csv
     create_csv_to_upload()
     convert2kml(filename)
     upload_to_drive(outpath_reskml)
     upload_to_FT(outpath_rescsv)
     for tmpfile in glob.glob(os.path.join(source_dir, '*_tmp*')):
         os.remove(tmpfile)
     os.remove(outpath_selcsv)     
   #sleep for 24h+1sec
   time.sleep(86401)

