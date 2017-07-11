# -*- coding: UTF-8 -*-
__author__ = "IG,authentification block to Google FT by JOE STORY "
__copyright__ = ""
__license__ = ""
__modified__ = "## IG"

import json
import sys
import requests
import httplib2
import csv
#google-api-client
from apiclient.discovery import build
#oauth2client=1.5.2 also need PyOpenSSL installed
from oauth2client.client import SignedJwtAssertionCredentials
import pandas as pd
import time,os
from apiclient.http import MediaFileUpload
import glob
import psycopg2
import shapefile

#'''set by user'''
#Script for downloading, overlaying and uploading to Fusion Table(FT) and Drive, fires data (Firms) for Indonesia peatlands, FT have to be open for access and editing for e-mail from .json key file
#see http://tech.thejoestory.com/2015/05/gspread-python-module-and-more-push-ups.html and http://tech.thejoestory.com/2015/12/fusion-table-api-insert-data-python.html 
#set sources / names and outputs!!TODO normal parser for arguments
#FT id code
tableId = "194jvU2DZMBX5tYg6VTCrrO_zwH8Vfo0KYG9jCumk"
#drive folder id code
folder_id = '0B2diDVTPYguodkhqOE9pRXllVlE'
#fire data interval
FIRE_LASTS ='24h'
#url to MODIS data
URL_MOD_FIRE_SHAPES = 'https://firms.modaps.eosdis.nasa.gov/active_fire/c6/text/MODIS_C6_Russia_and_Asia_%s.csv' % FIRE_LASTS
#url to VIIRS data
URL_VII_FIRE_SHAPES = 'https://firms.modaps.eosdis.nasa.gov/active_fire/viirs/text/VNP14IMGTDL_NRT_Russia_and_Asia_%s.csv' % FIRE_LASTS
#dirs for temporal and result files
source_dir='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source'
source_sel='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source/Temp'
result_dir='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source/Result'
upload_dir='d:/Thematic/Peatfires/Russia_Peatlands/Firms_source/ToUpload'
#filenames for polygons (peatlands from GFW Indonesia_Peat_Lands.shp)
filename_peatlands = 'mask2'
#''set by user''

#set working path2filenames
def set_outputs(filename):
  sourcepath = os.path.join(source_dir, '%s.csv'%(filename))
  outpath_selcsv = os.path.join(source_sel, '%s.csv'%(filename))
  outpath_selshp = os.path.join(source_sel, '%s.shp'%(filename))
  outpath_selvrt = os.path.join(source_dir, '%s.vrt'%(filename))
  outpath_tmpshp = os.path.join(source_dir, '%s_tmp.shp'%(filename))
  outpath_rescsv = os.path.join(result_dir, '%s.csv'%(filename))
  outpath_resshp = os.path.join(result_dir, '%s_fin.shp'%(filename))
  outpath_reskml = os.path.join(result_dir, '%s.kml'%(filename))
  outpath_resvrt = os.path.join(result_dir, '%s.vrt'%(filename))
  outpath_upload = os.path.join(upload_dir, '%s.csv'%(filename))
  return sourcepath,outpath_selcsv,outpath_selshp,outpath_selvrt,outpath_tmpshp,outpath_rescsv,outpath_resshp,outpath_reskml,outpath_resvrt,outpath_upload

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
   try:
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
     command="ogr2ogr -overwrite -skipfailures -f \"ESRI Shapefile\" %s %s && ogr2ogr -overwrite -f \"ESRI Shapefile\" %s %s"  % (source_dir,sourcepath,source_dir,outpath_selvrt)
     print(command)
     os.system(command)   
     #intersect   
     #command = "ogr2ogr -overwrite -lco encoding=UTF-8 -sql \"SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, A.*, B.* FROM %s_tmp A, %s B WHERE ST_Intersects(A.geometry, B.geometry)\" -dialect SQLITE %s %s -nln %s_tmp1" % (filename,filename_peatlands,source_dir,source_dir,filename)
     #print(command)
     #os.system(command)
     #conver back to csv
     #command = "ogr2ogr -overwrite -skipfailures -f CSV %s %s" % (outpath_selcsv,os.path.join(source_dir, '%s_tmp1.shp'%(filename))) 
     #print(command)
     #os.system(command)
   except:
       print('An error occured..')

#spatial join using PSQL

def sp_join_postgres(filename):
    #connecting to database
	conn_string = "host='localhost' dbname='gisbase' user='postgres' password='terra'"
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	try:
	  #cursor.execute('DROP TABLE %s_tmp CASCADE' %(filename))
	  command = "shp2pgsql -a -s 4326  -I -c -W UTF-8 %s > %s" % (os.path.join(source_dir, '%s_tmp.shp'%(filename)),os.path.join(source_dir, '%s.sql'%(filename)))
	  print(command)
	  os.system(command)
	  cursor.execute(open('%s'% os.path.join(source_dir, '%s.sql'%(filename)), "r").read())
	except:
	  print('err')
	try:
	  cursor.execute('ALTER TABLE %s_tmp DROP COLUMN gid'%(filename))
	  cursor.execute('INSERT INTO %s_russia SELECT * FROM %s_tmp'%(filename.split('_')[0],filename))
	  conn.commit()
	except:
	  print('err')
	#cursor.execute('DROP TABLE %s_jn'%(filename))
	try:
	  cursor.execute('CREATE TABLE %s_jn AS SELECT %s_tmp.*, mask2.gid as mgid, mask2.unique_id, mask2.type AS type,mask2.district,mask2.region FROM mask2, %s_tmp WHERE ST_Intersects(mask2.geom, %s_tmp.geom)'%(filename,filename,filename,filename))
	except:
	  print('err')
	cursor.execute('SELECT * FROM %s_jn'%(filename))
	res=cursor.fetchall()
	#print(res)
	res=pd.DataFrame(res)
	res.to_csv(outpath_selcsv, sep=',', encoding='utf-8', index=False,header=False)
	cursor.execute('DROP TABLE %s_tmp,%s_jn CASCADE' %(filename,filename))
	conn.commit()
	cursor.close()
	conn.close()
	
#find last rowId from FT
def find_last_id_in_FT():
   reqrowid = "SELECT ROWID FROM " + tableId 		
   request = service.query().sql(sql=reqrowid)
   rsp = request.execute()
   lastid = int(max(rsp["rows"])[0])
   return(lastid)

#compose new fields and create final csv ready to upload with the same columns as in FT
def create_csv_to_upload(filename):
   if os.path.isfile(outpath_selcsv):
      csvday = pd.read_csv(outpath_selcsv,header=None, names=('latitude','longitude','bright_ti4','scan','track','acq_date','acq_time','satellite','confidence','version','bright_ti5','frp','daynight','geom','id','unique_id','type','region','district'))
      tmppath = os.path.join(result_dir, 'tmp.csv')
      tmpcsv = []
      lastid = find_last_id_in_FT()
      for row in csvday.iterrows():
            id = row[0]+lastid
            lat = row[1]['latitude']
            lon = row[1]['longitude']
            acq_date = row[1]['acq_date']
            acq_time = row[1]['acq_time']
            peat = row[1]['unique_id']
            region = ''
            obl = row[1]['region']
            raion = row[1]['district']
            conf = row[1]['confidence']
            type1 = row[1]['type']
            type = filename.split('_')[0]
            note = ''
            whouploaded = 'GProbot'
            who = ' '
            link = "https://drive.google.com/drive/u/0/folders/0B2diDVTPYguodkhqOE9pRXllVlE"
            status_no = 1
            line = str(id) + "," + str(lat) + "," + str(lon) + "," + acq_date + "," + str(acq_time) + "," + str(peat) + "," + str(type1) + "," + region + "," + obl + "," + raion + "," + str(conf) + "," + type + "," + whouploaded + "," + who + "," + str(status_no) + "," + note + "," + link
            tmpcsv.append(line)
   else:
      print("File doesn't exist")
   try:
     tmpf = open(tmppath,'wb')
     for line in tmpcsv:
         tmpf.write(line + '\n')
     tmpf.close()
     df = pd.read_csv(tmppath,header=None, names=('GlobID', 'lat', 'lon','acq_date','acq_time','peat','type1','reg','obl','rai','confidence', 'type', 'whouploaded', 'who','status_no', 'note','link'))
     df.to_csv(outpath_upload, sep=',', encoding='utf-8', index=False,header=False)
     df.to_csv(outpath_rescsv, sep=',', encoding='utf-8', index=False,header=True)
     silent_remove(tmppath)
   except:
      print('Err')

#convert to kml (not works with latin1 encoding)
def convert2kml(filename):
  try:
    #create vrt and convert to shp
    f = open(outpath_resvrt, 'w')
    f.write("<OGRVRTDataSource>\n")
    f.write("  <OGRVRTLayer name=\"%s_fin\">\n" % (filename))
    f.write("    <SrcDataSource relativeToVRT=\"1\">%s</SrcDataSource>\n" % (result_dir))
    f.write("    <SrcLayer>%s</SrcLayer>\n" % (filename))
    f.write("    <GeometryType>wkbPoint</GeometryType>\n")
    f.write("    <LayerSRS>WGS84</LayerSRS>\n")
    f.write("    <GeometryField encoding=\"PointFromColumns\" x=\"lon\" y=\"lat\"/>\n")
    f.write("  </OGRVRTLayer>\n")
    f.write("</OGRVRTDataSource>\n")
    f.close()    
    command = "ogr2ogr -overwrite -skipfailures -f \"ESRI Shapefile\" %s %s && ogr2ogr -overwrite -f \"ESRI Shapefile\" %s %s" % (result_dir,outpath_rescsv,result_dir,outpath_resvrt)
    os.system(command)
    #conver to kml
    command = "ogr2ogr -overwrite -skipfailures -f KML %s %s" % (outpath_reskml,outpath_resshp)
    os.system(command)	
  except:
    print('An error occurs during the convertation %s' % (outpath_rescsv))
#convert to shp and kml
def csv2shp(outpath_rescsv):
	output_shp = shapefile.Writer(shapefile.POINT)
	# for every record there must be a corresponding geometry.
	output_shp.autoBalance = 1
	# create the field names and data type for each.
	# you can insert or omit lat-long here
	output_shp.field('GlobID','N')
	output_shp.field('lat','N')
	output_shp.field('lon','N')
	output_shp.field('acq_date','C',50)
	output_shp.field('acq_time','C',10)
	output_shp.field('peat','C',255)
	output_shp.field('type1','C',10)
	output_shp.field('reg','C',255)
	output_shp.field('obl','C',255)
	output_shp.field('rai','C',255)
	output_shp.field('confidence','C',50)
	output_shp.field('type','C',50)
	output_shp.field('whoupl','C',50)
	output_shp.field('who','C',50)
	output_shp.field('status_no','C',50)
	output_shp.field('note','C',50)
	output_shp.field('link','C',250)
	# count the features
	counter = 1
	# access the CSV file
	df = pd.read_csv(outpath_rescsv,sep=',')
	#loop through each of the rows and assign the attributes to variables
	for row in df.iterrows():
	  GlobID= row[1]['GlobID']
	  lat= row[1]['lat']
	  lon = row[1]['lon']
	  acq_date = row[1]['acq_date']
	  acq_time = row[1]['acq_time']
	  peat = row[1]['peat']
	  type1 = row[1]['type1']
	  reg = row[1]['reg']
	  obl = row[1]['obl']
	  rai = row[1]['rai']
	  confidence = row[1]['confidence']
	  type=row[1]['type']
	  whouploaded = row[1]['whouploaded']
	  who = row[1]['who']
	  status_no = row[1]['status_no']
	  note = row[1]['note']
	  link = row[1]['link']
	  # create the point geometry
	  output_shp.point(float(lon),float(lat))
	  # add attribute data
	  output_shp.record(GlobID, lat, lon,acq_date,acq_time,peat,type1,reg,obl,rai,confidence,type,whouploaded,who,status_no,note,link)
	  print "Feature " + str(counter) + " added to Shapefile."
	  counter = counter + 1
	# save the Shapefile
	try:
	 output_shp.save(outpath_resshp)
	 command = "ogr2ogr -overwrite -skipfailures -f KML %s %s" % (outpath_reskml,outpath_resshp)
	 os.system(command)
	except:
	 print('err')
	 
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
def upload_to_FT(outpath_upload):
   try:
     if os.path.isfile(outpath_upload):
        print(os.path.basename(outpath_upload))
        media=MediaFileUpload(outpath_upload,mimetype='application/octet-stream', resumable=True)
        request = service.table().importRows(tableId=tableId,media_body=media)
        nms =request.execute()
     else:
        print('File not exist')
   except:
        #time.sleep(60)
     print('An error occurs during the uploading file %s' % (os.path.basename(outpath_upload)))



if __name__ == "__main__":
  while True:
   log=os.path.join(source_dir, 'log.txt')
   logf = open(log, 'a')
   #current date!!TODO make normal format
   start=time.time()
   currtime = time.localtime()
   date=time.strftime('%d%m%Y',currtime)
   cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
   logf.write(cdate)
   #set modis filename
   filename_modis = 'modis_%s' % (date)
   #set viirs filename
   filename_viirs = 'viirs_%s' % (date)
   print 'Process started at %s'%(cdate)
   #create dictionary 
   sat_url={filename_modis:URL_MOD_FIRE_SHAPES,filename_viirs:URL_VII_FIRE_SHAPES}
   #build servises for uploading
   service = auth2FT()
   servicedrive = auth2drive()
   #start workflow for modis and viirs 
   for filename,url in sat_url.iteritems():
     sourcepath,outpath_selcsv,outpath_selshp,outpath_selvrt,outpath_tmpshp,outpath_rescsv,outpath_resshp,outpath_reskml,outpath_resvrt,outpath_upload=set_outputs(filename)
     read_csv_from_site(url)
     sp_join(filename)
     sp_join_postgres(filename)
     #create csv
     create_csv_to_upload(filename)
     csv2shp(outpath_rescsv)
     #convert2kml(filename)
     upload_to_drive(outpath_reskml)
     upload_to_FT(outpath_upload)
     #for tmpfile in glob.glob(os.path.join(source_dir, '*_tmp*')):
         #os.remove(tmpfile)
        #if os.path.isfile(outpath_selcsv):
        #os.remove(outpath_selcsv)
     #else:
        #continue      	 
   #sleep for 24h+1sec
   end=time.time()
   sleep=86401-(end-start)
   logf.close()
   time.sleep(sleep)

