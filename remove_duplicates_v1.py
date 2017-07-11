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
#tableId = "194jvU2DZMBX5tYg6VTCrrO_zwH8Vfo0KYG9jCumk"
tableIdfin = "194jvU2DZMBX5tYg6VTCrrO_zwH8Vfo0KYG9jCumk"
#drive folder id code
folder_id = '0B2diDVTPYguodkhqOE9pRXllVlE'
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
  outpath_ft2upload = os.path.join(upload_dir, '%s.csv'%(filename))
  outpath_ft_kml2upload = os.path.join(upload_dir, '%s.kml'%(filename))
  outpath_ft_shp2upload = os.path.join(upload_dir, '%s.shp'%(filename))
  outpath_ft = os.path.join(upload_dir, '%s_backup.csv'%(filename))
  return outpath_ft,outpath_ft2upload,outpath_ft_kml2upload,outpath_ft_shp2upload

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

#DOWNLOAD ft
def download_ft():
	try:
		query = "SELECT * FROM " + tableIdfin
		response = service.query().sql(sql=query).execute()
	except:
		print('err')
	return(response)

def remove_dupl_csv():
	try:
		fp = open("%s"%(outpath_ft2upload))
	except IOError:
		response = download_ft()
		data=pd.DataFrame(response[u'rows'],columns=response[u'columns'])
		data['ident']=data[u'Широта'].map(str)+ data[u'Долгота'].map(str)+ data[u'Дата съемки']
		data['Dupl']=data.duplicated([u'ident'])
		data1=data.drop_duplicates([u'ident'])
		data1.to_csv(outpath_ft2upload, sep=',', encoding='utf-8', index=False,header=False)
		data.to_csv(outpath_ft, sep=',', encoding='utf-8', index=False,header=True)


#convert to shp and kml
def csv2shp(outpath_ft2upload):
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
	df = pd.read_csv(outpath_ft2upload, header=None, names=('GlobID', 'lat', 'lon','acq_date','acq_time','peat','type1','reg','obl','rai','confidence', 'type', 'whouploaded', 'who','status_no', 'note','link','dupl','ident'))
	#loop through each of the rows and assign the attributes to variables
	for row in df.iterrows():
	  GlobID= row[1]['GlobID']
	  lat= row[1]['lat']
	  print(lat)
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
	 output_shp.save(outpath_ft_shp2upload)
	 command = "ogr2ogr -overwrite -skipfailures -f KML %s %s" % (outpath_ft_kml2upload,outpath_ft_shp2upload)
	 os.system(command)
	except:
	 print('err')
	 

#upload kml to drive
def upload_to_drive(outpath_ft_kml2upload):
   try:
     if os.path.isfile(outpath_ft_kml2upload):
        print(outpath_ft_kml2upload)
        file_metadata = {
           'name' : '%s'%(os.path.basename(outpath_ft_kml2upload)),
           'parents': [ folder_id ]}
        media=MediaFileUpload(outpath_ft_kml2upload,mimetype='text/csv', resumable=True)
        request = servicedrive.files().create(body=file_metadata, media_body=media, fields='id')
        nms =request.execute()
     else:
        print('File not exist')
   except:
        #time.sleep(60)
     print('An error occurs during the uploading file %s' % (outpath_ft_kml2upload))
#start remove duplicates from FT
def remove_dupl_ft():
	try:
		response = download_ft()
		data=pd.DataFrame(response[u'rows'],columns=response[u'columns'])
		data['ident']=data[u'Широта'].map(str)+ data[u'Долгота'].map(str)+ data[u'Дата съемки']
		data['Dupl']=data.duplicated([u'ident'])
		tmp=data.loc[data['Dupl'] == True]
		tmp1 = tmp.sort_values(by=[u'Номер точки',u'Дата съемки'], ascending=[False,False])
		rowids=tmp1[u'Номер точки'].tolist()
		for rowid in rowids:
			try:
				query = "DELETE FROM " + tableIdfin + " WHERE 'Номер точки' = " + str(rowid)
				delete_sel = service.query().sql(sql=query).execute()
			except:
				print('err')
				sleep=12
				time.sleep(sleep)
				continue	
	except:
		print('err1')
if __name__ == "__main__":
 while True:
   log=os.path.join(source_dir, 'log_rduplicates.txt')
   logf = open(log, 'a')
   #current date
   start=time.time()
   currtime = time.localtime()
   date=time.strftime('%d%m%Y',currtime)
   cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
   logf.write(cdate+'\n')
   #set filename
   filename = 'ft_%s' % (date)
   outpath_ft,outpath_ft2upload,outpath_ft_kml2upload,outpath_ft_shp2upload=set_outputs(filename)
   #build servises for uploading
   service = auth2FT()
   servicedrive = auth2drive()
   download_ft()
   remove_dupl_csv()
   csv2shp(outpath_ft2upload)
   upload_to_drive(outpath_ft_kml2upload)
   #remove_dupl_ft()
   end=time.time()
   sleep=86401-(end-start)
   logf.close()
   time.sleep(sleep)