__author__ = "Igor Glushkov (iglushko@greenpeace.org)"
__copyright__ = "ESRI"
__license__ = "Apache v.2"

import arcpy
from arcpy import env

arcpy.env.overwriteOutput = True

#Set by user
#path to working directory
env.workspace = r'd:\Thematic\Peatfires\Python\testdata'
outWorkspace = env.workspace

#path to fires data
fp = r'd:\Thematic\Peatfires\Python\testdata\source\fires.shp'
#path to polygon data
pol = r'd:\Thematic\Peatfires\Python\testdata\source\peats1.shp'
#name ID field -  unique name of polygon
field = "peat_id"

if arcpy.Exists("pointsLayer"):
	print "pointsLayer exists already"
else:
	arcpy.MakeFeatureLayer_management (fp, "pointsLayer")
	print "pointsLayer created"
if arcpy.Exists("polyLayer"):
	print "polyLayer exists already"
else:
	arcpy.MakeFeatureLayer_management (pol, "polyLayer")
	print "polyLayer created"

#select points in polygon
sel_fires=arcpy.SelectLayerByLocation_management('pointsLayer', "INTERSECT", 'polyLayer')
arcpy.MakeFeatureLayer_management (sel_fires, "sel_fires")
sel_peatlands=arcpy.SelectLayerByLocation_management('polyLayer', "INTERSECT", 'pointsLayer')
arcpy.MakeFeatureLayer_management (sel_peatlands, "sel_peatlands")

#Create a search cursor to step through the selected polygon features
cursor = arcpy.UpdateCursor(sel_peatlands)
for row in cursor:
	#print(row.getValue(field))
	featureName = row.getValue(field)
	#select each polygon
	print '"%s"=%s'%(field,featureName)
	tmp=arcpy.SelectLayerByAttribute_management('sel_peatlands', "NEW_SELECTION", '"%s"=%s'%(field,featureName))
	#arcpy.FeatureClassToFeatureClass_conversion(tmp , outWorkspace, str(featureName)+'selpol')		
	#select points inside slected polygon
	arcpy.MakeFeatureLayer_management (tmp, "sel_poly")
	f_in_poly=arcpy.SelectLayerByLocation_management('sel_fires', "INTERSECT", 'sel_poly')
	#arcpy.FeatureClassToFeatureClass_conversion(f_in_poly , outWorkspace, str(featureName)+'fires')
	#select earliest fires 
	arcpy.MakeFeatureLayer_management (f_in_poly, "fires"+str(featureName))
	#arcpy.FeatureClassToFeatureClass_conversion('fires' , outWorkspace, str(featureName)+'f_in_pol')	
	listdates = []
	cursor_new = arcpy.SearchCursor('fires'+str(featureName))
	#sum of fires
	counter=0
	for row1 in cursor_new:
		t=row1.getValue("ACQ_DATE")
		listdates.append(t)
		counter += 1	
	#select first date
	mindate = min(listdates)
	mindatestr = str(mindate)
	print mindatestr[0:10],counter
	#count
	row.setValue('MinDate', mindatestr)
	countstr=str(counter)
	cursor.updateRow(row)
	row.setValue('Count', countstr)
	cursor.updateRow(row)
	#where='"ACQ_DATE" = date\'' + mindatestr[0:10]+'\''
	#fmin=arcpy.SelectLayerByAttribute_management('fires'+str(featureName), "NEW_SELECTION", where)
	#save
	#arcpy.FeatureClassToFeatureClass_conversion(fmin , outWorkspace, str(featureName)+'minfire')


	
	
		

