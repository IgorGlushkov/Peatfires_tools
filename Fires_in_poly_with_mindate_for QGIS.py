##poly_layer=vector
##point_layer=vector
##count_field=string  pcount
##datefield=field point_layer
##pidfield=field poly_layer
##tmp_dir=folder
##Result=output vector

from qgis.core import *
from PyQt4.QtCore import *
from PyQt4.QtCore import QDate
import os,glob


#set input
polylayer = processing.getObject(poly_layer)
player = processing.getObject(point_layer)
#add new field count
points={feature.id(): feature for feature in processing.features(player)}
provider = polylayer.dataProvider()
provider.addAttributes([QgsField(count_field, QVariant.Int)])
polylayer.updateFields()
polylayer.startEditing()
#get field indexes
new_field_index = polylayer.fieldNameIndex(count_field)
date_field_index = player.fieldNameIndex(datefield)
poly_field_index=polylayer.fieldNameIndex(pidfield)
#add 0 as attribute
for feature in processing.features(polylayer):
    polylayer.changeAttributeValue(feature.id(), new_field_index,0)
polylayer.commitChanges()
#get all attr to array
polygons =[feature for feature in processing.features(polylayer)]
# create tmp layer for points within
p_tmp = QgsVectorLayer("Point", "temporary_points", "memory")
pt = p_tmp .dataProvider()
# changes are only possible when editing the layer
p_tmp.startEditing()
# add fields
pt.addAttributes([QgsField("date", QVariant.String),QgsField("polyid", QVariant.String)])
# Build the spatial index for faster lookup.
index = QgsSpatialIndex()
for p in processing.features(polylayer):
  index.insertFeature(p)

#intersect
def intersectwithindex():
    # Loop each feature in the layer again and get only the features that are going to touch.
    for point in processing.features(player):
        pnt = point.geometry()
        for id in index.intersects(pnt.boundingBox()):
            selpolygon =polygons[id]
            if pnt.within(selpolygon.geometry()):
               attr= [f.attributes() for f in processing.features(polylayer)]
               attr_sum=[f[new_field_index] for f in  attr]
               val=attr_sum[id]
               val+=1
               polylayer.startEditing()
               #add count to new filed
               polylayer.changeAttributeValue(selpolygon.id(), new_field_index, val)
               polylayer.commitChanges()
               attrp= [point.attributes()]
               attrpol= [selpolygon.attributes()]
               attr_id=[f[poly_field_index] for f in  attrpol]
               attr_date=[p[date_field_index] for p in  attrp]
               date=attr_date[0].toString('yyyy-MM-dd')
               #add date and polygon id to tmp layer
               point.setAttributes([str(date),str(attr_id[0])])
               pt.addFeatures([point])
               p_tmp.commitChanges()
               p_tmp.updateExtents()
               logf.write(str(date)+'_'+str( date)+'_'+str(attr_id[0])+'\n')
            
#select min date from tmp layer
def selectmindate():
    attr= [f.attributes() for f in processing.features(p_tmp)]
    datelist={}
    for f in attr:
        id=f[1]
        date=f[0]
        if id in datelist:
            datelist[id].append(date)
        else:
            datelist[id]=[date]
    for k,v in datelist.iteritems():
        mindate=min(v)
        # build the output layer path
        layer_path = os.path.join(tmp_dir, str(k) + '.shp')
        layer_path1 = os.path.join(tmp_dir, str(k)+'_'+str(mindate) + '.shp')
        # run the extract by attribute tool       
        logf.write( k+'_'+mindate+'\n')
        try:
            out1=processing.runalg('qgis:extractbyattribute', p_tmp,"polyid", 0, k, layer_path)
            out2=processing.runalg('qgis:extractbyattribute', layer_path,"date", 0, mindate, layer_path1)
        except:
            logf.write('Smthg wrong1')
 
def join_output():
    commandlist=[]
    driver='ESRI Shapefile'
    tmpfiles=glob.glob(os.path.join(tmp_dir, '*-*.shp'))
    for tmp in tmpfiles:
        if commandlist == []:
            command='ogr2ogr -overwrite -f \"%s\" %s %s' % (driver, Result,tmp)
            res_name = os.path.basename(Result)
            command1='ogr2ogr -overwrite -f \"%s\" -update -append  %s %s -nln %s' % (driver,Result, tmp,res_name.split('.')[0])
            commandlist.append(command)
            commandlist.append(command1)
        else:
            res_name = os.path.basename(Result)
            command='ogr2ogr -overwrite -f \"%s\" -update -append  %s %s -nln %s' % (driver,Result,tmp,res_name.split('.')[0])
            commandlist.append(command)
    return commandlist        
            
def remove_tmp():
            #remove tmpfiles
            output = [shp for shp in glob.glob(tmp_dir+ '/'+"*")]
            for out in output:
                os.remove(out)
                
log=os.path.join(tmp_dir, "log.txt")
logf = open(log, 'w')
#logf.write(str(poly_field_index)+'_'+str(date_field_index)+'_'+str(new_field_index)+'\n')
try:
    intersectwithindex()
except:
    logf.write('Smthg wrong')
    logf.close()   
QgsMapLayerRegistry.instance().addMapLayer(p_tmp)
try:
    selectmindate()
except:
    logf.write('Smthg wrong')
    logf.close()

try:
    commandlist=join_output()
except:
    logf.write('Smthg wrong')
    logf.close()

for command in commandlist:
    os.system(command)
    logf.write(command+'\n')
try:
    remove_tmp()
except:
    logf.write('Smthg wrong')
    logf.close()
logf.close()
