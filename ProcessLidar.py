#!/usr/bin/env python
# coding: utf-8

# In[1]:


import geopandas as gpd
import pandas as pd
#import geoplot as gplt
import laspy
import shapely
import os
from datetime import datetime
import numpy as np
from itertools import product
import math
import pyproj
import re
import pyspark


# In[2]:


resolution = 100  #grid resolution in meters


# In[3]:


def lp(v):
    print(f"[{datetime.now()}] {v}")

lp("Starting...")


# In[4]:


lp("Creating spark context")
conf = pyspark.SparkConf().setAppName("ProcessLidar").setMaster("spark://ip-10-0-4-160.ec2.internal:7077")
sc = pyspark.SparkContext(conf=conf)


# In[5]:


dataPath = f"{os.curdir}{os.sep}datasets{os.sep}"
fullDataPath = os.path.realpath(dataPath) + os.sep
lp(f"Path:  {dataPath}\t\tFullPath:{fullDataPath}")


# In[6]:


testLasFileName = os.listdir(f"{dataPath}laz")[5]
lp(f"Opening {testLasFileName} to get crs and more")
testLas = laspy.read(f"{dataPath}laz{os.sep}{testLasFileName}")


# In[7]:


neighborhoodDf = gpd.read_file(f"{dataPath}Neighborhoods.geojson").to_crs(testLas.vlrs[0].parse_crs())


# In[8]:


neighborhoodDf.plot()


# In[9]:


bounds = [int(b) for b in neighborhoodDf.total_bounds]


# In[10]:


xPixels = (bounds[2] - bounds[0]) / resolution
yPixels = (bounds[3] - bounds[1]) / resolution
lp(f"Resolution will be {xPixels} x {yPixels}  Runtime based on {xPixels*yPixels}")


# In[11]:


boxes = [shapely.box(x,y,x+resolution,y+resolution) for x, y in product(range(bounds[0], bounds[2], resolution), range(bounds[1], bounds[3], resolution))]
boxesDf = gpd.GeoDataFrame(geometry=boxes).set_crs(neighborhoodDf.crs)
boxesDf = boxesDf[boxesDf.intersects(neighborhoodDf.union_all())].copy()
boxesDf['AltitudeTotal'] = np.nan
boxesDf['WaterTotal'] = np.nan
boxesDf['Total'] = np.nan
boxesDf['RunTime'] = datetime.now()-datetime.now()


# In[12]:


ax = neighborhoodDf.plot()
boxesDf.plot(edgecolor='red', color=None, ax=ax)


# In[13]:


tileIndex = gpd.read_file(f"{dataPath}USGS_LA_2021GNO_1_C22_TileIndex{os.sep}USGS_LA_2021GNO_1_C22_TileIndex.shp").to_crs(neighborhoodDf.crs)
tileIndex.index = tileIndex['Name'].map(lambda f: f"{fullDataPath}laz{os.sep}USGS_LPC_LA_2021GreaterNewOrleans_C22_{f}.laz")
tileIndexBroadcast = sc.broadcast(tileIndex)
tileIndexBroadcast.value.head()


# In[14]:


wgsToUTM = pyproj.transformer.Transformer.from_crs(crs_from=pyproj.CRS.from_string('WGS84'), crs_to=neighborhoodDf.crs)
meridianReg = re.compile(r'PARAMETER\["central_meridian",\-([0-9]{2})\]')


def processLas(inputTup):
    startTime = datetime.now()
    
    BoxIdx = inputTup[0]
    boxBounds = inputTup[1:]
    
    lp(f"Processing for box[{BoxIdx}] {boxBounds}")

    altTotal = 0
    waterTotal = 0
    pointTotal = 0
    for idx, row in tileIndexBroadcast.value.iterrows():
        if not row['geometry'].intersects(shapely.box(*boxBounds)):
            #no intersection scip
            continue

        try:        
            las = laspy.read(idx)
        except FileNotFoundError:
            lp(f"path {idx} does not exist skipping" )
            continue
        
        
        central_meridian = int(meridianReg.findall(las.header.vlrs[0].string)[0]) * -1
    
        
        X = las.X
        Y = las.Y
        Z = las.Z
        cls = las.classification
    
        groundMask = np.isin(cls, [2,9])
        inBoundsMaskX = np.logical_and(X >= (boxBounds[0]*1000), (X <= (boxBounds[2]*1000)))
        inBoundsMaskY = np.logical_and(Y >= (boxBounds[1]*1000), (Y <= (boxBounds[3]*1000)))
        goodPointMask = np.logical_and(groundMask,inBoundsMaskX,inBoundsMaskY)
        
        x = X[goodPointMask]/1000.0 #- bounds[0]
        y = (Y[goodPointMask]/1000.0 + wgsToUTM.transform(30,central_meridian)[1]) #- bounds[1]
 
        altTotal += int(Z[goodPointMask].sum())
        waterTotal += np.count_nonzero(cls[goodPointMask] == 9)
        pointTotal += np.count_nonzero(goodPointMask)

    return (BoxIdx,altTotal, waterTotal, pointTotal, datetime.now() - startTime)


# In[15]:


#Just for sanity
testIdx = 7
processLas(list(boxesDf.bounds.itertuples())[testIdx])


# In[16]:


boxesRdd = sc.parallelize(boxesDf.bounds.itertuples(), 5000)


# In[17]:


boxesProcessedRdd = boxesRdd.map(processLas)


# In[18]:


lp(f"{boxesDf.shape[0]} boxes total")


# In[19]:


output = boxesProcessedRdd.collect()


# In[20]:


boxesDf.columns


# In[21]:


output[0]


# In[22]:


for out in output:
    boxesDf.loc[out[0], 'AltitudeTotal'] = out[1]
    boxesDf.loc[out[0], 'WaterTotal'] = out[2]
    boxesDf.loc[out[0], 'Total'] = out[3]
    boxesDf.loc[out[0], 'RunTime'] = out[4]


# In[30]:


lp(f"{boxesDf['RunTime'].mean()} avg runtime\t\ttotal:  {boxesDf['RunTime'].sum()}")


# In[24]:


boxesDf.head()


# In[25]:


outPath = f"{os.curdir}{os.sep}output{os.sep}"
if not os.path.exists(outPath):
    lp(f"Creating output path {outPath}")
    os.makedirs(outPath)


# In[26]:


boxesDf.to_pickle(f"{outPath}AggregateLidarData_{resolution}m.pickle")


# In[27]:


with open(f"{outPath}FinishTime_{resolution}m.txt", 'w') as f:
    f.write(f"Finished shape {boxesDf.shape}  stop time {datetime.now()}")


# In[28]:


boxesDf['AltCalc'] = boxesDf['AltitudeTotal'] / boxesDf['Total']
boxesDf.loc[pd.isna(boxesDf['AltCalc']),'AltCalc'] = 0
boxesDf.plot(column='AltCalc')


# In[29]:


lp("Done!")


# In[ ]:




