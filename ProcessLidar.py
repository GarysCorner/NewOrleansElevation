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


# In[32]:


resolution = 10  #grid resolution in meters
outPath = f"{os.curdir}{os.sep}output{os.sep}"
outputFileName = f"{outPath}AggregateLidarData_SomeHoods_{resolution}m.pickle"
captureHoods = ['GENTILLY TERRACE','GENTILLY WOODS','MARIGNY','BYWATER','ST. CLAUDE','PONTCHARTRAIN PARK','DESIRE AREA',\
               'FRENCH QUARTER','ST. BERNARD AREA', 'ST. CLAUDE', 'ST. ROCH']


# In[3]:


if not os.path.exists(outPath):
    lp(f"Creating output path {outPath}")
    os.makedirs(outPath)


# In[4]:


def lp(v):
    print(f"[{datetime.now()}] {v}")

lp("Starting...")


# In[5]:


lp("Creating spark context")
conf = pyspark.SparkConf().setAppName("ProcessLidar").setMaster("spark://127.0.0.1:7077")
sc = pyspark.SparkContext(conf=conf)


# In[6]:


dataPath = f"{os.curdir}{os.sep}datasets{os.sep}"
fullDataPath = os.path.realpath(dataPath) + os.sep
lp(f"Path:  {dataPath}\t\tFullPath:{fullDataPath}")


# In[7]:


testLasFileName = os.listdir(f"{dataPath}laz")[5]
lp(f"Opening {testLasFileName} to get crs and more")
testLas = laspy.read(f"{dataPath}laz{os.sep}{testLasFileName}")


# In[8]:


neighborhoodDf = gpd.read_file(f"{dataPath}Neighborhoods.geojson").to_crs(testLas.vlrs[0].parse_crs())


# In[9]:


sorted(neighborhoodDf['gnocdc_lab'].unique())


# In[10]:


if len(captureHoods) > 0:
    captureHoodsMask = neighborhoodDf['gnocdc_lab'].isin(captureHoods)
else:
    captureHoodsMask = np.repeat(True, neighborhoodDf.shape[0])


# In[11]:


ax = neighborhoodDf.plot()
neighborhoodDf[captureHoodsMask].plot(ax=ax, color='red')


# In[12]:


bounds = [int(b) for b in neighborhoodDf[captureHoodsMask].total_bounds]


# In[13]:


xPixels = (bounds[2] - bounds[0]) / resolution
yPixels = (bounds[3] - bounds[1]) / resolution
lp(f"Resolution will be {xPixels} x {yPixels}  Runtime based on {xPixels*yPixels}")


# In[14]:


boxes = [shapely.box(x,y,x+resolution,y+resolution) for x, y in product(range(bounds[0], bounds[2], resolution), range(bounds[1], bounds[3], resolution))]
boxesDf = gpd.GeoDataFrame(geometry=boxes).set_crs(neighborhoodDf.crs)
boxesDf = boxesDf[boxesDf.intersects(neighborhoodDf[captureHoodsMask].union_all())].copy()
boxesDf['AltitudeTotal'] = np.nan
boxesDf['WaterTotal'] = np.nan
boxesDf['Total'] = np.nan
boxesDf['RunTime'] = datetime.now()-datetime.now()


# In[15]:


ax = neighborhoodDf.plot()
boxesDf.plot(edgecolor='red', color=None, ax=ax)


# In[16]:


tileIndex = gpd.read_file(f"{dataPath}USGS_LA_2021GNO_1_C22_TileIndex{os.sep}USGS_LA_2021GNO_1_C22_TileIndex.shp").to_crs(neighborhoodDf.crs)
tileIndex.index = tileIndex['Name'].map(lambda f: f"{fullDataPath}laz{os.sep}USGS_LPC_LA_2021GreaterNewOrleans_C22_{f}.laz")
tileIndexBroadcast = sc.broadcast(tileIndex)
tileIndexBroadcast.value.head()


# In[17]:


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


# In[18]:


#Just for sanity
testIdx = 7
processLas(list(boxesDf.bounds.itertuples())[testIdx])


# In[19]:


boxesRdd = sc.parallelize(boxesDf.bounds.itertuples(), 300)


# In[20]:


boxesProcessedRdd = boxesRdd.map(processLas)


# In[21]:


lp(f"{boxesDf.shape[0]} boxes total")


# In[22]:


output = boxesProcessedRdd.collect()


# In[23]:


boxesDf.columns


# In[24]:


output[0]


# In[25]:


for out in output:
    boxesDf.loc[out[0], 'AltitudeTotal'] = out[1]
    boxesDf.loc[out[0], 'WaterTotal'] = out[2]
    boxesDf.loc[out[0], 'Total'] = out[3]
    boxesDf.loc[out[0], 'RunTime'] = out[4]


# In[26]:


lp(f"{boxesDf['RunTime'].mean()} avg runtime\t\ttotal:  {boxesDf['RunTime'].sum()}")


# In[27]:


boxesDf.head()


# In[28]:


boxesDf.to_pickle(outputFileName)


# In[29]:


with open(f"{outPath}FinishTime_{resolution}m.txt", 'w') as f:
    f.write(f"Finished shape {boxesDf.shape}  stop time {datetime.now()}")


# In[34]:


boxesDf['AltCalc'] = boxesDf['AltitudeTotal'] / boxesDf['Total']
boxesDf.loc[pd.isna(boxesDf['AltCalc']),'AltCalc'] = 0
ax = neighborhoodDf.plot(color='pink')
boxesDf.plot(column='AltCalc',ax=ax)


# In[31]:


lp("Done!")


# In[ ]:




