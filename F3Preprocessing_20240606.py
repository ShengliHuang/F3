#This code notes on 20230920:
# a) In each tile, F3 data have to be organized in a logical and consistent way, including data format, file name, projection, resolution etc.
# b) For each remote sensing year, the code prepare all the F3 required data, including remote sensing, cloudshadow, non-RS, project extent. Note the tiles are defined in the AreaExtentShape = r'D:\f3app\GeeFastEmapTiles.shp'  
# c) Many pre-processed rasters are from Google Earth Engine, including the remote sensing classification and segmentation

from osgeo import gdal, ogr, osr   
import glob
import numpy as np
import numpy.ma as ma
import os, sys, traceback, datetime, time
import sqlite3
import geopandas as gpd
import shutil
import shapefile
import multiprocessing
import subprocess
from pathlib import *
from scipy.ndimage import label, generate_binary_structure    #for segmentation, see https://docs.scipy.org/doc/scipy/reference/generated/scipy.ndimage.label.html 
from google.cloud import storage   #from gcloud import storage does not work, see see https://stackoverflow.com/questions/37003862/how-to-upload-a-file-to-google-cloud-storage-on-python-3
from google.cloud.storage import constants
import pyproj  #20231229 added for converting lat/long pairs to Albers Equal Area
import requests
from zipfile import ZipFile


####print("Can I define a function after it is called? the answer is no! In Python, functions must be defined before they are called")
####
####print("We may use paralle in the future: on year and run")
####
####print("Jamie use EPSG:5070 NAD83 / Conus Albers as their projection, which is the same as so called NAD_1983_Contiguous_USA_Albers. Mine is 4326 from GEE? I think we need to unify")
####print("Note NAD_1983_Contiguous_USA_Albers is different from NAD_1983 [2011]_Contiguous_USA_Albers")
#####EPSG 4326 is a geographic coordinate system that defines a location on a 3D model of the world using latitude and longitude values. It is also known as WGS84 and is used by GPS, Google Earth, and some US government agencies 12. EPSG 5070, on the other hand, is a projected coordinate system that is used for the conterminous United States (CONUS) Albers Equal Area projection 3. The difference between these two coordinate systems is that EPSG 4326 is a geographic coordinate system, while EPSG 5070 is a projected coordinate system. Geographic coordinate systems use latitude and longitude values to define locations on the earth’s surface, while projected coordinate systems use x and y coordinates to define locations on a flat surface
####
####
####print("we may have the error IDLE internal error in runcode(), see https://stackoverflow.com/questions/22851723/errno-10053-with-python-idle")
####print("solution: a) do not use IDLE from arcpy clone IDLE; b) use [eidt with IDLE ArcGIS pro] to open the py file")
####print("!!!!!!!!!!!!!!!!!!! All geospatial datasets must be int he same projection (Albers Equal Area NAD83)!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
####
#####Encryot and Decrypt may be used here by copying the function and see where to use it.
#####https://automating-gis-processes.github.io/CSC18/lessons/L6/reading-raster.html is worthy to read
####
#####!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#####https://stackoverflow.com/questions/7169845/using-python-how-can-i-access-a-shared-folder-on-windows-network
#####!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
####
####
####
######python gdal to extract the values for points from underlying rasters
########DEM and others are here: X:\GEE_Mosaics\30m\L48\singleband_mosaics_cog - X is the Fhaast1 drive
########the only wrinkle is that nodata is 32767 and NOT -32768. Just something to be aware of!
######from osgeo import gdal, ogr
######
####### Open the raster file in read mode
######raster = gdal.Open('raster.tif', gdal.GA_ReadOnly)
######
####### Open the point shapefile in read mode
######point_shapefile = ogr.Open('points.shp', 0)
######point_layer = point_shapefile.GetLayer()
######
####### Loop through each point in the shapefile
######for point_feature in point_layer:
######    # Get the geometry of the point
######    point_geometry = point_feature.GetGeometryRef()
######
######    # Get the x and y coordinates of the point
######    x = point_geometry.GetX()
######    y = point_geometry.GetY()
######
######    # Convert the x and y coordinates to pixel coordinates
######    x_offset, y_offset = gdal.ApplyGeoTransform(raster.GetGeoTransform(), x, y)
######    px = int(x_offset)
######    py = int(y_offset)
######
######    # Read the pixel value at the given coordinates
######    band = raster.GetRasterBand(1)
######    pixel_value = band.ReadAsArray(px, py, 1, 1)[0][0]
######
######    # Do something with the pixel value
######    print(pixel_value)
####
####
####


        
##
##        for TileFolder in ["AdditionalContinuousRaster","AdditionalDiscreteRaster","CommonShare","FieldPoint","InputZone","Intermediate","NonRSraster","Results","ResultsAnalysis","RSraster"]:
##            F3folder = F3PreprocessingInputPath + os.sep + TileFolder
##            print("F3folder=",F3folder)
##            if not os.path.exists(F3folder):
##                os.makedirs(F3folder)
##
##        #Added on 20230501: Download data from google bucket---start
##        InputRasterImages = RSgeoraster + NonRSgeoraster + InputZonegeoraster + AdditionalDiscretegoeraster + AdditionalContinuousgeoraster #This list the input geospatial rasters as input of F3
##        for InputRasterImage in InputRasterImages:
##            print("\n\nInputRasterImage=",InputRasterImage)
##            if not os.path.exists(InputRasterImage):
##                SourceFileInBucket = "gs://"+SourceBucketName+'/f3geerawdata/RemoteSensingYear'+str(Year)+'/'+Tile+'/'+InputRasterImage.split(os.sep)[-1]
##                InputRasterImage = DownloadDataFromGoogleBucketToLocalDirectory(SourceFileInBucket, InputRasterImage)                
##        #Added on 20230501: Download data from google bucket---end
##

##def DownloadDataFromGoogleBucketToLocalDirectory(SourceFileInBucket, InputRasterImage):
##    #20230428: Based on previous work(N:\project\f3_vol2\F3ArcPro2p5Python3Since20200724OnlyChangeByShengli\GCP_CloudBucket_20230420.py), using the gsuti to transfer the data between google bucket and local computer (can be down in two ways but here only one way. Note google SDK needs to be installed first)----start
##    print("20231215: more information about copying bucket, see https://stackoverflow.com/questions/31049535/how-can-i-move-data-directly-from-one-google-cloud-storage-project-to-another")
##    print("20231215: I guess SDK in initialized with fhaast-general-project, so the code knows which project to find the bucket")
##    gsutilCommand1AAA = "gsutil cp  " + SourceFileInBucket+" "+InputRasterImage    #This is to transfer the individual specific file to the working directory
##    os.system(gsutilCommand1AAA)  #This will run the gsutilCommand to download the data from google cloud bucket
##    print("gsutilCommand1AAA is: ",gsutilCommand1AAA)
##    print(TargetFile," was originally downloaded from ",SourceFileInBucket)
##    #gsutilCommand1BBB ="gsutil cp gs://"+SourceBucketName+'/*' + ' .'   #This is to transfer all files (excluding subdirectory) to the working directory, which may be useful in the future
##    #20230428: Based on previous work(N:\project\f3_vol2\F3ArcPro2p5Python3Since20200724OnlyChangeByShengli\GCP_CloudBucket_20230420.py), using the gsuti to transfer the data between google bucket and local computer (can be down in two ways but here only one way. Note google SDK needs to be installed first)----en      
##    return InputRasterImage


SourceBucketName = "fhaastf3"
RemoteSensingYearOfInterest = [2023] #[2005,2010,2015,2020,2023]
Tiles = ["MichiganTile3"] #["MichiganTile1","MichiganTile2","MichiganTile3","MichiganTile4","MichiganTile5"]#,"MichiganTile3"]   #This can be automatically but I prefer manual input
EverywherePath = r'F:\CUI\fhaastf3app\F3DataEveryWhere'
fhaastf3tilesShape =  EverywherePath + os.sep + 'fhaastf3tiles.shp'   #We will always use this shape file which will be frequently updated
F3PreprocessingInputOverallPath = r'U:\f3geepreprocessing' #r'\\afssxgtacnas070\FHAAST1\f3geepreprocessing' needs test
F3NoDataValue = -9999
F3ValidFillValue = -8888
F3Resolution = 30.0 #in meters
YearsOfAnnualChange = range(1989, 2022) 



def LandCoverChangeAnnualRasters(mylock,Year, Tile):
    HistoricalAndActualAnnualProductsPath = F3PreprocessingInputOverallPath + os.sep + 'HistoricalAndActualAnnualProducts'  #no \ at the end
    if not os.path.exists(HistoricalAndActualAnnualProductsPath):
        os.makedirs(HistoricalAndActualAnnualProductsPath)
    LandCoverChangeAnnualRastersPath = F3PreprocessingInputOverallPath + os.sep + 'LandCoverChangeAnnualRasters'  #no \ at the end
    if not os.path.exists(LandCoverChangeAnnualRastersPath):
        os.makedirs(LandCoverChangeAnnualRastersPath)
    LandCoverTile = LandCoverChangeAnnualRastersPath + os.sep + Tile
    if not os.path.exists(LandCoverTile):
        os.makedirs(LandCoverTile)
        
    for m in YearsOfAnnualChange:
        ChangeFromSource = F3PreprocessingInputOverallPath + os.sep + "RemoteSensingYear" + str(Year) + os.sep + "GeeRawData" + os.sep + Tile + os.sep + "ChangeFrom"+str(m)+"0101To"+str(m)+"1230.tif"
        ChangeFromTarget = LandCoverChangeAnnualRastersPath + os.sep + Tile + os.sep + "ChangeFrom"+str(m)+"0101To"+str(m)+"1230.tif"
        if not os.path.exists(ChangeFromTarget):
            shutil.copy(ChangeFromSource, ChangeFromTarget) 
    for n in YearsOfAnnualChange:
        FastLossProbFromSource = F3PreprocessingInputOverallPath + os.sep + "RemoteSensingYear" + str(Year) + os.sep + "GeeRawData" + os.sep + Tile + os.sep + "FastLossProbFrom"+str(n)+"0101To"+str(n)+"1230.tif"
        FastLossProbFromTarget = LandCoverChangeAnnualRastersPath + os.sep + Tile + os.sep + "FastLossProbFrom"+str(n)+"0101To"+str(n)+"1230.tif"
        if not os.path.exists(FastLossProbFromTarget):
            shutil.copy(FastLossProbFromSource, FastLossProbFromTarget)
    Message = "LandCoverChangeAnnualRasters finished"
    return Message
    
def F3GeeAndPreprocessing(mylock,Year, Tile):
    try:
        LandCoverChangeAnnualRastersMessage = LandCoverChangeAnnualRasters(mylock,Year, Tile)
        F3PreprocessingOutputPathRSY = F3PreprocessingInputOverallPath + os.sep + 'RemoteSensingYear'+str(Year)  #no \ at the end
        if not os.path.exists(F3PreprocessingOutputPathRSY):
            os.makedirs(F3PreprocessingOutputPathRSY)
        F3Log = F3PreprocessingOutputPathRSY+os.sep+"F3processingLog"+str(Year)+".txt"
        F3PreprocessingInputPath = F3PreprocessingOutputPathRSY + os.sep + "GeeRawData" + os.sep + Tile 
        if not os.path.exists(F3PreprocessingInputPath):
            os.makedirs(F3PreprocessingInputPath)
        F3PreprocessingInputPathRepresent = F3PreprocessingOutputPathRSY + os.sep + "Landsat_"+str(Year)+"_PixelLabel.tif"  #Use it as a indicator to avoid gsutil cp process if possible
        if not os.path.exists(F3PreprocessingInputPathRepresent):
            gsutilCommand ="gsutil cp -n gs://"+SourceBucketName+'/f3geerawdata/RemoteSensingYear'+str(Year)+'/'+Tile+'/*' + ' '+F3PreprocessingInputPath   #This is to transfer all files (excluding subdirectory) to the working directory, which may be useful in the future
            print(gsutilCommand," please note -n means no overwriting, * means everything, and the last is the working directory where the downloaded data are stored")
            os.system(gsutilCommand)

        ################################################################################################################

        RSgeoraster = [
            F3PreprocessingInputPath + os.sep + 'Landsat_'+str(Year)+'_red.tif',
            F3PreprocessingInputPath + os.sep + 'Landsat_'+str(Year)+'_nir.tif',  #I am not sure why f3gee_ was added before the file when they are downloaded from google bucket
            F3PreprocessingInputPath + os.sep + 'Landsat_'+str(Year)+'_swir1.tif',
            F3PreprocessingInputPath + os.sep + 'Landsat_'+str(Year)+'_PixelLabel.tif'
            ]   #no , at the end 
        NonRSgeoraster = [
            F3PreprocessingInputPath + os.sep + 'Annualmeantemperature.tif',  
            #F3PreprocessingInputPath + os.sep + 'Meandiurnalrange.tif',
            #F3PreprocessingInputPath + os.sep + 'Isothermality.tif',
            #F3PreprocessingInputPath + os.sep + 'Temperatureseasonality.tif',
            #F3PreprocessingInputPath + os.sep + 'Maxtemperatureofwarmestmonth.tif',
            #F3PreprocessingInputPath + os.sep + 'Mintemperatureofcoldestmonth.tif',
            #F3PreprocessingInputPath + os.sep + 'Temperatureannualrange.tif',
            #F3PreprocessingInputPath + os.sep + 'Meantemperatureofwettestquarter.tif',
            #F3PreprocessingInputPath + os.sep + 'Meantemperatureofdriestquarter.tif',
            F3PreprocessingInputPath + os.sep + 'Meantemperatureofwarmestquarter.tif',
            #F3PreprocessingInputPath + os.sep + 'Meantemperatureofcoldestquarter.tif',
            F3PreprocessingInputPath + os.sep + 'Annualprecipitation.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationofwettestmonth.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationofdriestmonth.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationseasonality.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationofwettestquarter.tif',
            F3PreprocessingInputPath + os.sep + 'Precipitationofdriestquarter.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationofwarmestquarter.tif',
            #F3PreprocessingInputPath + os.sep + 'Precipitationofcoldestquarter.tif'    
            ]   #no , at the end


        print("This folder InputZonegeoraster containes segmentation [must order from low snic value to high (i.e, from fine to coarse)]")
        print("This folder InputZonegeoraster containes clasification [must order from high classification value to low (i.e, from fine to coarse)]")
        InputZonegeoraster = [
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level1_ClusterKmeans18.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level2_ClusterKmeans16.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level3_ClusterKmeans14.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level4_ClusterKmeans12.tif',    
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level5_ClusterKmeans10.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level1_ClusterLVQ19.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level2_ClusterLVQ17.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level3_ClusterLVQ15.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level4_ClusterLVQ13.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level5_ClusterLVQ11.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level1_ClusterCascade17.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level2_ClusterCascade15.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level3_ClusterCascade13.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level4_ClusterCascade11.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level5_ClusterCascade9.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level1_Snic40.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level2_Snic70.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level3_Snic100.tif',    
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level4_Snic130.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level5_Snic400.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_Y'+str(Year)+'_Level6_Snic40_300m.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level1_Snic32.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level2_Snic67.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level3_Snic95.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level4_Snic135.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level5_Snic425.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_Y'+str(Year)+'_Level6_Snic32_300m.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level1_Snic48.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level2_Snic78.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level3_Snic108.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level4_Snic140.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level5_Snic450.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_Y'+str(Year)+'_Level6_Snic48_300m.tif'
            ]   #no , at the end 
        AdditionalDiscretegoeraster = [
            F3PreprocessingInputPath + os.sep + 'LandfireBPS.tif',
            F3PreprocessingInputPath + os.sep + 'Run1_BioClimateZone.tif',
            F3PreprocessingInputPath + os.sep + 'Run2_BioClimateZone.tif',
            F3PreprocessingInputPath + os.sep + 'Run3_BioClimateZone.tif',
            F3PreprocessingInputPath + os.sep + 'USCropLayer'+str(Year)+'0101to'+str(Year)+'1231.tif',  #20240306 added 
            F3PreprocessingInputPath + os.sep + 'LandCover'+str(Year)+'0101to'+str(Year)+'1231.tif',    #20240306 added
            F3PreprocessingInputPath + os.sep + 'Run1_PhenologyKmeans9.tif',  #20240510 added
            F3PreprocessingInputPath + os.sep + 'Run2_PhenologyKmeans11.tif',  #20240510 added
            F3PreprocessingInputPath + os.sep + 'Run3_PhenologyKmeans13.tif'  #20240510 added
            ]
        AdditionalContinuousgeoraster = [F3PreprocessingInputPath + os.sep + 'Dem30m.tif'
                                         #F3PreprocessingInputPath + os.sep + 'L_PalSARdBHH20190101to20200101.tif',
                                         #F3PreprocessingInputPath + os.sep + 'L_PalSARdBHV20190101to20200101.tif'
                                         ]

        #This part is to create the cloudwatershadowetc mask data by taking all tiles as a whole. Added on 20230531---start
        F3VoidMaskName = RSgeoraster[0].replace("red.tif","CloudShadowWaterSnow.tif")
        MyRaster = gdal.Open(RSgeoraster[0])
        Rows = MyRaster.RasterYSize
        Columns = MyRaster.RasterXSize
        F3InvalidNegaive9999 = np.ones([Rows, Columns])  #assign 1 first, and then we will assign -9999 to those invalid pixels
        for InputImage in RSgeoraster:
            Band = InputImage
            BandRaster = gdal.Open(Band, gdal.GA_ReadOnly)
            WeUseThisTrasnform = BandRaster.GetGeoTransform()
            WeUseThisProjection = BandRaster.GetProjectionRef()
            BandNoDataValue = BandRaster.GetRasterBand(1).GetNoDataValue()
            BandArray = BandRaster.GetRasterBand(1).ReadAsArray().astype('int32')  
            F3InvalidNegaive9999[(BandArray == BandNoDataValue)] = F3NoDataValue
            if "PixelLabel" in InputImage:
                F3InvalidNegaive9999[(BandArray > 0)] = F3NoDataValue  #"In the final PixelLabel.tif from GEE , water is 1, Perennial/permanent Ice/Snow is 2, Barren-Rock/Sand/Clay is 3, Landsat cloud/shadow/snow is 4, remaining value is 0"
            else:   
                F3InvalidNegaive9999[(BandArray == 0)] = F3NoDataValue  #20220906. I found they are all zero. Because there is no negative values in Band, so =0 (i.e. it is not AND but OR for the Green, Red, and NIR) is here 
            BandRaster = None
        driver = gdal.GetDriverByName("GTiff")
        outdata = driver.Create(F3VoidMaskName, Columns, Rows, 1, gdal.GDT_Int32, options=['COMPRESS=LZW'])  #see datatype at https://gis.stackexchange.com/questions/268898/using-signed-bytes-with-gdal
        outdata.SetGeoTransform(WeUseThisTrasnform)##sets same geotransform as input
        outdata.SetProjection(WeUseThisProjection)##sets same projection as input
        outdata.GetRasterBand(1).WriteArray(F3InvalidNegaive9999)
        outdata.GetRasterBand(1).SetNoDataValue(F3NoDataValue)  ##if you want these values transparent
        outdata.FlushCache() ##saves to disk!!
        outdata = None
        print(F3VoidMaskName, " is created")
        #This part is to create the cloudwatershadowetc mask data by taking all tiles as a whole. Added on 20230531---end


        #This part is to create the Forest1Nonforest0 mask data by taking all tiles as a whole. Added on 20240311---start
        Forest1Nonforest0Name = F3PreprocessingInputPath + os.sep + 'Forest1Nonforest0.tif'
        LCMSlandcover = F3PreprocessingInputPath + os.sep + 'LandCover'+str(Year)+'0101to'+str(Year)+'1231.tif'  #https://developers.google.com/earth-engine/datasets/catalog/USFS_GTAC_LCMS_v2022-8?hl=en
        UScroplayer = F3PreprocessingInputPath + os.sep + 'USCropLayer'+str(Year)+'0101to'+str(Year)+'1231.tif'  #https://developers.google.com/earth-engine/datasets/catalog/USDA_NASS_CDL
        LCMSlandcoverRaster = gdal.Open(LCMSlandcover)
        LCMSlandcoverArray = LCMSlandcoverRaster.GetRasterBand(1).ReadAsArray().astype('int16')
        print("LCMSlandcoverArray min and max = ",np.nanmin(LCMSlandcoverArray),np.nanmax(LCMSlandcoverArray))
        WeUseThisTrasnform = LCMSlandcoverRaster.GetGeoTransform()
        WeUseThisProjection = LCMSlandcoverRaster.GetProjectionRef()
        Rows = LCMSlandcoverRaster.RasterYSize
        Columns = LCMSlandcoverRaster.RasterXSize        
        UScroplayerRaster = gdal.Open(UScroplayer)
        UScroplayerArray = UScroplayerRaster.GetRasterBand(1).ReadAsArray().astype('int16')
        print("UScroplayerArray min and max = ",np.nanmin(UScroplayerArray),np.nanmax(UScroplayerArray))
        Forest1Nonforest0Array = np.full(LCMSlandcoverArray.shape, F3NoDataValue)  #assign 0 first, and then we will assign 1 to those forested pixels
        Forest1Nonforest0Array[LCMSlandcoverArray <= 9] = 1  #1 Trees,2 Tall Shrubs & Trees Mix (SEAK Only),3 Shrubs & Trees Mix, 4 Grass/Forb/Herb & Trees Mix, 5 Barren & Trees Mix, 6 Tall Shrubs (SEAK Only), 7 Shrubs, 8 Grass/Forb/Herb & Shrubs Mix, 9 Barren & Shrubs Mix 10 Grass/Forb/Herb, 11 Barren & Grass/Forb/Herb Mix, 12 Barren or Impervious, 13 Snow or Ice, 14 Water, 15 Non-Processing Area Mask
        Forest1Nonforest0Array[(UScroplayerArray >= 63) & (UScroplayerArray <= 81)] = 1
        Forest1Nonforest0Array[(UScroplayerArray >= 141) & (UScroplayerArray <= 152)] = 1
        Forest1Nonforest0Array[UScroplayerArray >= 190] = 1
        print("Forest1Nonforest0Array min and max = ",np.nanmin(Forest1Nonforest0Array),np.nanmax(Forest1Nonforest0Array))
        driver = gdal.GetDriverByName("GTiff")
        outdata = driver.Create(Forest1Nonforest0Name, Columns, Rows, 1, gdal.GDT_Byte, options=['COMPRESS=LZW'])  #see datatype at https://gis.stackexchange.com/questions/268898/using-signed-bytes-with-gdal
        outdata.SetGeoTransform(WeUseThisTrasnform)##sets same geotransform as input
        outdata.SetProjection(WeUseThisProjection)##sets same projection as input
        outdata.GetRasterBand(1).WriteArray(Forest1Nonforest0Array)
        outdata.GetRasterBand(1).SetNoDataValue(F3NoDataValue)  ##if you want these values transparent
        outdata.FlushCache() ##saves to disk!!
        outdata = None
        print(Forest1Nonforest0Name, " is created")
        AdditionalDiscretegoeraster.append(Forest1Nonforest0Name)
        print("AdditionalDiscretegoeraster=",AdditionalDiscretegoeraster)
        #This part is to create the Forest1Nonforest0 mask data by taking all tiles as a whole. Added on 20240311---END

        

        #This part is to remove raster ROI smaller than a provided threshold size (in pixels) and replaces them with the pixel value of the largest neighbour polyggon---start
        for InputTif in InputZonegeoraster:
            print("We remove small isolated area for ", InputTif)
            #https://www.mankier.com/1/gdal_sieve#:~:text=Description.%20gdal_sieve.py%20script%20removes%20raster%20polygons%20smaller%20than,that%20floating%20point%20values%20are%20rounded%20to%20integers.
            #https://stackoverflow.com/questions/54887745/gdal-sieve-filter-in-python
            InputTifOriginal = InputTif.replace(".tif","_original.tif")
            if os.path.exists(InputTifOriginal):
                print(InputTifOriginal," already exists, which means the image was already processed, so no need to process again")
                continue
            else:
                shutil.copy(InputTif, InputTifOriginal)  #make a copy to keep original one
                Image = gdal.Open(InputTif, 1)  # open image in read-write mode
                Band = Image.GetRasterBand(1)
                if (("Snic" in InputTif) and ("Level1" in InputTif)):
                    SmallestPixelNumber = 300
                if (("Snic" in InputTif) and ("Level2" in InputTif)):
                    SmallestPixelNumber = 500
                if (("Snic" in InputTif) and ("Level3" in InputTif)):
                    SmallestPixelNumber = 1000
                if (("Snic" in InputTif) and ("Level4" in InputTif)):
                    SmallestPixelNumber = 2000
                if (("Snic" in InputTif) and ("Level5" in InputTif)):
                    SmallestPixelNumber = 10000  #SmallestPixelNumber = 3000
                if (("Snic" in InputTif) and ("Level6" in InputTif)):
                    SmallestPixelNumber = 300  #Because level6 resolution is at 300m instead of 30m, the SmallestPixelNumber is setup the same as Level1
                if (("Cluster" in InputTif) and ("Level1" in InputTif)):
                    SmallestPixelNumber = 5
                if (("Cluster" in InputTif) and ("Level2" in InputTif)):
                    SmallestPixelNumber = 7 
                if (("Cluster" in InputTif) and ("Level3" in InputTif)):
                    SmallestPixelNumber = 9
                if (("Cluster" in InputTif) and ("Level4" in InputTif)):
                    SmallestPixelNumber = 11         
                if (("Cluster" in InputTif) and ("Level5" in InputTif)):
                    SmallestPixelNumber = 13  
                gdal.SieveFilter(srcBand=Band, maskBand=None, dstBand=Band, threshold=SmallestPixelNumber, connectedness=8, callback=gdal.TermProgress_nocb)  #hMaskBand – an optional mask band. All pixels in the mask band with a value other than zero will be considered suitable for inclusion in polygons. see https://gdal.org/api/gdal_alg.html#_CPPv415GDALSieveFilter15GDALRasterBandH15GDALRasterBandH15GDALRasterBandHiiPPc16GDALProgressFuncPv
                del Image, Band  # close the datasets.
                print("gdal.SieveFilter finished for ",InputTif) 
        #This part is to remove raster ROI smaller than a provided threshold size (in pixels) and replaces them with the pixel value of the largest neighbour polyggon---end


        #This part is to combine SNIC and Kemans to create InputZoneRaster, added on 20221121---start
        print("This section is based on entire segmentation and classification (i.e., not tile based), so the image may be too large. FYI")
        NewInputZonegeoraster = []
        Level6Snic = []  #added on 20230201
        for run in ["Run1","Run2","Run3"]: 
            RunInputZone = F3PreprocessingInputPath
            print("RunInputZone=",RunInputZone)
            InputZonegeorasterRun = glob.glob(RunInputZone+os.sep+run+"_Y"+str(Year)+"*.tif")  #Revised on 20230502, original one is InputZonegeorasterRun = glob.glob(RunInputZone+os.sep+run+"*.tif")
            print("\nInputZonegeorasterRun=",InputZonegeorasterRun)

            #Added on 20230201 to process level SNIC segmentation----start
            for k in InputZonegeorasterRun:
                if (("Level6" in k) and ("_300m.tif" in k) and ("Snic" in k) and (run in k) and ("_original" not in k)):
                    for j in k.split("_"):
                        if "Snic" in j:
                            SnicDigit = j.replace("Snic","S")
                    segments_fn = RunInputZone + os.sep + run+'_Y'+str(Year)+"_"+"L6_"+SnicDigit+".tif"
                    if not os.path.exists(segments_fn):
                       shutil.move(k, segments_fn)  #https://stackoverflow.com/questions/2491222/how-to-rename-a-file-using-python
                    Level6Snic.append(segments_fn)   #20231002: Move this sentence forward by delete the indent
            print("*************Level6Snic=",Level6Snic)
            #Added on 20230201 to process level SNIC segmentation----end   
            
            Segmentation = [k for k in InputZonegeorasterRun if (("Snic" in k) and (run in k) and ("_original" not in k))]
            print("\nSegmentation=",Segmentation)
            SegmentationDigit = [int(''.join(filter(str.isdigit, a.replace(".tif","").split(os.sep)[-1].split("_")[-1]))) for a in Segmentation]  #https://stackoverflow.com/questions/28526367/get-only-numbers-from-string-in-python
            SegmentationDigit.sort(reverse=False)  #reverse=True will sort the list descending. Default is reverse=False, which is ascending
            print("SegmentationDigit=",SegmentationDigit)
            SegmentationFromFineToCoarse = []
            for m in SegmentationDigit:
                for n in Segmentation:
                    SNICPointTif = "Snic"+str(m)+".tif"
                    if SNICPointTif in n.split(os.sep)[-1].split("_")[-1]:  #revised on 20230113
                        SegmentationFromFineToCoarse.append(n)
            print("SegmentationFromFineToCoarse=",SegmentationFromFineToCoarse)
           
            Classification = [k for k in InputZonegeorasterRun if (("Cluster" in k) and (run in k)  and ("_original" not in k) and ("_seg" not in k))]
            print("Classification=",Classification)
            ClassificationDigit = [int(''.join(filter(str.isdigit, a.replace(".tif","").split(os.sep)[-1].split("_")[-1]))) for a in Classification]  #https://stackoverflow.com/questions/28526367/get-only-numbers-from-string-in-python
            ClassificationDigit.sort(reverse=True) #reverse=True will sort the list descending. Default is reverse=False
            print("ClassificationDigit=",ClassificationDigit)
            ClassificationFromFineToCoarse = []
            for m in ClassificationDigit:
                for n in Classification:
                    #print("m,n=",m,n)
                    if run == "Run1":
                        ClassificationPointTif = "ClusterKmeans"+str(m)+".tif"
                    if run == "Run2":
                        ClassificationPointTif = "ClusterLVQ"+str(m)+".tif"
                    if run == "Run3":
                        ClassificationPointTif = "ClusterCascade"+str(m)+".tif"                
                    if ClassificationPointTif in n.split(os.sep)[-1].split("_")[-1]: #revised on 20230113
                        segments_fn = n.replace(".tif","_seg.tif")
                        if not os.path.exists(segments_fn):
                            #This part is to label the classification into segmentation, added on 20221121----start
                            InputTif = n
                            print(InputTif, " was a classification but now we want a segmentation start")
                            print("20230131 added: classification to labeling topic is very similar to the topic called [Connected Component Labeling]. Some websites are copied but commented out below FYI")
                            #https://iq.opengenus.org/connected-component-labeling/  connected-component-labeling is what I want
                            #https://stackoverflow.com/questions/51523765/how-to-use-opencv-connectedcomponents-to-get-the-images
                            #https://pyimagesearch.com/2021/02/22/opencv-connected-component-labeling-and-analysis/
                            #https://www.geeksforgeeks.org/python-opencv-connected-component-labeling-and-analysis/
                            #https://answers.opencv.org/question/189428/connectedcomponents-like-function-for-grayscale-image/
                            #https://stackoverflow.com/questions/61406283/could-the-cv2-connectedcomponents-process-the-grayscale-images
                            #https://pypi.org/project/connected-components-3d/
                            #https://homepages.inf.ed.ac.uk/rbf/HIPR2/label.htm#:~:text=Connected%20component%20labeling%20works%20by%20scanning%20an%20image%2C,share%20the%20same%20set%20of%20intensity%20values%20V.
                            driverTiff = gdal.GetDriverByName('GTiff')
                            ds = gdal.Open(InputTif)
                            band = ds.GetRasterBand(1).ReadAsArray()
                            NoDataValue = ds.GetRasterBand(1).GetNoDataValue()
                            band = ma.masked_values(band, NoDataValue)
                            bandMin = np.nanmin(band)
                            bandMax = np.nanmax(band)
                            if bandMin <= 0:  #if there is negative or 0 in classification, we use shift to make all values into positive; for NoDataValue, we assign originalmax + shift + 1
                                shift = abs(bandMin) + 1      
                                band[band != NoDataValue] = band[band != NoDataValue] + shift
                                band[band == NoDataValue] = bandMax + shift + 1
                            UniqueClassID = np.unique(band)
                            np.random.shuffle(UniqueClassID) #https://numpy.org/doc/stable/reference/random/generated/numpy.random.shuffle.html
                            print("UniqueClassID=",UniqueClassID)
                            Connection = [[1,1,1], [1,1,1], [1,1,1]]  #Connection = [[0,1,0], [1,1,1], [0,1,0]] is for four direction  ##Connection = [[1,1,1], [1,1,1], [1,1,1]], which is 8-direction and equivalent to Connection = generate_binary_structure(2,2)
                            cum_num = 0
                            segments = np.zeros_like(band)
                            for IndividualClassID in UniqueClassID:  #https://stackoverflow.com/questions/65631673/how-to-use-scipy-label-on-a-non-binary-image, with little change because we do not have 0
                                labeled_array, num_features = label((band==IndividualClassID).astype(np.int32), structure=Connection)  #https://docs.scipy.org/doc/scipy/reference/generated/scipy.ndimage.label.html
                                segments += np.where(labeled_array > 0, labeled_array + cum_num, 0).astype(segments.dtype)
                                cum_num += num_features
                            print("cum_num=",cum_num)
                            #segments_fn = n.replace(".tif","_seg.tif")
                            segments_ds = driverTiff.Create(segments_fn, ds.RasterXSize, ds.RasterYSize,1, gdal.GDT_Int32, options=['COMPRESS=LZW']) 
                            segments_ds.SetGeoTransform(ds.GetGeoTransform())
                            segments_ds.SetProjection(ds.GetProjectionRef())
                            segments_ds.GetRasterBand(1).SetNoDataValue(0)   #We used segments = np.zeros_like, so here set 0 as nodata
                            segments_ds.GetRasterBand(1).WriteArray(segments)
                            segments_ds = None
                            print(segments_fn, " was created from ", InputTif, " by labeling unqiue ROI")
                            print(InputTif, " was a classification but now we want a segmentation finished\n\n")
                            #This part is to label the classification into segmentation, added on 20221121----end
                        ClassificationFromFineToCoarse.append(segments_fn)
            print("ClassificationFromFineToCoarse=",ClassificationFromFineToCoarse)                    

            print("len(SegmentationFromFineToCoarse)=",len(SegmentationFromFineToCoarse))
            print("len(ClassificationFromFineToCoarse)=",len(ClassificationFromFineToCoarse))
            for LevelID in range(0,len(ClassificationFromFineToCoarse),1):
                print("\n\n\nLevelID=",LevelID)
                print("SegmentationFromFineToCoarse=",SegmentationFromFineToCoarse)
                print("ClassificationFromFineToCoarse=",ClassificationFromFineToCoarse)
                InputTifList = [SegmentationFromFineToCoarse[LevelID],ClassificationFromFineToCoarse[LevelID]]
                print("InputTifList=",InputTifList)
                print("Kirk combine start")

                segments_fn = RunInputZone + os.sep + run+'_Y'+str(Year)+"_"+"L"+str(LevelID+1)+"_S"+str(SegmentationDigit[LevelID])+"_C"+str(ClassificationDigit[LevelID])+".tif"
                if not os.path.exists(segments_fn):
                    ListTifArray = []
                    for k in InputTifList:
                        MyRaster = gdal.Open(k)
                        Rows = MyRaster.RasterYSize
                        Cols = MyRaster.RasterXSize
                        if k == InputTifList[0]:
                            data = MyRaster.GetRasterBand(1).ReadAsArray()
                            NoDataValue = MyRaster.GetRasterBand(1).GetNoDataValue()
                            data = ma.masked_values(data, NoDataValue)                
                        klist = MyRaster.GetRasterBand(1).ReadAsArray().flatten().tolist()  
                        ListTifArray.append(klist)  #here it is not [] but ()
                        print("Rows,Cols=",Rows,Cols)
                    dLU = {t:i+1 for i, t in enumerate(set(zip(*ListTifArray)))}
                    Final = np.array([dLU[T] for T in zip(*ListTifArray)]) #each pair has an unqiue ID (0,1,2..... etc), and the ID will be given to the element
                    band = Final.reshape((Rows, Cols))
                    print("Kirk combine end")

                    print("save result to tif start")   
                    driverTiff = gdal.GetDriverByName('GTiff')
                    segments_ds = driverTiff.Create(segments_fn, Cols, Rows,1, gdal.GDT_Int32, options=['COMPRESS=LZW']) 
                    segments_ds.SetGeoTransform(MyRaster.GetGeoTransform())
                    segments_ds.SetProjection(MyRaster.GetProjectionRef())
                    band[data.mask] = F3NoDataValue
                    segments_ds.GetRasterBand(1).WriteArray(band)
                    segments_ds.GetRasterBand(1).SetNoDataValue(F3NoDataValue)
                    segments_ds = None
                    print("save result to tif end")
                    print("For ", run, " the segments_fn=",segments_fn)           

                    #This will remove small isolated pixels so that the future computation can be faster---start
                    InputTif = segments_fn
                    Image = gdal.Open(InputTif, 1)  # open image in read-write mode
                    Band = Image.GetRasterBand(1)
                    if "L1" in InputTif:
                        SmallestPixelNumber = 150  #This is 1/10 of the above, will be automatic later
                    if "L2" in InputTif:
                        SmallestPixelNumber = 300   #This is 1/10 of the above, will be automatic later 
                    if "L3" in InputTif:
                        SmallestPixelNumber = 400   #This is 1/10 of the above, will be automatic later
                    if "L4" in InputTif:
                        SmallestPixelNumber = 600   #This is 1/10 of the above, will be automatic later       
                    if "L5" in InputTif:
                        SmallestPixelNumber = 2500   #SmallestPixelNumber = 800   #This is 1/10 of the above, will be automatic later 
                    gdal.SieveFilter(srcBand=Band, maskBand=None, dstBand=Band, threshold=SmallestPixelNumber, connectedness=8, callback=gdal.TermProgress_nocb)  #hMaskBand – an optional mask band. All pixels in the mask band with a value other than zero will be considered suitable for inclusion in polygons. see https://gdal.org/api/gdal_alg.html#_CPPv415GDALSieveFilter15GDALRasterBandH15GDALRasterBandH15GDALRasterBandHiiPPc16GDALProgressFuncPv
                    del Image, Band  # close the datasets.
                    print("gdal.SieveFilter finished for ",InputTif)
                NewInputZonegeoraster.append(segments_fn)
                #This will remove small isolated pixels so that the future computation can be faster---start
        print("AAA InputZonegeoraster=",InputZonegeoraster)
        print("Level6Snic=",Level6Snic)
        InputZonegeoraster = NewInputZonegeoraster + Level6Snic
        print("BBB InputZonegeoraster=",InputZonegeoraster)
        #This part is to combine SNIC and Kemans to create InputZoneRaster, added on 20221121---end

        InputGeospatialImages = Level6Snic + RSgeoraster + NonRSgeoraster + InputZonegeoraster + AdditionalDiscretegoeraster + AdditionalContinuousgeoraster #This list the input geospatial rasters as input of F3
        print("-------------------------InputGeospatialImages=",InputGeospatialImages)

        F3PreprocessingOutputPath = F3PreprocessingOutputPathRSY
        for run in ["Run1","Run2","Run3"]:
            for subdirectory in ["FieldPoint","RSraster","NonRSraster","InputZone","AdditionalDiscreteRaster","AdditionalContinuousRaster","CommonShare", "Intermediate", "Results","ResultsAnalysis"]:
                MyDirectory = F3PreprocessingOutputPath + os.sep + run + os.sep + Tile + os.sep + subdirectory
                if not os.path.exists(MyDirectory):
                    os.makedirs(MyDirectory)  
                    Readme = MyDirectory + os.sep + "readme.txt"  #Make sure there is at least one small file in each folder, otherwise the google cloud cannot transfer the directory
                    ReadmeFile = open(Readme, 'w')
                    content = "Dr. Shengli Huang created the file of " + Readme + " (or for Run[X+3] where X indicates the Run ID here. e.g., run1 or run4) at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
                    ReadmeFile.write(content)
                    ReadmeFile.close()
                MyDirectoryWarp = F3PreprocessingOutputPath + os.sep + Tile
                if not os.path.exists(MyDirectoryWarp):
                    os.makedirs(MyDirectoryWarp)
                if subdirectory == "FieldPoint":
                    print("placerholder for nothing here")
        #make sure the GEE only produce sing-band TIF, making our work easier
                            

        #This part is to warp the input data for each tile---start                   
        def Feature_to_Raster(input_shp, output_tiff, cellsize, field_name=False, NoData_value=F3NoDataValue):  #copy from https://www.programcreek.com/python/example/101827/gdal.RasterizeLayer
            """ Converts a shapefile into a raster. e.g., polygon to ratser or point to raster"""
            # Input
            inp_driver = ogr.GetDriverByName('ESRI Shapefile')
            inp_source = inp_driver.Open(input_shp, 0)  
            inp_lyr = inp_source.GetLayer()
            inp_srs = inp_lyr.GetSpatialRef()
            print("Projection of the shape file is: ",inp_srs.ExportToWkt())
            # Extent
            x_min, x_max, y_min, y_max = inp_lyr.GetExtent()
            x_ncells = int((x_max - x_min) / cellsize)
            y_ncells = int((y_max - y_min) / cellsize)
            # Output
            out_driver = gdal.GetDriverByName('GTiff')
            if os.path.exists(output_tiff):
                out_driver.Delete(output_tiff)
            out_source = out_driver.Create(output_tiff, x_ncells, y_ncells, 1, gdal.GDT_Int16, options=['COMPRESS=LZW'])
            out_source.SetGeoTransform((x_min, cellsize, 0, y_max, 0, -cellsize))
            out_source.SetProjection(inp_srs.ExportToWkt())   
            out_lyr = out_source.GetRasterBand(1)
            out_lyr.SetNoDataValue(NoData_value)
            # Rasterize
            if field_name:
                gdal.RasterizeLayer(out_source, [1], inp_lyr,
                                    options=["ATTRIBUTE={0}".format(field_name)])
            else:
                gdal.RasterizeLayer(out_source, [1], inp_lyr, burn_values=[1])
            # Save and/or close the data sources
            inp_source = None
            out_source = None
            # Return
            return output_tiff
        #Check https://stackoverflow.com/questions/44975952/get-feature-extent-using-gdal-ogr
        inDriver = ogr.GetDriverByName("ESRI Shapefile")
        inDataSource = inDriver.Open(fhaastf3tilesShape, 1)
        inLayer = inDataSource.GetLayer()  #https://www.e-education.psu.edu/geog489/node/2214, in this case just a single layer, so .GetLayer(0) should be the same
        layerDefinition = inLayer.GetLayerDefn()
        for i in range(layerDefinition.GetFieldCount()):
            print("Fields in this shape file are:", layerDefinition.GetFieldDefn(i).GetName())
        print("[inLayer.SetAttributeFilter() and inLayer.SetSpatialFilter()] can be used here to narrow down the selection")   #We can also filter vector layers by attribute and spatially. see https://www.e-education.psu.edu/geog489/node/2214


        inLayerCopy = inDataSource.GetLayer()  #This sentence is important. Without this, inLayerCopy will be changed after inLayerCopy.SetAttributeFilter()
        print("Save a polygon, see https://gis.stackexchange.com/questions/68650/ogr-how-to-save-layer-from-attributefilter-to-a-shapefile and https://gis.stackexchange.com/questions/189120/how-to-select-polygons-by-criteria-and-save-them-to-shp-file")
        myquery = "TileName = '" + Tile +"'" #inlyr.SetAttributeFilter("TileName = 'CaliforniaTile1'"), note it is "TileName = 'CaliforniaTile1'" instead of 'TileName = "CaliforniaTile1"'
        inLayerCopy.SetAttributeFilter(myquery) # inLayer.SetAttributeFilter(myquery)   #I think this sentence may cause the problem that I do not want to see: only one returned!
        drv = ogr.GetDriverByName( 'ESRI Shapefile' )
        TilePolygon = F3PreprocessingOutputPath+os.sep+Tile+os.sep+"Area_Extent"
        TilePolygonshp = TilePolygon + ".shp"
        outds = drv.CreateDataSource(TilePolygonshp)
        outlyr = outds.CopyLayer(inLayerCopy,TilePolygon)   #outlyr = outds.CopyLayer(inLayer,TilePolygon)
        del outlyr,outds
        print("Tile polygon just finished above")

        print("Check https://opensourceoptions.com/blog/use-python-to-convert-polygons-to-raster-with-gdal-rasterizelayer/ to convert polygon to raster")
        print(TilePolygonshp, " is the source shape file")
        TilePolygonTif = TilePolygon + ".tif"
        if not os.path.exists(TilePolygonTif):
            TilePolygonTifoutput = Feature_to_Raster(TilePolygonshp, TilePolygonTif, F3Resolution, field_name=False, NoData_value=F3NoDataValue)
        print(TilePolygonTif, "was created")
        TilePolygonRaster = gdal.Open(TilePolygonTif, gdal.GA_ReadOnly)
        TilePolygonRasterTrasnform = TilePolygonRaster.GetGeoTransform()
        #for osr, see https://gis.stackexchange.com/questions/60371/getting-coordinate-system-name-from-spatialreference-using-gdal-python
        #https://www.programcreek.com/python/example/91656/osgeo.gdal.GetDataTypeName for GDAL metadata
        #http://jgomezdans.github.io/gdal_notes/reprojection.html,
        #https://www.youtube.com/watch?v=n33MswNARkE, Clip Raster to a Polygon Extent using gdal.Warp
        #https://www.programcreek.com/python/example/116446/gdal.Warp
        #https://gis.stackexchange.com/questions/146322/gdal-translate-or-gdalwarp-to-define-projection
        TilePolygonRasterProjection = osr.SpatialReference() 
        TilePolygonRasterProjection.ImportFromWkt(TilePolygonRaster.GetProjection())  #it is not GetProjectionRef() but GetProjection()
        print("TilePolygonRasterProjection is: ",TilePolygonRasterProjection)
        
        inDataSource1 = inDriver.Open(TilePolygonshp, 1)
        inLayer1 = inDataSource1.GetLayer()  #https://www.e-education.psu.edu/geog489/node/2214, in this case just a single layer, so .GetLayer(0) should be the same
        print("inLayer1 is: ", inLayer1, " and the number of records is ",len(inLayer1))
        for feature in inLayer1:
            print("The tile ",feature.GetField('TileName'), " has the centroid coordinates of ", feature.GetGeometryRef().Centroid().ExportToWkt())
            geom=feature.GetGeometryRef()
            env = geom.GetEnvelope()  # Get Envelope returns a tuple (minX, maxX, minY, maxY), see https://pcjericks.github.io/py-gdalogr-cookbook/geometry.html
            print("env is: ",env)
            print("minX: %d, minY: %d, maxX: %d, maxY: %d" %(env[0],env[2],env[1],env[3]))
            minX = env[0]
            minY = env[2]
            maxX = env[1]
            maxY = env[3]
            print("We need to add a tile polygon, see https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html#create-a-new-layer-from-the-extent-of-an-existing-layer")
        outdataRows = []
        outdataColumns = []
        for InputImage in InputGeospatialImages:
            if os.path.exists(InputImage):
                outdataName = F3PreprocessingOutputPath+os.sep+Tile+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3warp.tif")
                print("InputImage=",InputImage," and outdataName=",outdataName)
                if not os.path.exists(outdataName):
                    if os.path.exists(InputImage):
                        print("InputImage=",InputImage)
                        print("outdataName=",outdataName)
                        print("20230503 added: We may have to investigate the reason and find a solution: ERROR 1: PROJ: Cannot open https://cdn.proj.org/us_noaa_cnhpgn.tif: Network functionality not available happened on the gdal.warp below")
                        OutTile = gdal.Warp(outdataName, InputImage, format='GTiff', outputBounds=[minX, minY, maxX, maxY], xRes=F3Resolution, yRes=F3Resolution, dstSRS=TilePolygonRasterProjection, resampleAlg=gdal.GRA_NearestNeighbour)  #NOTE: outdataName is before InputImage
                        OutTile = None # Close dataset
                        print(outdataName, "is done")
                outdataNameHuang = gdal.Open(outdataName)
                cols = outdataNameHuang.RasterXSize
                rows = outdataNameHuang.RasterYSize
                outdataRows.append(rows)
                outdataColumns.append(cols)
        print("outdataRows are:", outdataRows)
        print("outdataColumns are:", outdataColumns)
        if (len(set(outdataRows)) == 1) and (len(set(outdataColumns)) == 1): #check if a list has all identical elements, see https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical
            print("All output images have the same dimension")
            ExtentDimension = F3PreprocessingOutputPath + os.sep + Tile + os.sep + "ExtentAndDimension.txt"  #Make sure there is at least one small file in each folder, otherwise the google cloud cannot transfer the directory
            ExtentDimensionFile = open(ExtentDimension, 'w')
            content1 = "minX, minY, maxX, maxY: " + str(env[0]) + "," + str(env[2]) + ","+ str(env[1]) + ","+ str(env[3]) + "\n"
            ExtentDimensionFile.write(content1)
            content2 = "Columns: " + str(outdataColumns[0]) + "\n"
            ExtentDimensionFile.write(content2)
            content2 = "Rows: " + str(outdataRows[0]) + "\n"
            ExtentDimensionFile.write(content2)
            ExtentDimensionFile.close()
        else:
            print("not all output images have the same dimension, so more steps are required to preprocess the datasets")
            exit()
        #This part is to warp the input data for each tile---end

       
        #This part is to create the cloudwatershadowetc mask data---start
        TileName = Tile   #20240208: This is a big change
        F3VoidMaskName = F3PreprocessingOutputPath+os.sep+TileName + os.sep + "CloudShadowWaterSnowF3.tif"  
        print("The tile being processed is: ",TileName)
        print("The tile being processed is: ",TileName, file=open(F3Log, 'a'))
        ExtentDimension = F3PreprocessingOutputPath+os.sep+TileName + os.sep + "ExtentAndDimension.txt" 
        ExtentDimensionFile = open(ExtentDimension, 'r')
        Lines = ExtentDimensionFile.readlines()
        Lines = [i.replace('\n','') for i in Lines]
        for Line in Lines:
            if Line.startswith('Columns:'):
                Columns = int(Line.split(":")[1])
            if Line.startswith('Rows:'):
                Rows = int(Line.split(":")[1])
        print("Columns and Rows = ", Columns, Rows)
        ExtentDimensionFile.close()

        ##We used to consider three bands all equal to zero to be cloud mask, but after 20220928 we use PixelLabel.tif to do this work, so comment out here---start
        #F3InvalidNegaive9999 = np.ones([Rows, Columns])  #assign 1 first, and then we will assign -9999 to those invalid pixels
        #print("The algorithm for cloudwatershadowglacierbareland is:")
        #print("1) Remote sensing bands are all zero or the edge")
        #for InputImage in RSgeoraster:
        #    Band = F3PreprocessingOutputPath+os.sep+TileName+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3warp.tif")
        #    BandRaster = gdal.Open(Band, gdal.GA_ReadOnly)
        #    WeUseThisTrasnform = BandRaster.GetGeoTransform()
        #    WeUseThisProjection = BandRaster.GetProjectionRef()
        #    BandNoDataValue = BandRaster.GetRasterBand(1).GetNoDataValue()
        #    print("BandNoDataValue for ", InputImage, " is ", BandNoDataValue)
        #    BandArray = BandRaster.GetRasterBand(1).ReadAsArray().astype('int32')  #The float will be assign based on original type returned from gdal
        #    F3InvalidNegaive9999[(BandArray == BandNoDataValue)] = F3NoDataValue
        #    F3InvalidNegaive9999[(BandArray == 0)] = F3NoDataValue  #20220906. I found they are all zero. Because there is no negative values in Band, so =0 (i.e. it is not AND but OR for the Green, Red, and NIR) is here 
        #    BandRaster = None
        #print("2) Meadow layer has value: we may add this later in GEE using a vector and raster, because that may make the work easier")
        #We used to consider three bands all equal to zero to be cloud mask, but after 20220928 we use PixelLabel.tif to do this work, so comment out here---end

        #After 20220928, we used PixelLabel.tif for the cloud mask, the code is here, but 20221006, we still consider three bands all equal to zero to be cloud mask---start
        F3InvalidNegaive9999 = np.ones([Rows, Columns])  #assign 1 first, and then we will assign -9999 to those invalid pixels
        print("The algorithm for cloudwatershadowglacierbareland is:")
        print("1) Remote sensing bands are all zero or the edge")
        for InputImage in RSgeoraster:
            print("InputImage=",InputImage)
            Band = F3PreprocessingOutputPath+os.sep+TileName+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3warp.tif")
            BandRaster = gdal.Open(Band, gdal.GA_ReadOnly)
            WeUseThisTrasnform = BandRaster.GetGeoTransform()
            WeUseThisProjection = BandRaster.GetProjectionRef()
            BandNoDataValue = BandRaster.GetRasterBand(1).GetNoDataValue()
            print("BandNoDataValue for ", InputImage, " is ", BandNoDataValue)
            BandArray = BandRaster.GetRasterBand(1).ReadAsArray().astype('int32')  #The float will be assign based on original type returned from gdal
            F3InvalidNegaive9999[(BandArray == BandNoDataValue)] = F3NoDataValue
            if "PixelLabel" in InputImage:
                F3InvalidNegaive9999[(BandArray > 0)] = F3NoDataValue  #"In the final PixelLabel.tif from GEE , water is 1, Perennial/permanent Ice/Snow is 2, Barren-Rock/Sand/Clay is 3, Landsat cloud/shadow/snow is 4, remaining value is 0"
            else:   #Some pixels are zero because the RS images may not cover these areas.
                F3InvalidNegaive9999[(BandArray == 0)] = F3NoDataValue  #20220906. I found they are all zero. Because there is no negative values in Band, so =0 (i.e. it is not AND but OR for the Green, Red, and NIR) is here 
            BandRaster = None
        #After 20220928, we used PixelLabel.tif for the cloud mask, the code is here, but 20221006, we still consider three bands all equal to zero to be cloud mask---end
            
        print("We output the CloudWaterEtcMask.tif as below")  
        driver = gdal.GetDriverByName("GTiff")
        outdata = driver.Create(F3VoidMaskName, Columns, Rows, 1, gdal.GDT_Int32, options=['COMPRESS=LZW'])  #see datatype at https://gis.stackexchange.com/questions/268898/using-signed-bytes-with-gdal
        outdata.SetGeoTransform(WeUseThisTrasnform)##sets same geotransform as input
        outdata.SetProjection(WeUseThisProjection)##sets same projection as input
        outdata.GetRasterBand(1).WriteArray(F3InvalidNegaive9999)
        outdata.GetRasterBand(1).SetNoDataValue(F3NoDataValue)  ##if you want these values transparent
        outdata.FlushCache() ##saves to disk!!
        outdata = None
        print(F3VoidMaskName, " is created")

        #https://gis.stackexchange.com/questions/383739/deleting-gdal-created-raster is interesting for gdal manageing files
        #https://stackoverflow.com/questions/6996603/how-do-i-delete-a-file-or-folder-in-python for python to delete a file
        #This part is to create the cloudwatershadowetc mask data---end

        #This part is to assign the cloudwatershadowetc pixels to -9999---start
        F3VoidMaskName = F3PreprocessingOutputPath+os.sep+TileName + os.sep + "CloudShadowWaterSnowF3.tif"
        F3VoidMaskNameFile = gdal.Open(F3VoidMaskName, gdal.GA_ReadOnly)
        F3InvalidNegaive9999 = F3VoidMaskNameFile.GetRasterBand(1).ReadAsArray()
        Rows = F3VoidMaskNameFile.RasterYSize
        Columns = F3VoidMaskNameFile.RasterXSize        
        
        print("We are now processing the specific tile named ", TileName)
        print("We are now processing the specific tile named ", TileName, file=open(F3Log, 'a'))
        ExtentDimension = F3PreprocessingOutputPath+os.sep+TileName + os.sep + "ExtentAndDimension.txt" 
        F3VoidMaskName = F3PreprocessingOutputPath+os.sep+TileName + os.sep + "CloudShadowWaterSnowF3.tif" 
        for InputImage in InputGeospatialImages:
            if os.path.exists(InputImage):
                outdataNameF3Ready = F3PreprocessingOutputPath+os.sep+TileName+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                if not os.path.exists(outdataNameF3Ready):  
                    outdataName = F3PreprocessingOutputPath+os.sep+TileName+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3warp.tif")
                    print("We are assigning the cloudwatershadowetc pixels to -9999 to the F3warp tif image called outdataName=",outdataName)
                    outdataNameRaster = gdal.Open(outdataName, gdal.GA_ReadOnly)
                    WeUseThisTrasnform = outdataNameRaster.GetGeoTransform()
                    WeUseThisProjection = outdataNameRaster.GetProjectionRef()
                    Projection = osr.SpatialReference()
                    Projection.ImportFromWkt(WeUseThisProjection)
                    outdataNameNoDataValue = outdataNameRaster.GetRasterBand(1).GetNoDataValue()
                    PythonDataType = outdataNameRaster.GetRasterBand(1).DataType   #https://www.programcreek.com/python/example/91656/osgeo.gdal.GetDataTypeName for GDAL metadata
                    GdalDataType = gdal.GetDataTypeName(PythonDataType)  ##gdal.GetDataTypeName returns [Byte, UInt16, Int16, UInt32, Int32, Float32, Float64], see https://www.programcreek.com/python/example/91656/osgeo.gdal.GetDataTypeName
                    #print("PythonDataType=",PythonDataType)
                    #print("GdalDataType=",GdalDataType)
                    if GdalDataType == "Byte":
                        TypeOfInterest = gdal.GDT_Byte  #see GDAL Types at https://gist.github.com/CMCDragonkai/ac6289fa84bcc8888035744d7e00e2e6
                    if GdalDataType == "UInt16":
                        TypeOfInterest = gdal.GDT_UInt16
                    if GdalDataType == "Int16":
                        TypeOfInterest = gdal.GDT_Int16
                    if GdalDataType == "UInt32":
                        TypeOfInterest = gdal.GDT_UInt32
                    if GdalDataType == "Int32":
                        TypeOfInterest = gdal.GDT_Int32
                    if GdalDataType == "Float32":
                        TypeOfInterest = gdal.GDT_Float32
                    if GdalDataType == "Float64":
                        TypeOfInterest = gdal.GDT_Float64                 
                    outdataNameArray = outdataNameRaster.GetRasterBand(1).ReadAsArray()
                    #print("The data type of python array outdataNameArray is ",outdataNameArray.dtype)            
                    outdataNameArray[(outdataNameArray == outdataNameNoDataValue)] = F3NoDataValue
                    if "PixelLabel" not in InputImage:  #20220928: We add PixelLable layer, which should not use "F3InvalidNegaive9999 (i.e.,CloudShadowWaterSnow) to mask, because we want to keep its original values 
                        outdataNameArray[(F3InvalidNegaive9999 == F3NoDataValue)] = F3NoDataValue  #revise it accordingly
                    if "PixelLabel" in InputImage:
                        outdataNameArray[(F3InvalidNegaive9999 == F3NoDataValue) & (outdataNameArray == 0)] = 5 #20221006, we add 5 to depict the area that is not covered by remote sensing images, but still #water is 1, Perennial/permanent Ice/Snow is 2, Barren-Rock/Sand/Clay is 3, Landsat cloud/shadow/snow is 4, remaining value is 0"
                    
                    #This part convert the very large negative and positive GEE SNIC segmentation values into normal values---start               
                    if (InputImage in Level6Snic) or (InputImage in InputZonegeoraster) or ((InputImage in AdditionalDiscretegoeraster) and ("BioClimateZone.tif" in InputImage)):  #20221130: added the part of ((InputImage in AdditionalDiscretegoeraster) and ("BioClimateZone.tif" in InputImage))
                        print("outdataNameArray shape=",outdataNameArray.shape, " at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

                        #This is to use Kirk's combine idea to replace the original value with the index. adopted on 20221202---start
                        outdataNameArrayshape = outdataNameArray.shape
                        ListTifArray = []            
                        klist = outdataNameArray.flatten().tolist()  
                        ListTifArray.append(klist)  
                        dLU = {t:i+1 for i, t in enumerate(set(zip(*ListTifArray)))}
                        Final = np.array([dLU[T] for T in zip(*ListTifArray)]) #each pair has an unqiue ID (0,1,2..... etc), and the ID will be given to the element
                        outdataNameArray = Final.reshape(outdataNameArrayshape)
                        outdataNameArray[(outdataNameArray == outdataNameNoDataValue)] = F3NoDataValue
                        print("outdataNameArrayUniqueValues done ", " at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) 
                        #This is to use Kirk's combine idea to replace the original value with the index. adopted on 20221202---end
                                   
                        ##This section use a loop and very slow, so I decided to use Kirk's combine approach above to replace it and thus commened out here. Revised on 20221202---start 
                        #outdataNameArrayUniqueValues = np.unique(outdataNameArray)   #np.unique can return index, see https://numpy.org/doc/stable/reference/generated/numpy.unique.html
                        #outdataNameArrayUniqueValuesList =  outdataNameArrayUniqueValues.tolist()
                        #outdataNameArrayUniqueValuesList.remove(F3NoDataValue)  #a=[1,2,3,4,5]; b=a.remove(5), then b is empty and a is [1,2,3,4], see https://www.programiz.com/python-programming/methods/list/remove 
                        #print("outdataNameArrayUniqueValuesList and length=",outdataNameArrayUniqueValuesList, len(outdataNameArrayUniqueValuesList), " at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))      
                        #for ElementValue in outdataNameArrayUniqueValues:
                        #    #print("ElementValue=",ElementValue)
                        #    if ElementValue != F3NoDataValue:  #if ElementValue == F3NoDataValue, outdataNameArray will not change (i.e. keep value of F3NoDataValue)
                        #        ElementIndex = outdataNameArrayUniqueValuesList.index(ElementValue)
                        #        outdataNameArray[outdataNameArray == ElementValue] = ElementIndex + 1  #20221201, I added 1 here to avoid the value=0
                        ##This section use a loop and very slow, so I decided to use Kirk's combine approach above to replace it and thus commened out here. Revised on 20221202---end
                    #This part convert the very large negative and positive GEE SNIC segmentation values into normal values---end
                              
                    #outdataNameF3Ready = F3PreprocessingOutputPath+os.sep+TileName+os.sep+InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                    driver = gdal.GetDriverByName("GTiff")
                    outdata = driver.Create(outdataNameF3Ready, Columns, Rows, 1, TypeOfInterest, options=['COMPRESS=LZW']) #outdata = driver.Create(outdataNameF3Ready, Columns, Rows, 1, gdal.GDT_Float32)  #see datatype at https://gis.stackexchange.com/questions/268898/using-signed-bytes-with-gdal
                    outdata.SetGeoTransform(WeUseThisTrasnform)##sets same geotransform as input
                    outdata.SetProjection(WeUseThisProjection)##sets same projection as input
                    outdata.GetRasterBand(1).WriteArray(outdataNameArray)   #outdata.GetRasterBand(1).WriteArray(outdataNameArray) error?
                    outdata.GetRasterBand(1).SetNoDataValue(F3NoDataValue)  ##if you want these values transparent
                    outdata.FlushCache() ##saves to disk!!
                    outdata = None
                    outdataNameRaster = None

                #print("Below we will copy the data to each run. This will have duplicate data occupying more space, but the organization is very clear. Since the input data is very little compared to F3 output, I prefer this option")
                for run in ["Run1","Run2","Run3"]:
                    if InputImage in RSgeoraster:
                        print("cat miao = ", InputImage)
                        if ("PixelLabel" in InputImage):
                            print("cat miao = ", InputImage)
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "RSraster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun): 
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/
                        InputImageLast = InputImage.split(os.sep)[-1]
                        #if (run == "Run1") and (("PCA1" in InputImageLast) or ("PCA2" in InputImageLast) or ("PCA3" in InputImageLast)):
                        if (run == "Run1") and (("red" in InputImageLast) or ("nir" in InputImageLast) or ("swir1" in InputImageLast)):  #20230113 newly revised
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "RSraster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun):  
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/
                        #if (run == "Run2") and (("red" in InputImageLast) or ("nir" in InputImageLast) or ("swir1" in InputImageLast)):
                        if (run == "Run2") and (("red" in InputImageLast) or ("nir" in InputImageLast) or ("swir1" in InputImageLast)):  #20230113 newly revised
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "RSraster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun):  
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/
                        #if (run == "Run3") and (("green" in InputImageLast) or ("red" in InputImageLast) or ("nir" in InputImageLast)):
                        if (run == "Run3") and (("red" in InputImageLast) or ("nir" in InputImageLast) or ("swir1" in InputImageLast)):  #20230113 newly revised
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "RSraster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun):  
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/
                    if InputImage in NonRSgeoraster:
                        outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "NonRSraster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                        if not os.path.exists(outdataNameF3ReadyForEachRun): 
                            shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/

                    if InputImage in Level6Snic:
                        if run in InputImage:   #Runxxx_snicxxxx is the file name of segmentation
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "InputZone" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun):
                                print("XXXXXXXXXXXXXXXXXXXXXXXXxInputImage=",InputImage)
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method
                                print("XXXXXXXXXXXXXXXXXXXXXXXXxoutdataNameF3ReadyForEachRun got copied (i.e.) ",outdataNameF3ReadyForEachRun)
                    if InputImage in InputZonegeoraster:    
                        if run in InputImage:   #Runxxx_snicxxxx is the file name of segmentation
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "InputZone" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun): 
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method
                    if InputImage in AdditionalDiscretegoeraster:
                        if (("LandfireBPS" in InputImage) or ("USCropLayer" in InputImage) or ("LandCover" in InputImage) or ("Forest1Nonforest0" in InputImage)):
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "AdditionalDiscreteRaster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun): 
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/ 
                        if (run in InputImage) and ("BioClimateZone" in InputImage):   #Runxxx_BioClimateZone is the file name of BioClimateZone
                            InputImage = InputImage.replace(run+"_","")  #remove run_ (e.g., "Run1_") from the file name 
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "AdditionalDiscreteRaster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun): 
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/                   
                        if (run in InputImage) and ("Phenology" in InputImage):   #Runxxx_Phenology is the file name of Phenology
                            InputImage = InputImage.replace(run+"_","")  #remove run_ (e.g., "Run1_") from the file name 
                            outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "AdditionalDiscreteRaster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                            if not os.path.exists(outdataNameF3ReadyForEachRun): 
                                shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/                   
                    if InputImage in AdditionalContinuousgeoraster:
                        outdataNameF3ReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "AdditionalContinuousRaster" + os.sep + InputImage.split(os.sep)[-1].replace(".tif","F3.tif")
                        if not os.path.exists(outdataNameF3ReadyForEachRun): 
                            shutil.copy(outdataNameF3Ready, outdataNameF3ReadyForEachRun)   #Syntax: shutil.move(source, destination, copy_function = copy2), see https://www.geeksforgeeks.org/python-shutil-move-method/    
                    F3VoidMaskNameReadyForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "RSraster" + os.sep + F3VoidMaskName.split(os.sep)[-1]
                    if not os.path.exists(F3VoidMaskNameReadyForEachRun): 
                        shutil.copy(F3VoidMaskName, F3VoidMaskNameReadyForEachRun)
                    ExtentDimensionForEachRun = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "InputZone" + os.sep + ExtentDimension.split(os.sep)[-1]
                    if not os.path.exists(ExtentDimensionForEachRun): 
                        shutil.copy(ExtentDimension, ExtentDimensionForEachRun)
                Area_Extent_files = glob.glob(F3PreprocessingOutputPath+os.sep+TileName+os.sep+"Area_Extent.*")
                for Area_Extent_file in Area_Extent_files:
                    for run in ["Run1","Run2","Run3"]:
                         Area_Extent_file_target = F3PreprocessingOutputPath + os.sep + run + os.sep + TileName + os.sep + "InputZone" + os.sep + Area_Extent_file.split(os.sep)[-1]
                         if not os.path.exists(Area_Extent_file_target):
                             shutil.copy(Area_Extent_file, Area_Extent_file_target)
        #This part is to assign the cloudwatershadowetc pixels to -9999---end


        #20230421 added to make the Tif file names shorter; otherwise the name may be too long after combine---start
        for run in ["Run1","Run2","Run3"]:
            for Tile in Tiles:
                for subfolder in ["AdditionalContinuousRaster","AdditionalDiscreteRaster","InputZone","NonRSraster","RSraster"]:
                    Files = glob.glob(F3PreprocessingOutputPath+os.sep+run+os.sep+Tile+os.sep+subfolder+os.sep+"*.tif")
                    for m in Files:
                        mNewName = os.sep.join(m.split(os.sep)[:-1]) + os.sep + m.split(os.sep)[-1].replace("Run","R").replace("Landsat","Img").replace("F3.tif",".tif")
                        if not os.path.exists(mNewName):
                            print(m, "changed to ", mNewName,"\n")
                            os.rename(m, mNewName) #https://stackoverflow.com/questions/2491222/how-to-rename-a-file-using-python
                        else:
                            os.remove(m)  #20240209, I do not want to keep a F3.tif in the computer
        #20230421 added to make the Tif file names shorter; otherwise the name may be too long after combine---end
                
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = F3PreprocessingInputOverallPath+os.sep+"F3GeeAndPreprocessingError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for F3GeeAndPreprocessing with inputs of " + repr(Year)+ repr(Tile)+ " with the following details:\n"
        #print(Content2)
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


if __name__ == '__main__':   
    print("\n\n\nWe cannot run in terminal; we must run under DOS command, because it is a parallel python!")
    NumberOfProcessors = int(os.cpu_count())  #os.environ["NUMBER_OF_PROCESSORS"] does not work for unix but work for windows
    print("NumberOfProcessors=",NumberOfProcessors)
    UserDefinedRatio = 0.3   
    UserDefinedMaximumProcessors = int(NumberOfProcessors * UserDefinedRatio)

    #Note: if you have the error of [Exception in thread Thread-1 (_handle_workers): need at most 63 handles, got a sequence of length 88"], then try below
    #20240124: I found the error "ValueError: [Exception in thread Thread-1 (_handle_workers): need at most 63 handles, got a sequence of length 88"]
    #https://stackoverflow.com/questions/65252807/multiprocessing-pool-pool-on-windows-cpu-limit-of-63
    UserDefinedMaximumProcessors = min(60,int(NumberOfProcessors * UserDefinedRatio))  #20240124: so I use the 60 here to solve the problem above
    UserDefinedMaximumProcessors = min(10,UserDefinedMaximumProcessors) #Each tile will use about 15%, so ideally cannot be more than 6 plus 3-4 python IDLE etc.
    print("We only use ",UserDefinedMaximumProcessors," processors. The maximum value of 60 is used to solve the problem of [ValueError: [Exception in thread Thread-1 (_handle_workers): need at most 63 handles, got a sequence of length 88]")
    
    mymanager = multiprocessing.Manager()
    mylock = mymanager.Lock() # is "lock = multiprocessing.Lock()" OK? I guess it may not work, see https://www.thecodingforums.com/threads/multiprocessing-and-locks.679436/ and http://stackoverflow.com/questions/25557686/python-sharing-a-lock-between-processes
    myqueue = mymanager.Queue() #I do not use here, but want to keep here for future use. But rememebr this may be required for multiprocessing internally     

    #This section will download data from Google bucket and then preprocess them into the format required by subsequent F3 processing---start
    MaximalSimutaneousTask = min(len(RemoteSensingYearOfInterest)*len(Tiles)*1*1*1, UserDefinedMaximumProcessors) #here 1*1*1 indicate BaseManagement,BaseYear,BaseMetric
    NumberOfSimutaneousTask = max(min(NumberOfProcessors-2, MaximalSimutaneousTask),1)  
    print("\n\n\nNumberOfSimutaneousTask for BaseManagement,BaseYear,BaseMetric=",NumberOfSimutaneousTask)
    F3pool = multiprocessing.Pool(processes=NumberOfSimutaneousTask)
    for Year in RemoteSensingYearOfInterest:
        for Tile in Tiles:
            F3TileRepresentFile = F3PreprocessingInputOverallPath + os.sep + 'RemoteSensingYear' + str(Year) + os.sep + "Run1" + os.sep + Tile + os.sep + "RSraster" + os.sep + "Img_"+str(Year)+"_red.tif"
            print(F3TileRepresentFile)
            if not os.path.exists(F3TileRepresentFile):
                print(Year,Tile," started at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                F3pool.apply_async(F3GeeAndPreprocessing, [mylock,Year, Tile])
    F3pool.close()
    F3pool.join()
    print("AAA finished at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #This section will download data from Google bucket and then preprocess them into the format required by subsequent F3 processing---end


    #This part is to delete the tile folder---start
    DeleteTheTileFolder = "Yes"  #Options are "Yes" and "No"
    if DeleteTheTileFolder == "Yes":
        for Year in RemoteSensingYearOfInterest:
            for Tile in Tiles:
                TileFolder = F3PreprocessingInputOverallPath + os.sep + 'RemoteSensingYear'+str(Year) + os.sep + Tile
                if os.path.exists(TileFolder):
                    shutil.rmtree(TileFolder)
                    print(TileFolder, " was deleted")
    #This part is to delete the tile folder---end


