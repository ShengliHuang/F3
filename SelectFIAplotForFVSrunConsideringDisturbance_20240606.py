import os, sys, traceback, datetime, time
from osgeo import gdal, ogr, osr
import glob
import rasterio
from rasterio.fill import fillnodata
import numpy as np
import numpy.ma as ma
import math 
from math import floor
import shutil
import multiprocessing
import subprocess
import csv
import sqlite3
from rasterio.plot import show
from scipy.interpolate import griddata
from scipy.stats import mode  #https://www.geeksforgeeks.org/how-to-calculate-the-mode-of-numpy-array/
from scipy import stats
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import storage   #from gcloud import storage does not work, see see https://stackoverflow.com/questions/37003862/how-to-upload-a-file-to-google-cloud-storage-on-python-3
from google.cloud.storage import constants



print("!!!!!!!!!!!!!!!!!!!!!!!!!!THIS FILE HAS TO RUN UNDER DOS WINDOW NOT IN IDLE, BECAUSE GSUTIL DOES NOT WORK UNDER IDLE")


##Some comments on 20230601:
##Since I have no access to O:\inventory\fastemap\ShengliHuangTrue.sec, I do not want to do more work, but below is the function that I want to add in the future:
##a) Background: in current F3 (traditional F3), the FVS does not use the remesearement, which is not good. For example, plot was meassured in 2010, when we run rsy2012 for FVSyears of 2013-2050, if the plot was remeasured in 2018, the 2018 measurement is not used, which is a waste
##b) To address this issue, I am planning to drive FVS when there is a new measurement. Details (with rsy2012 as the example):
##    1) Rename the current RSY2012_MaxGap7.csv to RSY2012_MaxGap7_Tradition.csv;
##    2) Keep the current RSY2012_MaxGap7_DisturbedFVS.csv
##    3) Make a new one called RSY2012_MaxGap7_Remeasured.csv, whwill will list the items like 91428092014. The items should be:
##        3.1 Remeasurement Year is later than the RSY2012;
##        3.2 Fast loss and slow loss do not happen between RSY2012 and remeasuremnt year (i.e., the remeasured plot is natural succession without disturbance)
##        3.3 The remeasured plot must be already in the Tradition and DisturbedFVS plots
##    4) Create the key for *_Tradition.csv,*_DisturbedFVS.csv,*_Remeasured.csv and have the category (Tradition, DisturbedFVS, Remeasured) added into the file name (We may add the categtory as an argument in FVSKeyBatAndRun/FVSKeyBatAndRunDisturbedFVS function)
##    5) When merged to create the final .db as the F3 input, for a specific year, using the remeasured FVS modeling as a priority  



#This is to choose the FIA plots and measurment year based on change detection products. The geographic is the TIF extent.




##I planned to develop this as parallel, but eventually the parallel is not necessary, because the processing does not take very long time.

#Marcus suggestions: Add tile name as another field in the final list.csv file, but this may not be necessary because this step does not limit to individuall title.
##Tile1-Tile2-Tile3
##Tile2
##Tile3
##Tile3-Tile4
#or maybe each plot can list several times (i.e., several records)

t0 = time.time()
EntireStateShortNameList = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"] 
Tiles = ["MichiganTile3"]#["MichiganTile1","MichiganTile2","MichiganTile3","MichiganTile4","MichiganTile5"]#,"MichiganTile3"]   #This can be automatically but I prefer manual input
fhaastf3tilesShape = r'F:\CUI\fhaastf3app\F3DataEveryWhere' + os.sep + 'fhaastf3tiles.shp'
EntireUS_FIA_ReprojectAlbers = r'D:\CUI\subplots\L48\L48_subplotLocations_GivenByBill_Albers.shp'
EverywherePath = r'F:\CUI\fhaastf3app\F3DataEveryWhere'
ReferencedTif = EverywherePath + os.sep + 'USAlbersEqualAreaNAD83.tif'   #we need a small US Albers Equal Area NAD83 tif here
EntireUS_FIA_NoCoordinates = r'D:\CUI\subplots\L48\L48_subplotLocations_GivenByBill.shp'
FVSINPUTDBpath = r'F:\CUI\fhaastf3app\FIA'
FVSprocessingpath = r'F:\CUI\fhaastf3app\FVS'
LandCoverChangeAnnualRastersPath = r'U:\f3geepreprocessing\LandCoverChangeAnnualRasters'
RemoteSensingYearOfInterest = [2023]
Factor = -11.1
F3NoDataValue = -9999
YearsOfAnnualChange = range(1989, 2022)   #This will return [1989, 1990....2019, 2020,2021], where 2022 is not returned. Please note in GEE, the year is [for (var year = 1990; year <= 2022; year++)] with the reason of one year shift in disturbance event (see GEE comment)
F3Log = os.getcwd()+os.sep+"F3ScreenFIAlog.txt"
ThePathWhereThePreprocessedDataComeFrom = r'U:\f3geepreprocessing'
ThePathWhereF3ToBeRun = r'F:\CUI\fhaastf3app'




    

print("The purpose is to use USFS annual annual change Google datasets to select the plot date for FVS processing, which can save the time for FVS processing")
print("Note the FIA datamart in FIA website does not include off-grid intensified plots")
print("See https://developers.google.com/earth-engine/datasets/catalog/USFS_GTAC_LCMS_v2021-7?hl=en#bands, #1:Stable, 2:Slow Loss, 3:Fast Loss, 4:Gain, 5:Non-Processing Area Mask")
print("Note: in California, all FIADB plot has 5 digits, but I found in Michigan it can have 1 or 2 digits. Please think it over for the code revision. E.g., in MIchian all the plots < 10000 were surveyed before 1993")

def CreateTilesMosaicRunAverageFolder(Year):
    try:
        F3SupplentaryForTilesMosaicRunAverage = [
            EverywherePath + os.sep + 'NorthArrow.tif',
            EverywherePath + os.sep + 'verdana.ttf',
            EverywherePath + os.sep + 'F3MetricInformation.xlsx',
            EverywherePath + os.sep + 'DropPlotAndInventoryYear.txt',
            EverywherePath + os.sep + 'DataCheck.txt',
            EverywherePath + os.sep + 'FVS_ForestType_Code.pdf'
            ]
        RemoteSensingYear = ThePathWhereF3ToBeRun + os.sep + "RemoteSensingYear"+str(Year)
        if not os.path.exists(RemoteSensingYear):
            os.mkdir(RemoteSensingYear)
        TilesMosaicRunAverageForThisYear = ThePathWhereF3ToBeRun + os.sep + "RemoteSensingYear"+str(Year)+os.sep+"TilesMosaicRunAverage"
        if not os.path.exists(TilesMosaicRunAverageForThisYear):
            os.mkdir(TilesMosaicRunAverageForThisYear)
        F3SupplentaryForTilesMosaicRunAverageNew = [k.replace("F3DataEveryWhere","RemoteSensingYear"+str(Year)+os.sep+"TilesMosaicRunAverage") for k in F3SupplentaryForTilesMosaicRunAverage]
        for k in range(0,len(F3SupplentaryForTilesMosaicRunAverage),1):
            if not os.path.exists(F3SupplentaryForTilesMosaicRunAverageNew[k]):
                shutil.copy(F3SupplentaryForTilesMosaicRunAverage[k], F3SupplentaryForTilesMosaicRunAverageNew[k])
                print(F3SupplentaryForTilesMosaicRunAverage[k]," copied to ", F3SupplentaryForTilesMosaicRunAverageNew[k])
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateTilesMosaicRunAverageFolder with inputs of "+ repr(Year) + " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog
    
def CopyTheDataFromPreprocessedPathToF3Path(Year,Tile):
    try:
        SourceFolder0 = ThePathWhereThePreprocessedDataComeFrom + os.sep + "LandCoverChangeAnnualRasters" + os.sep + Tile
        TargetFolder0 = ThePathWhereF3ToBeRun + os.sep + "LandCoverChangeAnnualRasters" + os.sep + Tile
        if not os.path.exists(TargetFolder0):
            print(SourceFolder0," to ",TargetFolder0)
            shutil.copytree(SourceFolder0, TargetFolder0, dirs_exist_ok=True)   #https://stackoverflow.com/questions/1868714/how-do-i-copy-an-entire-directory-of-files-into-an-existing-directory-using-pyth      
        for run in ["Run1","Run2","Run3"]:
            SourceFolder = ThePathWhereThePreprocessedDataComeFrom + os.sep + "RemoteSensingYear"+str(Year) + os.sep + run + os.sep + Tile
            TargetFolder =  ThePathWhereF3ToBeRun + os.sep + "RemoteSensingYear"+str(Year) + os.sep + run + os.sep + Tile
            print(SourceFolder," to ",TargetFolder," being copied")
            if not os.path.exists(TargetFolder):
                print(SourceFolder," to ",TargetFolder, " just copied")
                shutil.copytree(SourceFolder, TargetFolder, dirs_exist_ok=True)       
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CopyTheDataFromPreprocessedPathToF3Path with inputs of "+ repr(Year) + " " + repr(Tile)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def CreateDisturbanceScreendFIAplotForFVSrun(Tile):
    try:
        ShengliHuangKeyFile = r'D:\CUI\subplots\L48' + os.sep + Tile + ".hsl"
        plotlist,xlist,ylist,StatesCodeInThisHSL,StatesFullNameInThisHSL,StatesShortNameInThisHSL,SpeciesTranslatorInThisHSL = PlotIDandXandY(ShengliHuangKeyFile,Factor)
        #print("\n\n1111111111111111111",plotlist,xlist,ylist,StatesCodeInThisHSL,StatesFullNameInThisHSL,StatesShortNameInThisHSL,SpeciesTranslatorInThisHSL)
        EachTileSqliteFIADBs = [FVSINPUTDBpath+os.sep+"SQLite_FIADB_"+k+".db" for k in StatesShortNameInThisHSL]
        #print("\n\n222222222222222222",EachTileSqliteFIADBs)
        ThisPlotAllJoinFieldList,FiaPlotListUnique = CreatePlotIDandTheMeasurementMonthYear(EachTileSqliteFIADBs,plotlist)
        #print("\n\n3333333333333333333",ThisPlotAllJoinFieldList,FiaPlotListUnique)
        AnnualChangeVariables,Tile,ulx,uly,lrx,lry,AnnualChangeRasters,srcCols,srcRows = CreateAnnualChangeVariables(Tile,YearsOfAnnualChange)
        #print("\n\n444444444444444444444444",AnnualChangeVariables,Tile,ulx,uly,lrx,lry,AnnualChangeRasters,srcCols,srcRows)
        FieldMetricCSV,xlist,ylist = FIAplotAnnualChangeWithTheTile(Tile,ulx,uly,lrx,lry,AnnualChangeVariables,AnnualChangeRasters,srcCols,srcRows,ThisPlotAllJoinFieldList,FiaPlotListUnique)
        #print("\n\n5555555555555555555",FieldMetricCSV)
        FieldMetricCSV_final,FieldMetricCSV_finalDisturbedFVS,FieldMetricCSV_finalDropped,FieldMetricCSV_finalRemeasured = CreateFinalCleanFIAplotsToRunFVSForTheTile(Tile,RemoteSensingYearOfInterest,plotlist,xlist,ylist)
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateDisturbanceScreendFIAplotForFVSrun with inputs of "+ repr(Tile)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def CreateFIAconfidentialPlotBinaryForEachTile(Tile):
    try:
        FIAtrueCoordinatesSource = [EntireUS_FIA_ReprojectAlbers]   #In the future, we need to put the entire US FIA shape file here (i.e., only list one shape file)
        ProjectName = Tile
        SelectTiles = [Tile]
        TilePolygonshpForThisProject = SelectSubTilesFromfhaastf3tilesShape(fhaastf3tilesShape,SelectTiles,ProjectName)
        print("TilePolygonshpForThisProject=",TilePolygonshpForThisProject)
        TilePolygonshpForThisProjectBuffer = createBuffer(TilePolygonshpForThisProject, "Buffer")  #Buffer is a field in fhaastf3tilesxxx.shp to define the buffer distance in meters
        print("TilePolygonshpForThisProjectBuffer=",TilePolygonshpForThisProjectBuffer)
        FIAconfidentialPlotBinary =  r'D:\CUI\subplots\L48' + os.sep + ProjectName + ".hsl" 
        FIAconfidentialPlotBinaryCreated = SelectPointsFromPolygonWithGDAL(TilePolygonshpForThisProjectBuffer,FIAtrueCoordinatesSource,FIAconfidentialPlotBinary,Factor)
        print("FIAconfidentialPlotBinary=",FIAconfidentialPlotBinaryCreated)
        return FIAconfidentialPlotBinaryCreated
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateFIAconfidentialPlotBinaryForEachTile with inputs of "+ repr(Tile)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def DownloadedFVSreadyDataFromFIAdatamartWebsite(StateShortNameList):
    try:
        print("ENTIRE can be considered a state here, but it is a big zip file with all FIA data in one DB file. Too Big, I do not like it")
        for state in StateShortNameList:
            print("\n",state," is being processed")
            url = "https://apps.fs.usda.gov/fia/datamart/Databases/SQLite_FIADB_"+state+".zip"   
            response = requests.get(url, allow_redirects=True)
            LocalFileName = r"F:\CUI\fhaastf3app\FIA\SQLite_FIADB_"+state+".zip"
            if not os.path.exists(LocalFileName):
                print(LocalFileName," is being downloaded from FIA DataMart format")
                with open(LocalFileName, "wb") as f:
                    f.write(response.content)
            LocalFileNameDB = LocalFileName.replace(".zip",".db")
            directory_to_extract_to = os.sep.join(LocalFileName.split(os.sep)[:-1])
            if not os.path.exists(LocalFileNameDB):
                print(LocalFileNameDB," is being extracted from zip format")
                with ZipFile(LocalFileName, 'r') as myzip:
                    myzip.extractall(directory_to_extract_to)    #I can write a python code to read the content of a zip file without unzip, butI prefer not to do it for F3 project for simplity. SQLite Zipfile Module can be check later
        StateShortNameListDownloaded = ",".join(StateShortNameList)+" are downloaded from https://apps.fs.usda.gov/fia/datamart/Databases and unzipped"
        print(StateShortNameListDownloaded)
        return StateShortNameListDownloaded
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for DownloadedFVSreadyDataFromFIAdatamartWebsite with inputs of "+ repr(StateShortNameList)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog

def convert_to_albers(lat, lon):  #20231229, get from bing GPT chat
    try:
        #When run, get a warning saying pyproj.Proj is depreciated. so maybe we will use "import pyproj4" and then "in_proj = pyproj4.Proj(proj='latlong', datum='NAD83')"?
        #https://pyproj4.github.io/pyproj/stable/gotchas.html#upgrading-to-pyproj-2-from-pyproj-1
        #We recommended using the pyproj.transformer.Transformer and pyproj.crs.CRS in place of the pyproj.Proj and pyproj.transformer.transform().
        in_proj = pyproj.Proj(proj='latlong', datum='NAD83')   #datume can be typed as "WGS84" if you want. Here I use NAD83 because the FIA_FVSready provides Lat/Long in NAD83
        out_proj = pyproj.Proj(proj='aea', datum='NAD83', lat_1=29.5, lat_2=45.5, lat_0=23, lon_0=-96)   #datume can be typed as "WGS84" if you want. lat_1=29.5, lat_2=45.5, lat_0=23, lon_0=-96 agrees with widely-used US Albers Equal Area
        x, y = pyproj.transform(in_proj, out_proj, lon, lat)
        return x, y
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for convert_to_albers with inputs of "+ repr(lat)+ repr(lon)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog

def CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseWithMeasurement(FVSINPUTDB):
    try:
        shapeDataFile = FVSINPUTDB.replace(".db","_fuzzy.shp")
        if not os.path.exists(shapeDataFile):
            #Read sqlite DB---------------------------------------------------------------------start
            conn = sqlite3.connect(FVSINPUTDB)  #https://www.sqlitetutorial.net/sqlite-python/
            cur= conn.cursor()
            cur.execute("SELECT * FROM FVS_STANDINIT_PLOT")  #Ask Marcus to give the table name consistently
            columns = [column[0] for column in cur.description]
            #print("columns=",columns)
            STAND_IDIndex = columns.index("STAND_ID")
            #print("STAND_IDIndex=",STAND_IDIndex)
            STAND_CNIndex = columns.index("STAND_CN")
            INV_YEARIndex = columns.index("INV_YEAR")
            INV_MONTHIndex = columns.index("INV_MONTH")
            INV_DAYIndex = columns.index("INV_DAY")
            VARIANTIndex = columns.index("VARIANT")
            LATITUDEIndex = columns.index("LATITUDE")
            LONGITUDEIndex = columns.index("LONGITUDE")
            ELEVFTIndex = columns.index("ELEVFT")
            DatumIndex = columns.index("DATUM")
            rows = cur.fetchall()
            #Read sqlite DB---------------------------------------------------------------------end

            #Get each record from sqlite DB for X, Y, and FIADA plot. Copied from old code---------------------------------------------------------------------start
            CoordinateXlist = []
            CoordinateYlist = []
            ELEVFTlist = []
            FIADB_PLOTlist = []
            FiaMeasurementTimelist = []
            MeasurementYearList = []
            MeasurementMonthList = []
            rowid = 0
            for row in rows:
                rowid = rowid + 1
                #print("rowid=",rowid)
                TITLE = "F3 project" #(max length of 72)
                STAND_ID = str(row[STAND_IDIndex])  #According to John Shaw, Stand_ID = concatenate((PLOT.STATECD(4) + PLOT.INVYR(4) + PLOT.CYCLE(2) + PLOT.SUBCYCLE(2) + PLOT.UNITCD(2) + PLOT.COUNTYCD(3) + PLOT.PLOT(5)).
                STAND_CN = str(row[STAND_CNIndex])
                INV_YEAR = int(row[INV_YEARIndex])        
                INV_MONTH = row[INV_MONTHIndex]
                if INV_MONTH is None:
                    INV_MONTH = 7
                else:
                    INV_MONTH = int(INV_MONTH)
                INV_DAY = row[INV_DAYIndex]
                if INV_DAY is None:
                    INV_DAY = 15
                else:
                    INV_DAY = int(INV_DAY)
                VARIANT = str(row[VARIANTIndex])
                LATUTUDE = float(str(row[LATITUDEIndex]))
                LONGITUDE = float(str(row[LONGITUDEIndex]))
                ELEVFT = row[ELEVFTIndex]
                STAND_IDANDTITLE = STAND_ID + " " * (27 - len(STAND_ID)) + TITLE  #From system-generated key file, I found the TITLE always starts at 28th position        
                FIADB_PLOT = CreateFIADBUniquePlotIDfromFvsReadyStandID(STAND_ID) #revised from original on 20231229: FIADB_PLOT = STAND_ID[-5:]  #STAND_ID[-5:] is to get the PlotFIADB from stand such as 0006200105010500350682 whose last 5 digits are FiadbPlot
                FiaMeasurementTime = FIADB_PLOT + str(INV_MONTH).zfill(2) + str(INV_YEAR)  ##see zfill at https://stackoverflow.com/questions/134934/display-number-with-leading-zeros
                AlbersX, AlbersY = convert_to_albers(LATUTUDE, LONGITUDE)  #revised on 20231229        
                CoordinateXlist.append(AlbersX)
                CoordinateYlist.append(AlbersY)
                print(ELEVFT)
                if ELEVFT is None:
                    ELEVFTlist.append(None)  
                else:
                    ELEVFTlist.append(float(ELEVFT))    #ELEVFTlist.append(float(ELEVFT)). There are some nulls, so give 100 temp
                FIADB_PLOTlist.append(int(FIADB_PLOT))  #.zfill(5) can be used for string type
                MeasurementYearList.append(INV_YEAR)
                MeasurementMonthList.append(INV_MONTH)
                FiaMeasurementTimelist.append(FiaMeasurementTime)
                conn.commit()
            #Get each record from sqlite DB for X, Y, and FIADA plot. Copied from old code---------------------------------------------------------------------end
            
        #Create a shape file from X, Y, and FIADA plot---------------------------------------------------------------------start
        if not os.path.exists(shapeDataFile):
            gdalDS = gdal.Open(ReferenceProjection)
            srs = osr.SpatialReference() #srs = osgeo.osr.SpatialReference() does not work
            srs.ImportFromWkt(gdalDS.GetProjection())  #gdalDS = gdal.Open(CloudWaterShadowMask) was used before. Here clearly we need to define ourself
            
            #srs.ImportFromProj4('+proj=utm +zone=15 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
            #https://www.tabnine.com/code/java/methods/org.gdal.osr.SpatialReference/ImportFromProj4
            #https://gis.stackexchange.com/questions/20298/is-it-possible-to-get-the-epsg-value-from-an-osr-spatialreference-class-using-th
            #At least we can create a Lat/Long ratser and read it as before.
            
            driver = ogr.GetDriverByName('ESRI Shapefile') #driver = osgeo.ogr.GetDriverByName('ESRI Shapefile') does not work
            shapeData = driver.CreateDataSource(shapeDataFile)
            layer = shapeData.CreateLayer('ogr_pts', srs, ogr.wkbPoint)   #layer = shapeData.CreateLayer('ogr_pts', srs, osgeo.ogr.wkbPoint) does not work
            layerDefinition = layer.GetLayerDefn()

            new_field_defn = ogr.FieldDefn("FIADB_PLOT", ogr.OFTReal)  ##see https://snyk.io/advisor/python/ogr/functions/ogr.FieldDefn, https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            print("I always think the FIADB_PLOT is five digits, but from Michigan I learnt it may be like 00001; therefore, we can not use int() anymore. Please use the information here to revise all code")
            print("Michigan FIADBPLOT and MeasureYear combination is not unique, which makes me a little worried at this time. I will think it over")
            print("It seems we may remove the earliest inventory dataset?")
            new_field_defn.SetWidth(50)  
            new_field_defn.SetPrecision(11)
            layer.CreateField(new_field_defn)

            new_field_defn = ogr.FieldDefn("ELEVFT", ogr.OFTReal)  #https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            new_field_defn.SetWidth(50)
            new_field_defn.SetPrecision(11)
            layer.CreateField(new_field_defn)

            new_field_defn = ogr.FieldDefn("MeasYear", ogr.OFTReal)  #https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            new_field_defn.SetWidth(50)
            new_field_defn.SetPrecision(5)
            layer.CreateField(new_field_defn)

            new_field_defn = ogr.FieldDefn("MeasMont", ogr.OFTReal)  #https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            new_field_defn.SetWidth(50)
            new_field_defn.SetPrecision(5)
            layer.CreateField(new_field_defn)
            
            new_field_defn = ogr.FieldDefn("MeasTime", ogr.OFTString)  #https://snyk.io/advisor/python/ogr/functions/ogr.FieldDefn includes string, integer, float etc.
            new_field_defn.SetWidth(100)
            new_field_defn.SetPrecision(5)
            layer.CreateField(new_field_defn)

            
            i = 0
            for k in range(0,len(CoordinateXlist),1):
                print("CoordinateXlist[k],CoordinateYlist[k],FIADB_PLOTlist[k],ELEVFTlist[k]=",CoordinateXlist[k],CoordinateYlist[k],FIADB_PLOTlist[k],ELEVFTlist[k])
                point = ogr.Geometry(ogr.wkbPoint) #point = osgeo.ogr.Geometry(osgeo.ogr.wkbPoint) does not work
                point.SetPoint(0, CoordinateXlist[k], CoordinateYlist[k])
                feature = ogr.Feature(layerDefinition)  #feature = osgeo.ogr.Feature(layerDefinition) does not work
                feature.SetGeometry(point)
                feature.SetFID(i)
                feature.SetField("FIADB_PLOT", FIADB_PLOTlist[k])  #https://gis.stackexchange.com/questions/74708/how-to-change-the-field-value-of-a-shapefile-using-gdal-ogr
                feature.SetField("ELEVFT", ELEVFTlist[k])
                feature.SetField("MeasYear", MeasurementYearList[k])
                feature.SetField("MeasMont", MeasurementMonthList[k])
                feature.SetField("MeasTime", FiaMeasurementTimelist[k])            
                layer.CreateFeature(feature)
                i += 1
            shapeData.Destroy()
            print("We created the shape file of ",shapeDataFile)
        #Create a shape file from X, Y, and FIADA plot---------------------------------------------------------------------end

        #Convert the Geographic Longitude and Latitude to Albers Equal Area Projection---------------------------------------------------------------------start
        #Convert the Geographic Longitude and Latitude to Albers Equal Area Projection---------------------------------------------------------------------end
        return shapeDataFile

    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseWithMeasurement with inputs of "+ repr(FVSINPUTDB)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog

def CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseOnlyPlotLocation(FVSINPUTDB):
    try:
        shapeDataFile = FVSINPUTDB.replace(".db","_fuzzyPlot.shp")
        if not os.path.exists(shapeDataFile):
            #Read sqlite DB---------------------------------------------------------------------start
            conn = sqlite3.connect(FVSINPUTDB)  #https://www.sqlitetutorial.net/sqlite-python/
            cur= conn.cursor()
            cur.execute("SELECT * FROM FVS_STANDINIT_PLOT")  #Ask Marcus to give the table name consistently
            columns = [column[0] for column in cur.description]
            #print("columns=",columns)
            STAND_IDIndex = columns.index("STAND_ID")
            #print("STAND_IDIndex=",STAND_IDIndex)
            STAND_CNIndex = columns.index("STAND_CN")
            INV_YEARIndex = columns.index("INV_YEAR")
            INV_MONTHIndex = columns.index("INV_MONTH")
            INV_DAYIndex = columns.index("INV_DAY")
            VARIANTIndex = columns.index("VARIANT")
            LATITUDEIndex = columns.index("LATITUDE")
            LONGITUDEIndex = columns.index("LONGITUDE")
            ELEVFTIndex = columns.index("ELEVFT")
            DatumIndex = columns.index("DATUM")
            rows = cur.fetchall()
            #Read sqlite DB---------------------------------------------------------------------end

            #Get each record from sqlite DB for X, Y, and FIADA plot. Copied from old code---------------------------------------------------------------------start
            CoordinateXlist = []
            CoordinateYlist = []
            ELEVFTlist = []
            FIADB_PLOTlist = []
            FiaMeasurementTimelist = []
            rowid = 0
            for row in rows:
                rowid = rowid + 1
                #print("rowid=",rowid)
                TITLE = "F3 project" #(max length of 72)
                STAND_ID = str(row[STAND_IDIndex])  #According to John Shaw, Stand_ID = concatenate((PLOT.STATECD(4) + PLOT.INVYR(4) + PLOT.CYCLE(2) + PLOT.SUBCYCLE(2) + PLOT.UNITCD(2) + PLOT.COUNTYCD(3) + PLOT.PLOT(5)).
                STAND_CN = str(row[STAND_CNIndex])
                INV_YEAR = int(row[INV_YEARIndex])
                INV_MONTH = row[INV_MONTHIndex]
                if INV_MONTH is None:
                    INV_MONTH = 7
                else:
                    INV_MONTH = int(INV_MONTH)
                INV_DAY = row[INV_DAYIndex]
                if INV_DAY is None:
                    INV_DAY = 15
                else:
                    INV_DAY = int(INV_DAY)
                VARIANT = str(row[VARIANTIndex])
                LATUTUDE = float(str(row[LATITUDEIndex]))
                LONGITUDE = float(str(row[LONGITUDEIndex]))
                ELEVFT = row[ELEVFTIndex]
                STAND_IDANDTITLE = STAND_ID + " " * (27 - len(STAND_ID)) + TITLE  #From system-generated key file, I found the TITLE always starts at 28th position        
                FIADB_PLOT = CreateFIADBUniquePlotIDfromFvsReadyStandID(STAND_ID) #revised from original on 20231229: FIADB_PLOT = STAND_ID[-5:]  #STAND_ID[-5:] is to get the PlotFIADB from stand such as 0006200105010500350682 whose last 5 digits are FiadbPlot
                FiaMeasurementTime = FIADB_PLOT + str(INV_MONTH).zfill(2) + str(INV_YEAR)  ##see zfill at https://stackoverflow.com/questions/134934/display-number-with-leading-zeros
                AlbersX, AlbersY = convert_to_albers(LATUTUDE, LONGITUDE)  #revised on 20231229        
                CoordinateXlist.append(AlbersX)
                CoordinateYlist.append(AlbersY)
                if ELEVFT is None:
                    ELEVFTlist.append(9999)
                else:
                    ELEVFTlist.append(int(ELEVFT))    #ELEVFTlist.append(float(ELEVFT)). There are some nulls, so give 100 temp
                FIADB_PLOTlist.append(int(FIADB_PLOT))  #.zfill(5) can be used for string type
                FiaMeasurementTimelist.append(int(FiaMeasurementTime))
                conn.commit()
            #Get each record from sqlite DB for X, Y, and FIADA plot. Copied from old code---------------------------------------------------------------------end

            #I only want to get ONE record for each plot, and the code below was referenced from the bing GPT chat of "python code to find mean of indexed value in a list"---start
            CoordinateXlistArray = np.array(CoordinateXlist)
            CoordinateYlistArray = np.array(CoordinateYlist)
            FIADB_PLOTlistArray = np.array(FIADB_PLOTlist)
            ELEVFTlistArray = np.array(ELEVFTlist)
            FIADB_PLOTUnique_indices = np.unique(FIADB_PLOTlistArray)
            CoordinateXMean = []
            CoordinateYMean = []
            FIADB_PLOTMean = []
            ELEVFTMean = []
            for index in FIADB_PLOTUnique_indices:
                mask = FIADB_PLOTlistArray == index
                FIADB_PLOTMean.append(np.mean(FIADB_PLOTlistArray[mask]))
                CoordinateXMean.append(np.mean(CoordinateXlistArray[mask]))
                CoordinateYMean.append(np.mean(CoordinateYlistArray[mask]))
                Temp1 = ELEVFTlistArray[mask]
                Temp2 = np.ma.masked_equal(Temp1, 9999)
                Temp3 = np.nanmean(Temp2)
                ELEVFTMean.append(Temp3)
            #I only want to get ONE record for each plot, and the code below was referenced from the bing GPT chat of "python code to find mean of indexed value in a list"---end

        #Create a shape file from X, Y, and FIADA plot---------------------------------------------------------------------start
        if not os.path.exists(shapeDataFile):
            gdalDS = gdal.Open(ReferenceProjection)
            srs = osr.SpatialReference() #srs = osgeo.osr.SpatialReference() does not work
            srs.ImportFromWkt(gdalDS.GetProjection())  #gdalDS = gdal.Open(CloudWaterShadowMask) was used before. Here clearly we need to define ourself
            
            #srs.ImportFromProj4('+proj=utm +zone=15 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')
            #https://www.tabnine.com/code/java/methods/org.gdal.osr.SpatialReference/ImportFromProj4
            #https://gis.stackexchange.com/questions/20298/is-it-possible-to-get-the-epsg-value-from-an-osr-spatialreference-class-using-th
            #At least we can create a Lat/Long ratser and read it as before.
            
            driver = ogr.GetDriverByName('ESRI Shapefile') #driver = osgeo.ogr.GetDriverByName('ESRI Shapefile') does not work
            shapeData = driver.CreateDataSource(shapeDataFile)
            layer = shapeData.CreateLayer('ogr_pts', srs, ogr.wkbPoint)   #layer = shapeData.CreateLayer('ogr_pts', srs, osgeo.ogr.wkbPoint) does not work
            layerDefinition = layer.GetLayerDefn()

            new_field_defn = ogr.FieldDefn("FIADB_PLOT", ogr.OFTReal)  ##see https://snyk.io/advisor/python/ogr/functions/ogr.FieldDefn, https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            print("I always think the FIADB_PLOT is five digits, but from Michigan I learnt it may be like 00001; therefore, we can not use int() anymore. Please use the information here to revise all code")
            print("Michigan FIADBPLOT and MeasureYear combination is not unique, which makes me a little worried at this time. I will think it over")
            print("It seems we may remove the earliest inventory dataset?")
            new_field_defn.SetWidth(50)  
            new_field_defn.SetPrecision(11)
            layer.CreateField(new_field_defn)

            new_field_defn = ogr.FieldDefn("ELEVFT", ogr.OFTReal)  #https://stackoverflow.com/questions/39982877/python-crashes-when-adding-a-field-to-a-shapefile-with-ogr
            new_field_defn.SetWidth(50)
            new_field_defn.SetPrecision(11)
            layer.CreateField(new_field_defn)       

            i = 0
            for k in range(0,len(CoordinateXMean),1):
                print("CoordinateXMean[k],CoordinateYMean[k],FIADB_PLOTMean[k],ELEVFTMean[k]=",CoordinateXMean[k],CoordinateYMean[k],FIADB_PLOTMean[k],ELEVFTMean[k])
                point = ogr.Geometry(ogr.wkbPoint) #point = osgeo.ogr.Geometry(osgeo.ogr.wkbPoint) does not work
                point.SetPoint(0, CoordinateXMean[k], CoordinateYMean[k])
                feature = ogr.Feature(layerDefinition)  #feature = osgeo.ogr.Feature(layerDefinition) does not work
                feature.SetGeometry(point)
                feature.SetFID(i)
                feature.SetField("FIADB_PLOT", FIADB_PLOTMean[k])  #https://gis.stackexchange.com/questions/74708/how-to-change-the-field-value-of-a-shapefile-using-gdal-ogr
                print("*******************************ELEVFTMean[k]=",ELEVFTMean[k])
                if np.ma.is_masked(ELEVFTMean[k]):   #sometimes using "a is None", sometimes using "np.ma.is_masked(a)", a lot of confusing options
                    feature.SetField("ELEVFT", 9999)
                else:
                    feature.SetField("ELEVFT", ELEVFTMean[k])
                layer.CreateFeature(feature)
                i += 1
            shapeData.Destroy()
            print("We created the shape file of ",shapeDataFile)
        #Create a shape file from X, Y, and FIADA plot---------------------------------------------------------------------end

        #Convert the Geographic Longitude and Latitude to Albers Equal Area Projection---------------------------------------------------------------------start
        #Convert the Geographic Longitude and Latitude to Albers Equal Area Projection---------------------------------------------------------------------end
        return shapeDataFile
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseOnlyPlotLocation with inputs of "+ repr(FVSINPUTDB)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog




def ExtentOfReferencedImageOrShapeFile(Referenced):   #Referenced should be a Tif file or Shape file in Albers Equal Area project
    try:
        if ".tif" in Referenced:
            driverTiff = gdal.GetDriverByName('GTiff')
            ds = gdal.Open(Referenced)  
            dsArray = ds.GetRasterBand(1).ReadAsArray()
            dsNoDataValue = ds.GetRasterBand(1).GetNoDataValue()
            srcCols = ds.RasterXSize
            srcRows = ds.RasterYSize
            #print("srcCols,srcRows=",srcCols,srcRows)
            #print("dsArray.shape=",dsArray.shape)
            WeUseThisProjection = ds.GetProjectionRef()
            WeUseThisGeoTransform = ds.GetGeoTransform()
            upx, xres, xskew, upy, yskew, yres = WeUseThisGeoTransform
            ulx = float(upx + 0*xres + 0*xskew)
            uly = float(upy + 0*yskew + 0*yres)
            llx = float(upx + 0*xres + srcRows*xskew) 
            lly = float(upy + 0*yskew + srcRows*yres)      
            lrx = float(upx + srcCols*xres + srcRows*xskew)
            lry = float(upy + srcCols*yskew + srcRows*yres)
            urx = float(upx + srcCols*xres + 0*xskew)
            ury = float(upy + srcCols*yskew + 0*yres)
            #print("ulx,uly,lrx,lry=",ulx,uly,lrx,lry)
            print("float(x_min), float(x_max), float(y_min), float(y_max)=",float(ulx), float(lrx), float(lry), float(uly))
            return float(ulx), float(lrx), float(lry), float(uly)  #this should correspond to float(x_min), float(x_max), float(y_min), float(y_max) below for shape file
        if ".shp" in Referenced:
            inp_driver = ogr.GetDriverByName('ESRI Shapefile')
            inp_source = inp_driver.Open(Referenced, 0)  
            inp_lyr = inp_source.GetLayer()
            inp_srs = inp_lyr.GetSpatialRef()
            print("Projection of the shape file is: ",inp_srs.ExportToWkt())
            x_min, x_max, y_min, y_max = inp_lyr.GetExtent()
            print("float(x_min), float(x_max), float(y_min), float(y_max)=",float(x_min), float(x_max), float(y_min), float(y_max))
            return float(x_min), float(x_max), float(y_min), float(y_max)
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for ExtentOfReferencedImageOrShapeFile with inputs of "+ repr(Referenced)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def SelectSubTilesFromfhaastf3tilesShape(fhaastf3tilesShape,SelectTiles,ProjectName):
    try:
        TilePolygon = r'F:\CUI\fhaastf3app' + os.sep + 'F3LocalProjectData' + os.sep + ProjectName + ".shp"
        if not os.path.exists(TilePolygon):
            #Check https://stackoverflow.com/questions/44975952/get-feature-extent-using-gdal-ogr
            inDriver = ogr.GetDriverByName("ESRI Shapefile")
            inDataSource = inDriver.Open(fhaastf3tilesShape, 1)
            inLayer = inDataSource.GetLayer()  #https://www.e-education.psu.edu/geog489/node/2214, in this case just a single layer, so .GetLayer(0) should be the same
            layerDefinition = inLayer.GetLayerDefn()
            for i in range(layerDefinition.GetFieldCount()):
                print("Fields in this shape file are:", layerDefinition.GetFieldDefn(i).GetName())
            print("[inLayer.SetAttributeFilter() and inLayer.SetSpatialFilter()] can be used here to narrow down the selection")   #We can also filter vector layers by attribute and spatially. see https://www.e-education.psu.edu/geog489/node/2214
            SelectTiles = ["TileName="+"'"+k+"'" for k in SelectTiles]
            SetAttributeFilterQuery = " or ".join(SelectTiles)  #Very special
            print(SetAttributeFilterQuery)
            inLayer.SetAttributeFilter(SetAttributeFilterQuery) #inLayer.SetAttributeFilter("TileName='MichiganTile1' or TileName='MichiganTile2'") works
            print("check how to use SetAttributeFilter at https://gis.stackexchange.com/questions/325054/ogr-setattributefilter-with-multiple-fields and https://stackoverflow.com/questions/46023949/python-gdal-setattributefilter-not-working")
            drv = ogr.GetDriverByName( 'ESRI Shapefile' )
            outds = drv.CreateDataSource(TilePolygon)
            outlyr = outds.CopyLayer(inLayer,TilePolygon)   #outlyr = outds.CopyLayer(inLayer,TilePolygon)
            del outlyr,outds
            print(TilePolygon, " was just finished above")
        return TilePolygon
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for SelectSubTilesFromfhaastf3tilesShape with inputs of "+ repr(fhaastf3tilesShape)+ repr(SelectTiles)+ repr(ProjectName)+" with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def createBuffer(inputfn, bufferField):  #20240126: referenced from "python code to buffer a GIS shape file" in BING chat. Note bufferDistance must be integer
    try:
        outputBufferfn = inputfn.replace(".shp","_buff.shp")
        if not os.path.exists(outputBufferfn):
            inputds = ogr.Open(inputfn)
            inputlyr = inputds.GetLayer()
            shpdriver = ogr.GetDriverByName('ESRI Shapefile')
            if os.path.exists(outputBufferfn):
                shpdriver.DeleteDataSource(outputBufferfn)
            outputBufferds = shpdriver.CreateDataSource(outputBufferfn)
            bufferlyr = outputBufferds.CreateLayer(outputBufferfn, geom_type=ogr.wkbPolygon)
            featureDefn = bufferlyr.GetLayerDefn()
            inputlyr_defn = inputlyr.GetLayerDefn()  # Add fields from source layer to the output layer
            for i in range(inputlyr_defn.GetFieldCount()):  #Keep original fields names
                field_defn = inputlyr_defn.GetFieldDefn(i)
                bufferlyr.CreateField(field_defn)
            for feature in inputlyr:
                ingeom = feature.GetGeometryRef()
                bufferValue = feature.GetField(bufferField)   #This uses bufferField to get the buffervalue. We can assign a buffervalue directly 
                geomBuffer = ingeom.Buffer(bufferValue)
                outFeature = ogr.Feature(featureDefn)
                for i in range(inputlyr_defn.GetFieldCount()):  # #Keep original fields values
                    field_name = inputlyr_defn.GetFieldDefn(i).GetName()
                    outFeature.SetField(field_name, feature.GetField(i))
                outFeature.SetGeometry(geomBuffer)
                bufferlyr.CreateFeature(outFeature)
                outFeature = None
            #This code defines a function createBuffer() that takes three arguments: inputfn, outputBufferfn, and bufferField. The function reads the input shapefile, creates a new shapefile for
            #the output buffer, and iterates over each feature in the input layer. For each feature, it retrieves the buffer distance from the specified field and creates a buffer around the feature
            #geometry using that distance. The resulting buffer feature is then written to the output layer.
        return outputBufferfn
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for createBuffer with inputs of "+ repr(inputfn)+ repr(bufferField)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog



def ReturnStateFullAndShortNameFromCode(StateCodeList):   #gdal x and y, stated automatical
    try:
        print("The lookup table come from https://www.bls.gov/respondents/mwr/electronic-data-interchange/appendix-d-usps-state-abbreviations-and-fips-codes.htm")
        print("I compareed with FVS manual appendix B for consistence on the state code. I confirm they match well")
        StateFullShortCode = ["Alabama-AL-1",
                              "Alaska-AK-2",
                              "Arizona-AZ-4",
                              "Arkansas-AR-5",
                              "California-CA-6",
                              "Colorado-CO-8",
                              "Connecticut-CT-9",
                              "Delaware-DE-10",
                              "District of Columbia-DC-11",
                              "Florida-FL-12",
                              "Georgia-GA-13",
                              "Hawaii-HI-15",
                              "Idaho-ID-16",
                              "Illinois-IL-17",
                              "Indiana-IN-18",
                              "Iowa-IA-19",
                              "Kansas-KS-20",
                              "Kentucky-KY-21",
                              "Louisiana-LA-22",
                              "Maine-ME-23",
                              "Maryland-MD-24",
                              "Massachusetts-MA-25",
                              "Michigan-MI-26",
                              "Minnesota-MN-27",
                              "Mississippi-MS-28",
                              "Missouri-MO-29",
                              "Montana-MT-30",
                              "Nebraska-NE-31",
                              "Nevada-NV-32",
                              "New Hampshire-NH-33",
                              "New Jersey-NJ-34",
                              "New Mexico-NM-35",
                              "New York-NY-36",
                              "North Carolina-NC-37",
                              "North Dakota-ND-38",
                              "Ohio-OH-39",
                              "Oklahoma-OK-40",
                              "Oregon-OR-41",
                              "Pennsylvania-PA-42",
                              "Puerto Rico-PR-72",
                              "Rhode Island-RI-44",
                              "South Carolina-SC-45",
                              "South Dakota-SD-46",
                              "Tennessee-TN-47",
                              "Texas-TX-48",
                              "Utah-UT-49",
                              "Vermont-VT-50",
                              "Virginia-VA-51",
                              "Virgin Islands-VI-78",
                              "Washington-WA-53",
                              "West Virginia-WV-54",
                              "Wisconsin-WI-55",
                              "Wyoming-WY-56",
                              ]
        StateFullNameList = []
        StateShortNameList = []
        for StateCode in StateCodeList:
            for k in range(0,len(StateFullShortCode),1):
                if StateFullShortCode[k].split("-")[2].zfill(4) == StateCode:
                    StateFullName = StateFullShortCode[k].split("-")[0]
                    StateShortName = StateFullShortCode[k].split("-")[1]
                    StateFullNameList.append(StateFullName)
                    StateShortNameList.append(StateShortName)
        return StateFullNameList,StateShortNameList
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for ReturnStateFullAndShortNameFromCode with inputs of "+ repr(StateCodeList)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog



def SelectPointsFromPolygonWithGDAL(PolygonShapeFile,FIAtrueCoordinatesSource,FIAconfidentialPlotBinary,Factor):
    try:
        if not os.path.exists(FIAconfidentialPlotBinary):
            FIAconfidentialPlotBinaryFile = open(FIAconfidentialPlotBinary, "wb")  #https://stackoverflow.com/questions/24420820/how-to-write-on-new-string-uses-byte-wb-mode
            SelectedStateCode = []
            HslLines = []
            inDriver = ogr.GetDriverByName("ESRI Shapefile")
            polygon_ds = inDriver.Open(PolygonShapeFile)
            polygon_layer = polygon_ds.GetLayer()
            polygon_field_names = [field.name for field in polygon_layer.schema]
            print("polygon_field_names=",polygon_field_names)
            for polygon_feature in polygon_layer:
                polygon_geometry = polygon_feature.GetGeometryRef()  #THis returns the geometry from the polygon, see spatial geometry and spatialfilter at https://pcjericks.github.io/py-gdalogr-cookbook/layers.html#spatial-filter
                EastOrWesternFVSvariant = polygon_feature.GetField('WestEast')  #WestEast is a field in fhaastf3tiles shape file indicating "WesternSpeciesTranslator" or "EasternSpeciesTranslator"
                print("EastOrWesternFVSvariant=",EastOrWesternFVSvariant)
                for PointShapeFile in FIAtrueCoordinatesSource:
                    if os.path.exists(PointShapeFile):
                        point_ds = inDriver.Open(PointShapeFile)
                        point_layer = point_ds.GetLayer()
                        point_layer.SetSpatialFilter(polygon_geometry)  #20240222 added to narrow down the selection by using gdal .SetSpatialFilter. Without this, processing may need 3-4 times longer
                        #point_layer.BuildSpatialIndex()  # Create a spatial index for the point layer; however, on 20240126 I found we have an error. I commented out and found no infulence
                        for point_feature in point_layer:
                            point_geometry = point_feature.GetGeometryRef()
                            if polygon_geometry.Contains(point_geometry):  #note the word "contains"
                                StateCode = str(int(point_feature.GetField('STATECD'))).zfill(4)   #four digits. 20240129: Important it is GetField('STATECD') not GetField['STATECD']
                                CountyCode = str(int(point_feature.GetField('COUNTYCD'))).zfill(3)  #three digits
                                UnitCode = str(int(point_feature.GetField('UNITCD'))+10).zfill(2)  #two digits. !!!!VERY IMPORTANT: we have a +10 here. Very special for F3
                                PlotCode = str(int(point_feature.GetField('PLOT'))).zfill(5)   #five digits
                                UIDCode = str(point_feature.GetField('UID'))   #In UID, -1 is the central coordinates of the center subplot, -2 is the north, -3 is the SouthEast, and -4 is the SouthWest
                                SUBPCode = str(point_feature.GetField('SUBP'))
                                F3PlotUniqueID = UnitCode + StateCode + CountyCode +  PlotCode
                                F3PlotUniqueIDAlberX = float(point_feature.GetField('albersx'))   #If only Lat/Long are availabe, then we have to use F3PlotUniqueIDAlberX, F3PlotUniqueIDAlberY = convert_to_albers(LATUTUDE, LONGITUDE)
                                F3PlotUniqueIDAlberY = float(point_feature.GetField('albersy'))   #If only Lat/Long are availabe, then we have to use F3PlotUniqueIDAlberX, F3PlotUniqueIDAlberY = convert_to_albers(LATUTUDE, LONGITUDE)
                                if SUBPCode == "1":   #20240124: SUBP=1 means it is the central subplot. We can also use UIDCode[-2:] == "_1"
                                    FIADB_PLOTvalue = str(int(F3PlotUniqueID) * Factor)   #20230509 correction: str(int(row.getValue("PLOT_FIADB")) * factor) was replace by str(float(row.getValue("PLOT_FIADB")) * factor)
                                    POINT_Xvalue = str(float(F3PlotUniqueIDAlberX * Factor))
                                    POINT_Yvalue = str(float(F3PlotUniqueIDAlberY * Factor))
                                    Line = FIADB_PLOTvalue+","+POINT_Xvalue+","+POINT_Yvalue+"\n"
                                    HslLines.append(Line)
                                    SelectedStateCode.append(StateCode)
                        point_layer.ResetReading()
                        point_ds = None
            polygon_ds = None
            SelectedStateCodeUnique = list(set(SelectedStateCode))
            StateFullNameList,StateShortNameList = ReturnStateFullAndShortNameFromCode(SelectedStateCodeUnique)
            FIAconfidentialPlotBinaryLine = "###"+FIAconfidentialPlotBinary+" was created by Dr. Shengli Huang with a factor of "+str(Factor)+" at " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"\n"
            SelectedStateCodeUniqueLine = "###The states Code (from FVS) spatially covered  by "+PolygonShapeFile+" include "+",".join(SelectedStateCodeUnique)+"\n"
            StateFullNameListLine = "###The states full name spatially covered  by "+PolygonShapeFile+" include "+",".join(StateFullNameList)+"\n"
            StateShortNameListLine = "###The states short name spatially covered  by "+PolygonShapeFile+" include "+",".join(StateShortNameList)+"\n"
            EastOrWesternFVSvariantLine = "###FVS easter variants (CS, LS, NE, SN) and western variants (AK, BM, CA, CI, CR, EC, EM, IE, KT, NC, OC, OP, PN, SO, TT, UT, WC, WS) have different species crosswalk between FIA and FVS. This tile uses " + EastOrWesternFVSvariant +"\n"
            FIAconfidentialPlotBinaryFile.write(FIAconfidentialPlotBinaryLine.encode('utf-8'))
            FIAconfidentialPlotBinaryFile.write(SelectedStateCodeUniqueLine.encode('utf-8'))
            FIAconfidentialPlotBinaryFile.write(StateFullNameListLine.encode('utf-8'))
            FIAconfidentialPlotBinaryFile.write(StateShortNameListLine.encode('utf-8'))
            FIAconfidentialPlotBinaryFile.write(EastOrWesternFVSvariantLine.encode('utf-8'))
            for HslLine in HslLines:
                FIAconfidentialPlotBinaryFile.write(HslLine.encode('utf-8'))
            FIAconfidentialPlotBinaryFile.close()
        return FIAconfidentialPlotBinary
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for SelectPointsFromPolygonWithGDAL with inputs of "+repr(PolygonShapeFile)+repr(FIAtrueCoordinatesSource)+repr(FIAconfidentialPlotBinary)+repr(StateCodeList)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


    
def AddXandYCoordinatesForPointGISshapeFile(PointGISshapeFile,FieldXname, FieldYname):
    try:
        #20240130: The error message “SetFeature : unsupported operation on a read-only datasource” is raised when you try to modify a read-only data source. This error can occur when you try to modify a shapefile that is opened in read-only mode.
        #To fix this error, you need to open the data source in write mode. You can do this by passing the GDAL_OF_UPDATE flag to the Open() method of the gdal module
        #ds = gdal.OpenEx('your_shapefile.shp', gdal.OF_VECTOR | gdal.OF_UPDATE)  # Open the dataset in update mode
        
        inDriver = ogr.GetDriverByName("ESRI Shapefile")
        shapefile = inDriver.Open(PointGISshapeFile,1)  # Open the shapefile. I added 1 here to indicate write mode
        layer = shapefile.GetLayer()  # Get the layer

        field_names = []
        layer_defn = layer.GetLayerDefn()
        for i in range(layer_defn.GetFieldCount()):  # Set the attribute values from source layer
            field_name = layer_defn.GetFieldDefn(i).GetName()
            field_names.append(field_name)
        print("field_names=",field_names)
        if ((FieldXname in field_names) or (FieldYname in field_names)):
            print(FieldXname, " and ", FieldXname, " are already in the fields, so no need to add them, quit!")
        else:
            layer.CreateField(ogr.FieldDefn(FieldXname, ogr.OFTReal))  # Add x and y fields
            layer.CreateField(ogr.FieldDefn(FieldYname, ogr.OFTReal))
            for feature in layer:  # Loop through the features
                geometry = feature.GetGeometryRef() # Get the geometry
                x = geometry.GetX()  # Get the x and y coordinates
                y = geometry.GetY()
                #print("x,y=",x,y)
                feature.SetField(FieldXname, x)  # Set the x and y fields
                feature.SetField(FieldYname, y)
                layer.SetFeature(feature)   # Update the feature
        shapefile = None  # Close the shapefile
        return PointGISshapeFile
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for AddXandYCoordinatesForPointGISshapeFile with inputs of "+repr(PointGISshapeFile)+repr(FieldXname)+repr(FieldYname)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html 
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def ReprojectGISshapeFileToAtifProjection(SourceShapeFile, ReferencedTif): #referenced from the bing chat "python code of gdal to reproject a GIS shape file and keep the attribute table"
    try:
        ReprojectedShapeFile = SourceShapeFile.replace(".shp","_Albers.shp")  #I need we the target projection is Albers. If it is not, please give a new name
        if not os.path.exists(ReprojectedShapeFile):
            tif = gdal.Open(ReferencedTif)  #tif with projections I want
            driver = ogr.GetDriverByName("ESRI Shapefile") #shapefile with the from projection
            dataSource = driver.Open(SourceShapeFile, 1)
            sourcelayer = dataSource.GetLayer()
            sourceprj = sourcelayer.GetSpatialRef()  #set spatial reference and transformation
            targetprj = osr.SpatialReference(wkt = tif.GetProjection())
            transform = osr.CoordinateTransformation(sourceprj, targetprj)
            to_fill = ogr.GetDriverByName("Esri Shapefile")
            ds = to_fill.CreateDataSource(ReprojectedShapeFile)
            outlayer = ds.CreateLayer('', targetprj, ogr.wkbPoint)  #20240130 ogr.wkbPolygon changed to ogr.wkbPoint because I have an error [Attempt to write non-polygon (POINT) geometry to POLYGON type shapefile]
            ###This part is not used for the coding here, but very useful for future projection set up, so keep here for a treasure---start
            #out_srs = osr.SpatialReference() # Define the output projection
            #out_srs.ImportFromEPSG(4326)  # WGS84
            #out_driver = ogr.GetDriverByName('ESRI Shapefile')
            #out_ds = out_driver.CreateDataSource('your_output_shapefile.shp')
            #out_layer = out_ds.CreateLayer('out_layer', out_srs, ogr.wkbPoint) # Create the output shapefile
            ###This part is not used for the coding here, but very useful for future projection set up, so keep here for a treasure---end
            sourcelayer_defn = sourcelayer.GetLayerDefn()  # Add fields from source layer to the output layer
            for i in range(sourcelayer_defn.GetFieldCount()):
                field_defn = sourcelayer_defn.GetFieldDefn(i)
                outlayer.CreateField(field_defn)
            i = 0  #apply transformation
            for feature in sourcelayer:
                transformed = feature.GetGeometryRef()
                transformed.Transform(transform)
                geom = ogr.CreateGeometryFromWkb(transformed.ExportToWkb())
                defn = outlayer.GetLayerDefn()
                feat = ogr.Feature(defn)
                for i in range(sourcelayer_defn.GetFieldCount()):  # Set the attribute values from source layer
                    field_name = sourcelayer_defn.GetFieldDefn(i).GetName()
                    feat.SetField(field_name, feature.GetField(i))
                feat.SetGeometry(geom)
                outlayer.CreateFeature(feat)
                i += 1
                feat = None
            ds = None
        return ReprojectedShapeFile
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for ReprojectGISshapeFileToAtifProjection with inputs of "+repr(SourceShapeFile)+repr(ReferencedTif)+ " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html 
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog




def PlotIDandXandY(ShengliHuangKeyFile,Factor):
    try:
        ShengliHuangKey = open(ShengliHuangKeyFile, "rb")
        MyBinaryLine = ShengliHuangKey.readlines()  #readline only read one line
        MyBinaryLine = [i.decode().replace('\n','') for i in MyBinaryLine]  #We had error [A bytes-like object is required, not 'str'], so We must use decode() here, see https://stackoverflow.com/questions/50829364/cannot-split-a-bytes-like-object-is-required-not-str
        plotlist = []
        xlist = []
        ylist = []
        for line in MyBinaryLine:
            if line.startswith('###'):
                if "The states Code (from FVS) spatially covered" in line:
                    StatesCodeInThisHSL = line.split(" include ")[1].split(",")  #Noth this sentence with the word [ include ] is very important
                if "The states full name spatially covered" in line:
                    StatesFullNameInThisHSL = line.split(" include ")[1].split(",")  #Noth this sentence with the word [ include ] is very important
                if "The states short name spatially covered" in line:
                    StatesShortNameInThisHSL = line.split(" include ")[1].split(",")  #Noth this sentence with the word [ include ] is very important
                if "FVS easter variants (CS, LS, NE, SN) and western variants (AK, BM, CA, CI, CR, EC, EM, IE, KT, NC, OC, OP, PN, SO, TT, UT, WC, WS) have" in line:
                    SpeciesTranslatorInThisHSL = line.split("This tile uses ")[1]  #Noth this sentence with the word [This tile use ] is very important
            if not line.startswith('###'):  #[###] was added in the file on 20240202
                plot = round(float(line.split(",")[0]) / float(Factor))  #20230509: change int() to round(). e.g. round(2.999)=3 while int(2.999)=2
                x = float(line.split(",")[1]) / float(Factor)
                y = float(line.split(",")[2]) / float(Factor)
                plotlist.append(str(plot))  ##20240209: change plot to str(plot) to assure the type is consistent with the type of element in plotlist
                xlist.append(x)
                ylist.append(y)
        ShengliHuangKey.close()
        return plotlist,xlist,ylist,StatesCodeInThisHSL,StatesFullNameInThisHSL,StatesShortNameInThisHSL,SpeciesTranslatorInThisHSL
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for PlotIDandXandY with inputs of " + repr(ShengliHuangKeyFile)+ repr(Factor)+" with the following details:\n"
        #print(Content2)
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def CreateFIADBUniquePlotIDfromFvsReadyStandID(STAND_ID):   
    try:
        #note in FVS_ready database, Stand_ID = concatenate((PLOT.STATECD(4) + PLOT.INVYR(4) + PLOT.CYCLE(2) + PLOT.SUBCYCLE(2) + PLOT.UNITCD(2) + PLOT.COUNTYCD(3) + PLOT.PLOT(5)).
        StateCode = STAND_ID[0:4]  #The first 4 digits in FIA_FVSready FVS_STANDINIT_PLOT table is state code. Four digits
        #UnitCode is usually like 03, 05, 08 etc. Below I change it to 13, 15, and 18 etc to make sure the FIADB_PLOT will always have the same number of digits for processing
        UnitCode = str(int(STAND_ID[12:14])+10)  #Survey unit code. Forest Inventory and Analysis survey unit identification number. Survey units are usually groups of counties within each State. Two digits
        CountyCode = STAND_ID[14:17]  ##The last 8 digits to 5 digits in FIA_FVSready FVS_STANDINIT_PLOT table is county code. Three digits
        PlotCode = STAND_ID[-5:]   #The last 5 digits in FIA_FVSready FVS_STANDINIT_PLOT table is plot code. Five digits
        FIADB_PLOT = UnitCode + StateCode + CountyCode +  PlotCode   #Since 20231229, I decided to create unique plot id. The original STABD_ID=0001197204000300190045, then FIADB_PLOT= 13000100190045 
        return FIADB_PLOT    
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateFIADBUniquePlotIDfromFvsReadyStandID with inputs of " + repr(STAND_ID)+" with the following details:\n"
        #print(Content2)
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def CreatePlotIDandTheMeasurementMonthYear(EachTileSqliteFIADBs,plotlist):
    try:
        #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&plotlist=",plotlist, file=open(F3Log, 'a'))
        #This part read .csv or .db file to get the plotID and the measurment time---start
        FiaPlotList = []
        FiaMeasurementTime = []
        for EachTileSqliteFIADB in EachTileSqliteFIADBs:
            FileSize = os.path.getsize(EachTileSqliteFIADB)
            FileTime = os.path.getctime(EachTileSqliteFIADB)
            print(EachTileSqliteFIADB," is a sqlite file; it is actally downloaded from FIA datamart with time of ",FileTime, " and with size of ", FileSize, " bytes")
            conn = sqlite3.connect(EachTileSqliteFIADB)  #https://www.sqlitetutorial.net/sqlite-python/
            cur= conn.cursor()
            PlotYearMonthTabe = "FVS_STANDINIT_PLOT"  #Options are "PLOT" or "FVS_STANDINIT_PLOT", note as of 20240102, only option FVS_STANDINIT_PLOT is working due to the change of unique PlotID
            if PlotYearMonthTabe == "PLOT":    
                cur.execute("SELECT * FROM PLOT")  #Ask Marcus to give the table name consistently. 20230331: I think we may need the FVS tables. Let us see
                columns = [column[0] for column in cur.description]
                print("columns=",columns)
                FiaPlotIndex = columns.index("PLOT")
                STATECDIndex = columns.index("STATECD")
                COUNTYIndex = columns.index("COUNTYCD")
                MEASYEARIndex = columns.index("MEASYEAR")
                MEASMONIndex = columns.index("MEASMON")
                UNITCDIndex = columns.index("UNITCD")
                rows = cur.fetchall()
                for row in rows:
                    UnitCode = str(int(row[UNITCDIndex])+10)
                    FiaPlot = UnitCode + str(int(row[STATECDIndex])).zfill(4) + str(int(row[COUNTYIndex])).zfill(3) + str(int(row[FiaPlotIndex])).zfill(5)  #20240102 added
                    if FiaPlot in plotlist:   #added on 20240124, note the ".hsl" part was moved before this part
                        FiaPlotList.append(FiaPlot)
                        JoinField = FiaPlot + str(int(row[MEASMONIndex])).zfill(2) + str(int(row[MEASYEARIndex]))  #https://stackoverflow.com/questions/134934/display-number-with-leading-zeros
                        FiaMeasurementTime.append(JoinField)  #JoinField is like 59104092016
                cur.close()
            if PlotYearMonthTabe == "FVS_STANDINIT_PLOT":  #added on 20230403 for more flexibility and conected to FVS modeling   
                cur.execute("SELECT * FROM FVS_STANDINIT_PLOT")  #Ask Marcus to give the table name consistently. 20230331: I think we may need the FVS tables. Let us see
                columns = [column[0] for column in cur.description]
                #print("columns=",columns)
                STAND_IDIndex = columns.index("STAND_ID")
                MEASYEARIndex = columns.index("INV_YEAR")
                MEASMONIndex = columns.index("INV_MONTH")
                rows = cur.fetchall()
                for row in rows:
                    STAND_ID = str(row[STAND_IDIndex])
                    FiaPlot = CreateFIADBUniquePlotIDfromFvsReadyStandID(STAND_ID) #20240102 added to replace FiaPlot = str(STAND_ID[-5:]) for new UniquePotID
                    #print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&FiaPlot=",FiaPlot, file=open(F3Log, 'a'))
                    if FiaPlot in plotlist:   #added on 20240124, note the ".hsl" part was moved before this part
                        print(FiaPlot," is included", file=open(F3Log, 'a'))
                        FiaPlotList.append(FiaPlot)
                        JoinField = FiaPlot + str(int(row[MEASMONIndex])).zfill(2) + str(int(row[MEASYEARIndex]))  #https://stackoverflow.com/questions/134934/display-number-with-leading-zeros
                        FiaMeasurementTime.append(JoinField)
                cur.close()
        FiaPlotListUnique = [i for n,i in enumerate(FiaPlotList) if i not in FiaPlotList[:n]]  #unqiue
        ThisPlotAllJoinFieldList = []
        for k in FiaPlotListUnique:
            ThisPlotJoinField = []
            mid = -1
            for m in FiaPlotList:
                mid = mid + 1
                if k == m:
                    ThisPlotJoinField.append(FiaMeasurementTime[mid])
            ThisPlotAllJoinField = "-".join(ThisPlotJoinField)  #ThisPlotAllJoinField is like 59104102005-59104092016-59104072022
            ThisPlotAllJoinFieldList.append(ThisPlotAllJoinField)
        return ThisPlotAllJoinFieldList,FiaPlotListUnique
        #This part read .csv or .db file to get the plotID and the measurment time---start
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreatePlotIDandTheMeasurementMonthYear with inputs of " + repr(EachTileSqliteFIADBs)+ repr(len(plotlist))+" with the following details:\n"
        #print(Content2)
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def CreateAnnualChangeVariables(Tile,YearsOfAnnualChange):
    try:
        #This part indicates the Annual Change and FastLossProb raster and their variable names as Yxxxx----start
        print("Please update YearsOfAnnualChange frequently according to the GEE data update. Please make sure you have the corresponding data, otherwise we will have problem")
        AnnualChangeRasters = []
        for Year in YearsOfAnnualChange:    
            ThisYearAnnualChange = LandCoverChangeAnnualRastersPath + os.sep + Tile + os.sep+"ChangeFrom"+str(Year)+"0101To"+str(Year)+"1230.tif"
            AnnualChangeRasters.append(ThisYearAnnualChange)
            ThisYearAnnualFastLoss = LandCoverChangeAnnualRastersPath + os.sep + Tile + os.sep + "FastLossProbFrom"+str(Year)+"0101To"+str(Year)+"1230.tif"
        print("AnnualChangeRasters=",AnnualChangeRasters)

        AnnualChangeInfo = AnnualChangeRasters[0]  #Use the first TIF to get the extent and projection etc.
        driverTiff = gdal.GetDriverByName('GTiff')
        ds = gdal.Open(AnnualChangeInfo)
        AnnualChangeInfoArray = ds.GetRasterBand(1).ReadAsArray()
        AnnualChangeInfoNoDataValue = ds.GetRasterBand(1).GetNoDataValue()
        print("AnnualChangeInfoNoDataValue=",AnnualChangeInfoNoDataValue)
        srcCols = ds.RasterXSize
        srcRows = ds.RasterYSize
        WeUseThisProjection = ds.GetProjectionRef()
        WeUseThisGeoTransform = ds.GetGeoTransform()
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx",WeUseThisGeoTransform)
        upx, xres, xskew, upy, yskew, yres = WeUseThisGeoTransform
        ulx = float(upx + 0*xres + 0*xskew)
        uly = float(upy + 0*yskew + 0*yres)
        llx = float(upx + 0*xres + srcRows*xskew) 
        lly = float(upy + 0*yskew + srcRows*yres)      
        lrx = float(upx + srcCols*xres + srcRows*xskew)
        lry = float(upy + srcCols*yskew + srcRows*yres)
        urx = float(upx + srcCols*xres + 0*xskew)
        ury = float(upy + srcCols*yskew + 0*yres)
        print("ulx,uly,lrx,lry=",ulx,uly,lrx,lry)
        print("------Step1ContinuousPrediction start")
        AnnualChangeVariables = ["Y"+str(YearsOfAnnualChange[k]) for k in range(0,len(AnnualChangeRasters),1)]
        print("AnnualChangeVariables=",AnnualChangeVariables)
        print("if ((x >= ulx) and (x <= lrx) and (y >= lry) and (y <= uly)) is used, so this TIF will be used to choose partial FIA plots (not all FIA plots are used)")    
        #This part indicates the Annual Change and FastLossProb raster and their variable names as Yxxxx----end
        return AnnualChangeVariables,Tile,ulx,uly,lrx,lry,AnnualChangeRasters,srcCols,srcRows
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateAnnualChangeVariables with inputs of "+ repr(Tile) + " " + repr(YearsOfAnnualChange)+" with the following details:\n"
        #print(Content2)
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog


def FIAplotAnnualChangeWithTheTile(Tile,ulx,uly,lrx,lry,AnnualChangeVariables,AnnualChangeRasters,srcCols,srcRows,ThisPlotAllJoinFieldList,FiaPlotListUnique):
    try:
        #This part creates a CSV file showing each plot's annual change values (note 3 means fast loss)----start
        FieldMetricCSV = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"FIAplotAndChangeDetection.csv"
        ShengliHuangKeyFile = r'D:\CUI\subplots\L48' + os.sep + Tile + ".hsl"
        plotlist,xlist,ylist,StatesCodeInThisHSL,StatesFullNameInThisHSL,StatesShortNameInThisHSL,SpeciesTranslatorInThisHSL = PlotIDandXandY(ShengliHuangKeyFile,Factor)
        if not os.path.exists(FieldMetricCSV):
            print("^^^^^^^^^^^^^^^^^^^^FieldMetricCSV=",FieldMetricCSV)
            FieldMetricCSVFile = open(FieldMetricCSV, 'w')
            Headline = "FIA_PlotID" + "," + ",".join(AnnualChangeVariables) + ",FiaMeasurementTimeJoinList" +"\n"
            print(Headline)
            FieldMetricCSVFile.write(Headline)          

            rowlist = []
            collist = []
            FiaPlotValidList = []
            FiaMeasurementTimeJoinList = []

            Kid = -1
            for plot in plotlist:
                Kid = Kid + 1       
                CoordinateX = xlist[Kid]
                CoordinateY = ylist[Kid]
                if ((CoordinateX >= ulx) and (CoordinateX <= lrx) and (CoordinateY >= lry) and (CoordinateY <= uly)):  #make sure it falls within the extent
                    srcline = str(int(plot))
                    #print("srcline=",srcline, file=open(F3Log, 'a'))
                    for localname in AnnualChangeRasters[0:1]:
                        #print("localname=",localname)
                        row,col = rasterio.open(localname).index(CoordinateX, CoordinateY)
                        #print("row,col=",row,col)
                        if ((row >= 0) and (row < srcRows) and (col >= 0) and (col < srcCols)):
                            nid = -1
                            for n in FiaPlotListUnique:
                                nid = nid + 1
                                if n == plot:
                                    JoinValue = ThisPlotAllJoinFieldList[nid]
                                    rowlist.append(row)
                                    collist.append(col)
                                    FiaPlotValidList.append(plot)
                                    FiaMeasurementTimeJoinList.append(JoinValue)
                                    break
            AnnualChangeRasters_all = np.full((len(AnnualChangeRasters),srcRows,srcCols), F3NoDataValue)
            #print("rowlist=",rowlist, file=open(F3Log, 'a'))
            p=-1
            for localname in AnnualChangeRasters:
                p = p + 1
                driverTiff = gdal.GetDriverByName('GTiff')
                ds = gdal.Open(localname)
                localnameArray = ds.GetRasterBand(1).ReadAsArray()
                localnameNoDataValue = ds.GetRasterBand(1).GetNoDataValue()
                AnnualChangeRasters_all[p,:,:] = localnameArray
            for q in range(0,len(rowlist),1):
                ThePlotValueArray = AnnualChangeRasters_all[:,rowlist[q],collist[q]]
                ThePlotValueList0 = ThePlotValueArray.tolist()  #if array is not converted into list, the following .join cannot be done
                ThePlotValueList = [str(x) for x in ThePlotValueList0]
                print("ThePlotValueList is ",ThePlotValueList)
                if ((str(F3NoDataValue) in ThePlotValueList) or (str(localnameNoDataValue) in ThePlotValueList)):
                    print("This plot falls in void area, we do not use it for further processing")
                    continue
                else:
                    LineToCSV = FiaPlotValidList[q] + "," + ",".join(ThePlotValueList) + "," + FiaMeasurementTimeJoinList[q] + "\n"
                    print("q=", q, " and LineToCSV: ",LineToCSV, file=open(F3Log, 'a'))
                    FieldMetricCSVFile.write(LineToCSV)
            FieldMetricCSVFile.close()
            print(FieldMetricCSV, " was created, but we may improve the speed later because the rasterio.open() can be done only once")
        return FieldMetricCSV,xlist,ylist
        #This part creates a CSV file showing each plot's annual change values (note 3 means fast loss)----end
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for FIAplotAnnualChangeWithTheTile with inputs of "+ repr(Tile) + " " + repr(ulx)+ " " + repr(uly)+ " " + repr(lrx)+ " " + repr(lry)+ " " + repr(AnnualChangeVariables)+ " " + repr(AnnualChangeRasters)+ " " + repr(srcCols)+ " " + repr(srcRows)+ " " + repr(len(ThisPlotAllJoinFieldList)) + " " + repr(len(FiaPlotListUnique)) + " with the following details:\n"
        #print(Content2)                  
        Content3 = repr(strTrace) + "\n\n\n"  #see https://docs.python.org/2/library/traceback.html
        #print(Content3)
        ErrorLogFile.write(Content1)
        ErrorLogFile.write(Content2)
        ErrorLogFile.write(Content3)
        ErrorLogFile.close()
        return ErrorLog



def CreateFinalCleanFIAplotsToRunFVSForTheTile(Tile,RemoteSensingYearOfInterest,plotlist,xlist,ylist):
    try:
        FieldMetricCSV = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"FIAplotAndChangeDetection.csv"
        #This part use 3 fast loss (between survey date and remote sensing date) to drop plot. It is also dropped if the survey is far later than remote sensing date----start
        for k in RemoteSensingYearOfInterest:
            if k > max(YearsOfAnnualChange):
                print("We are OK to do this, but the disturbance may not be complete because the disturbance from ", max(YearsOfAnnualChange)," to ",k," are not reflected in the LCLUC dataset. Please ba cautious!")
            if k < min(YearsOfAnnualChange):
                print("We cannot do it, because the disturbance from ", k," to ",min(YearsOfAnnualChange)," are not available in the LCLUC dataset. We have to quit!")
                exit()
        MaximumYearGap = 7  #If a FIA is measured far later than RemoteSensingTargetYear, it may not be suitable because we can not grow it back using FVS
        for RemoteSensingTargetYear in RemoteSensingYearOfInterest:
            OriginalRemoteSensingTargetYear = RemoteSensingTargetYear
            if RemoteSensingTargetYear > max(YearsOfAnnualChange):  #Added on 20240124
                RemoteSensingTargetYear = max(YearsOfAnnualChange)
            print("*_Dropped.csv are those plots that are not considered in _Traditional.csv plots, including a) disturbance between measurement year and RStarget year; and b) in void area; and c) beyong the tolerance year range")
            print("*_Traditional.csv are those plots that are traditionally treated (i.e., no disturbanced between measurement year and RStarget year; Within a specific year range)")
            print("*_DuplicateDisturbedFVS.csv are those disturbed plots (measurment year is before RStarget year) and have mortality information from GEE annual change products")
            print("*_DisturbedFVS.csv are based on DuplicateDisturbedFVS.csv. But if a plot get distuebd twice, the closest measurment (compared to RStarget year) is chosen. Note all these plots are listed here but it does not mean all of them will be used for RStarget year modeling")
            print("*_Remeasured.csv are remeasured plots (after RStarget year and not disturbed). In FVS run, actual remeasurment should be chosen as a priority")
            
            FieldMetricCSVFile = open(FieldMetricCSV, "r")
            FieldMetricCSV_final = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_Traditional.csv"  #THis list all plots that will be used for final FVS run
            FieldMetricCSV_finalFile = open(FieldMetricCSV_final, "w")
            FieldMetricCSV_finalDisturbedFVS = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_DuplicateDisturbedFVS.csv"  #THis list all plots that will be used for final FVS run
            FieldMetricCSV_finalDisturbedFVSFile = open(FieldMetricCSV_finalDisturbedFVS, "w")
            FieldMetricCSV_finalDropped = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_Dropped.csv"  #This list the dropped plots
            FieldMetricCSV_finalDroppedFile = open(FieldMetricCSV_finalDropped, "w")
            FieldMetricCSV_finalRemeasured = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_Remeasured.csv"  #THis list all plots that will be used for final FVS run
            FieldMetricCSV_finalRemeasuredFile = open(FieldMetricCSV_finalRemeasured, "w")
            Lines = FieldMetricCSVFile.readlines()  #readline only read one line
            Lines = [i.replace('\n','') for i in Lines]     
            Fields = Lines[0].split(",")
            print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&Fields=",Fields)
            
            MeasureYearsNotDisturbedPlotList = []
            MeasureYearsNotDisturbedFiaMeasurementTimeJoin = []
            DisturbedSelectedPlotList = []
            DisturbedSelectedFiaMeasurementTimeJoin = []
            RemeasuredPlotList = []
            RemeasuredFiaMeasurementTimeJoin = []

            for Line in Lines[1:]:
                LineSplit = Line.split(",")
                FiaPlotID = LineSplit[0]
                FiaMeasurementTimeJoin = [k for k in LineSplit[-1].split("-")] 
                FiaPlotMeasureYears = [k[-4:] for k in LineSplit[-1].split("-")]  #50014092019-50014092009, separate it using "-" and then use the last four digits as year
                print("\nFiaPlotID,FiaPlotMeasureYears=",FiaPlotID,FiaPlotMeasureYears)
                MeasureYearsNotDisturbedForThisPlot = []
                for FiaPlotMeasureYear in FiaPlotMeasureYears:
                    RemoteSensingTargetYearField = "Y"+str(RemoteSensingTargetYear)
                    RemoteSensingTargetYearFieldIndex = Fields.index(RemoteSensingTargetYearField)
                    FiaPlotMeasureYearField = "Y"+str(FiaPlotMeasureYear)
                    FiaPlotMeasureYearFieldIndex = Fields.index(FiaPlotMeasureYearField)
                    if RemoteSensingTargetYearFieldIndex >= FiaPlotMeasureYearFieldIndex:
                        AnnualChangeValue = LineSplit[FiaPlotMeasureYearFieldIndex:RemoteSensingTargetYearFieldIndex+1]  #We use RemoteSensingTargetYearFieldIndex+1 instead of RemoteSensingTargetYearFieldIndex here, because we want to AGGRESSIVELY discard the disturbed plot
                    if RemoteSensingTargetYearFieldIndex < FiaPlotMeasureYearFieldIndex:
                        AnnualChangeValue = LineSplit[RemoteSensingTargetYearFieldIndex:FiaPlotMeasureYearFieldIndex+1]  #We use FiaPlotMeasureYearFieldIndex+1 instead of FiaPlotMeasureYearFieldIndex here, because we want to AGGRESSIVELY discard the disturbed plot
                        #This section is to get the remeasured plots with three criteria listed below (Criteria 3 in a separate position)----------------------------------------------------start
                        ##Criteria 1: Remeasurement Year is later than the RSYyear
                        ##Criteria 2: Fast loss and slow loss do not happen between RSY2012 and remeasuremnt year (i.e., the remeasured plot is natural succession without disturbance)
                        ##Criteria 3: The remeasured plot must be already in the Tradition or DisturbedFVS plots
                        ##Criteria 4: Plot and MeasureTime pair does not exist in the Tradition or DisturbedFVS plots
                        if "2" not in AnnualChangeValue: ##Criteria 2: Fast loss and slow loss do not happen between RSY2012 and remeasuremnt year (i.e., the remeasured plot is natural succession without disturbance), note#1:Stable, 2:Slow Loss, 3:Fast Loss, 4:Gain, 5:Non-Processing Area Mask
                            if "3" not in AnnualChangeValue:
                                if not all(m == "0" for m in AnnualChangeValue):
                                    print("For ", RemoteSensingTargetYear, " ", FiaPlotID,"", FiaPlotMeasureYear, " is not disturbed and remeasured between ",Fields[RemoteSensingTargetYearFieldIndex]," and ",Fields[FiaPlotMeasureYearFieldIndex] ,file=open(F3Log, 'a'))
                                    RemeasuredPlotList.append(FiaPlotID)
                                    FiaMeasurementTimeJoinForThisPlot = FiaMeasurementTimeJoin[FiaPlotMeasureYears.index(str(FiaPlotMeasureYear))]
                                    RemeasuredFiaMeasurementTimeJoin.append(FiaMeasurementTimeJoinForThisPlot)
                        #This section is to get the remeasured plots with three criteria listed below (Criteria 3 in a separate position)----------------------------------------------------end            
                    #print("AnnualChangeValue=",AnnualChangeValue, " and RemoteSensingTargetYearFieldIndex,FiaPlotMeasureYearFieldIndex=",RemoteSensingTargetYearFieldIndex,FiaPlotMeasureYearFieldIndex)
                    if "3" in AnnualChangeValue:
                        DropLine1 = "FiaPlotID of "+FiaPlotID+" with measurement year of "+str(FiaPlotMeasureYear)+" is discarded because it was disturbed as indicaited by the value of 3 in USFS annual change product" + "\n"
                        FieldMetricCSV_finalDroppedFile.write(DropLine1)
                        #20230424 added: oroginally we removed the disturbed plot, but here we added the disturbance information for DRAST purpose---start
                        if RemoteSensingTargetYearFieldIndex >= FiaPlotMeasureYearFieldIndex:  #Only when MeasureYear is before RemoteSensingTargetYear, FVS mortality modeling is meaningful
                            FiaMeasurementTimeJoinForThisPlot = FiaMeasurementTimeJoin[FiaPlotMeasureYears.index(str(FiaPlotMeasureYear))]
                            #print("see https://www.tutorialspoint.com/last-occurrence-of-some-element-in-a-list-in-python to see the ClosestDisturbanceYearIndex")
                            ClosestDisturbanceYearIndex = max(index for index, item in enumerate(AnnualChangeValue) if item == "3")  #If a plot was burned more than once, we only use the closest disturbance event
                            DisturbanceYear = int(FiaPlotMeasureYear) + ClosestDisturbanceYearIndex      
                            for m in range(0, len(plotlist),1): 
                                if plotlist[m] == FiaPlotID:
                                    PlotCoordinates = str(xlist[m]) + "," + str(ylist[m])
                                    CoordinateX = float(xlist[m])
                                    CoordinateY = float(ylist[m])
                            localname = LandCoverChangeAnnualRastersPath +os.sep + Tile +os.sep + "FastLossProbFrom"+str(DisturbanceYear)+"0101To"+str(DisturbanceYear)+"1230.tif"
                            row,col = rasterio.open(localname).index(CoordinateX, CoordinateY)
                            ds = gdal.Open(localname)
                            localnameArray = ds.GetRasterBand(1).ReadAsArray()
                            localnameNoDataValue = ds.GetRasterBand(1).GetNoDataValue()                   
                            MortalityPercentage = localnameArray[row,col]  #If we do not have GEE fast-loss probality, we can use RAVG (Fast-loss data, see https://developers.google.com/earth-engine/datasets/catalog/USFS_GTAC_LCMS_v2021-7?hl=en#bands)
                            
                            DisturbedLine = "FiaPlotID of ",FiaPlotID, " with measurement year of "+str(FiaPlotMeasureYear)+ " with the DisturbanceYear of " + str(DisturbanceYear)+ " with mortality of " + str(MortalityPercentage) + "% is chosen for FVS modeling, especially for DRAST modeling"
                            #print("DisturbedLine=",DisturbedLine,file=open(F3Log, 'a'))
                            DisturbedLineContent = FiaMeasurementTimeJoinForThisPlot +"," + str(DisturbanceYear) + "/" + str(MortalityPercentage)
                            print("DisturbedLineContent=",DisturbedLineContent,file=open(F3Log, 'a'))
                            FieldMetricCSV_finalDisturbedFVSFile.write(DisturbedLineContent+"\n")
                            DisturbedSelectedPlotList.append(FiaPlotID) #Added on 20230601
                            FiaMeasurementTimeJoinForThisPlot = FiaMeasurementTimeJoin[FiaPlotMeasureYears.index(str(FiaPlotMeasureYear))]
                            DisturbedSelectedFiaMeasurementTimeJoin.append(FiaMeasurementTimeJoinForThisPlot)
                        #20230424 added: oroginally we removed the disturbed plot, but here we added the disturbance information for DRAST purpose---end
                    elif all(k == "0" for k in AnnualChangeValue):  #https://datascienceparichay.com/article/python-check-if-all-elements-in-list-are-zero/
                        DropLine2 = "FiaPlotID of "+FiaPlotID+" with measurement year of "+str(FiaPlotMeasureYear)+" is discarded because it was located in void/masked area as indicaited by ALL the value of 0 in USFS annual change product" + "\n"
                        FieldMetricCSV_finalDroppedFile.write(DropLine2)
                    elif ((RemoteSensingTargetYearFieldIndex - FiaPlotMeasureYearFieldIndex) < (-1 * MaximumYearGap)):
                        DropLine3 = "FiaPlotID of "+FiaPlotID+" with measurement year of "+str(FiaPlotMeasureYear)+" is discarded because it was measured after RemoteSensingTargetYear="+str(RemoteSensingTargetYear)+" and beyond the MaximumYearGap of "+str(MaximumYearGap)+" years" + "\n"          
                        FieldMetricCSV_finalDroppedFile.write(DropLine3)
                    else:
                        MeasureYearsNotDisturbedForThisPlot.append(int(FiaPlotMeasureYear))
                        FiaMeasurementTimeJoinForThisPlot = FiaMeasurementTimeJoin[FiaPlotMeasureYears.index(str(FiaPlotMeasureYear))]
                        MeasureYearsNotDisturbedFiaMeasurementTimeJoin.append(FiaMeasurementTimeJoinForThisPlot)
                                
                if len(MeasureYearsNotDisturbedForThisPlot) >= 1:
                    print("MeasureYearsNotDisturbedForThisPlot=",MeasureYearsNotDisturbedForThisPlot)
                    MeasureYearsNotDisturbedForThisPlotComparedWithRemoteSensingTargetYear = [abs(k-int(RemoteSensingTargetYear)) for k in MeasureYearsNotDisturbedForThisPlot]
                    print("MeasureYearsNotDisturbedForThisPlotComparedWithRemoteSensingTargetYear=",MeasureYearsNotDisturbedForThisPlotComparedWithRemoteSensingTargetYear)
                    ClosestYear = MeasureYearsNotDisturbedForThisPlot[MeasureYearsNotDisturbedForThisPlotComparedWithRemoteSensingTargetYear.index(min(MeasureYearsNotDisturbedForThisPlotComparedWithRemoteSensingTargetYear))]
                    print("ClosestYear=",ClosestYear)
                    print("FiaPlotMeasureYears=",FiaPlotMeasureYears)
                    FiaMeasurementTimeJoinForThisPlot = FiaMeasurementTimeJoin[FiaPlotMeasureYears.index(str(ClosestYear))]
                    print("FiaMeasurementTimeJoinForThisPlot=",FiaMeasurementTimeJoinForThisPlot)
                    print("FiaPlotID of ",FiaPlotID, " with measurement year of ", ClosestYear, " should be chosen for FVS modeling, and the FiaMeasurementTimeJoinForThisPlot=",FiaMeasurementTimeJoinForThisPlot)
                    MeasureYearsNotDisturbedPlotList.append(FiaPlotID)  #Added on 20230601
                    for m in range(0, len(plotlist),1): 
                        if plotlist[m] == FiaPlotID:
                            PlotCoordinates = str(xlist[m]) + "," + str(ylist[m])

                    AddPlotCoordinates = "No"  #Options are "yes" or "No"
                    if AddPlotCoordinates == "Yes":
                        FieldMetricCSV_finalFile.write(FiaMeasurementTimeJoinForThisPlot+","+PlotCoordinates+"\n")
                    if AddPlotCoordinates == "No":
                        FieldMetricCSV_finalFile.write(FiaMeasurementTimeJoinForThisPlot+"\n")
                else:
                    print("FiaPlotID of ",FiaPlotID, " has to be dropped because there is no SUITABLE field measurement (e.g., disturbed between survey time and remote sensing time) data for RemoteSensingTargetYear=",RemoteSensingTargetYear)  



            #This section is to get the remeasured plots with three criteria listed below (Criteria 3 is here)----------------------------------------------------start
            print("\nRemoteSensingTargetYear=",RemoteSensingTargetYear," RemeasuredPlotList is: ", RemeasuredPlotList, file=open(F3Log, 'a'))
            print("\nRemoteSensingTargetYear=",RemoteSensingTargetYear," MeasureYearsNotDisturbedPlotList is: ", MeasureYearsNotDisturbedPlotList, file=open(F3Log, 'a'))
            print("\nRemoteSensingTargetYear=",RemoteSensingTargetYear," DisturbedSelectedPlotList is: ", DisturbedSelectedPlotList, file=open(F3Log, 'a'))
            for k in range(0,len(RemeasuredPlotList),1):  ##Criteria 3: The remeasured plot must be already in the Tradition or DisturbedFVS plots
                if (RemeasuredPlotList[k] in MeasureYearsNotDisturbedPlotList) or (RemeasuredPlotList[k] in DisturbedSelectedPlotList):
                    print(RemeasuredPlotList[k]," is in MeasureYearsNotDisturbedPlotList or DisturbedSelectedPlotList",file=open(F3Log, 'a'))
                    print("This is for Criteria 3:  The remeasured plot must be already in the Tradition or DisturbedFVS plots")
                    if (RemeasuredFiaMeasurementTimeJoin[k] not in MeasureYearsNotDisturbedFiaMeasurementTimeJoin) and (RemeasuredFiaMeasurementTimeJoin[k] not in DisturbedSelectedFiaMeasurementTimeJoin):
                        print("This is for Criteria 4: Plot and MeasureTime pair does not exist in the Tradition or DisturbedFVS plots")
                        RemeasuredLine = RemeasuredFiaMeasurementTimeJoin[k] + "\n"   #Plot already chosen but years has not been chosen
                        print(RemeasuredFiaMeasurementTimeJoin," is not in MeasureYearsNotDisturbedFiaMeasurementTimeJoin and DisturbedSelectedFiaMeasurementTimeJoin",file=open(F3Log, 'a'))
                        FieldMetricCSV_finalRemeasuredFile.write(RemeasuredLine)
            #This section is to get the remeasured plots with three criteria listed below (Criteria 3 is here)----------------------------------------------------end
                
            FieldMetricCSV_finalFile.close()
            FieldMetricCSV_finalDisturbedFVSFile.close()
            FieldMetricCSV_finalDroppedFile.close()
            FieldMetricCSV_finalRemeasuredFile.close()
        #This part use 3 fast loss (between survey date and remote sensing date) to drop plot. It is also dropped if the survey is far later than remote sensing date----start            


        #This section keeps the latest measurment year if a plot was disturbed more than once (e.g., a plot was measured in 2001 and 2011; when selecting this plot for RSY2013, we will only keep the 2011 measurment)---start
        for RemoteSensingTargetYear in RemoteSensingYearOfInterest:
            FieldMetricCSV_finalDisturbedFVS = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_DuplicateDisturbedFVS.csv"  #THis list all plots that will be used for final FVS run
            FieldMetricCSV_finalDisturbedFVSFile = open(FieldMetricCSV_finalDisturbedFVS, "r")
            FieldMetricCSV_finalDisturbedFVSNoDuplicate = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile+os.sep+"RSY"+str(OriginalRemoteSensingTargetYear)+"_MaxGap"+str(MaximumYearGap)+"_DisturbedFVS.csv"  #THis list all plots that will be used for final FVS run
            FieldMetricCSV_finalDisturbedFVSNoDuplicateFile = open(FieldMetricCSV_finalDisturbedFVSNoDuplicate, "w")
            
            Lines = FieldMetricCSV_finalDisturbedFVSFile.readlines()  #readline only read one line
            Lines = [i.replace('\n','') for i in Lines]
            
            #89546122003,2013/64
            
            Plots = [int(k[0:5]) for k in Lines]
            PlotsMeasurementMonth = [int(k[5:7]) for k in Lines]
            PlotsMeasurementYear = [int(k[7:11]) for k in Lines]

            PlotsUnique = [i for n,i in enumerate(Plots) if i not in Plots[:n]]
            for Plot in PlotsUnique:
                PlotIndexList = [index for index, item in enumerate(Plots) if item == Plot]
                PlotMeasurementYear = [PlotsMeasurementYear[m] for m in PlotIndexList]
                PlotMeasurementYearMax = max(PlotMeasurementYear)
                PlotMeasurementYearMaxIndex = PlotMeasurementYear.index(PlotMeasurementYearMax)
                print("Chosen:",Plot,PlotIndexList,PlotMeasurementYear,PlotMeasurementYearMax,PlotMeasurementYearMaxIndex)
                FinalLine = Lines[PlotIndexList[PlotMeasurementYearMaxIndex]] + "\n"
                print("FinalLine=",FinalLine)
                FieldMetricCSV_finalDisturbedFVSNoDuplicateFile.write(FinalLine)
            FieldMetricCSV_finalDisturbedFVSFile.close()
            FieldMetricCSV_finalDisturbedFVSNoDuplicateFile.close()
        #This section keeps the latest measurment year if a plot was disturbed more than once (e.g., a plot was measured in 2001 and 2011; when selecting this plot for RSY2013, we will only keep the 2011 measurment)---end
        print("It took \t" + str(int((time.time() - t0)/60))," minutes to screen the FIA data for FVS run")
        return FieldMetricCSV_final,FieldMetricCSV_finalDisturbedFVS,FieldMetricCSV_finalDropped,FieldMetricCSV_finalRemeasured
    except Exception as e:
        print("Sorry, there is an error : ",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        strTrace = traceback.format_exception(exc_type, exc_value, exc_traceback)
        ErrorLog = os.getcwd()+os.sep+"F3ScreenFIAError.txt"
        ErrorLogFile = open(ErrorLog, 'a')
        Content1 = "Dr. Shengli Huang detected an error at "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"  
        #print(Content1)    
        Content2 = "The error was for CreateFinalCleanFIAplotsToRunFVSForTheTile with inputs of "+ repr(Tile) + " " + repr(RemoteSensingYearOfInterest) + " " + repr(len(plotlist)) + " with the following details:\n"
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
    UserDefinedRatio = 0.6   
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

    #This section will copy the data from preproced path to the CUI f3 path where the confidential data are stored---start
    MaximalSimutaneousTask = min(len(RemoteSensingYearOfInterest)*len(Tiles)*1*1*1, UserDefinedMaximumProcessors) #here 1*1*1 indicate BaseManagement,BaseYear,BaseMetric
    NumberOfSimutaneousTask = max(min(NumberOfProcessors-2, MaximalSimutaneousTask),1)  
    print("\n\n\nNumberOfSimutaneousTask for BaseManagement,BaseYear,BaseMetric=",NumberOfSimutaneousTask)
    F3pool0 = multiprocessing.Pool(processes=NumberOfSimutaneousTask)
    FVSprocessingFiaPlotScreenPath = FVSprocessingpath +os.sep+"FiaPlotScreen"
    if not os.path.exists(FVSprocessingFiaPlotScreenPath):
        os.mkdir(FVSprocessingFiaPlotScreenPath)
    for Year in RemoteSensingYearOfInterest:
        TilesMosaicRunAverageFolder = CreateTilesMosaicRunAverageFolder(Year)
        for Tile in Tiles:
            FVSprocessingTilepath = FVSprocessingpath +os.sep+"FiaPlotScreen"+os.sep+Tile
            if not os.path.exists(FVSprocessingTilepath):
                os.mkdir(FVSprocessingTilepath)
            for run in ["Run1","Run2","Run3"]:
                TargetRunFolder = ThePathWhereF3ToBeRun + os.sep + "RemoteSensingYear"+str(Year) + os.sep + run
                if not os.path.exists(TargetRunFolder):
                    os.mkdir(TargetRunFolder)
            TileRepresentativeFolder = ThePathWhereF3ToBeRun + os.sep + "RemoteSensingYear"+str(Year) + os.sep + "Run1" + os.sep + Tile
            if not os.path.exists(TileRepresentativeFolder):
                    F3pool0.apply_async(CopyTheDataFromPreprocessedPathToF3Path, [Year,Tile])   
    F3pool0.close()
    F3pool0.join()
    print("AAA finished at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #This section will copy the data from preproced path to the CUI f3 path where the confidential data are stored---end

    print("GGGGGGGOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
    DownloadFVSreadyDataFromFIAdatamartWebsite = "No"  #Options are Yes or No
    if DownloadFVSreadyDataFromFIAdatamartWebsite == "Yes":
        StateShortNameListDownloaded = DownloadedFVSreadyDataFromFIAdatamartWebsite(EntireStateShortNameList)
        print("DownloadedFVSreadyDataFromFIAdatamartWebsite returned:",StateShortNameListDownloaded)
    US_FIA_AddXY_ReprojectAlbers = "No"
    if US_FIA_AddXY_ReprojectAlbers == "Yes":
        PointGISshapeFile = AddXandYCoordinatesForPointGISshapeFile(EntireUS_FIA_NoCoordinates,"lon", "lat")
        print("PointGISshapeFile=",PointGISshapeFile)
        ReprojectedShapeFile = ReprojectGISshapeFileToAtifProjection(PointGISshapeFile, ReferencedTif)
        print("ReprojectedShapeFile=",ReprojectedShapeFile)
        ReprojectedShapeFileWithAlbers = AddXandYCoordinatesForPointGISshapeFile(ReprojectedShapeFile,"albersx", "albersy")
        print("ReprojectedShapeFileWithAlbers=",ReprojectedShapeFileWithAlbers)
    CreateFuzzyFIAshapeFile = "No"  #Another option is "No"
    if CreateFuzzyFIAshapeFile == "Yes":  #Another option is "No"
        for state in EntireStateShortNameList:  
            FVSINPUTDB = FVSINPUTDBpath +os.sep+ "SQLite_FIADB_" + state + ".db"
            FVSreadySqliteDatabaseFuzzyShape = CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseWithMeasurement(FVSINPUTDB)
            FVSreadySqliteDatabaseFuzzyShapeOnlyPlotLocation = CreateFiaFuzzyShapeFileFromFVSreadySqliteDatabaseOnlyPlotLocation(FVSINPUTDB)
            print("FVSreadySqliteDatabaseFuzzyShape is:",FVSreadySqliteDatabaseFuzzyShape)
            print("FVSreadySqliteDatabaseFuzzyShapeOnlyPlotLocation is:",FVSreadySqliteDatabaseFuzzyShapeOnlyPlotLocation)

    print("hhhhhhhhhhhhhhhhhhhhhhhOOOOOOOOOOOOOOOOOOOOO")
    #This section will download data from Google bucket and then preprocess them into the format required by subsequent F3 processing---start
    MaximalSimutaneousTask = min(len(RemoteSensingYearOfInterest)*len(Tiles)*1*1*1, UserDefinedMaximumProcessors) #here 1*1*1 indicate BaseManagement,BaseYear,BaseMetric
    NumberOfSimutaneousTask = max(min(NumberOfProcessors-2, MaximalSimutaneousTask),1)  
    print("\n\n\nNumberOfSimutaneousTask for BaseManagement,BaseYear,BaseMetric=",NumberOfSimutaneousTask)
    F3pool = multiprocessing.Pool(processes=NumberOfSimutaneousTask)
    for Year in RemoteSensingYearOfInterest:
        for Tile in Tiles:
            HSLfile = r'D:\CUI\subplots\L48' + os.sep + Tile + ".hsl"
            if not os.path.exists(HSLfile):
                    print(Year,Tile," started at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    F3pool.apply_async(CreateFIAconfidentialPlotBinaryForEachTile, [Tile])   
    F3pool.close()
    F3pool.join()
    print("BBB finished at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #This section will download data from Google bucket and then preprocess them into the format required by subsequent F3 processing---end

    print("kkkkkkkkkkkkkkkkkkkkkkOOOOOOOOOOOO")
    #This section will Create Disturbance_ScreendFIAplot (with month and date) For FVS run---start
    MaximalSimutaneousTask = min(len(RemoteSensingYearOfInterest)*len(Tiles)*1*1*1, UserDefinedMaximumProcessors) #here 1*1*1 indicate BaseManagement,BaseYear,BaseMetric
    NumberOfSimutaneousTask = max(min(NumberOfProcessors-2, MaximalSimutaneousTask),1)  
    print("\n\n\nNumberOfSimutaneousTask for BaseManagement,BaseYear,BaseMetric=",NumberOfSimutaneousTask)
    F3pool2 = multiprocessing.Pool(processes=NumberOfSimutaneousTask)
    for Year in RemoteSensingYearOfInterest:
        for Tile in Tiles:
            FieldMetricCSV = FVSprocessingFiaPlotScreenPath+os.sep+Tile+os.sep+"FIAplotAndChangeDetection.csv"
            if not os.path.exists(FieldMetricCSV):
                print("creating ",FieldMetricCSV)
                F3pool2.apply_async(CreateDisturbanceScreendFIAplotForFVSrun, [Tile])   
    F3pool2.close()
    F3pool2.join()
    print("CCC finished at ", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    #This section will download data from Google bucket and then preprocess them into the format required by subsequent F3 processing---end


    





