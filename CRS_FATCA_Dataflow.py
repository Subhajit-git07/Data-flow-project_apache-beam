import pandas as pd
import argparse
import apache_beam as beam
from google.cloud import storage
from apache_beam.options.pipeline_options import PipelineOptions
import io 
from io import StringIO 
import logging
import datetime
from datetime import date
from datetime import datetime, timedelta
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import pvalue
import uuid
from zipfile import ZipFile
import requests
import urllib.request
import json
import time  
import sys
import logging as logg
import os
from google.cloud import logging 
from google.cloud.logging.handlers import CloudLoggingHandler
import google.cloud.logging
import csv
import numpy as np
from decimal import Decimal
import zipfile
from functools import reduce                           


#Bigquery import's :
#from apache_beam.io.gcp.internal.clients import bigquery
from google.cloud import bigquery as BG
import re

#Stage1 Processing
recon_prefix = 'Reconciliation/'
mdt_prefix = 'MDT_SR/'
input_prefix='EYFirst_SR/'
log_prefix = 'Logs_SR/'
dest_prefix = 'EY First/'
bq_log_folder = 'Bigquery_cold_storage/' + 'SR/'
move_csv_storage = 'CSV_cold_storage/'

#Stage2 Processing
#set folder prefix
CI_prefix = 'CI_SR/'

api_eyfirst_processed = 'EYFIRST_API_Processed_Files/'
api_eyfirst_failed = 'EYFIRST_API_Failed_Files/'

Api_folder = 'EYFirst_API_LOG/'

#FATCA folders
FATCA_csv_input_prefix = 'FATCA_SR/'
FATCA_api_output_prefix = 'API_Input FATCA_SR/'

#CRS folders
CRS_csv_input_prefix = 'CRS_SR/'
CRS_api_output_prefix = 'API_Input CRS_SR/'


#------------------------------------------------ Read MDT excel file -------------------------------------
class Stage1_Read_from_xls(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, something, filename):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=mdt_prefix, delimiter=delimiter)
        mdt = pd.DataFrame()

        try:
            for blob in blobs:
            
                blobpath = blob.name
                blob_path = blobpath.split('/')
                blob_name = blob_path[-1]
                
                if (filename == blob_name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO                    
                    df = pd.read_excel(BytesIO(fwf_data), header=[0],engine='xlrd')


            if mdt.empty:{}
            else:
                df=mdt
            
            yield df
        except UnboundLocalError as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':filename+' MDT is not present in Bucket',"Error Message":str(exc_obj),"Class name": 'Stage1_Read_from_xls','Error_type':str(exc_type), 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (filename+' MDT is not present in Bucket: '+str(exc_obj))  

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Stage1_Read_from_xls', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))            
 

#------------------------------------------------ Read CRS FileInventory excel file -------------------------------------
class Read_FileInventory_xls(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, something, filename, MDT):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=mdt_prefix, delimiter=delimiter)
        df = pd.DataFrame()
        try:
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_excel(BytesIO(fwf_data), header=[0],engine='xlrd')
                    
            
            if len(df)<1:
                df = MDT 
                df['Destination File Name Instance'] = df['Destination File Name'].str[:-7] + df['Instance'].astype(str) +'_'+ df['Destination File Name'].str[-7:]
                df['MDT_Filename'] =df['Destination File Name Instance'].str.slice(0,-7,1)
                df['PrimaryKey']=df['MDT_Filename']+df['CTCR Process ID']
                df['Record_Action']=df['Record Action']
                df['Active_Flag']=df.Record_Action.apply(lambda x: 0 if x.upper() == 'DELETE' else 1)
                df['File_Received']=0
                df['File_Count']=0
                df['DateTime']=datetime.now()
                df['Regime'] = df['Destination File Name'].str[0:3]
                #df['Status'] =''
                df = df.drop(columns=['Record_Action'])
                
            
            MDT['Destination File Name Instance'] = MDT['Destination File Name'].str[:-7] + MDT['Instance'].astype(str) +'_'+ MDT['Destination File Name'].str[-7:]
            MDT['MDT_Filename'] =MDT['Destination File Name Instance'].str.slice(0,-7,1)
            MDT['PrimaryKey']=MDT['MDT_Filename']+MDT['CTCR Process ID']
            MDT['Active_Flag']=0
            MDT['File_Received']=0
            MDT['File_Count']=0
            MDT['DateTime']=''
            #MDT['Status'] =''
            MDT['Regime'] = MDT['Destination File Name'].str[0:3]

            
            #Changing File_received to 0 and Status is blank before starting any processing
            df['File_Received'] = 0 
            #df['Status'] = '' 
            
            
            if len(df)>0:       
                for index, row in MDT.iterrows():
                    val = MDT["PrimaryKey"][index]
                    try:
                        index_df = df[df["PrimaryKey"]== val].index 
                        
                        if len(index_df)>0:
                            
                            MDT.loc[index,'Active_Flag'] = df.loc[index_df[0],'Active_Flag']
                            MDT.loc[index,'File_Received'] = df.loc[index_df[0],'File_Received']
                            MDT.loc[index,'File_Count'] = df.loc[index_df[0],'File_Count']
                            MDT.loc[index,'DateTime'] = df.loc[index_df[0],'DateTime']
                            #MDT.loc[index,'Status'] = df.loc[index_df[0],'Status']

                            index_df = df[df["PrimaryKey"]== val].index
                            
                            objdf = df.iloc[index_df[0]]
                            objmdt = MDT.iloc[index]
                            
                            if(objdf.equals(objmdt)):
                                
                                continue
                            else :
                                
                                df.iloc[index_df[0]] = MDT.iloc[index] 
                                                       
                            
                                if ((df.iloc[index_df[0],0]).upper() != 'DELETE'):
                                    df.iloc[index_df[0],16] = 1
                                    df.iloc[index_df[0],19] = datetime.now()

                                                                    
                                elif (((df.iloc[index_df[0],0]).upper() == 'DELETE')):
                                    df.iloc[index_df[0],16] = 0
                                    df.iloc[index_df[0],19] = datetime.now()

 
                        else:
                            
                            i = len(df)
                            df.loc[i] = MDT.loc[index]
                            if ((df.iloc[i,0]).upper() != 'DELETE'):
                                df.iloc[i,16] = 1
                                df.iloc[i,17] = 0
                                df.iloc[i,18] = 0
                                df.iloc[i,19] = datetime.now()
                                
                                
                            elif ((df.iloc[i,0]).upper() == 'DELETE'):
                                df.iloc[i,16] = 0
                                df.iloc[i,17] = 0
                                df.iloc[i,18] = 0
                                df.iloc[i,19] = datetime.now()

                    except:{}


            yield df
          

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_FileInventory_xls', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))   			
 

#---------------------------------Validate File Inventory----------------------------------------------------
class Validate_from_FileInventory(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, element, file_prefix):
        try:
            df = element
            df = df.applymap(str)
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            valid_mdt_list = list()
            
            
            for blob in blobs:
                blobpath = blob.name
                blob_path = blobpath.split('/')
                blob_name = blob_path[-1]
                TrimmedFileName = blob_name[:-7]
                filedf = pd.DataFrame()
                i=0
                blob_found = 0

                
                for i in range(len(df)):
                    if (str(df.at[i, 'MDT_Filename']) == TrimmedFileName and df.at[i, 'Active_Flag']== '1'):

                        df.at[i, 'File_Received'] = 1
                        df.at[i, 'File_Count'] = int(df.at[i, 'File_Count']) + 1
                        #df.at[i,'Status']='Found'
                        valid_mdt_list.append(blob_name[:-7])
                        blob_found = 1
                        break
                #updating record
                
                                   
                if blob_found == 0 and file_prefix in blob.name:
                    tempdf = pd.DataFrame(columns = ['BlobPath', 'Source_FileName','MDT_Filename','Status','Date','Time'])
                    tempdf.at[0,'BlobPath'] = blobpath
                    tempdf.at[0,'Source_FileName'] = blob_name
                    #tempdf.at[0,'Status']='Not Found'
                    tempdf.at[0,'Date']= date.today()
                    time =datetime.now()
                    tempdf.at[0,'Time']=time.strftime("%H:%M:%S")
                    

            Mdt_list = set(df['Destination File Name Instance'].astype(str).str.slice(0,-7,1).tolist())
            Mdt_list= list(filter(None, Mdt_list))

            not_arrived = set(Mdt_list).difference(set(valid_mdt_list))
            not_arrived_list = list(not_arrived)

            for i in range((len(df))):
                if (str(df.at[i,'MDT_Filename']) in not_arrived_list and (df.at[i, 'Active_Flag']== '1')):# and str(df.at[i,'Status']) !='Found'):

                    df.at[i, 'File_Received'] = 0
                    #df.at[i,'Status']='File not arrived'
                    

            df = df.reset_index(drop = True)
            #Validated_FileInventory = df[~(df['Status']=='Not Found')]
            #drop_dup = df[df['Status']=='Not Found'].drop_duplicates(subset=['BlobPath'], keep="last")
            #Validated_MDT = Validated_MDT.append(drop_dup,ignore_index = True)
            
            df.drop_duplicates(keep='first', inplace=True)

            yield df
           

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Validate_from_FileInventory', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#-------------------------------Merge CRS and FATCA File Inventory-------------------------------------------------------
class MergeInventory(beam.DoFn):
    def process(self,element,crsfi):
        try:    
            fatcafi = element
            crsfi = crsfi
            df = pd.DataFrame()
            df = df.append(fatcafi)
            df = df.append(crsfi)
            yield df
            
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MergeInventory', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))      
            
#-------------------------------Writing File Inventory MDT CSV File-------------------------------------------------------
class Write_FileInventoryMDT_to_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename):
        try:    
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blob = bucket.blob(mdt_prefix + destination_blob_name)
            element.to_excel(filename,index=False)
            blob.upload_from_filename(filename)
            yield element
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_FileInventoryMDT_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))            
            
            
            
 
#-------------------------------Writing MDT CSV File-------------------------------------------------------
class Write_MDT_to_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename):
        try:    

            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blob = bucket.blob(mdt_prefix + destination_blob_name)
            element.to_excel(filename,index=False)
            blob.upload_from_filename(filename)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_MDT_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#--------------- Process dataframe to groupby found file based on ctcr process id--------------------------
class Process_Validation(beam.DoFn):
    def process(self,something,valid_MDT):
        try:
            
            mdt = valid_MDT[valid_MDT['Active_Flag'] == '1']

            grouping_list =['CTCR Process ID','File Type','PrimaryKey','MDT_Filename','File_Received']
            mdt = mdt.replace(r'^\s*$', np.nan, regex=True)

            try:
                df = mdt.groupby(grouping_list).size().unstack()

                valid = df.loc[df.groupby('CTCR Process ID')[1].filter(lambda x: len(x[pd.isnull(x)] ) <1).index]
                valid =pd.DataFrame(valid.stack())
                process_list = list(valid.groupby('CTCR Process ID').groups.keys())

            except:
                process_list =list()
            mdt = pd.DataFrame(mdt)
            process_df = mdt[mdt['CTCR Process ID'].isin(process_list)]
            
            
            yield process_df


        except KeyError as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':'KeyError for '+str(exc_obj),'Class Name':'Process_Validation' ,'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)
            raise TypeError (str(exc_obj))

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_Validation', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#------------------------------ CRS File Level Validation Function-------------------------------

class CRS_Validation_File(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,valid_MDT, unique_id, etl_start_datetime):

        # Creating Dataframe and list to align with MDF specifications
        try:

            MDT_df = element
            Mdt_found = MDT_df[MDT_df['File_Received'] == 1]
            Mdt_not_found = valid_MDT[(valid_MDT['File_Received'] != 1) & (valid_MDT['Active_Flag'] == '1')] 

            validated_list = Mdt_found['MDT_Filename'].tolist()
            Mdt_found = Mdt_found.reset_index(drop=True)


            #Log_Info_df = pd.DataFrame(columns = ['Date','Time','CTCR Process ID','File','Source System','Message'])
            Log_Info_df = pd.DataFrame(columns = ['ETL ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime', 'ETL_End_DateTime', 'Error_ID'])
            record_len = {'IP TAXDOM':101,'ACCOUNT':212,'IP RELATION':101,'IP ACCOUNT':93,'BALANCE':122,'PAYMENTS':161,'IP_ADDN_QA':150}
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            Rec_type =''
            File_type  =''
            file_ver  =''
            sys_id  =''
            file_date  =''
            file_time  =''
            log = list()
            regime = 'CRS'
            #error_id = "Error id"
            for blob in blobs:
                blobpath = blob.name
                blob_path = blobpath.split('/')
                blob_name = blob_path[-1]
                TrimmedFileName = blob_name[:-7]

                if any(str(file_value) in blob.name for file_value in validated_list) and 'CRS_' in blob.name:
                    
                    source_system = ''
                    for i in range(len(Mdt_found)):
                        if TrimmedFileName == Mdt_found.at[i,'MDT_Filename']:
                            source_system = Mdt_found.at[i,'Source System ID']
                            source_file = blob_name
                            process_id = Mdt_found.at[i,'CTCR Process ID']

                        else :
                            continue                


                    file_data = blob.download_as_bytes()
                    file_data_str = file_data.decode("utf-8")
                    file_detail= StringIO(file_data_str)
                    file_rec = csv.reader(file_detail)
                    file_data_list = file_data_str.split('\n')

                                              
                    file_data_list = [i.replace('\r', '') for i in file_data_list]


                    file_data_list =[i for i in file_data_list if i]

                    count = len(file_data_list)-2
                    try:
                        header = file_data_list[0]        #.strip() header->69 char(removing strip)
                        record_size = file_data_list[1:-1]

                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0005")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                        
                        continue

                    Rec_type = header[:1]
                    File_type = header[1:21]
                    file_ver = header[21:31]
                    sys_id = header[31:51]
                    file_date = header[51:61]
                    file_time = header[61:69]
                    rec_len = list()
                    File_type_head = ['CRS IP','IP TAXDOM','ACCOUNT','IP RELATION','IP ACCOUNT','BALANCE','PAYMENTS','IP_ADDN_QA']

                    time =datetime.now()
                    # Header length validation
                    if len(file_data_list[0]) >= 69:{} #.strip() removing from header
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0006")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # empty file check
                    if count >= 0:
                        rec_len =[len(i) for i in record_size]
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0007")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Record type check on header to be 0
                    if Rec_type == '0':{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0008")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #trailer type check to be 9
                    if len(file_data_list[-1].strip()) > 0 and file_data_list[-1].strip()[0] == '9':{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0009")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #Trailer Length 
                    if len(str(file_data_list[-1])) >=10:{} #.strip() removing from trailer
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0010")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #Source System id check with MDT file
                    if source_system == sys_id.strip():{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0011")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)


                    # Detail record count with trailer 
                    try:
                        if int(file_data_list[-1].strip()[1:]) == count:{}
                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("CRS_R_0012")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0012")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
						
                    #File Header check 
                    if File_type.strip() in File_type_head:{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0013")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #File version check
                    if(File_type.strip() =='CRS IP'):
                        if (str(file_ver) =='0000000001' or str(file_ver) =='0000000002') and file_ver.strip():{}
                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("CRS_R_0014")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #source system id empty check
                    if sys_id.strip():{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0016")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Header valid date check
                    try:
                        datetime.strptime(file_date,"%Y-%m-%d")
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0018")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Header valid time check
                    try:
                    	datetime.strptime(file_time,"%H:%M:%S")
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("CRS_R_0020")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                    
                    #Record length check with respect to file type
                    if File_type.strip() in File_type_head :
                        if(File_type.strip() in record_len.keys() and all(i >= int(record_len[File_type.strip()]) for i in rec_len)):{}
                        elif(File_type.strip() =='CRS IP' and file_ver =='0000000001' and all(i >= 1050 for i in rec_len)):{}
                        elif(File_type.strip() =='CRS IP' and file_ver =='0000000002' and all(i >= 1500 for i in rec_len)):{}
                        elif(File_type.strip() =='IP_ADDN_QA' and file_ver =='0000000001' and all(i >= 150 for i in rec_len)):{}
                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("CRS_R_0021")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)


            mdt = valid_MDT[valid_MDT['Active_Flag'] == '1']
            grouping_list =['CTCR Process ID','File Type','PrimaryKey','MDT_Filename','File_Received']
            mdt = mdt.replace(r'^\s*$', np.nan, regex=True)
            
            try:
                df = mdt.groupby(grouping_list).size().unstack()
                
                mdt_not_foundValid = df.loc[df.groupby('CTCR Process ID')[0].filter(lambda x: len(x[pd.isnull(x)])>1).index]
                mdt_not_foundValid =pd.DataFrame(mdt_not_foundValid.stack())
                arrived_list = list(mdt_not_foundValid.groupby('CTCR Process ID').groups.keys())
                
                arrivedvalid_list = Mdt_found['CTCR Process ID'].unique().tolist()
                notarrivedlist = list((set(arrived_list)-set(arrivedvalid_list)))

            except:
                notarrivedlist =list()


            notarrived_df = Mdt_not_found[Mdt_not_found['CTCR Process ID'].isin(notarrivedlist)]
            
            time =datetime.now()
            for index,row in notarrived_df.iterrows():
                log.clear()
                log.append(unique_id)
                log.append(row['CTCR Process ID'])
                log.append(regime)
                log.append(row['Source System ID'])
                log.append(row['Destination File Name Instance'])
                log.append(etl_start_datetime)
                log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                log.append("CRS_R_0035")

                log_series = pd.Series(log, index = Log_Info_df.columns)
                Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

            yield Log_Info_df
            

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Validation_File', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#------------------------------ FATCA File Level Validation Function -------------------------------

class FATCA_Validation_File(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,valid_MDT,unique_id, etl_start_datetime):

        # Creating Dataframe and list to align with MDF specifications
        try:
            MDT_df = element
            Mdt_found = MDT_df[MDT_df['File_Received'] == 1]
            Mdt_not_found = valid_MDT[(valid_MDT['File_Received'] != 1) & (valid_MDT['Active_Flag'] == '1')]  
            validated_list = Mdt_found['MDT_Filename'].tolist()
            Mdt_found = Mdt_found.reset_index(drop=True)


            #Log_Info_df = pd.DataFrame(columns = ['Date','Time','CTCR Process ID','File','Source System','Message'])
            Log_Info_df = pd.DataFrame(columns = ['ETL ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime', 'ETL_End_DateTime', 'Error_ID'])
            record_len = {'ACCOUNT':212,'IP RELATION':98,'IP ACCOUNT':93,'BALANCE':122,'PAYMENTS':161,'Involved Party':150}
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            Rec_type =''
            File_type  =''
            file_ver  =''
            sys_id  =''
            file_date  =''
            file_time  =''
            log = list()
            regime = 'FTR'
            #error_id = "Error id"
            time =datetime.now()

            for blob in blobs:
                blobpath = blob.name
                blob_path = blobpath.split('/')
                blob_name = blob_path[-1]
                
                TrimmedFileName = blob_name[:-7]

                if any(str(file_value) in blob.name for file_value in validated_list) and 'FTR_' in blob.name:
                    
                    source_system = ''
                    for i in range(len(Mdt_found)):
                        if TrimmedFileName == Mdt_found.at[i,'MDT_Filename']:
                            source_system = Mdt_found.at[i,'Source System ID']
                            source_file = blob_name
                            process_id = Mdt_found.at[i,'CTCR Process ID']
                        else :
                            continue                

                        
                        
                    file_data = blob.download_as_bytes()
                    file_data_str = file_data.decode("utf-8")
                    file_detail= StringIO(file_data_str)
                    file_rec = csv.reader(file_detail)
                    file_data_list = file_data_str.split('\n')

                    #removing for length check
                    file_data_list = [i.replace('\r', '') for i in file_data_list]
                    #file_data_list = [i.strip() for i in file_data_list]


                    file_data_list =[i for i in file_data_list if i]


                    count = len(file_data_list)-2
                    rec_len = list()
                    try:
                        header = file_data_list[0]     #.strip() header->69 char(removing strip)
                        record_size = file_data_list[1:-1]

                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0005")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                        continue

                    Rec_type = header[:1]
                    File_type = header[1:21]
                    file_ver = header[21:31]
                    sys_id = header[31:51]
                    file_date = header[51:61]
                    file_time = header[61:69]

                    File_type_head = ['INVOLVED PARTY','ACCOUNT','IP RELATION','IP ACCOUNT','BALANCE','PAYMENTS','IP_ADDN_CA']
                    log = list()
                    time =datetime.now()

                    # Header length validation
                    if len(file_data_list[0]) >= 69:{}    # .strip() removing for header
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0006")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                    # empty file check
                    if count >= 0:
                        rec_len =[len(i) for i in record_size]

                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0007")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Record type check on header to be 0
                    if Rec_type == '0':{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0008")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    if len(str(file_data_list[-1])) >= 10:{}  #.strip() removing from trailer
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0011")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                    #trailer type check to be 9
                    if len(file_data_list[-1].strip()) > 0 and file_data_list[-1].strip()[0] == '9':{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0010")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #Source System id check with MDT file
                    if source_system == sys_id.strip():{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0012")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Detail record count with trailer 
                    try:
                        if int(file_data_list[-1].strip()[1:]) == count:{}
                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("FTR_R_0013")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0013")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #File Header check 
                    if File_type.strip() in File_type_head:{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0014")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #File version check
                    if(file_ver == 'INVOLVED PARTY'):
                        if (str(file_ver) =='0000000001' or str(file_ver) =='0000000002') and file_ver.strip():{}
                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("FTR_R_0016")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #source system id empty check
                    if sys_id.strip():{}
                    else:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0017")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Header valid date check
                    try:
                        datetime.strptime(file_date,"%Y-%m-%d")
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0019")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    # Header valid time check
                    try:
                        datetime.strptime(file_time,"%H:%M:%S")
                    except:
                        log.clear()
                        log.append(unique_id)
                        log.append(process_id)
                        log.append(regime)
                        log.append(source_system)
                        log.append(source_file)
                        log.append(etl_start_datetime)
                        log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                        log.append("FTR_R_0021")
                        log_series = pd.Series(log, index = Log_Info_df.columns)
                        Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

                    #Record length check with respect to file type
                    if File_type.strip() in File_type_head:
                        if(File_type.strip() in record_len.keys() and all(i >= int(record_len[File_type.strip()]) for i in rec_len)):{}
                        elif(File_type.strip() =='INVOLVED PARTY' and file_ver =='0000000001' and all(i >= 992 for i in rec_len)): {}
                        elif(File_type.strip() =='INVOLVED PARTY' and file_ver =='0000000002' and all(i >= 1500 for i in rec_len)):{}
                        elif(File_type.strip() =='IP_ADDN_CA' and all(i >= 150 for i in rec_len)):{}

                        else:
                            log.clear()
                            log.append(unique_id)
                            log.append(process_id)
                            log.append(regime)
                            log.append(source_system)
                            log.append(source_file)
                            log.append(etl_start_datetime)
                            log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                            log.append("FTR_R_0022")
                            log_series = pd.Series(log, index = Log_Info_df.columns)
                            Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)


            mdt = valid_MDT[valid_MDT['Active_Flag'] == '1']
            grouping_list =['CTCR Process ID','File Type','PrimaryKey','MDT_Filename','File_Received']
            mdt = mdt.replace(r'^\s*$', np.nan, regex=True)
            
            try:
                df = mdt.groupby(grouping_list).size().unstack()
                
                mdt_not_foundValid = df.loc[df.groupby('CTCR Process ID')[0].filter(lambda x: len(x[pd.isnull(x)])>1).index]
                mdt_not_foundValid =pd.DataFrame(mdt_not_foundValid.stack())
                arrived_list = list(mdt_not_foundValid.groupby('CTCR Process ID').groups.keys())
                
                arrivedvalid_list = Mdt_found['CTCR Process ID'].unique().tolist()
                notarrivedlist = list((set(arrived_list)-set(arrivedvalid_list)))

            except:
                notarrivedlist =list()


            notarrived_df = Mdt_not_found[Mdt_not_found['CTCR Process ID'].isin(notarrivedlist)]
            
            time =datetime.now()
            for index,row in notarrived_df.iterrows():
                log.clear()
                log.append(unique_id)
                log.append(row['CTCR Process ID'])
                log.append(regime)
                log.append(row['Source System ID'])
                log.append(row['Destination File Name Instance'])
                log.append(etl_start_datetime)
                log.append(datetime.now().strftime("%d-%m-%Y %H:%M:%S"))
                log.append("FTR_R_0034")

                log_series = pd.Series(log, index = Log_Info_df.columns)
                Log_Info_df = Log_Info_df.append(log_series, ignore_index=True)

            yield Log_Info_df
            


        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Validation_File', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#---------------------------Log file creation ------------------------
class Write_log_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename):
        try:
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blob = bucket.blob(log_prefix + destination_blob_name)
            element.to_csv(filename,index=False)
            blob.upload_from_filename(filename)
            yield element
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_log_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#----------------------------Csv file creation list -------------------------------

class File_creation(beam.DoFn):
    def process(self,element,log):
        try:

            process_mdt = element
            process_list = process_mdt['CTCR Process ID'].tolist()
            log_list  = log['CTCR Process ID'].tolist()

            id_to_process = list(list(set(process_list)-set(log_list)) + list(set(log_list)-set(process_list))) 

            csv_create = process_mdt[process_mdt['CTCR Process ID'].isin(id_to_process)]
            csv_create.drop_duplicates()

            yield csv_create

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'File_creation', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#------------------------------- Adding composite key to process dataframe -------------------------------------------------------------------------
def df_creation(col_list,col_width,f_name,valid_df,name):
    fwf_data = f_name.download_as_bytes()
    fwf_data_str = fwf_data.decode("utf-8")
    f= StringIO(fwf_data_str)
    temp_df = pd.read_fwf(f, names = col_list, widths=col_width,skiprows = 1,converters={c:str for c in col_list})
    temp_df.dropna(how="all", inplace=True)
    temp_df.drop(temp_df.tail(1).index,inplace=True)


    if("_ACCT_BAL_" in f_name.name):
        fwf_data = f_name.download_as_bytes()
        fwf_data_str = fwf_data.decode("utf-8")
        f= StringIO(fwf_data_str)
        raw_str = f.read().split("\n")
        raw_str =[i for i in raw_str if i]
        raw_str = raw_str[1:-1]
        l=list()
        m=list()
        for i in range(len(raw_str)):
            l.append(raw_str[i][43:60])
            m.append(raw_str[i][64:78])
        temp_df['Balance_Amount'] = l
        temp_df['USD_Balance_Amount'] = m

    if("_PAY_" in f_name.name):
        fwf_data = f_name.download_as_bytes()
        fwf_data_str = fwf_data.decode("utf-8")
        f= StringIO(fwf_data_str)
        
        raw_str = f.read().split("\n")
        raw_str =[i for i in raw_str if i]
        raw_str = raw_str[1:-1]
        l=list()
        m=list()

        for i in range(len(raw_str)):
            l.append(raw_str[i][86:100])
            m.append(raw_str[i][103:117])
        temp_df['Payment_Amount'] = l
        temp_df['Payment_Amount_USD'] = m                                

    d = valid_df[valid_df['MDT_Filename']==name]
    d = d.reset_index(drop = True)
    p_id = d['CTCR Process ID'][0]
    s_id = d['Source System ID'][0]
    country = d['Country'][0]
    temp_df["CompositeKey"] = p_id+ "_"+ s_id+"_"
    temp_df['CTCR Process ID'] = p_id
    temp_df['Source System ID'] = s_id
    temp_df['Source File Index'] = temp_df.index
    temp_df['MDT_Country'] = country

    return temp_df

#------------------------------- Read CRS Flat file to process under different columns ---------------------------------------------------------------

class CRS_Read_from_fwf(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, something, filetype, valid_df):
        try:
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            List = valid_df['MDT_Filename'].tolist()

            df = pd.DataFrame()

            for blob in blobs:
                temp_df = pd.DataFrame()
                file_name = str(blob.name).split('/')
                file_name = file_name[-1]
                TrimmedFileName = file_name[:-7]

                
                if ('CRS_ACC_' in blob.name and filetype == 'Account' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Account_Type','Account_Sub-Type','Account_Name','Effective_From_Date','Effective_To_Date','Account_Status','Account_Dormancy_Status','Account_open_date','Account_close_date']
                    col_width = [1,1,8,30,3,3,120,10,10,3,3,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
                    
    
                elif ('CRS_ACCT_BAL_' in blob.name and filetype == 'AccountBalance' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Balance_Currency_Code','Balance_Amount','Closed_Account_Status','Balance_Type','USD_Balance_Amount','USD_Cross_Rate','USD_Cross_Rate_Date','Balance_Start_Date','Balance_End_Date']
                    col_width = [1,1,8,30,3,17,1,3,14,14,10,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
                elif ('CRS_PAY_' in blob.name and filetype == 'Payment' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Payment_Id','Payment_Type','Payment_Code','Payment_Date','Payment_Amount','Payment_Amount_CCY','Payment_Amount_USD','USD_Cross_Rate','USD_Cross_Rate_Date','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,3,3,10,14,3,14,14,10,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)+"_"+temp_df['Payment_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    
                    df = df.reset_index(drop = True)
                    
 
                elif ('CRS_IP_ACCT_' in blob.name and filetype == 'IPAccount' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Account_Id','Account_Relationship_Type','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,3,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
                elif ('CRS_IP_IP_' in blob.name and filetype == 'IPIP' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Parent_Party_Id','Child_Party_Id','Relationship_Type','Relationship_Ownership_%','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,6,5,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Parent_Party_Id'].astype(str)+"_"+temp_df['Child_Party_Id'].astype(str)+"_"+temp_df['Relationship_Type'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
    
                elif ('CRS_IP_TD_' in blob.name and filetype == 'IPTD' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Reporting_Category','Country_of_Tax_Residence','Tax_Id_type','Tax_ID','Active_Domicile_Flag','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,4,3,3,30,1,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)+"_"+temp_df['Country_of_Tax_Residence'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
                elif ('CRS_IP_ADDN_QA_' in blob.name and TrimmedFileName in List and filetype == 'IP_ADDN_QA'):
                    
                    fwf_data = blob.download_as_string()
                    fwf_data_str = fwf_data.decode("utf-8")
                    f= StringIO(fwf_data_str)
                    header_widths = [1,20,10,20,10,8]
                    header_df = pd.read_fwf(f,names = ['Record_Type','File_Type','File_Version','Source_System_Id','File_Date','File_Time'],widths=header_widths,nrows = 1)
                    fileversion = header_df.at[0,'File_Version']

                    if(fileversion == 1):
     
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','QA Birth Country Code','QA Birth Former Country Name','Filler']
                        col_width = [1,1,8,30,3,2,35,70]
                        empty_col_width = [0,0,0,0,0,0,0,0]
    
                    if(fileversion == 2):
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','Customer Opening Date','QA Birth Country Code','QA Birth Former Country Name','Filler']
                        col_width = [1,1,8,30,3,2,35,70]
                        empty_col_width = [0,0,0,0,0,0,0,0]
    
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    

                    if len(temp_df) == 0:
                        
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
                elif ('CRS_IP_' in blob.name and TrimmedFileName in List and filetype == 'IP' and 'CRS_IP_IP_' not in blob.name and 'CRS_IP_ACCT_' not in blob.name and 'CRS_IP_TD_' not in blob.name and 'CRS_IP_ADDN_QA_' not in blob.name):
                    fwf_data = blob.download_as_bytes()
                    fwf_data_str = fwf_data.decode("utf-8")
                    f= StringIO(fwf_data_str)
                    header_widths = [1,20,10,20,10,8]
                    header_df = pd.read_fwf(f,names = ['Record_Type','File_Type','File_Version','Source_System_Id','File_Date','File_Time'],widths=header_widths,nrows = 1)
                    fileversion = header_df.at[0,'File_Version']
                    if(fileversion == 1):
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','Tax_Id_type','FATCA_Tax_Id','FATCA_Tax_Id_Country_Code_of_issue','GIIN','Country_of_Residence','FATCA_Classification','FATCA__Sub-Classification','Chapter_3_Status','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Account_Holder_Dormancy_Status','Effective_From_Date','Effective_To_Date','Place_of_Birth','Entity_Type','CRS_SC_Status_','LOB_Indicator','Filler']
                        col_width = [1,1,8,30,3,3,20,3,19,3,10,10,10,80,80,10,100,100,5,100,5,100,5,100,5,100,5,30,20,3,3,10,10,30,4,6,0,18]
                        empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
                    if(fileversion == 2):
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','Tax_Id_type','FATCA_Tax_Id','FATCA_Tax_Id_Country_Code_of_issue','GIIN','Country_of_Residence','FATCA_Classification','FATCA__Sub-Classification','Chapter_3_Status','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Account_Holder_Dormancy_Status','Effective_From_Date','Effective_To_Date','Place_of_Birth','Entity_Type','CRS_SC_Status_','LOB_Indicator','Filler']
                        col_width = [1,1,8,30,3,3,20,3,19,3,10,10,10,80,80,10,100,100,5,100,5,100,5,100,5,100,5,30,20,3,3,10,10,30,4,6,4,464]
                        empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)

            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Read_from_fwf', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#------------------------------- Read FATCA Flat file to process under different columns -------------------------------------------------------------

class FATCA_Read_from_fwf(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, something, filetype, valid_df):
        try:
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            List = valid_df['MDT_Filename'].tolist()

            df = pd.DataFrame()
            for blob in blobs:
                temp_df = pd.DataFrame()
                file_name = blob.name.split('/')
                file_name = file_name[-1]
                TrimmedFileName = file_name[:-7]
                if ('FTR_ACC_' in blob.name and filetype == 'Account' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Account_Type','Account_Sub-Type','Account_Name','Effective_From_Date','Effective_To_Date','Account_Status','Account_Dormancy_Status','Account_open_date','Account_close_date']
                    col_width = [1,1,8,30,3,3,120,10,10,3,3,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
                    
                        
                elif ('FTR_ACCT_BAL_' in blob.name and filetype == 'AccountBalance' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Balance_Currency_Code','Balance_Amount','Closed_Account_Status','Balance_Type','USD_Balance_Amount','USD_Cross_Rate','USD_Cross_Rate_Date','Balance_Start_Date','Balance_End_Date']
                    col_width = [1,1,8,30,3,17,1,3,14,14,10,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
    
                elif ('FTR_PAY_' in blob.name and filetype == 'Payment' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Account_Id','Payment_Id','Payment_Type','Payment_Code','Payment_Date','Payment_Amount','Payment_Amount_CCY','Payment_Amount_USD','USD_Cross_Rate','USD_Cross_Rate_Date','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,3,3,10,14,3,14,14,10,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)+"_"+temp_df['Payment_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
                elif ('FTR_IP_ACCT_' in blob.name and filetype == 'IPAccount' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Account_Id','Account_Relationship_Type','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,3,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Account_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
    
                elif ('FTR_IP_IP_' in blob.name and filetype == 'IPIP' and TrimmedFileName in List):
                    col_list = ['Record_Type','Record_Action','Institution_Id','Parent_Party_Id','Child_Party_Id','Relationship_Type','Relationship_Ownership_%','Effective_From_Date','Effective_To_Date']
                    col_width = [1,1,8,30,30,3,5,10,10]
                    empty_col_width = [0,0,0,0,0,0,0,0,0]
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Parent_Party_Id'].astype(str)+"_"+temp_df['Child_Party_Id'].astype(str)+"_"+temp_df['Relationship_Type'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)

                elif ('FTR_IP_ADDN_CA_' in blob.name and filetype == 'IPAddCA' and TrimmedFileName in List):

                    # col_list = ['Record_Type','File_Type','File_Version','Source_System_Id','File_Date','File_Time']
                    # col_width = [1,20,10,20,10,8]
                    # empty_col_width = [0,0,0,0,0,0]
                    col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date','Filler']
                    col_width = [1,1,8,30,3,30,10,67]
                    empty_col_width = [0,0,0,0,0,0,0,0]

                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)

                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)

                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)
    
        
                elif ('FTR_IP_' in blob.name and filetype == 'IP' and 'FTR_IP_IP_' not in blob.name and 'FTR_IP_ACCT_' not in blob.name and 'FTR_IP_ADDN_CA_' not in blob.name and TrimmedFileName in List):
                    fwf_data = blob.download_as_bytes()
                    fwf_data_str = fwf_data.decode("utf-8")
                    f= StringIO(fwf_data_str)
                    header_widths = [1,20,10,20,10,8]
                    header_df = pd.read_fwf(f,names = ['Record_Type','File_Type','File_Version','Source_System_Id','File_Date','File_Time'],widths=header_widths,nrows = 1)
                    fileversion = header_df.at[0,'File_Version']
                    if(fileversion == 1):
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','Tax_Id_type','Tax_Id','Tax_Id_Country_Code_of_issue','GIIN','Country_of_Residence','FATCA_Classification','FATCA_Sub-Classification','Chapter_3_Status','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Account_Holder_Dormancy_Status','Effective_From_Date','Effective_To_Date','LOB_Indicator','Filler']
                        col_width = [1,1,8,30,3,3,20,3,19,3,10,10,10,80,80,10,100,100,5,100,5,100,5,100,5,100,5,30,20,3,3,10,10,0,0]
                        empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
                    if(fileversion == 2):
                        col_list = ['Record_Type','Record_Action','Institution_Id','Involved_Party_Id','Involved_Party_Type','Tax_Id_type','Tax_Id','Tax_Id_Country_Code_of_issue','GIIN','Country_of_Residence','FATCA_Classification','FATCA_Sub-Classification','Chapter_3_Status','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Account_Holder_Dormancy_Status','Effective_From_Date','Effective_To_Date','LOB_Indicator','Filler']
                        col_width = [1,1,8,30,3,3,20,3,19,3,10,10,10,80,80,10,100,100,5,100,5,100,5,100,5,100,5,30,20,3,3,10,10,4,504]
                        empty_col_width = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    
    
                    temp_df = df_creation(col_list,col_width,blob,valid_df,TrimmedFileName)
                    if len(temp_df) == 0:
                        temp_df = df_creation(col_list,empty_col_width,blob,valid_df,TrimmedFileName)
                    temp_df['Key'] = temp_df['Institution_Id'].astype(str)+"_"+temp_df['Involved_Party_Id'].astype(str)
                    temp_df["CompositeKey"] = temp_df["CompositeKey"].astype(str) + temp_df['Key'].astype(str)
                    temp_df['SourceFileName'] = file_name
                    df = df.append(temp_df)
                    df = df.reset_index(drop = True)

            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Read_from_fwf', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#---------------------------- Write csv file --------------------------------------
class Write_df_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename,folder_prefix, unique_id):
        try:

            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            newdf = element
            prefix = folder_prefix
            delimiter='/'
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)

            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            if(len(newdf.columns)>0):                         
                newdf["Stage1_id"] = unique_id
                newdf["Stage1_date_time"] = dt_string

            if len(newdf)>=0  and len(newdf.columns)>0:
                                                                   
                blob = bucket.blob(prefix + destination_blob_name)
                newdf.to_csv(filename,index=False)
                blob.upload_from_filename(filename)

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_df_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#-----------------------------Move And Delete all processed files -----------------------------------------------------------

class Move_and_delete(beam.DoFn):

    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path

    def process(self, element,unique_id,regime):
        try:
            df = element
            List = list()
            for i in range(len(df)):
                if (df.at[i,'GDF_ProcessId'] == unique_id and df.at[i,'Regime'] == regime and df.at[i,'Load_Status'] != 'Insufficient File'):
                    List.append(df.at[i,'Source_File_Name'])
                    
            
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            dest_bucket_name = self.output_path
            source_bucket = storage_client.get_bucket(bucket_name)
            destination_bucket = storage_client.get_bucket(dest_bucket_name)
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)

            for blob in blobs:
                source_blob = blob
                source_blob_path = blob.name
                source_blob_name = blob.name.split('/')[-1]
                if (len(List)>0):
                    if (source_blob_name in List):
                        dest_blob_name = dest_prefix+regime+"/"+source_blob_name 
                        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, dest_blob_name)
                        source_blob.delete()

                    
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Move_and_delete', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#-----------------------------Generate UniqueID for this run-----------------------------------------------------------
class unique_id(beam.DoFn):

    def process(self, element):
        try:
            id = str(uuid.uuid1())
            yield id

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'unique_id', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
        
 
class etl_start_datetime(beam.DoFn):

    def process(self, element):
        try:
            dTime = datetime.today().strftime("%d-%m-%Y %H:%M:%S")
            yield dTime

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'etl_start_datetime', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
			
class merge_file_level_log(beam.DoFn):

    def process(self, element, element2, element3):
        try:
            df = pd.DataFrame()
            previous_log = element
            crs_file_level_log = element2
            fatca_file_level_log = element3
            df = df.append(pd.DataFrame(previous_log))
            df = df.append(pd.DataFrame(crs_file_level_log))
            df = df.append(pd.DataFrame(fatca_file_level_log))
            yield df
			
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'merge_file_level_log', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
        

class merge_record_level_log(beam.DoFn):

    def process(self, element, element2):
        try:
            df = element
            record_level_log = element2
            if len(df)>0:
                record_level_log = record_level_log.append(df)
                record_level_log = record_level_log.reindex(record_level_log.columns, axis=1)
                yield record_level_log
            else:
                yield record_level_log
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'merge_record_level_log', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class read_file_level_log_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
        
    def process(self, something, filename):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)
        file_level_log = pd.DataFrame()
        try:
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    file_level_log = pd.read_csv(BytesIO(fwf_data), header=[0])
				
            if len(file_level_log) < 1:
                file_level_log['ETL ProcessId'] = ""
                file_level_log['CTCR Process ID'] = ""
                file_level_log['Regime'] = ""
                file_level_log['Source System'] = ""
                file_level_log['Source_Filename'] = ""
                file_level_log['ETL_Start_DateTime'] = ""
                file_level_log['ETL_End_DateTime'] = ""
                file_level_log['Error_ID'] = ""
				
            yield file_level_log
				
			
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'read_file_level_log_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 
        
class read_record_level_log_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
        
    def process(self, something, filename):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)
        record_level_log = pd.DataFrame()
        try:
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    record_level_log = pd.read_csv(BytesIO(fwf_data), header=[0])
				
            if len(record_level_log) < 1:
                record_level_log['ETL_ProcessId'] = ""
                record_level_log['CTCR Process ID'] = ""
                record_level_log['Regime'] = ""
                record_level_log['Source System'] = ""
                record_level_log['Source_Filename'] = ""
                record_level_log['ETL_Start_DateTime'] = ""
                record_level_log['ETL_End_DateTime'] = ""
                record_level_log['Error_ID'] = ""
                record_level_log['Validation field'] = ""
                record_level_log['Current Value'] = ""
                record_level_log['Primary Key'] = ""
                record_level_log['CSV File Index'] = ""
				
            yield record_level_log
				
			
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'read_record_level_log_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 
 #Read USDMS excel file
class read_usdms(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, element):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            cur_df = pd.DataFrame()
            for blob in blobs:
                if ("USDMS" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exchange_df = pd.read_excel(BytesIO(fwf_data),engine='xlrd')
            yield exchange_df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'read_usdms', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
			
			
			
class read_deMinimis(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, element):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            cur_df = pd.DataFrame()
            for blob in blobs:
                if ("DeMinimis" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    deMinimis = pd.read_csv(BytesIO(fwf_data))
            yield deMinimis

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'read_deMinimis', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

                             
#-------------------------------STAGE 2 FILE PROCESSING-------------------------------------------------------------------------------------------#
#--------------------------------------------JOINS---------------------------------------------------------

#Inner Join on key
class Join(beam.DoFn):
    def process(self, element, join_with,key,flag):
        try:
            if flag == 1:
                df = [element,join_with]
                #result = pd.merge(element, join_with, how='inner', on=key)
                result = reduce(lambda  left,right: pd.merge(left,right,on=key,how='inner'), df)

                yield result
            
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Join', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Inner Join on key having different names
class JoinDiffKeys(beam.DoFn):
    def process(self, element, join_with,leftkey,rightkey,flag):
        try:
            if flag == 1:
                df = [element, join_with]
                #result = pd.merge(element, join_with, how='inner', left_on=leftkey, right_on=rightkey)
                result = reduce(lambda  left,right: pd.merge(left,right,left_on=leftkey, right_on=rightkey,how='inner'), df)
                yield result
            
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'JoinDiffKeys', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))            

#Left Join on key        
class LeftJoin(beam.DoFn):
    def process(self, element, join_with,key,flag):
        try:

            if flag == 1:
                
                #result = pd.merge(element, join_with, how='left', on=key)
                df = [element, join_with]
                result = reduce(lambda  left,right: pd.merge(left,right,on=key,how='left'), df)

                yield result
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'LeftJoin', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


            
class Fatca_AccountBalanceAmount_Format(beam.DoFn):
    def process(self, element):
        try:
            df = element

            #Fix (15,2)
            for i in range(len(df)):

                if len(str(df.at[i,'Balance_Amount']).strip()) == 0:
                                                                   
                    df.at[i,'Balance_Amount'] = df.at[i,'USD_Balance_Amount']
                    df.at[i,'Balance_Currency_Code'] = 'USD'

                if df.at[i,'Balance_Amount'].strip().lstrip('0') == '':
                    df.at[i,'Balance_Amount'] = Decimal(0)

                elif len(str(df.at[i,'Balance_Amount'])[-2:].strip()) != 0:
                    df.at[i,'Balance_Amount'] = df.at[i,'Balance_Amount'].lstrip('0')
                    df.at[i,'Balance_Amount'] = Decimal(str(df.at[i,'Balance_Amount'])[:-2] + "." + str(df.at[i,'Balance_Amount'])[-2:])
                else:
                    df.at[i,'Balance_Amount'] = df.at[i,'Balance_Amount'].lstrip('0')
                    df.at[i,'Balance_Amount'] = Decimal(df.at[i,'Balance_Amount'])
                
            df['Balance_Amount']= df['Balance_Amount'].astype(str)
                                                              
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Fatca_AccountBalanceAmount_Format', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))    

class CRS_AccountBalanceAmount_Format(beam.DoFn):
    def process(self, element):
        try:
            df = element
			#Fix (15,2)
            for i in range(len(df)):

                if len(str(df.at[i,'Balance_Amount']).strip()) == 0:
                                                                   
                    df.at[i,'Balance_Amount'] = df.at[i,'USD_Balance_Amount']
                    df.at[i,'Balance_Currency_Code'] = 'USD'

                if df.at[i,'Balance_Amount'].strip().lstrip('0') == '':
                    df.at[i,'Balance_Amount'] = Decimal(0)

                elif len(str(df.at[i,'Balance_Amount'])[-2:].strip()) != 0:
                    df.at[i,'Balance_Amount'] = df.at[i,'Balance_Amount'].lstrip('0')
                    df.at[i,'Balance_Amount'] = Decimal(str(df.at[i,'Balance_Amount'])[:-2] + "." + str(df.at[i,'Balance_Amount'])[-2:])
                else:
                    df.at[i,'Balance_Amount'] = df.at[i,'Balance_Amount'].lstrip('0')
                    df.at[i,'Balance_Amount'] = Decimal(df.at[i,'Balance_Amount'])
                
            df['Balance_Amount']= df['Balance_Amount'].astype(str)
                                      
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_AccountBalanceAmount_Format', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))    
            
class Process_AccountBalance(beam.DoFn):
    def process(self, element,flag):
        try:

            if flag == 1:
                df = element
                df1 = df[['Institution_Id','Account_Id','Balance_Currency_Code','Balance_Amount']]

                yield df1
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'Process_AccountBalance'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_AccountBalance', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))         
  
  
class Process_AccountDetails(beam.DoFn):
    def process(self,element):
        try:

            df = element
            
            df.loc[df.Account_Type.isin(['D','X']), 'Financials_Other_Amount'] = ''
            df.loc[df.Account_Type.isin(['D','X']), 'Financials_Other_Currency'] = ''
            df.loc[df.Account_Type.isin(['D','DI','EI','CV','X']), 'Financials_Dividends_Amount'] = ''
            df.loc[df.Account_Type.isin(['D','DI','EI','CV','X']), 'Financials_Dividends_Currency'] = ''
            df.loc[df.Account_Type.isin(['D','DI','EI','CV','X']), 'Financials_Gross_Proceeds_Redemptions_Amount'] = ''
            df.loc[df.Account_Type.isin(['D','DI','EI','CV','X']), 'Financials_Gross_Proceeds_Redemptions_Currency'] = ''
            df.loc[df.Account_Type.isin(['DI','EI','CV','X']), 'Financials_Interest_Amount'] = ''
            df.loc[df.Account_Type.isin(['DI','EI','CV','X']), 'Financials_Interest_Currency'] = ''
            
            df['Financials_Dividends_Amount'] = pd.to_numeric(df['Financials_Dividends_Amount'], errors='coerce') 
            df['Financials_Gross_Proceeds_Redemptions_Amount'] = pd.to_numeric(df['Financials_Gross_Proceeds_Redemptions_Amount'], errors='coerce')
            df['Financials_Interest_Amount'] = pd.to_numeric(df['Financials_Interest_Amount'], errors='coerce')
            df['Financials_Other_Amount'] = pd.to_numeric(df['Financials_Other_Amount'], errors='coerce')

            #df['Financials_Dividends_Amount'] = df['Financials_Dividends_Amount'].apply(lambda x :x if len(str(x)) < 0 else(0.00 if float(x)<0 else x))
            #df['Financials_Gross_Proceeds_Redemptions_Amount'] = df['Financials_Gross_Proceeds_Redemptions_Amount'].apply(lambda x :x if len(str(x)) < 0 else(0.00 if float(x)<0 else x))
            #df['Financials_Interest_Amount'] = df['Financials_Interest_Amount'].apply(lambda x :x if len(str(x)) < 0 else(0.00 if float(x)<0 else x))
            #df['Financials_Other_Amount'] = df['Financials_Other_Amount'].apply(lambda x :x if len(str(x)) < 0 else(0.00 if float(x)<0 else x))

            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_AccountDetails', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))    

#Individual transformation of IP-Account        
class Process_IPAccountRel(beam.DoFn):
    def process(self, element,flag):
        try:

            if flag == 1 :
                df = element
                df1 = df[['Institution_Id','Involved_Party_Id','Account_Id','Account_Relationship_Type','CTCR Process ID']]
     
                yield df1
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'Process_IPAccountRel'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_IPAccountRel', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))            
            
#Individual transformation of CRS Parent IP
class CRS_Process_Parent_IP(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df1 = df[['Involved_Party_Id','Entity_Name','Institution_Id','Parent_Account_Number','CTCR Process ID','Account_Holder_Dormancy_Status_x']]
            df1 = df1.rename(columns = {"Involved_Party_Id":"Parent_Party_Id","Parent_Account_Number":"SubsOwner_Account_Number"})
            yield df1

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_Parent_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 
 
#Individual transformation of FATCA Parent IP
class FATCA_Process_Parent_IP(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df1 = df[['Involved_Party_Id','Entity_Name','Institution_Id','Parent_Account_Number','CTCR Process ID']]
            df1 = df1.rename(columns = {"Involved_Party_Id":"Parent_Party_Id","Parent_Account_Number":"SubsOwner_Account_Number"})
            yield df1
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_Parent_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class Fatca_PaymentAmount_Format(beam.DoFn):
    def process(self,element):
        try:
            df = element
            for i in range(len(df)):
               
                   
                if len(str(df.at[i,'Payment_Amount']).strip()) == 0:
                                                                    
                    df.at[i,'Payment_Amount'] = df.at[i,'Payment_Amount_USD']
                    df.at[i,'Payment_Amount_CCY'] = 'USD'                                     

                if len(str(df.at[i,'Payment_Amount'])[-2:].strip())==2:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/100,2),".2f")


                elif len(str(df.at[i,'Payment_Amount'])[-2].strip())==1:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/10,2),".2f")
        
                elif len(str(df.at[i,'Payment_Amount'])[-1].strip())==1:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/100,2),".2f")
        
                else:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount'],2),".2f")
            
            df['Payment_Amount'] = pd.to_numeric(df['Payment_Amount'], errors='coerce')

            yield df
        
                 
                                                                                                     
                                                                                            
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Fatca_PaymentAmount_Format', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#-----------------------------------------FATCA INDIVIDUAL TRANSFORMATION--------------------------------------------------


class FATCA_Process_Payments(beam.DoFn):
    def process(self,element,flag):
        try:
            if flag==1:   
                df = element
                
                df = df.groupby(['Institution_Id','Account_Id','Payment_Type','Payment_Code']).agg({'Payment_Date':max,'Payment_Amount': sum,'Payment_Amount_CCY': 'first'})


                df1 = df.pivot_table(index=["Institution_Id","Account_Id"], columns='Payment_Code', values=['Payment_Amount','Payment_Amount_CCY'], aggfunc='first')
                df1.columns = ["_".join((j,i)) for i,j in df1.columns]
                df1 = df1.reset_index()
                df_column_list = df1.columns.tolist()
                df_column_check = ["DIV_Payment_Amount","DIV_Payment_Amount_CCY","GPR_Payment_Amount","GPR_Payment_Amount_CCY","INT_Payment_Amount","INT_Payment_Amount_CCY","OTH_Payment_Amount","OTH_Payment_Amount_CCY"]
                columns_add = []

                for i in df_column_check:
                    if (i in df_column_list):
                        if (i== "DIV_Payment_Amount"):
                            df1 = df1.rename(columns = {"DIV_Payment_Amount":"Financials_Dividends_Amount"})
                        elif (i== "DIV_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"DIV_Payment_Amount_CCY":"Financials_Dividends_Currency"})
                        elif (i== "GPR_Payment_Amount"):
                            df1 = df1.rename(columns = {"GPR_Payment_Amount":"Financials_Gross_Proceeds_Redemptions_Amount"})
                        elif (i== "GPR_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"GPR_Payment_Amount_CCY":"Financials_Gross_Proceeds_Redemptions_Currency"})
                        elif (i== "INT_Payment_Amount"):
                            df1 = df1.rename(columns = {"INT_Payment_Amount":"Financials_Interest_Amount"})
                        elif (i== "INT_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"INT_Payment_Amount_CCY":"Financials_Interest_Currency"})
                        elif (i== "OTH_Payment_Amount"):
                            df1 = df1.rename(columns = {"OTH_Payment_Amount":"Financials_Other_Amount"})
                        elif (i== "OTH_Payment_Amount_CCY"):
                           df1 = df1.rename(columns = {"OTH_Payment_Amount_CCY":"Financials_Other_Currency"})
                        else:
                            continue
                        
                        
                    else :
                        if (i== "DIV_Payment_Amount"):
                            columns_add.append('Financials_Dividends_Amount')
                        elif (i== "DIV_Payment_Amount_CCY"):
                            columns_add.append('Financials_Dividends_Currency')
                        elif (i== "GPR_Payment_Amount"):
                            columns_add.append('Financials_Gross_Proceeds_Redemptions_Amount')
                        elif (i== "GPR_Payment_Amount_CCY"):
                            columns_add.append('Financials_Gross_Proceeds_Redemptions_Currency')
                        elif (i== "INT_Payment_Amount"):
                            columns_add.append('Financials_Interest_Amount')
                        elif (i== "INT_Payment_Amount_CCY"):
                            columns_add.append('Financials_Interest_Currency')
                        elif (i== "OTH_Payment_Amount"):
                            columns_add.append('Financials_Other_Amount')
                        elif (i== "OTH_Payment_Amount_CCY"):
                            columns_add.append('Financials_Other_Currency')
                        else:
                            continue
                        
                        
                for newcol in columns_add:
                        df1[newcol]= None
                yield df1
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'FATCA_Process_Payments'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_Payments', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
             
class FATCA_Process_Accounts(beam.DoFn):
    def process(self, element,flag):
        try:
            if flag==1:
                df = element
                df['DormantAccount']= df.Account_Dormancy_Status.apply(lambda x: 'Yes' if x == 'D' else 'No')
                df['Account_Closed']= df.Account_Status.apply(lambda x: 'Yes' if x == 'C' else ('No' if 'O' else ''))
                df = df.drop(columns=['Account_Dormancy_Status','Account_Status','Account_Name','Effective_From_Date','Effective_To_Date','Account_open_date','Account_close_date'])

                yield df
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'FATCA_Process_Accounts'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)                

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_Accounts', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))          
            
 #Individual transformation of IP Relationships
class FATCA_Process_IPIP(beam.DoFn):
    def process(self, element):
        try:    
            df = element
            df1 = df[['Institution_Id','Parent_Party_Id','Child_Party_Id','Relationship_Type']]
            yield df1

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_IPIP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
           
            
#Individual transformation of IP
class FATCA_Process_IP(beam.DoFn):
    def process(self,element,flag):
        try:
                                          
            if flag == 1:
                df = element
                df = df.astype(str)

                
                # Fix
                df = df.assign(TIN_MX = df['Tax_Id'],Accounts_TIN_issued_by=df['Tax_Id_Country_Code_of_issue'])

                df['Tax_Id_type']= (np.select(condlist=[df['Tax_Id_type'] == 'T', df['Tax_Id_type'] == 'E'], choicelist=['TIN', 'EIN'],default=''))
                df['Tax_Id_type'] = (np.select(condlist=[df['Involved_Party_Type'] == 'I'],choicelist=[''],default=df['Tax_Id_type']))
                df['TIN_MX_FLAG'] = df.Tax_Id_Country_Code_of_issue.apply(lambda x: 'Y' if x == 'MX' else 'N')
                df['Tax_Id'] = df['Tax_Id'].fillna('').astype(str).replace({'\.0':''},regex=True)

                df1 = df[['Institution_Id','Involved_Party_Id','Involved_Party_Type','Tax_Id_type','Tax_Id','Tax_Id_Country_Code_of_issue','Accounts_TIN_issued_by','Country_of_Residence','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Effective_From_Date']]

                df['TIN_MX'] = df.TIN_MX_FLAG.apply(lambda x: '' if x == 'N' else df.TIN_MX)

                df1['seq'] = df1.groupby(['Institution_Id','Involved_Party_Id']).cumcount()+1
                df1["seq"]= df1["seq"].astype(str)
                df2 = df1.pivot_table(index=['Institution_Id','Involved_Party_Id'], columns='seq', values=['Country_of_Residence','Tax_Id_type','Tax_Id','Accounts_TIN_issued_by'], aggfunc='first')
                df2.columns = ["_".join((j,i)) for i,j in df2.columns]
                df2 = df2.reset_index()
                df3 = df[['Institution_Id','Involved_Party_Id','Involved_Party_Type','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Effective_From_Date','FATCA_Classification','FATCA_Sub-Classification','TIN_MX','MDT_Country','Account_Holder_Dormancy_Status']]
                
                if set(['CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date']).issubset(df.columns):

                    df3 = pd.concat([df3, df[['CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date']]], axis=1)
                
                
                df_column_list = df2.columns.tolist()

                df_column_check = ["1_Country_of_Residence","2_Country_of_Residence","3_Country_of_Residence","4_Country_of_Residence","5_Country_of_Residence","6_Country_of_Residence","7_Country_of_Residence","8_Country_of_Residence","1_Tax_Id","2_Tax_Id","3_Tax_Id","4_Tax_Id","5_Tax_Id","6_Tax_Id","7_Tax_Id","8_Tax_Id","1_Tax_Id_type","2_Tax_Id_type","3_Tax_Id_type","4_Tax_Id_type","5_Tax_Id_type","6_Tax_Id_type","7_Tax_Id_type","8_Tax_Id_type","1_Accounts_TIN_issued_by","2_Accounts_TIN_issued_by","3_Accounts_TIN_issued_by","4_Accounts_TIN_issued_by","5_Accounts_TIN_issued_by","6_Accounts_TIN_issued_by","7_Accounts_TIN_issued_by","8_Accounts_TIN_issued_by"]
                columns_add = []

                for i in df_column_check:
                    if (i in df_column_list):
                        if (i== "1_Country_of_Residence"):
                            df2 = df2.rename(columns = {"1_Country_of_Residence":"Accounts_Res_Country_Code"})
                        elif (i== "2_Country_of_Residence"):
                            df2 = df2.rename(columns = {"2_Country_of_Residence":"Accounts_Res_Country_Code2"})
                        elif (i== "3_Country_of_Residence"):
                            df2 = df2.rename(columns = {"3_Country_of_Residence":"Accounts_Res_Country_Code3"})
                        elif (i== "4_Country_of_Residence"):
                            df2 = df2.rename(columns = {"4_Country_of_Residence":"Accounts_Res_Country_Code4"})
                        elif (i== "5_Country_of_Residence"):
                            df2 = df2.rename(columns = {"5_Country_of_Residence":"Accounts_Res_Country_Code5"})
                        elif (i== "6_Country_of_Residence"):
                            df2 = df2.rename(columns = {"6_Country_of_Residence":"Accounts_Res_Country_Code6"})
                        elif (i== "7_Country_of_Residence"):
                            df2 = df2.rename(columns = {"7_Country_of_Residence":"Accounts_Res_Country_Code7"})
                        elif (i== "8_Country_of_Residence"):
                            df2 = df2.rename(columns = {"8_Country_of_Residence":"Accounts_Res_Country_Code8"})
                        elif (i== "1_Tax_Id"):
                            df2 = df2.rename(columns = {"1_Tax_Id":"Accounts_TIN"})
                        elif (i== "2_Tax_Id"):
                            df2 = df2.rename(columns = {"2_Tax_Id":"Accounts_TIN2"})
                        elif (i== "3_Tax_Id"):
                            df2 = df2.rename(columns = {"3_Tax_Id":"Accounts_TIN3"})
                        elif (i== "4_Tax_Id"):
                            df2 = df2.rename(columns = {"4_Tax_Id":"Accounts_TIN4"})
                        elif (i== "5_Tax_Id"):
                            df2 = df2.rename(columns = {"5_Tax_Id":"Accounts_TIN5"})
                        elif (i== "6_Tax_Id"):
                            df2 = df2.rename(columns = {"6_Tax_Id":"Accounts_TIN6"})
                        elif (i== "7_Tax_Id"):
                            df2 = df2.rename(columns = {"7_Tax_Id":"Accounts_TIN7"})
                        elif (i== "8_Tax_Id"):
                            df2 = df2.rename(columns = {"8_Tax_Id":"Accounts_TIN8"})
                        elif (i== "1_Tax_Id_type"):
                            df2 = df2.rename(columns = {"1_Tax_Id_type":"Accounts_TIN_Type"})
                        elif (i== "2_Tax_Id_type"):
                            df2 = df2.rename(columns = {"2_Tax_Id_type":"Accounts_TIN_Type2"})
                        elif (i== "3_Tax_Id_type"):
                            df2 = df2.rename(columns = {"3_Tax_Id_type":"Accounts_TINType3"})
                        elif (i== "4_Tax_Id_type"):
                            df2 = df2.rename(columns = {"4_Tax_Id_type":"Accounts_TINType4"})
                        elif (i== "5_Tax_Id_type"):
                            df2 = df2.rename(columns = {"5_Tax_Id_type":"Accounts_TINType5"})
                        elif (i== "6_Tax_Id_type"):
                            df2 = df2.rename(columns = {"6_Tax_Id_type":"Accounts_TINType6"})
                        elif (i== "7_Tax_Id_type"):
                            df2 = df2.rename(columns = {"7_Tax_Id_type":"Accounts_TINType7"})
                        elif (i== "8_Tax_Id_type"):
                            df2 = df2.rename(columns = {"8_Tax_Id_type":"Accounts_TINType8"})
                        elif (i== "1_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"1_Accounts_TIN_issued_by":"Accounts_TIN_issued_by"})
                        elif (i== "2_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"2_Accounts_TIN_issued_by":"Accounts_TIN_issued_by2"})
                        elif (i== "3_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"3_Accounts_TIN_issued_by":"Accounts_TIN_issued_by3"})
                        elif (i== "4_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"4_Accounts_TIN_issued_by":"Accounts_TIN_issued_by4"})
                        elif (i== "5_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"5_Accounts_TIN_issued_by":"Accounts_TIN_issued_by5"})
                        elif (i== "6_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"6_Accounts_TIN_issued_by":"Accounts_TIN_issued_by6"})
                        elif (i== "7_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"7_Accounts_TIN_issued_by":"Accounts_TIN_issued_by7"})
                        elif (i== "8_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"8_Accounts_TIN_issued_by":"Accounts_TIN_issued_by8"})
                        else:
                            pass
                    else :
                        if (i== "1_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code')
                        elif (i== "2_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code2')
                        elif (i== "3_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code3')
                        elif (i== "4_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code4')
                        elif (i== "5_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code5')
                        elif (i== "6_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code6')
                        elif (i== "7_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code7')
                        elif (i== "8_Country_of_Residence"):
                            columns_add.append('Accounts_Res_Country_Code8')
                        elif (i== "1_Tax_Id"):
                            columns_add.append('Accounts_TIN')
                        elif (i== "2_Tax_Id"):
                            columns_add.append('Accounts_TIN2')
                        elif (i== "3_Tax_Id"):
                            columns_add.append('Accounts_TIN3')
                        elif (i== "4_Tax_Id"):
                            columns_add.append('Accounts_TIN4')
                        elif (i== "5_Tax_Id"):
                            columns_add.append('Accounts_TIN5')
                        elif (i== "6_Tax_Id"):
                            columns_add.append('Accounts_TIN6')
                        elif (i== "7_Tax_Id"):
                            columns_add.append('Accounts_TIN7')
                        elif (i== "8_Tax_Id"):
                            columns_add.append('Accounts_TIN8')
                        elif (i== "1_Tax_Id_type"):
                            columns_add.append('Accounts_TIN_Type')
                        elif (i== "2_Tax_Id_type"):
                            columns_add.append('Accounts_TIN_Type2')
                        elif (i== "3_Tax_Id_type"):
                            columns_add.append('Accounts_TINType3')
                        elif (i== "4_Tax_Id_type"):
                            columns_add.append('Accounts_TINType4')
                        elif (i== "5_Tax_Id_type"):
                            columns_add.append('Accounts_TINType5')
                        elif (i== "6_Tax_Id_type"):
                            columns_add.append('Accounts_TINType6')
                        elif (i== "7_Tax_Id_type"):
                            columns_add.append('Accounts_TINType7')
                        elif (i== "8_Tax_Id_type"):
                            columns_add.append('Accounts_TINType8')
                        elif (i== "1_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by')
                        elif (i== "2_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by2')
                        elif (i== "3_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by3')
                        elif (i== "4_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by4')
                        elif (i== "5_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by5')
                        elif (i== "6_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by6')
                        elif (i== "7_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by7')
                        elif (i== "8_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by8')
                        else:
                            pass

                for newcol in columns_add:
                    df2[newcol]= None

                df3 = df3.assign(Account_Holder_Type='',ReportingYear=pd.DatetimeIndex(df3['Effective_From_Date']).year.astype(str),Building_Identifier='',Street_Name='',District_Name='',Accounts_Address_Free='',City=df3['Address_Town/City'],Post_Code = df3['Address_Post_Code'],Country_Code = df3['Address_Country_Code'],ProvinceStateCodeOrName='',Customer_ID = df3['Involved_Party_Id'])
                df3 = df3.assign(BirthInfoYear=pd.DatetimeIndex(df1['Individual_DOB']).year.astype(str),BirthInfoMonth=pd.DatetimeIndex(df1['Individual_DOB']).month.astype(str),BirthInfoDay=pd.DatetimeIndex(df1['Individual_DOB']).day.astype(str))
                
                df3['BirthInfoYear'] = df3['BirthInfoYear'].fillna('').astype(str).replace({'nan':''},regex=True)
                df3['BirthInfoMonth'] = df3['BirthInfoMonth'].fillna('').astype(str).replace({'nan':''},regex=True)
                df3['BirthInfoDay'] = df3['BirthInfoDay'].fillna('').astype(str).replace({'nan':''},regex=True)

                df3['Category_of_Account_Holder'] = df3.Involved_Party_Type.apply(lambda x: 'Individual' if x == 'I' else ('Organisation' if 'E' else x))
                df3['Post_Code'] = df3['Post_Code'].fillna('').astype(str).replace({'\.0':''},regex=True)
                df3['Account_Holder_Dormancy_Status']= df3.Account_Holder_Dormancy_Status.apply(lambda x: 'Yes' if x == 'D' else 'No')

                All_columns = list(df3.columns)

                for row in df3.values:

                    if row[All_columns.index('MDT_Country')] != 'CA' and row[All_columns.index('Individual_DOB')] is not None:
                        row[All_columns.index('BirthInfoYear')] = ''
                        row[All_columns.index('BirthInfoMonth')] = ''
                        row[All_columns.index('BirthInfoDay')] = ''

                    else:
                        pass

                    FreeText = ''
                    FreeAddress = ''
                    FreeBI_Text = ''

                    if row[All_columns.index('Address_Line_1_Format_Code')] is not None and row[All_columns.index('Address_Line_1')] is not None:
                        
                        # print(row[All_columns.index('Address_Line_1_Format_Code')],row[All_columns.index('Address_Line_1')])
                        if row[All_columns.index('Address_Line_1_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_1')]

                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_1')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_1')]
                        
                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_1_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_1')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_1')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_2_Format_Code')] is not None and row[All_columns.index('Address_Line_2')] is not None:

                        if row[All_columns.index('Address_Line_2_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_2')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_2')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_2')]
                        
                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_2_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_2')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_2')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_3_Format_Code')] is not None and row[All_columns.index('Address_Line_3')] is not None:

                        if row[All_columns.index('Address_Line_3_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_3')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_3')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_3')]
                        
                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_3_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_3')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_3')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_4_Format_Code')] is not None and row[All_columns.index('Address_Line_4')] is not None:

                        if row[All_columns.index('Address_Line_4_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_4')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_4')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_4')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_4')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_4')]
                        
                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_4_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_4')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_4')]
                            else :
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_4')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_5_Format_Code')] is not None and row[All_columns.index('Address_Line_5')] is not None:

                        if row[All_columns.index('Address_Line_5_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_5')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_5')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_5')]
                        
                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_5_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_5')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_5')]
                        else:
                            pass

                    FreeText_list = FreeText.split(",")
                    FreeAddress_list = FreeAddress.split(",")
                    FreeBI_Text_list = FreeBI_Text.split(",")

                    if len(FreeText_list) > 1:
                        row[All_columns.index('Street_Name')] = FreeText

                    if len(FreeAddress_list) > 1:
                        row[All_columns.index('Accounts_Address_Free')] = FreeAddress

                    if len(FreeBI_Text_list) > 1:
                        row[All_columns.index('Building_Identifier')] = FreeBI_Text
                        
                df_list = [df2, df3]
                df4 = reduce(lambda  left,right: pd.merge(left,right,on=['Institution_Id','Involved_Party_Id'],how='inner'), df_list)

                #df4 = pd.merge(df2, df3, how='inner', on=['Institution_Id','Involved_Party_Id'])

                df4 = df4.drop(columns=['Involved_Party_Type','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Effective_From_Date'])
    
				# pipe symbol replace in address line
                pipe_rm_column = ['Building_Identifier','Street_Name','District_Name','City','Post_Code','Country_Code','ProvinceStateCodeOrName','Accounts_Address_Free']
                
                for i in pipe_rm_column:
                    df4[i] = df4[i].apply(lambda x: x.replace('|',' '))
                yield df4          

            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'FATCA_Process_IP'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#Individual transformation of Child IP
class FATCA_Process_Child_IP(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df['SubsOwner_Substantial_owner_type']=df['Category_of_Account_Holder']
            df = df.rename(columns={"Individual_Forenames":"SubsOwner_First_Name","Individual_Surname":"SubsOwner_Last_Name","Street_Name":"SubsOwner_Street_Name","City":"SubsOwner_City","Post_Code":"SubsOwner_Post_Code","Country_Code":"SubsOwner_Country_Code","Accounts_Res_Country_Code":"SubsOwner_ResCountryCode","Accounts_Res_Country_Code2":"SubsOwner_ResCountryCode2","Accounts_Res_Country_Code3":"SubsOwner_ResCountryCode3","Accounts_Res_Country_Code4":"SubsOwner_ResCountryCode4","Accounts_Res_Country_Code5":"SubsOwner_ResCountryCode5","Accounts_Res_Country_Code6":"SubsOwner_ResCountryCode6","Accounts_Res_Country_Code7":"SubsOwner_ResCountryCode7","Accounts_Res_Country_Code8":"SubsOwner_ResCountryCode8","Accounts_TIN":"SubsOwner_TIN","Accounts_TIN2":"SubsOwner_TIN2","Accounts_TIN3":"SubsOwner_TIN3","Accounts_TIN4":"SubsOwner_TIN4","Accounts_TIN5":"SubsOwner_TIN5","Accounts_TIN6":"SubsOwner_TIN6","Accounts_TIN7":"SubsOwner_TIN7","Accounts_TIN8":"SubsOwner_TIN8","Accounts_TIN_issued_by":"SubsOwner_TINIssuedBy","Accounts_TIN_issued_by2":"SubsOwner_TINissuedby2","Accounts_TIN_issued_by3":"SubsOwner_TINissuedby3","Accounts_TIN_issued_by4":"SubsOwner_TINissuedby4","Accounts_TIN_issued_by5":"SubsOwner_TINissuedby5","Accounts_TIN_issued_by6":"SubsOwner_TINissuedby6","Accounts_TIN_issued_by7":"SubsOwner_TINissuedby7","Accounts_TIN_issued_by8":"SubsOwner_TINissuedby8","District_Name":"SubsOwner_District_Name","Building_Identifier":"SubsOwner_Building_Identifier","Category_of_Account_Holder":"SubsOwner_Indivdual_Organisation_Indicator","Individual_DOB":"SubsOwner_Date_Of_Birth"})
            #df['SubsOwner_Account_Number']=df['Involved_Party_Id']
            df['SubsOwner_Customer_ID']=df['Involved_Party_Id']  
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_Child_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#Individual transformation of ControlPerson
class FATCA_ProcessControlPerson(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df = df.drop(columns=['Accounts_TIN_Type','Accounts_TIN_Type2','Accounts_TINType3','Accounts_TINType4','Accounts_TINType5','Accounts_TINType6','Accounts_TINType7','Accounts_TINType8','Child_Party_Id','Involved_Party_Id','Parent_Party_Id','Account_Holder_Type'])
            df = df.rename(columns = {'Account_Holder_Dormancy_Status':'Account_Holder_Dormancy_Status_x',"Entity_Name_x":"SubsOwner_Account_Holder_Entity_Name","ReportingYear":"SubsOwner_ReportingYear","Institution_Id":"FI_ID","Entity_Name_y":"SubsOwner_Entity_Name","ProvinceStateCodeOrName":"SubsOwner_Province","BirthInfoDay":"SubsOwner_BirthInfoDay","BirthInfoMonth":"SubsOwner_BirthInfoMonth","BirthInfoYear":"SubsOwner_BirthInfoYear"})

            #columns_add = ['Financials_Other_Currency','SubsOwner_Address_Free','SubsOwner_Address_Type','SubsOwner_Birth_City','SubsOwner_Birth_CountryCode','SubsOwner_Country_Subentity','SubsOwner_Country_Subentity','SubsOwner_Floor_Identifier','SubsOwner_ItalianTIN','SubsOwner_Middle_Name','SubsOwner_Name_Type','SubsOwner_POB','SubsOwner_Suite_Identifier','SubsOwner_Identification_Number','SubsOwner_NoTINReason','SubsOwner_NoTINReason2','SubsOwner_NoTINReason3','SubsOwner_NoTINReason4','SubsOwner_Birth_FormerCountryName','SubsOwner_NoTINReason5',	'SubsOwner_NoTINReason6',	'SubsOwner_NoTINReason7',	'SubsOwner_NoTINReason8',	'SubsOwner_Nationality5',	'SubsOwner_Nationality6',	'SubsOwner_Nationality7',	'SubsOwner_Nationality8',	'SubsOwner_StateCode',	'SubsOwner_TINType5',	'SubsOwner_TINType6',	'SubsOwner_TINType7',	'SubsOwner_TINType8',	'SubsOwner_India_Customer_ID',	'Nationality2',	'Nationality3',	'Nationality4',	'SubsOwner_Aadhaar_Number',	'SubsOwner_AddressFreeCN',	'SubsOwner_CityCN',	'SubsOwner_DistrictNameCN',	'SubsOwner_DueDiligenceInd',	'SubsOwner_Fathers_name',	'SubsOwner_Gender',	'SubsOwner_Identification_Type',	'SubsOwner_NameCN',	'SubsOwner_Nationality',	'SubsOwner_Occupation_Type',	'SubsOwner_PAN',	'SubsOwner_Postal_Address_Line_1',	'SubsOwner_Postal_Address_Line_2',	'SubsOwner_PostCodeCN',	'SubsOwner_Spouses_Name',	'SubsOwner_TIN_Type',	'SubsOwner_TIN_Type2',	'SubsOwner_TIN_Type3',	'SubsOwner_TIN_Type4',	'SubsOwner_US_TIN']

            #for newcol in columns_add:
            #   df[newcol]= None
            df['SubsOwner_ReportingType'] = 'FATCA'


            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessControlPerson', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#Individual transformation of Account Holder     
class FATCA_ProcessFinalAccount(beam.DoFn):
    def process(self,element):
        try:
            df = element
            
                                       
                
                                                                       
                                                                                                                  
                                       
            df = df.rename(columns={"Account_Id":"Accounts_Account_Number","DormantAccount":"Accounts_DormantAccount","Balance_Currency_Code":"Financials_Account_Currency","Balance_Amount":"Financials_Balance","Account_Closed":"Accounts_Account_Closed","Individual_Forenames":"Accounts_First_Name","Individual_Surname":"Accounts_Last_Name","Individual_DOB":"Accounts_Date_Of_Birth","Entity_Name":"Accounts_Entity_Name","Category_of_Account_Holder":"Accounts_Category_of_Account_Holder","Account_Holder_Type":"Accounts_Account_Holder_Type","ReportingYear":"Accounts_ReportingYear","Building_Identifier":"Accounts_Building_Identifier","Street_Name":"Accounts_Street_Name","District_Name":"Accounts_District_Name","City":"Accounts_City","Post_Code":"Accounts_Post_Code","Country_Code":"Accounts_Country_Code","Institution_Id":"FI_ID","Customer_ID":"Accounts_Customer_ID","BirthInfoYear":"Accounts_BirthInfoYear","BirthInfoMonth":"Accounts_BirthInfoMonth","BirthInfoDay":"Accounts_BirthInfoDay","ProvinceStateCodeOrName":"Accounts_Province"})
            
            #columns_add = ['Accounts_Birth_City','Accounts_UndocumentedAccount','Accounts_Reporting_Type',	'Accounts_Account_Number_Type',	'Accounts_Individual_Name_Type',	'Accounts_Middle_Name',	'Accounts_Birth_CountryCode',	'Accounts_Entity_Name_Type',	'Accounts_Address_Type',	'Accounts_Suite_Identifier',	'Accounts_Floor_Identifier',	'Accounts_POB',	'Accounts_Country_Subentity',	'Financials_Aggregate_Amount',	'Financials_Aggregate_Currency',	'Financials_Aggregate_Desc',	'Financials_Depository_Interest_Amount',	'Financials_Depository_Interest_Currency',	'Financials_Depository_Interest_Desc',	'Financials_Income_Amount',	'Financials_Income_Currency',	'Financials_Income_Desc',	'Financials_Dividends_Desc',	'Financials_Gross_Proceeds_Redemptions_Desc',	'Financials_Interest_Desc',	'Financials_Other_Desc',	'Accounts_ItalianTIN',	'Accounts_TIN_MX',	'Accounts_NoTINReason',	'Accounts_NoTINReason2',	'Accounts_NoTINReason3',	'Accounts_NoTINReason4',	'CanadianIdentificationNumber',	'Accounts_Birth_FormerCountryName',	'Accounts_Non_US_TIN',	'Accounts_NoTINReason5',	'Accounts_NoTINReason6',	'Accounts_Gross_Proceeds_From_Sale_Of_Property',	'Accounts_Account_Holder_Type_for_US_Reportable_Person',	'Accounts_Account_Holder_Type_for_Other_Reportable_Person',	'Accounts_Nationality5',	'Accounts_Nationality6',	'Accounts_Nationality7',	'Accounts_Nationality8',	'Accounts_India_Customer_ID',	'Account_Average',	'Account_Average_Currency',	'Accounts_Aadhaar_Number',	'Accounts_Account_Category',	'Accounts_Account_Treatment',	'Accounts_Account_Type',	'Accounts_Accounts_Status',	'Accounts_AddressFreeCN',	'Accounts_Branch_Address',	'Accounts_Branch_City/Town',	'Accounts_Branch_Country_Code',	'Accounts_Branch_Email',	'Accounts_Branch_Fax',	'Accounts_Branch_Fax_STD Code',	'Accounts_Branch_Mobile',	'Accounts_Branch_Name',	'Accounts_Branch_Number_Type',	'Accounts_Branch_Postal_Code',	'Accounts_Branch_Reference_Number',	'Accounts_Branch_State_Code',	'Accounts_Branch_Telephone_Number',	'Accounts_Branch_Telephone_Number_STD Code',	'Accounts_CityCN',	'Accounts_Country_Of_Incorporation',	'Accounts_Date of Account Closure',	'Accounts_Date_Of_Incorporation',	'Accounts_DistrictNameCN',	'Accounts_DueDiligenceInd',	'Accounts_Entity_Constitution_Type',	'Accounts_EntityNameCN',	'Accounts_Fathers_Name',	'Accounts_Gender',	'Accounts_Identification_Issuing_Country',	'Accounts_Identification_Number',	'Accounts_Identification_Type',	'Accounts_IDNumber',	'Accounts_IDType' ,	'Accounts_NameCN',	'Accounts_Nationality',	'Accounts_Nationality2',	'Accounts_Nationality3',	'Accounts_Nationality4',	'Accounts_Nature_Of_Business',	'Accounts_Occupation_Type',	'Accounts_PAN',	'Accounts_PhoneNo',	'Accounts_Place_Of_Incorporation',	'Accounts_Postal_Address_Line_1',	'Accounts_PostCodeCN',	'Accounts_SelfCertification',	'Accounts_Spouses_Name',	'Accounts_TopbalanceAccount',	'Financials_Aggregate_Gross_Amount_Credited',	'Financials_Aggregate_Gross_Amount_Debited',	'Accounts_NoTINReason7',	'Accounts_NoTINReason8',	'Postal_Address_Line_2',	'Account_Preexisting_Undocumented_Indicator',	'Account_State']
            df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_First_Name'] = ''
            df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_Last_Name'] = ''
            #for newcol in columns_add:
            #        df[newcol]= None
            df['Accounts_Reporting_Type'] = 'FATCA' 
            df['Entity_Type'] = ''                      
            df = df.drop_duplicates(keep='first')
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessFinalAccount', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Extract columns needed to join with IPAccountRel
class FATCA_ProcessRFinalDF(beam.DoFn):
    def process(self,element):
        try:
            df = element
            #df = df.rename(columns={"Institution_Id_x":"Institution_Id"})
            df = df[['Institution_Id','Account_Id']]
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessRFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
class FATCA_Process_Inst_IP(beam.DoFn):
    def process(self,element):
        try:
            df = element
            #df['Institution_Id'] = df['Institution_Id_y']
            df['Parent_Account_Number'] = df['Account_Id']
            df = df.drop(columns=['Account_Id','Account_Relationship_Type'])
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_Inst_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#Individual transformation of FI along with CP and Account Holder 
class FATCA_ProcessFI_CP_Acct(beam.DoFn):
    def process(self,element):
        try:
            df = element
            if len(df)>0:

                df['FFI_ResCountryCode']=df['Country of Tax Residence']
                df['FFI_Reporting_Type']='FATCA'
                df['FFI_Nil_Indicator']= 'No'
                df['FFI_Sender_AIN_type']="ABN"
                df['FFI_ABN_number']=df['AU FATCA TIN(Reporting FI)']
                df['TCountry'] = df['Country']
                df['FFI_Sender_AIN'] = ""
                df['FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
                #"EAG Entity Legal Name (Full)":"FFI_Name"
                df = df.rename(columns={"Country to be reported to":"FFI_Receiving_Country","Street (35 char limit)":"FFI_Street_Name","Postcode     (8 char limit)":"FFI_Post_Code","Town            (35 char limit)":"FFI_City","Country":"FFI_Country_Code","Telephone":"FFI_Contact","FATCA Email Address":"FFI_ContactPerson_Email_Organisation","FATCA GIIN (35 char limit)":"FFI_GIIN","FATCA Filer Category":"FFI_FilerCategory","ReportingYear":"FFI_ReportingYear","Sponsor GIIN (19 char limit)":"SPO_TIN","Sponsor Country of Tax Residence":"SPO_Res_Country_Code","Transmitter / sponsor street (40 char limit)":"SPO_Street_Name","Transmitter / sponsor city (40 char limit)":"SPO_City","Transmitter / sponsor Zip code (9 char limit)":"SPO_Post_Code","Transmitter / sponsor country":"SPO_Country_Code","Transmitter / Sponsor Company Name (40 char limit)":"SPO_Sponsoring_Entity_Name","Transmitter / Sponsor Company Contact Person (40 char limit) required for Canada":"FFI_Reporting_ContactPerson_Name","Transmitter / Sponsor Telephone Number  (15 char limit) required for Canada":"FFI_ContactPerson_Telephone_Direct","Transmitter / Sponsor Email Address  (48 char limit) required for Canada":"FFI_ContactPerson_Email_Personal","Sponsor Filer Category":"SPO_Filer_Category","UK Due Diligence Indicator (Only needed for UK HMRC)":"FFI_Due_Dil_Ind","UK FATCA User ID (Only needed for UK HMRC)":"FFI_AEOI_ID","UK FI Register ID (Needed for UK HMRC, Canada to use BNRZ number)":"FFI_Registration_ID","UK Threshold Indicator (Only needed for UK HMRC)":"FFI_Treshold_Ind","UK Insurance election":"FFI_Insurance_Election","UK Dormant account election":"FFI_Dormant_Acc_Election","TCountry":"FFI_Transmitting_Country"})
                df['FFI_Reporting_Type']='FATCA'
				#Header Title fix
                df['FFI_HeaderTitle'] = df['FFI_Name']
                columns_add = ['FFI_Inissuedby', 'FFI_SendingCompanyIN', 'FFI_INType', 'FFI_IN','FFI_AccountID','FFI_Address_Type','FFI_AuthSteuernummer','FFI_BCE_KBO_number','FFI_Building_Identifier','FFI_City_Postal','FFI_Country_Postal','FFI_Country_Subentity','FFI_District_Name','FFI_Floor_Identifier','FFI_IdentificationNumberExtension','FFI_IN2','FFI_NationalTIN','FFI_NIF','FFI_PersonalIdentificationNumber','FFI_POB','FFI_Postal_Code_Postal','FFI_Reporting_ContactPerson_First_Name','FFI_Suite_Identifier','FFI_TIN_Italian','FFI_Warning','SPO_BCE_KBO_Number','SPO_Building_Identifier','SPO_Country_Subentity','SPO_District_Name','SPO_Floor_Identifier','SPO_Intermediary_NIF','SPO_Intermediary_TIN','SPO_Intermediary_TIN_Italy','SPO_Is_Intermediary_Ind','SPO_POB','SPO_Suite_Identifier','SPO_TINIssuedBy','FFI_ABN_number','SPO_ABN_number','FFI_No_Account_To_Report','FilerContactPhoneAreaCode','FFI_ContactPhoneExtensionNumber','FFI_State_Code','SPProvinceStateCodeOrName','FFI_ContactPerson_Telephone_Number_STD Code',	'SPO_State_Code',	'FFI_Transmitter_Number',	'FFI_BZ_ID',	'FFI_FI_Identification_Number',	'FFI_Principal_Officer_Name',	'FFI_Mobile',	'FFI_Fax',	'FFI_FIProvinceStateCodeOrName',	'FFI_Attention_Note',	'FFI_ContactPerson_Telephone_Number',	'FFI_DueDiligenceInd',	'FFI_EntityID',	'FFI_EntityIDType',	'FFI_FIID',	'FFI_FilerAccountNumber',	'FFI_ITDREIN',	'FFI_MyCRS_ID',	'FFI_Postal_Address_Line_1',	'FFI_Postal_Address_Line_2',	'FFI_Principal_Officer_Designation',	'FFI_Registration_Number',	'FFI_Report_Type',	'FFI_Reporting_Entity_Category',	'FFI_ReportingFIType',	'FFI_ReportingID',	'FFI_Sector_And_Subsector_Codes',	'SPAddressLine',	'SPO_SPGIIN',	'FFI_ContactPerson_Fax_STDCode',	'FFI_ContactPerson_Fax',	'FFI_FIAddressLine',	'FFI_Name_Type',	'PoolReportReportingFIGIIN',	'PoolReportMessageRefId',	'PoolReportDocRefId']

                for newcol in columns_add:
                        df[newcol]= None
                '''
                #performance
                All_columns = list(df.columns)

                for row in df.values:
                    if row[All_columns.index('FFI_ResCountryCode')] =='SG' :
                        row[All_columns.index('FFI_Contact')] = row[All_columns.index("SG-FATCA Contact (200 Char Limit)")]
                    if row[All_columns.index('FFI_ResCountryCode')] =='NZ' :
                        row[All_columns.index('FFI_Contact')] = row[All_columns.index("NZ-FATCA Contact(200 Char Limit)")]
                    if row[All_columns.index('FFI_ResCountryCode')] =='AU' :
                        row[All_columns.index('FFI_Sender_AIN')] = row[All_columns.index("AU FATCA TIN(Reporting FI)")]
                '''

                df['FFI_Contact'] = np.where(df.FFI_ResCountryCode.eq('SG'), df['SG-FATCA Contact (200 Char Limit)'],df['FFI_Contact'])
                df['FFI_Contact'] = np.where(df.FFI_ResCountryCode.eq('NZ'), df['NZ-FATCA Contact(200 Char Limit)'], df['FFI_Contact'])
                df['FFI_Sender_AIN'] = np.where(df.FFI_ResCountryCode.eq('AU'),  df['AU FATCA TIN(Reporting FI)'], df['FFI_Sender_AIN'])

                #GB Fix
                df['FFI_AEOI_ID1'] = df['FFI_AEOI_ID'].fillna('').astype(str)
                df[['FFI_AEOI_ID2','last1']] = [[i.split('.')[0],i.split('.')[1]] if ((len(i.split('.')) == 2) & (i.replace('.','',1).isdigit())) else [i,''] for i in list(df['FFI_AEOI_ID1'])]
                df['FFI_AEOI_ID'] = np.where(df.FFI_ResCountryCode.eq('GB'), df['FFI_AEOI_ID2'], df['FFI_AEOI_ID'])

                # Canada Changes
                                                                                                                                                    

                # 'Sponsor GIIN (19 char limit)' has been renamed as 'SPO_TIN' 
                
                df['SPO_SPGIIN'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['SPO_TIN'], df['SPO_SPGIIN'])
                df['SPO_State_Code'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['Transmitter / sponsor CAN province'], df['SPO_State_Code'])
                df['FFI_FilerAccountNumber'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA FI Register ID (Canada to use BNRZ Number) (15 Char)'], df['FFI_FilerAccountNumber'])
                df['FFI_Transmitter_Number'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA Transmitter number (Required for Canada) 35 char limit'], df['FFI_Transmitter_Number'])
                df['FFI_ContactPerson_Email_Organisation'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA Contact Email ID (Required for Canada) 35 char limit'], df['FFI_ContactPerson_Email_Organisation'])
                
                
                # Contact Telephone person
                # CA Contact Telephone Number (Required for Canada) 35 char limit
                '''
                df['Telephone Number Data'] = df['CA Contact Telephone Number (Required for Canada) 35 char limit'].fillna('').astype(str).replace({'':'0-0-0'},regex=True)

                df[['First','Middle','Last']] = df['Telephone Number Data'].str.split('-',expand=True,)
                df['Telephone_Number'] = df['Middle'].astype(str) + "-" + df['Last'].astype(str)
                '''
                df['Telephone Number Data'] = df['CA Contact Telephone Number (Required for Canada) 35 char limit'].fillna('').astype(str)
				
                df[['First','Middle','Last']] = [[i.split('-')[0],i.split('-')[1],i.split('-')[2]] if len(i.split('-')) == 3  else ['','',''] for i in list(df['Telephone Number Data'])]
                df['Telephone_Number'] = df['Middle'].astype(str) + "-" + df['Last'].astype(str)
                    
                df['FFI_ContactPerson_Telephone_Number_STD Code'] = np.where((df.FFI_ResCountryCode.eq('CA')) & (df['Telephone Number Data'] != '0-0-0'),  df['First'], df['FFI_ContactPerson_Telephone_Number_STD Code'])
                df['FFI_ContactPerson_Telephone_Number'] = np.where((df.FFI_ResCountryCode.eq('CA')) & (df['Telephone Number Data'] != '0-0-0'),  df['Telephone_Number'], df['FFI_ContactPerson_Telephone_Number'])
                
                # GB Changes
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Dormant_Acc_Election.str.strip()=='')|(df.FFI_Dormant_Acc_Election.isna())),'FFI_Dormant_Acc_Election'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Due_Dil_Ind.str.strip()=='')|(df.FFI_Due_Dil_Ind.isna())),'FFI_Due_Dil_Ind'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Insurance_Election.str.strip()=='')|(df.FFI_Insurance_Election.isna())),'FFI_Insurance_Election'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Treshold_Ind.str.strip()=='')|(df.FFI_Treshold_Ind.isna())),'FFI_Treshold_Ind'] = 'N/A'

                #df.loc[(df.FFI_ResCountryCode =='GB'), 'FFI_Reporting_Type'] = 'Combined'

                df = df[['FFI_Reporting_Type','FFI_ReportingYear','FFI_Name','FFI_FilerCategory','FFI_GIIN','FFI_IN','FFI_Inissuedby','FFI_INType','FFI_SendingCompanyIN','FFI_ResCountryCode','FFI_Transmitting_Country','FFI_Receiving_Country',	'FFI_Nil_Indicator',	'FFI_Address_Type',	'FFI_Country_Code',	'FFI_Street_Name',	'FFI_Building_Identifier',	'FFI_Suite_Identifier',	'FFI_Floor_Identifier',	'FFI_District_Name','FFI_POB','FFI_Post_Code','FFI_City','FFI_Country_Subentity','FFI_Warning','FFI_Contact','SPO_Sponsoring_Entity_Name',	'SPO_Is_Intermediary_Ind',	'SPO_Filer_Category','SPO_TIN','SPO_Res_Country_Code','SPO_Intermediary_TIN','SPO_TINIssuedBy','SPO_Country_Code','SPO_Street_Name','SPO_Building_Identifier',	'SPO_Suite_Identifier',	'SPO_Floor_Identifier',	'SPO_District_Name','SPO_POB','SPO_Post_Code','SPO_City','SPO_Country_Subentity','SPO_Intermediary_TIN_Italy',	'SPO_Intermediary_NIF',	'SPO_BCE_KBO_Number','FFI_AuthSteuernummer','FFI_AccountID','FFI_Postal_Code_Postal','FFI_City_Postal','FFI_Country_Postal','FFI_Reporting_ContactPerson_Name',	'FFI_Reporting_ContactPerson_First_Name','FFI_ContactPerson_Email_Personal','FFI_ContactPerson_Email_Organisation',	'FFI_ContactPerson_Telephone_Direct','FFI_AEOI_ID','FFI_Registration_ID','FFI_Due_Dil_Ind','FFI_Treshold_Ind','FFI_Insurance_Election','FFI_Dormant_Acc_Election','FFI_IN2','FFI_NationalTIN','FFI_IdentificationNumberExtension','FFI_HeaderTitle','FFI_PersonalIdentificationNumber','FFI_BCE_KBO_number','FFI_TIN_Italian','FFI_NIF','FFI_ABN_number','SPO_ABN_number','FFI_Sender_AIN_type','FFI_Sender_AIN','FFI_ContactPerson_Telephone_Number_STD Code','FFI_ContactPhoneExtensionNumber','FFI_State_Code','SPO_State_Code','FFI_Transmitter_Number','FFI_BZ_ID','FFI_FI_Identification_Number','FFI_Principal_Officer_Name','FFI_Mobile','FFI_Fax','FFI_FIProvinceStateCodeOrName','FFI_Attention_Note','FFI_ContactPerson_Telephone_Number','FFI_DueDiligenceInd','FFI_EntityID','FFI_EntityIDType','FFI_FIID','FFI_FilerAccountNumber','FFI_ITDREIN','FFI_MyCRS_ID','FFI_Postal_Address_Line_1','FFI_Postal_Address_Line_2','FFI_Principal_Officer_Designation','FFI_Registration_Number','FFI_Report_Type','FFI_Reporting_Entity_Category','FFI_ReportingFIType','FFI_ReportingID','FFI_Sector_And_Subsector_Codes','SPAddressLine',	'SPO_SPGIIN','FFI_ContactPerson_Fax_STDCode','FFI_ContactPerson_Fax','FFI_FIAddressLine','FFI_Name_Type','PoolReportReportingFIGIIN','PoolReportMessageRefId','PoolReportDocRefId','Abbrev. (Insitution ID)                 (8 char limit)','Service Institution ID']]
                df1 = df.drop_duplicates(keep='first')
                yield df1
            else:
                yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessFI_CP_Acct', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#-------------------------------------------CRS INDIVIDUAL TRANSFORMATION--------------------------------------------------

#Individual transformation of Accounts
class CRS_Process_Accounts(beam.DoFn):
    def process(self, element,flag):
        try:

            if flag == 1:
                df = element
                df['DormantAccount']= df.Account_Dormancy_Status.apply(lambda x: 'Yes' if x == 'D' else 'No')
                df['Account_Closed']= df.Account_Status.apply(lambda x: 'Yes' if x == 'C' else ('No' if 'O' else ''))
                df = df.drop(columns=['Account_Dormancy_Status','Account_Status','Account_Sub-Type','Account_Name','Effective_From_Date','Effective_To_Date','Account_open_date','Account_close_date'])
      
                yield df
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'CRS_Process_Accounts'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_Accounts', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class CRS_PaymentAmount_Format(beam.DoFn):
    def process(self,element):
        try:
            df = element
            for i in range(len(df)):
                
                   
                if len(str(df.at[i,'Payment_Amount']).strip()) == 0:
                                                                    
                    df.at[i,'Payment_Amount'] = df.at[i,'Payment_Amount_USD']
                    df.at[i,'Payment_Amount_CCY'] = 'USD'                                     
            
                                                          
                       

                if len(str(df.at[i,'Payment_Amount'])[-2:].strip())==2:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/100,2),".2f")
                    

                elif len(str(df.at[i,'Payment_Amount'])[-2].strip())==1:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/10,2),".2f")
        
                elif len(str(df.at[i,'Payment_Amount'])[-1].strip())==1:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount']/100,2),".2f")
        
                else:
                    df.at[i,'Payment_Amount'] = pd.to_numeric(df.at[i,'Payment_Amount'], errors='coerce')
                    df.at[i,'Payment_Amount'] = format(round(df.at[i,'Payment_Amount'],2),".2f")
            
            df['Payment_Amount'] = pd.to_numeric(df['Payment_Amount'], errors='coerce')

            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_PaymentAmount_Format', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
        
#Individual transformation of Payments
class CRS_Process_Payments(beam.DoFn):
    def process(self,element,flag):
        try:

            if flag == 1:
                df = element
                df = df.reset_index(drop = True)


                                                                                                              
                df = df.groupby(['Institution_Id','Account_Id','Payment_Type','Payment_Code']).agg({'Payment_Date':max,'Payment_Amount': sum,'Payment_Amount_CCY': 'first'})
                df1 = df.pivot_table(index=["Institution_Id","Account_Id"], columns='Payment_Code', values=['Payment_Amount','Payment_Amount_CCY'], aggfunc='first')
                df1.columns = ["_".join((j,i)) for i,j in df1.columns]
                df1 = df1.reset_index()
                df_column_list = df1.columns.tolist()
                df_column_check = ["DIV_Payment_Amount","DIV_Payment_Amount_CCY","GPR_Payment_Amount","GPR_Payment_Amount_CCY","INT_Payment_Amount","INT_Payment_Amount_CCY","OTH_Payment_Amount","OTH_Payment_Amount_CCY"]
                columns_add = []
                for i in df_column_check:
                    if (i in df_column_list):
                        if (i== "DIV_Payment_Amount"):
                            df1 = df1.rename(columns = {"DIV_Payment_Amount":"Financials_Dividends_Amount"})
                        elif (i== "DIV_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"DIV_Payment_Amount_CCY":"Financials_Dividends_Currency"})
                        elif (i== "GPR_Payment_Amount"):
                            df1 = df1.rename(columns = {"GPR_Payment_Amount":"Financials_Gross_Proceeds_Redemptions_Amount"})
                        elif (i== "GPR_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"GPR_Payment_Amount_CCY":"Financials_Gross_Proceeds_Redemptions_Currency"})
                        elif (i== "INT_Payment_Amount"):
                            df1 = df1.rename(columns = {"INT_Payment_Amount":"Financials_Interest_Amount"})
                        elif (i== "INT_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"INT_Payment_Amount_CCY":"Financials_Interest_Currency"})
                        elif (i== "OTH_Payment_Amount"):
                            df1 = df1.rename(columns = {"OTH_Payment_Amount":"Financials_Other_Amount"})
                        elif (i== "OTH_Payment_Amount_CCY"):
                            df1 = df1.rename(columns = {"OTH_Payment_Amount_CCY":"Financials_Other_Currency"})
                        else:
                            continue
    
                    else :
                        if (i== "DIV_Payment_Amount"):
                            columns_add.append('Financials_Dividends_Amount')
                        elif (i== "DIV_Payment_Amount_CCY"):
                            columns_add.append('Financials_Dividends_Currency')
                        elif (i== "GPR_Payment_Amount"):
                            columns_add.append('Financials_Gross_Proceeds_Redemptions_Amount')
                        elif (i== "GPR_Payment_Amount_CCY"):
                            columns_add.append('Financials_Gross_Proceeds_Redemptions_Currency')
                        elif (i== "INT_Payment_Amount"):
                            columns_add.append('Financials_Interest_Amount')
                        elif (i== "INT_Payment_Amount_CCY"):
                            columns_add.append('Financials_Interest_Currency')
                        elif (i== "OTH_Payment_Amount"):
                            columns_add.append('Financials_Other_Amount')
                        elif (i== "OTH_Payment_Amount_CCY"):
                            columns_add.append('Financials_Other_Currency')
                        else:
                            continue
                        
                        
                for newcol in columns_add:
                        df1[newcol]= None
   
                yield df1
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'CRS_Process_Payments'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_Payments', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Reportability on Account Type
#Report all as Other payments for EI, DI, CV (as per Decision tree)
#Currency for EI, DI, CV should be taken care after clarification during multiple curency on same payment type.As of now, we are putting the first element in curr_code_element and that is what displays in Other currency for EI, DI CV


#Individual transformation of IP Relationships
class CRS_Process_IPIP(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df = df.reset_index(drop=True)
            df1 =df[['Institution_Id','Parent_Party_Id','Child_Party_Id','Relationship_Type']]
            df1['SubsOwner_Substantial_owner_type']=''
            df1['Relationship_Type'] = df1['Relationship_Type'].astype(str)
            '''
            for i in range(len(df1)):
                if('21' in str(df1.loc[i,'Relationship_Type'])):
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS801'
                elif('22' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS802'
                elif('23' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS803'
                elif('24' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS804'
                elif('25' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS805'
                elif('26' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS806'
                elif('27' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS807'
                elif('28' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS808'
                elif('29' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS809'
                elif('30' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS810'
                elif('31' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS811'
                elif('32' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS812'
                elif('33' in str(df1.loc[i,'Relationship_Type'])):                
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = 'CRS813'
                else:
                    df1.loc[i,'SubsOwner_Substantial_owner_type'] = ''
            '''
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000021', 'CRS801', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000022', 'CRS802', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000023', 'CRS803', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000024', 'CRS804', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000025', 'CRS805', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000026', 'CRS806', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000027', 'CRS807', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000028', 'CRS808', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000029', 'CRS809', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000030', 'CRS810', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000031', 'CRS811', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000032', 'CRS812', df1['SubsOwner_Substantial_owner_type'])
            df1['SubsOwner_Substantial_owner_type'] = np.where((df1['Relationship_Type']) == '000033', 'CRS813', df1['SubsOwner_Substantial_owner_type'])
            df1 = df1.drop(columns=['Relationship_Type'])

            yield df1
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_IPIP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))           
 
 
 #Individual transformation of Child IP
class CRS_Process_Child_IP(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df = df.rename(columns={"Individual_Forenames":"SubsOwner_First_Name","Individual_Surname":"SubsOwner_Last_Name","Street_Name":"SubsOwner_Street_Name","City":"SubsOwner_City","Post_Code":"SubsOwner_Post_Code","Country_Code":"SubsOwner_Country_Code","Accounts_Res_Country_Code":"SubsOwner_ResCountryCode","Accounts_Res_Country_Code2":"SubsOwner_ResCountryCode2","Accounts_Res_Country_Code3":"SubsOwner_ResCountryCode3","Accounts_Res_Country_Code4":"SubsOwner_ResCountryCode4","Accounts_Res_Country_Code5":"SubsOwner_ResCountryCode5","Accounts_Res_Country_Code6":"SubsOwner_ResCountryCode6","Accounts_Res_Country_Code7":"SubsOwner_ResCountryCode7","Accounts_Res_Country_Code8":"SubsOwner_ResCountryCode8","Accounts_TIN":"SubsOwner_TIN","Accounts_TIN2":"SubsOwner_TIN2","Accounts_TIN3":"SubsOwner_TIN3","Accounts_TIN4":"SubsOwner_TIN4","Accounts_TIN5":"SubsOwner_TIN5","Accounts_TIN6":"SubsOwner_TIN6","Accounts_TIN7":"SubsOwner_TIN7","Accounts_TIN8":"SubsOwner_TIN8","Accounts_TIN_issued_by":"SubsOwner_TINIssuedBy","Accounts_TIN_issued_by2":"SubsOwner_TINissuedby2","Accounts_TIN_issued_by3":"SubsOwner_TINissuedby3","Accounts_TIN_issued_by4":"SubsOwner_TINissuedby4","Accounts_TIN_issued_by5":"SubsOwner_TINissuedby5","Accounts_TIN_issued_by6":"SubsOwner_TINissuedby6","Accounts_TIN_issued_by7":"SubsOwner_TINissuedby7","Accounts_TIN_issued_by8":"SubsOwner_TINissuedby8","District_Name":"SubsOwner_District_Name","Building_Identifier":"SubsOwner_Building_Identifier","Category_of_Account_Holder":"SubsOwner_Indivdual_Organisation_Indicator","Individual_DOB":"SubsOwner_Date_Of_Birth"})
            #df['SubsOwner_Account_Number']=df['Involved_Party_Id']
            df['SubsOwner_Customer_ID']=df['Involved_Party_Id']
            #df['Institution_Id']=df['Institution_Id_x']
            #df = df.drop(columns=['Institution_Id_x','Institution_Id_y'])
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_Child_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 

#Individual transformation of IP-Tax Domicile
class CRS_Process_IPTD(beam.DoFn):
    def process(self,element,flag):
        try:

            if flag == 1:
                df = element
                df = df.reset_index(drop=True)
                df = df.loc[df['Active_Domicile_Flag'] != 'I']
                
                df1 = df[['Institution_Id','Involved_Party_Id','Country_of_Tax_Residence','Tax_Id_type','Tax_ID']]
                df1['Accounts_TIN_issued_by'] = df1['Country_of_Tax_Residence']
                df1['Tax_Id_type']= (np.select(condlist=[df1['Tax_Id_type'] == 'T', df1['Tax_Id_type'] == 'E'], choicelist=['TIN', 'EIN'],default=''))
                
                df1['seq'] = df1.groupby(['Institution_Id','Involved_Party_Id']).cumcount()+1
                df1["seq"]= df1["seq"].astype(str)
                df1["Tax_ID"] = df1["Tax_ID"].fillna('').astype(str).replace({'\.0':''},regex=True)
                
                df1.loc[(df1['Tax_ID'] == ''),['Tax_Id_type','Accounts_TIN_issued_by']]=""
                
                df2 = df1.pivot_table(index=["Institution_Id","Involved_Party_Id"], columns='seq', values=['Country_of_Tax_Residence','Tax_Id_type','Tax_ID','Accounts_TIN_issued_by'], aggfunc='first')
                df2.columns = ["_".join((j,i)) for i,j in df2.columns]
                df2 = df2.reset_index()
        
                df_column_list = df2.columns.tolist()
                df_column_check = ["1_Country_of_Tax_Residence","2_Country_of_Tax_Residence","3_Country_of_Tax_Residence","4_Country_of_Tax_Residence","5_Country_of_Tax_Residence","6_Country_of_Tax_Residence","7_Country_of_Tax_Residence","8_Country_of_Tax_Residence","1_Tax_ID","2_Tax_ID","3_Tax_ID","4_Tax_ID","5_Tax_ID","6_Tax_ID","7_Tax_ID","8_Tax_ID","1_Tax_Id_type","2_Tax_Id_type","3_Tax_Id_type","4_Tax_Id_type","5_Tax_Id_type","6_Tax_Id_type","7_Tax_Id_type","8_Tax_Id_type","1_Accounts_TIN_issued_by","2_Accounts_TIN_issued_by","3_Accounts_TIN_issued_by","4_Accounts_TIN_issued_by","5_Accounts_TIN_issued_by","6_Accounts_TIN_issued_by","7_Accounts_TIN_issued_by","8_Accounts_TIN_issued_by"]
                columns_add = []
                for i in df_column_check:
                    if (i in df_column_list):
                        if (i== "1_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"1_Country_of_Tax_Residence":"Accounts_Res_Country_Code"})
                        elif (i== "2_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"2_Country_of_Tax_Residence":"Accounts_Res_Country_Code2"})
                        elif (i== "3_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"3_Country_of_Tax_Residence":"Accounts_Res_Country_Code3"})
                        elif (i== "4_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"4_Country_of_Tax_Residence":"Accounts_Res_Country_Code4"})
                        elif (i== "5_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"5_Country_of_Tax_Residence":"Accounts_Res_Country_Code5"})
                        elif (i== "6_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"6_Country_of_Tax_Residence":"Accounts_Res_Country_Code6"})
                        elif (i== "7_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"7_Country_of_Tax_Residence":"Accounts_Res_Country_Code7"})
                        elif (i== "8_Country_of_Tax_Residence"):
                            df2 = df2.rename(columns = {"8_Country_of_Tax_Residence":"Accounts_Res_Country_Code8"})
                        elif (i== "1_Tax_ID"):
                            df2 = df2.rename(columns = {"1_Tax_ID":"Accounts_TIN"})
                        elif (i== "2_Tax_ID"):
                            df2 = df2.rename(columns = {"2_Tax_ID":"Accounts_TIN2"})
                        elif (i== "3_Tax_ID"):
                            df2 = df2.rename(columns = {"3_Tax_ID":"Accounts_TIN3"})
                        elif (i== "4_Tax_ID"):
                            df2 = df2.rename(columns = {"4_Tax_ID":"Accounts_TIN4"})
                        elif (i== "5_Tax_ID"):
                            df2 = df2.rename(columns = {"5_Tax_ID":"Accounts_TIN5"})
                        elif (i== "6_Tax_ID"):
                            df2 = df2.rename(columns = {"6_Tax_ID":"Accounts_TIN6"})
                        elif (i== "7_Tax_ID"):
                            df2 = df2.rename(columns = {"7_Tax_ID":"Accounts_TIN7"})
                        elif (i== "8_Tax_ID"):
                            df2 = df2.rename(columns = {"8_Tax_ID":"Accounts_TIN8"})
                        elif (i== "1_Tax_Id_type"):
                            df2 = df2.rename(columns = {"1_Tax_Id_type":"Accounts_TIN_Type"})
                        elif (i== "2_Tax_Id_type"):
                            df2 = df2.rename(columns = {"2_Tax_Id_type":"Accounts_TIN_Type2"})
                        elif (i== "3_Tax_Id_type"):
                            df2 = df2.rename(columns = {"3_Tax_Id_type":"Accounts_TINType3"})
                        elif (i== "4_Tax_Id_type"):
                            df2 = df2.rename(columns = {"4_Tax_Id_type":"Accounts_TINType4"})
                        elif (i== "5_Tax_Id_type"):
                            df2 = df2.rename(columns = {"5_Tax_Id_type":"Accounts_TINType5"})
                        elif (i== "6_Tax_Id_type"):
                            df2 = df2.rename(columns = {"6_Tax_Id_type":"Accounts_TINType6"})
                        elif (i== "7_Tax_Id_type"):
                            df2 = df2.rename(columns = {"7_Tax_Id_type":"Accounts_TINType7"})
                        elif (i== "8_Tax_Id_type"):
                            df2 = df2.rename(columns = {"8_Tax_Id_type":"Accounts_TINType8"})
                        elif (i== "1_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"1_Accounts_TIN_issued_by":"Accounts_TIN_issued_by"})
                        elif (i== "2_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"2_Accounts_TIN_issued_by":"Accounts_TIN_issued_by2"})
                        elif (i== "3_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"3_Accounts_TIN_issued_by":"Accounts_TIN_issued_by3"})
                        elif (i== "4_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"4_Accounts_TIN_issued_by":"Accounts_TIN_issued_by4"})
                        elif (i== "5_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"5_Accounts_TIN_issued_by":"Accounts_TIN_issued_by5"})
                        elif (i== "6_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"6_Accounts_TIN_issued_by":"Accounts_TIN_issued_by6"})
                        elif (i== "7_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"7_Accounts_TIN_issued_by":"Accounts_TIN_issued_by7"})
                        elif (i== "8_Accounts_TIN_issued_by"):
                            df2 = df2.rename(columns = {"8_Accounts_TIN_issued_by":"Accounts_TIN_issued_by8"})
                        else:
                            continue
                    else :
                        if (i== "1_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code')
                        elif (i== "2_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code2')
                        elif (i== "3_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code3')
                        elif (i== "4_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code4')
                        elif (i== "5_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code5')
                        elif (i== "6_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code6')
                        elif (i== "7_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code7')
                        elif (i== "8_Country_of_Tax_Residence"):
                            columns_add.append('Accounts_Res_Country_Code8')
                        elif (i== "1_Tax_ID"):
                            columns_add.append('Accounts_TIN')
                        elif (i== "2_Tax_ID"):
                            columns_add.append('Accounts_TIN2')
                        elif (i== "3_Tax_ID"):
                            columns_add.append('Accounts_TIN3')
                        elif (i== "4_Tax_ID"):
                            columns_add.append('Accounts_TIN4')
                        elif (i== "5_Tax_ID"):
                            columns_add.append('Accounts_TIN5')
                        elif (i== "6_Tax_ID"):
                            columns_add.append('Accounts_TIN6')
                        elif (i== "7_Tax_ID"):
                            columns_add.append('Accounts_TIN7')
                        elif (i== "8_Tax_ID"):
                            columns_add.append('Accounts_TIN8')
                        elif (i== "1_Tax_Id_type"):
                            columns_add.append('Accounts_TIN_Type')
                        elif (i== "2_Tax_Id_type"):
                            columns_add.append('Accounts_TIN_Type2')
                        elif (i== "3_Tax_Id_type"):
                            columns_add.append('Accounts_TINType3')
                        elif (i== "4_Tax_Id_type"):
                            columns_add.append('Accounts_TINType4')
                        elif (i== "5_Tax_Id_type"):
                            columns_add.append('Accounts_TINType5')
                        elif (i== "6_Tax_Id_type"):
                            columns_add.append('Accounts_TINType6')
                        elif (i== "7_Tax_Id_type"):
                            columns_add.append('Accounts_TINType7')
                        elif (i== "8_Tax_Id_type"):
                            columns_add.append('Accounts_TINType8')
                        elif (i== "1_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by')
                        elif (i== "2_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by2')
                        elif (i== "3_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by3')
                        elif (i== "4_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by4')
                        elif (i== "5_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by5')
                        elif (i== "6_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by6')
                        elif (i== "7_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by7')
                        elif (i== "8_Accounts_TIN_issued_by"):
                            columns_add.append('Accounts_TIN_issued_by8')
                        else:
                            continue
                for newcol in columns_add:
                    df2[newcol]= None

                yield df2
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'CRS_Process_IPTD'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
                
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_IPTD', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#Individual transformation of ControlPerson
class CRS_ProcessControlPerson(beam.DoFn):
    def process(self, element):
        try:
            df = element
            df = df.rename(columns = {"Entity_Name_y":"SubsOwner_Account_Holder_Entity_Name","ReportingYear":"SubsOwner_ReportingYear","Institution_Id":"FI_ID","Entity_Name_x":"SubsOwner_Entity_Name","ProvinceStateCodeOrName":"SubsOwner_Province","BirthInfoDay":"SubsOwner_BirthInfoDay","BirthInfoMonth":"SubsOwner_BirthInfoMonth","BirthInfoYear":"SubsOwner_BirthInfoYear"})
            #columns_add = ['Financials_Other_Currency','SubsOwner_Address_Free','SubsOwner_Address_Type','SubsOwner_Birth_City','SubsOwner_Birth_CountryCode','SubsOwner_Country_Subentity','SubsOwner_Country_Subentity','SubsOwner_Floor_Identifier','SubsOwner_ItalianTIN','SubsOwner_Middle_Name','SubsOwner_Name_Type','SubsOwner_POB','SubsOwner_Suite_Identifier','SubsOwner_Identification_Number','SubsOwner_NoTINReason','SubsOwner_NoTINReason2','SubsOwner_NoTINReason3','SubsOwner_NoTINReason4','SubsOwner_Birth_FormerCountryName','SubsOwner_NoTINReason5',	'SubsOwner_NoTINReason6',	'SubsOwner_NoTINReason7',	'SubsOwner_NoTINReason8',	'SubsOwner_Nationality5',	'SubsOwner_Nationality6',	'SubsOwner_Nationality7',	'SubsOwner_Nationality8',	'SubsOwner_StateCode',	'SubsOwner_TINType5',	'SubsOwner_TINType6',	'SubsOwner_TINType7',	'SubsOwner_TINType8',	'SubsOwner_India_Customer_ID',	'Nationality2',	'Nationality3',	'Nationality4',	'SubsOwner_Aadhaar_Number',	'SubsOwner_AddressFreeCN',	'SubsOwner_CityCN',	'SubsOwner_DistrictNameCN',	'SubsOwner_DueDiligenceInd',	'SubsOwner_Fathers_name',	'SubsOwner_Gender',	'SubsOwner_Identification_Type',	'SubsOwner_NameCN',	'SubsOwner_Nationality',	'SubsOwner_Occupation_Type',	'SubsOwner_PAN',	'SubsOwner_Postal_Address_Line_1',	'SubsOwner_Postal_Address_Line_2',	'SubsOwner_PostCodeCN',	'SubsOwner_Spouses_Name',	'SubsOwner_TIN_Type',	'SubsOwner_TIN_Type2',	'SubsOwner_TIN_Type3',	'SubsOwner_TIN_Type4',	'SubsOwner_US_TIN']
            #for newcol in columns_add:
            #        df[newcol]= None
            df['SubsOwner_ReportingType'] = 'CRS'
            df = df.drop_duplicates(keep='first')
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessControlPerson', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Individual transformation of IP
class CRS_Process_IP(beam.DoFn):
    def process(self,element,flag):
        try:

            if flag == 1:
                df = element
                df = df.astype(str)
                df = df.reset_index(drop=True)
                df1 = df[['Institution_Id','Involved_Party_Id','Involved_Party_Type','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Effective_From_Date','Place_of_Birth','Entity_Type','CRS_SC_Status_','MDT_Country','LOB_Indicator','Account_Holder_Dormancy_Status']]


                df1 = df1.assign(Customer_ID=df1['Involved_Party_Id'],Country_Code=df1['Address_Country_Code'],City=df1['Address_Town/City'],Post_Code=df1['Address_Post_Code'],ReportingYear=pd.DatetimeIndex(df1['Effective_From_Date']).year.astype(str),Account_Holder_Type='',Building_Identifier='',Street_Name='',District_Name='',ProvinceStateCodeOrName='',Accounts_Address_Free='')
                df1 = df1.assign(BirthInfoYear=pd.DatetimeIndex(df1['Individual_DOB']).year.astype(str),BirthInfoMonth=pd.DatetimeIndex(df1['Individual_DOB']).month.astype(str),BirthInfoDay=pd.DatetimeIndex(df1['Individual_DOB']).day.astype(str))

                df1['Category_of_Account_Holder'] = df1.Involved_Party_Type.apply(lambda x: 'Individual' if x == 'I' else 'Organisation')

                df1['Accounts_UndocumentedAccount'] = df1.CRS_SC_Status_.apply(lambda x: '1' if x == '000007' else '0')
                df1['Post_Code'] = df1['Post_Code'].fillna('').astype(str).replace({'\.0':''},regex=True)
                df1['Account_Holder_Dormancy_Status']= df1.Account_Holder_Dormancy_Status.apply(lambda x: 'Yes' if x == 'D' else 'No')
                df1['OrganizationAccountHolderTypeCode']= df1.Entity_Type.apply(lambda x: 'CRS101' if x == 'EN08' or x == 'EN10' else ('CRS102' if x=='EN07' else ''))
                
                #QATAR changes
                if set(['QA Birth Country Code','QA Birth Former Country Name']).issubset(df.columns):
                    df1 = pd.concat([df1, df[['QA Birth Country Code','QA Birth Former Country Name']]], axis=1)
                
                All_columns = list(df1.columns)

                for row in df1.values:

                    if row[All_columns.index('MDT_Country')] != 'GB' and row[All_columns.index('Involved_Party_Type')] == 'E' and (row[All_columns.index('Entity_Type')] in ['EN01','EN08','EN10']):
                        row[All_columns.index('Account_Holder_Type')] = 'CRS101'

                    elif row[All_columns.index('MDT_Country')] != 'GB' and row[All_columns.index('Involved_Party_Type')] == 'E' and (row[All_columns.index('Entity_Type')] in  ['EN07']):
                        row[All_columns.index('Account_Holder_Type')] = 'CRS102'

                    elif row[All_columns.index('Involved_Party_Type')] == 'E' and (row[All_columns.index('Entity_Type')] in ['EN01','EN08','EN10']):
                        row[All_columns.index('Account_Holder_Type')] = 'Passive Non Financial Entity with Controlling Person(s)'

                    elif row[All_columns.index('Involved_Party_Type')] == 'E' and (row[All_columns.index('Entity_Type')] in ['EN07']):
                        row[All_columns.index('Account_Holder_Type')] = 'Reportable Entity (Organisation)'

                    elif row[All_columns.index('Involved_Party_Type')] == 'I' and row[All_columns.index('MDT_Country')] == 'GB':
                        row[All_columns.index('Account_Holder_Type')] = 'Reportable Person (Individual)'

                    elif row[All_columns.index('Involved_Party_Type')] == 'E':
                        row[All_columns.index('Account_Holder_Type')] = ''

                    else:
                        pass


                    if row[All_columns.index('MDT_Country')] != 'CA':

                        row[All_columns.index('OrganizationAccountHolderTypeCode')] = ''

                        if row[All_columns.index('Individual_DOB')] is not None:
                            row[All_columns.index('BirthInfoYear')] = ''
                            row[All_columns.index('BirthInfoMonth')] = ''
                            row[All_columns.index('BirthInfoDay')] = ''

                    else:
                        pass

                    FreeText = ''
                    FreeAddress = ''
                    FreeBI_Text = ''

                    if row[All_columns.index('Address_Line_1_Format_Code')] is not None and row[All_columns.index('Address_Line_1')] is not None:
                        
                        # print(row[All_columns.index('Address_Line_1_Format_Code')],row[All_columns.index('Address_Line_1')])
                        if row[All_columns.index('Address_Line_1_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_1')]

                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_1')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_1')]
                        
                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_1_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_1')]

                        elif row[All_columns.index('Address_Line_1_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_1')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_1')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_1')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_2_Format_Code')] is not None and row[All_columns.index('Address_Line_2')] is not None:

                        if row[All_columns.index('Address_Line_2_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_2')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_2')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_2')]
                        
                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_2_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_2')]

                        elif row[All_columns.index('Address_Line_2_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_2')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_2')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_2')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_3_Format_Code')] is not None and row[All_columns.index('Address_Line_3')] is not None:

                        if row[All_columns.index('Address_Line_3_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_3')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_3')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_3')]
                        
                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_3_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_3')]

                        elif row[All_columns.index('Address_Line_3_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_3')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_3')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_3')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_4_Format_Code')] is not None and row[All_columns.index('Address_Line_4')] is not None:

                        if row[All_columns.index('Address_Line_4_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_4')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_4')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_4')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_4')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_4')]
                        
                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_4_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_4')]

                        elif row[All_columns.index('Address_Line_4_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_4')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_4')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_4')]
                        else:
                            pass

                    if row[All_columns.index('Address_Line_5_Format_Code')] is not None and row[All_columns.index('Address_Line_5')] is not None:

                        if row[All_columns.index('Address_Line_5_Format_Code')] == 'AF02':
                            row[All_columns.index('Building_Identifier')] = row[All_columns.index('Address_Line_5')]
                            if len(FreeBI_Text) == 0 :
                                FreeBI_Text = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeBI_Text = FreeBI_Text + ',' +row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF03':
                            row[All_columns.index('Street_Name')] = row[All_columns.index('Address_Line_5')]
                            if len(FreeText) == 0 :
                                FreeText = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeText = FreeText + ',' + row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF04':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF05' and (row[All_columns.index('City')] is None or row[All_columns.index('City')] == ""):
                            row[All_columns.index('City')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF06' and (row[All_columns.index('Post_Code')] is None or row[All_columns.index('Post_Code')] == ""):
                            row[All_columns.index('Post_Code')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF10' and (row[All_columns.index('Country_Code')] is None or row[All_columns.index('Country_Code')] == ""):
                            row[All_columns.index('Country_Code')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF07':
                            row[All_columns.index('District_Name')] = row[All_columns.index('Address_Line_5')]
                        
                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF08' or row[All_columns.index('Address_Line_5_Format_Code')] == 'AF11':
                            row[All_columns.index('ProvinceStateCodeOrName')] = row[All_columns.index('Address_Line_5')]

                        elif row[All_columns.index('Address_Line_5_Format_Code')] == 'AF99':
                            row[All_columns.index('Accounts_Address_Free')] = row[All_columns.index('Address_Line_5')]
                            if(len(FreeAddress)==0):
                                FreeAddress = row[All_columns.index('Address_Line_5')]
                            else : 
                                FreeAddress = FreeAddress + ',' + row[All_columns.index('Address_Line_5')]
                        else:
                            pass

                    FreeText_list = FreeText.split(",")
                    FreeAddress_list = FreeAddress.split(",")
                    FreeBI_Text_list = FreeBI_Text.split(",")

                    if len(FreeText_list) > 1:
                        row[All_columns.index('Street_Name')] = FreeText

                    if len(FreeAddress_list) > 1:
                        row[All_columns.index('Accounts_Address_Free')] = FreeAddress

                    if len(FreeBI_Text_list) > 1:
                        row[All_columns.index('Building_Identifier')] = FreeBI_Text

                df1 = df1.drop(columns=['Involved_Party_Type','Involved_Party_Type','Address_Line_1','Address_Line_1_Format_Code','Address_Line_2','Address_Line_2_Format_Code','Address_Line_3','Address_Line_3_Format_Code','Address_Line_4','Address_Line_4_Format_Code','Address_Line_5','Address_Line_5_Format_Code','Address_Town/City','Address_Post_Code','Address_Country_Code','Effective_From_Date'])
   
				# pipe symbol replace in address line
                pipe_rm_column = ['Building_Identifier','Street_Name','District_Name','City','Post_Code','Country_Code','ProvinceStateCodeOrName','Accounts_Address_Free']
                
                for i in pipe_rm_column:
                    df1[i] = df1[i].apply(lambda x: x.replace('|',' '))

                yield df1
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process after record validation',"Class name": 'CRS_Process_IP'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Individual transformation of Account Holder       
class CRS_ProcessFinalAccount(beam.DoFn):
    def process(self,element):
        try:
            df = element

                          

						  
            df = df.rename(columns={"Account_Id":"Accounts_Account_Number","DormantAccount":"Accounts_DormantAccount","Balance_Currency_Code":"Financials_Account_Currency","Balance_Amount":"Financials_Balance","Account_Closed":"Accounts_Account_Closed","Individual_Forenames":"Accounts_First_Name","Individual_Surname":"Accounts_Last_Name","Individual_DOB":"Accounts_Date_Of_Birth","Entity_Name":"Accounts_Entity_Name","Place_of_Birth":"Accounts_Birth_City","Category_of_Account_Holder":"Accounts_Category_of_Account_Holder","Account_Holder_Type":"Accounts_Account_Holder_Type","ReportingYear":"Accounts_ReportingYear","Building_Identifier":"Accounts_Building_Identifier","Street_Name":"Accounts_Street_Name","District_Name":"Accounts_District_Name","City":"Accounts_City","Post_Code":"Accounts_Post_Code","Country_Code":"Accounts_Country_Code","Institution_Id":"FI_ID","Customer_ID":"Accounts_Customer_ID","BirthInfoYear":"Accounts_BirthInfoYear","BirthInfoMonth":"Accounts_BirthInfoMonth","BirthInfoDay":"Accounts_BirthInfoDay","ProvinceStateCodeOrName":"Accounts_Province"})
            
            #columns_add = ['Accounts_Reporting_Type',	'Accounts_Account_Number_Type',	'Accounts_Individual_Name_Type',	'Accounts_Middle_Name',	'Accounts_Birth_CountryCode',	'Accounts_Entity_Name_Type',	'Accounts_Address_Type',	'Accounts_Suite_Identifier',	'Accounts_Floor_Identifier',	'Accounts_POB',	'Accounts_Country_Subentity',	'Financials_Aggregate_Amount',	'Financials_Aggregate_Currency',	'Financials_Aggregate_Desc',	'Financials_Depository_Interest_Amount',	'Financials_Depository_Interest_Currency',	'Financials_Depository_Interest_Desc',	'Financials_Income_Amount',	'Financials_Income_Currency',	'Financials_Income_Desc',	'Financials_Dividends_Desc',	'Financials_Gross_Proceeds_Redemptions_Desc',	'Financials_Interest_Desc',	'Financials_Other_Desc',	'Accounts_ItalianTIN',	'Accounts_TIN_MX',	'Accounts_NoTINReason',	'Accounts_NoTINReason2',	'Accounts_NoTINReason3',	'Accounts_NoTINReason4',	'CanadianIdentificationNumber',	'Accounts_Birth_FormerCountryName',	'Accounts_Non_US_TIN',	'Accounts_NoTINReason5',	'Accounts_NoTINReason6',	'Accounts_Gross_Proceeds_From_Sale_Of_Property',	'Accounts_Account_Holder_Type_for_US_Reportable_Person',	'Accounts_Account_Holder_Type_for_Other_Reportable_Person',	'Accounts_Nationality5',	'Accounts_Nationality6',	'Accounts_Nationality7',	'Accounts_Nationality8',	'Accounts_India_Customer_ID',	'Account_Average',	'Account_Average_Currency',	'Accounts_Aadhaar_Number',	'Accounts_Account_Category',	'Accounts_Account_Treatment',	'Accounts_Account_Type',	'Accounts_Accounts_Status',	'Accounts_AddressFreeCN',	'Accounts_Branch_Address',	'Accounts_Branch_City/Town',	'Accounts_Branch_Country_Code',	'Accounts_Branch_Email',	'Accounts_Branch_Fax',	'Accounts_Branch_Fax_STD Code',	'Accounts_Branch_Mobile',	'Accounts_Branch_Name',	'Accounts_Branch_Number_Type',	'Accounts_Branch_Postal_Code',	'Accounts_Branch_Reference_Number',	'Accounts_Branch_State_Code',	'Accounts_Branch_Telephone_Number',	'Accounts_Branch_Telephone_Number_STD Code',	'Accounts_CityCN',	'Accounts_Country_Of_Incorporation',	'Accounts_Date of Account Closure',	'Accounts_Date_Of_Incorporation',	'Accounts_DistrictNameCN',	'Accounts_DueDiligenceInd',	'Accounts_Entity_Constitution_Type',	'Accounts_EntityNameCN',	'Accounts_Fathers_Name',	'Accounts_Gender',	'Accounts_Identification_Issuing_Country',	'Accounts_Identification_Number',	'Accounts_Identification_Type',	'Accounts_IDNumber',	'Accounts_IDType' ,	'Accounts_NameCN',	'Accounts_Nationality',	'Accounts_Nationality2',	'Accounts_Nationality3',	'Accounts_Nationality4',	'Accounts_Nature_Of_Business',	'Accounts_Occupation_Type',	'Accounts_PAN',	'Accounts_PhoneNo',	'Accounts_Place_Of_Incorporation',	'Accounts_Postal_Address_Line_1',	'Accounts_PostCodeCN',	'Accounts_SelfCertification',	'Accounts_Spouses_Name',	'Accounts_TopbalanceAccount',	'Financials_Aggregate_Gross_Amount_Credited',	'Financials_Aggregate_Gross_Amount_Debited',	'Accounts_NoTINReason7',	'Accounts_NoTINReason8',	'Postal_Address_Line_2',	'Account_Preexisting_Undocumented_Indicator',	'Account_State']
            df = df.drop(columns=['Account_Relationship_Type','Involved_Party_Id'])
            df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_First_Name'] = ''
            df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_Last_Name'] = ''
            #for newcol in columns_add:
            #       df[newcol]= None
            df['Accounts_Reporting_Type'] = 'CRS' 
            #df.loc[df.Accounts_Birth_CountryCode == '', 'Accounts_Birth_City'] = ''
            df = df.drop_duplicates(keep='first')  
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessFinalAccount', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Extract columns needed to join with IPAccountRel
class CRS_ProcessRFinalDF(beam.DoFn):
    def process(self,element):
        try:
            df = element
            df = df[['Institution_Id','Account_Id','Account_Holder_Dormancy_Status']]
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessRFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
        
class CRS_Process_Inst_IP(beam.DoFn):
    def process(self,element):
        try:
            df = element
            #df['Institution_Id'] = df['Institution_Id_y']
            df['Parent_Account_Number'] = df['Account_Id']
            df = df.drop(columns=['Account_Id','Entity_Type','Account_Relationship_Type'])
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_Inst_IP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Individual transformation of FI along with CP and Account Holder
class CRS_ProcessFI_CP_Acct(beam.DoFn):
    def process(self,element):
        try:
            df = element
            if len(df)>0:
                df['FFI_ResCountryCode']=df['Country of Tax Residence']
                df['FFI_Reporting_Type']= 'CRS'
                df['FFI_Nil_Indicator']= 'No'
                df['FFI_Sender_AIN_type']= 'ABN'
                df['FFI_IN'] = df['CRS Local Tax Number']
                df['FFI_INType'] = 'ABN'
                df['FFI_Inissuedby'] = df['Country of Tax Residence']
                df['FFI_Sender_AIN'] = df['CRS Local Tax Number']
                df['FFI_SendingCompanyIN'] = df['CRS Local Tax Number']
                df['FFI_ABN_number'] = df['AU FATCA TIN(Reporting FI)']
                df['FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
                df['TCountry'] = df['Country']
                ##"EAG Entity Legal Name (Full)":"FFI_Name",
                df['FFI_Reporting_Type']='CRS'
                df = df.rename(columns={"Country to be reported to":"FFI_Receiving_Country","Street (35 char limit)":"FFI_Street_Name","Postcode     (8 char limit)":"FFI_Post_Code","Town            (35 char limit)":"FFI_City","Country":"FFI_Country_Code","Telephone":"FFI_Contact","FATCA Email Address":"FFI_ContactPerson_Email_Organisation","FATCA GIIN (35 char limit)":"FFI_GIIN","FATCA Filer Category":"FFI_FilerCategory","ReportingYear":"FFI_ReportingYear","Sponsor GIIN (19 char limit)":"SPO_TIN","Sponsor Country of Tax Residence":"SPO_Res_Country_Code","Transmitter / sponsor street (40 char limit)":"SPO_Street_Name","Transmitter / sponsor city (40 char limit)":"SPO_City","Transmitter / sponsor Zip code (9 char limit)":"SPO_Post_Code","Transmitter / sponsor country":"SPO_Country_Code","Transmitter / Sponsor Company Name (40 char limit)":"SPO_Sponsoring_Entity_Name","Transmitter / Sponsor Company Contact Person (40 char limit) required for Canada":"FFI_Reporting_ContactPerson_Name","Transmitter / Sponsor Telephone Number  (15 char limit) required for Canada":"FFI_ContactPerson_Telephone_Direct","Transmitter / Sponsor Email Address  (48 char limit) required for Canada":"FFI_ContactPerson_Email_Personal","Sponsor Filer Category":"SPO_Filer_Category","UK Due Diligence Indicator (Only needed for UK HMRC)":"FFI_Due_Dil_Ind","UK FATCA User ID (Only needed for UK HMRC)":"FFI_AEOI_ID","UK FI Register ID (Needed for UK HMRC, Canada to use BNRZ number)":"FFI_Registration_ID","UK Threshold Indicator (Only needed for UK HMRC)":"FFI_Treshold_Ind","UK Insurance election":"FFI_Insurance_Election","UK Dormant account election":"FFI_Dormant_Acc_Election","TCountry":"FFI_Transmitting_Country"})
    

                #Header Title fix
                df['FFI_HeaderTitle'] = df['FFI_Name']

                columns_add = ['FFI_AccountID','FFI_Address_Type','FFI_AuthSteuernummer','FFI_BCE_KBO_number','FFI_Building_Identifier','FFI_City_Postal','FFI_Country_Postal','FFI_Country_Subentity','FFI_District_Name','FFI_Floor_Identifier','FFI_IdentificationNumberExtension','FFI_IN2','FFI_NationalTIN','FFI_NIF','FFI_PersonalIdentificationNumber','FFI_POB','FFI_Postal_Code_Postal','FFI_Reporting_ContactPerson_First_Name','FFI_Suite_Identifier','FFI_TIN_Italian','FFI_Warning','SPO_BCE_KBO_Number','SPO_Building_Identifier','SPO_Country_Subentity','SPO_District_Name','SPO_Floor_Identifier','SPO_Intermediary_NIF','SPO_Intermediary_TIN','SPO_Intermediary_TIN_Italy','SPO_Is_Intermediary_Ind','SPO_POB','SPO_Suite_Identifier','SPO_TINIssuedBy','FFI_ABN_number','SPO_ABN_number','FFI_No_Account_To_Report','FilerContactPhoneAreaCode','FFI_ContactPhoneExtensionNumber','FFI_State_Code','SPProvinceStateCodeOrName','FFI_ContactPerson_Telephone_Number_STD Code',	'SPO_State_Code',	'FFI_Transmitter_Number',	'FFI_BZ_ID',	'FFI_FI_Identification_Number',	'FFI_Principal_Officer_Name',	'FFI_Mobile',	'FFI_Fax',	'FFI_FIProvinceStateCodeOrName',	'FFI_Attention_Note',	'FFI_ContactPerson_Telephone_Number',	'FFI_DueDiligenceInd',	'FFI_EntityID',	'FFI_EntityIDType',	'FFI_FIID',	'FFI_FilerAccountNumber',	'FFI_ITDREIN',	'FFI_MyCRS_ID',	'FFI_Postal_Address_Line_1',	'FFI_Postal_Address_Line_2',	'FFI_Principal_Officer_Designation',	'FFI_Registration_Number',	'FFI_Report_Type',	'FFI_Reporting_Entity_Category',	'FFI_ReportingFIType',	'FFI_ReportingID',	'FFI_Sector_And_Subsector_Codes',	'SPAddressLine',	'SPO_SPGIIN',	'FFI_ContactPerson_Fax_STDCode',	'FFI_ContactPerson_Fax',	'FFI_FIAddressLine',	'FFI_Name_Type',	'PoolReportReportingFIGIIN',	'PoolReportMessageRefId',	'PoolReportDocRefId']


                for newcol in columns_add:
                        df[newcol]= None
                
                #MO Fix

                df['MO-CRS AEOI ID(4 Char Limit)1'] = df['MO-CRS AEOI ID(4 Char Limit)'].fillna('').astype(str)
                df[['MO-CRS AEOI ID(4 Char Limit)2','last1']] = [[i.split('.')[0],i.split('.')[1]] if ((len(i.split('.')) == 2) & (i.replace('.','',1).isdigit())) else [i,''] for i in list(df['MO-CRS AEOI ID(4 Char Limit)1'])]
                df['FFI_AEOI_ID'] = np.where(df.FFI_ResCountryCode.eq('MO'), df['MO-CRS AEOI ID(4 Char Limit)2'], df['FFI_AEOI_ID'])
                #GB Fix
                df['FFI_AEOI_ID1'] = df['FFI_AEOI_ID'].fillna('').astype(str)
                df[['FFI_AEOI_ID2','last1']] = [[i.split('.')[0],i.split('.')[1]] if ((len(i.split('.')) == 2) & (i.replace('.','',1).isdigit())) else [i,''] for i in list(df['FFI_AEOI_ID1'])]
                df['FFI_AEOI_ID'] = np.where(df.FFI_ResCountryCode.eq('GB'), df['FFI_AEOI_ID2'], df['FFI_AEOI_ID'])


                df['FFI_Contact'] = np.where(df.FFI_ResCountryCode.eq('MT'), df['MT-CRS Contact (255 Char Limit)'],df['FFI_Contact'])
                df['FFI_AEOI_ID'] = np.where(df.FFI_ResCountryCode.eq('HK'), df['HK-CRS AEOI ID(7 Character Limit)'], df['FFI_AEOI_ID'])
																																		 
                df['FFI_Contact'] = np.where(df.FFI_ResCountryCode.eq('JE'), df['JE-CRS Contact (255 Char Limit)'], df['FFI_Contact'])
                df['FFI_Contact'] = np.where(df.FFI_ResCountryCode.eq('NZ'), df['NZ-CRS Contact(255 Char Limit)'], df['FFI_Contact'])

                # Canada Changes

                # 'Sponsor GIIN (19 char limit)' has been renamed as 'SPO_TIN'
                df['SPO_SPGIIN'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['SPO_TIN'], df['SPO_SPGIIN'])
                df['SPO_State_Code'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['Transmitter / sponsor CAN province'], df['SPO_State_Code'])
                df['FFI_FilerAccountNumber'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA FI Register ID (Canada to use BNRZ Number) (15 Char)'], df['FFI_FilerAccountNumber'])
                df['FFI_Transmitter_Number'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA Transmitter number (Required for Canada) 35 char limit'], df['FFI_Transmitter_Number'])
                df['FFI_ContactPerson_Email_Organisation'] = np.where(df.FFI_ResCountryCode.eq('CA'),  df['CA Contact Email ID (Required for Canada) 35 char limit'], df['FFI_ContactPerson_Email_Organisation'])

                # Contact Telephone person
                # CA Contact Telephone Number (Required for Canada) 35 char limit
                '''
                df['Telephone Number Data'] = df['CA Contact Telephone Number (Required for Canada) 35 char limit'].fillna('').astype(str).replace({'':'0-0-0'},regex=True)

                df[['First','Middle','Last']] = df['Telephone Number Data'].str.split('-',expand=True,)
                df['Telephone_Number'] = df['Middle'].astype(str) + "-" + df['Last'].astype(str)
                '''
                df['Telephone Number Data'] = df['CA Contact Telephone Number (Required for Canada) 35 char limit'].fillna('').astype(str)

                df[['First','Middle','Last']] = [[i.split('-')[0],i.split('-')[1],i.split('-')[2]] if len(i.split('-')) == 3  else ['','',''] for i in list(df['Telephone Number Data'])]
                df['Telephone_Number'] = df['Middle'].astype(str) + "-" + df['Last'].astype(str)
                df['FFI_ContactPerson_Telephone_Number_STD Code'] = np.where((df.FFI_ResCountryCode.eq('CA')) & (df['Telephone Number Data'] != '0-0-0'),  df['First'], df['FFI_ContactPerson_Telephone_Number_STD Code'])
                df['FFI_ContactPerson_Telephone_Number'] = np.where((df.FFI_ResCountryCode.eq('CA')) & (df['Telephone Number Data'] != '0-0-0'),  df['Telephone_Number'], df['FFI_ContactPerson_Telephone_Number'])
                
                # GB Changes
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Dormant_Acc_Election.str.strip()=='')|(df.FFI_Dormant_Acc_Election.isna())),'FFI_Dormant_Acc_Election'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Due_Dil_Ind.str.strip()=='')|(df.FFI_Due_Dil_Ind.isna())),'FFI_Due_Dil_Ind'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Insurance_Election.str.strip()=='')|(df.FFI_Insurance_Election.isna())),'FFI_Insurance_Election'] = 'N/A'
                df.loc[(df.FFI_ResCountryCode=='GB') & ((df.FFI_Treshold_Ind.str.strip()=='')|(df.FFI_Treshold_Ind.isna())),'FFI_Treshold_Ind'] = 'N/A'

                #SG changes

                df['FFI_EntityID'] = np.where(df.FFI_ResCountryCode.eq('SG'), df['CRS Local Tax Number'],df['FFI_EntityID'])
                df['FFI_EntityIDType'] = np.where(df.FFI_ResCountryCode.eq('SG'), df['SG-CRS Singapore Entity ID Type'],df['FFI_EntityIDType'])
                df['FFI_INType'] = np.where(df.FFI_ResCountryCode.eq('SG'), df['SG-CRS Singapore Entity ID Type'],df['FFI_INType'])
                
                #JP changes
                # df['FFI_INType'] = np.where(df.FFI_ResCountryCode.eq('JP'),'JPCN',df['FFI_INType'])
                df['CRS Local Tax Number'] = df['CRS Local Tax Number'].astype(str)
                df.loc[((df['FFI_ResCountryCode']=='JP') & (df['CRS Local Tax Number'].str.len() == 13)),'FFI_INType'] = 'JPCN'
                df.loc[((df['FFI_ResCountryCode']=='JP') & (df['CRS Local Tax Number'].str.len() == 19)),'FFI_INType'] = 'GIIN'
                																											   
                # MY Chnages
                df['FFI_MyCRS_ID'] = np.where(df.FFI_ResCountryCode.eq('MY'), df['MY MY CRS ID (8 digits)'],df['FFI_MyCRS_ID'])
                df['FFI_AEOI_ID'] = np.where(df.FFI_ResCountryCode.eq('MY'), df['MY MY CRS ID (8 digits)'], df['FFI_AEOI_ID'])
                #KR changes
                #df['FFI_INType'] = np.where(df.FFI_ResCountryCode.eq('KR'),'KIIN',df['FFI_INType'])
                

                df = df[['FFI_Reporting_Type',	'FFI_ReportingYear',	'FFI_Name',	'FFI_FilerCategory',	'FFI_GIIN',	'FFI_IN',	'FFI_Inissuedby',	'FFI_INType',	'FFI_SendingCompanyIN',	'FFI_ResCountryCode',	'FFI_Transmitting_Country',	'FFI_Receiving_Country',	'FFI_Nil_Indicator',	'FFI_Address_Type',	'FFI_Country_Code',	'FFI_Street_Name',	'FFI_Building_Identifier',	'FFI_Suite_Identifier',	'FFI_Floor_Identifier',	'FFI_District_Name',	'FFI_POB',	'FFI_Post_Code',	'FFI_City',	'FFI_Country_Subentity',	'FFI_Warning',	'FFI_Contact',	'SPO_Sponsoring_Entity_Name',	'SPO_Is_Intermediary_Ind',	'SPO_Filer_Category',	'SPO_TIN',	'SPO_Res_Country_Code',	'SPO_Intermediary_TIN',	'SPO_TINIssuedBy',	'SPO_Country_Code',	'SPO_Street_Name',	'SPO_Building_Identifier',	'SPO_Suite_Identifier',	'SPO_Floor_Identifier',	'SPO_District_Name',	'SPO_POB',	'SPO_Post_Code',	'SPO_City',	'SPO_Country_Subentity',	'SPO_Intermediary_TIN_Italy',	'SPO_Intermediary_NIF',	'SPO_BCE_KBO_Number',	'FFI_AuthSteuernummer',	'FFI_AccountID',	'FFI_Postal_Code_Postal',	'FFI_City_Postal',	'FFI_Country_Postal',	'FFI_Reporting_ContactPerson_Name',	'FFI_Reporting_ContactPerson_First_Name',	'FFI_ContactPerson_Email_Personal',	'FFI_ContactPerson_Email_Organisation',	'FFI_ContactPerson_Telephone_Direct',	'FFI_AEOI_ID',	'FFI_Registration_ID',	'FFI_Due_Dil_Ind',	'FFI_Treshold_Ind',	'FFI_Insurance_Election',	'FFI_Dormant_Acc_Election',	'FFI_IN2',	'FFI_NationalTIN',	'FFI_IdentificationNumberExtension',	'FFI_HeaderTitle',	'FFI_PersonalIdentificationNumber',	'FFI_BCE_KBO_number',	'FFI_TIN_Italian',	'FFI_NIF',	'FFI_ABN_number',	'SPO_ABN_number',	'FFI_Sender_AIN_type',	'FFI_Sender_AIN',	'FFI_ContactPerson_Telephone_Number_STD Code',	'FFI_ContactPhoneExtensionNumber',	'FFI_State_Code',	'SPO_State_Code',	'FFI_Transmitter_Number',	'FFI_BZ_ID',	'FFI_FI_Identification_Number',	'FFI_Principal_Officer_Name',	'FFI_Mobile',	'FFI_Fax',	'FFI_FIProvinceStateCodeOrName',	'FFI_Attention_Note',	'FFI_ContactPerson_Telephone_Number',	'FFI_DueDiligenceInd',	'FFI_EntityID',	'FFI_EntityIDType',	'FFI_FIID',	'FFI_FilerAccountNumber',	'FFI_ITDREIN',	'FFI_MyCRS_ID',	'FFI_Postal_Address_Line_1',	'FFI_Postal_Address_Line_2',	'FFI_Principal_Officer_Designation',	'FFI_Registration_Number',	'FFI_Report_Type',	'FFI_Reporting_Entity_Category',	'FFI_ReportingFIType',	'FFI_ReportingID',	'FFI_Sector_And_Subsector_Codes',	'SPAddressLine',	'SPO_SPGIIN',	'FFI_ContactPerson_Fax_STDCode',	'FFI_ContactPerson_Fax',	'FFI_FIAddressLine',	'FFI_Name_Type',	'PoolReportReportingFIGIIN',	'PoolReportMessageRefId',	'PoolReportDocRefId','Abbrev. (Insitution ID)                 (8 char limit)','Service Institution ID']]

                #df.loc[(df.FFI_ResCountryCode =='GB'), 'FFI_Reporting_Type'] = 'Combined'																		 
                df1 = df.drop_duplicates(keep='first')
                yield df1
            else:
                yield df
            
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessFI_CP_Acct', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#-------------------------------------CRS READING CSV FILE---------------------------------------------------------------------------

#Reading from csv and filtering data which got inserted latest
class CRS_Read_from_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self, element, filename,uniqueid,filedf):
        
        try:
            if(element == 1):
                prefix = CRS_csv_input_prefix
                delimiter='/'
                storage_client = storage.Client()
                bucket_name = self.input_path
                blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
                for blob in blobs:
                    if (filename in blob.name):
                        fwf_data = blob.download_as_bytes()
                        fwf_data_str = fwf_data.decode("utf-8")
                        f= StringIO(fwf_data_str)
                        df = pd.read_csv(f,dtype=str,keep_default_na=False)
                        break

                    else:
                        df = pd.DataFrame()
                        
                if(len(df)>0):
                    df['CSV File Index'] = df.index
                    df = df[df.Stage1_id == uniqueid]
                    df = df.reset_index()
                else:
                    df['CSV File Index']=''

        

                yield df
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                #exc_type, exc_obj, exc_tb = sys.exc_info()
                #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                logger.log_struct({'severity':'Critical','message':'CRS_Flag is 0 in CRS_Read_from_csv'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
                
        
        except UnboundLocalError as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':filename+' is not present in Bucket',"Error Message":str(exc_obj),"Class name": 'CRS_Read_from_csv','Error_type':str(exc_type), 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (filename+' is not present in Bucket: '+str(exc_obj))         
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Read_from_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
     
#-------------------------------------FATCA READING CSV FILE---------------------------------------------------------------------------
   
#Reading from csv and filtering data which got inserted latest
class FATCA_Read_from_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self, element, filename, uniqueid,filedf):
        try:
            if(element == 1):
                prefix = FATCA_csv_input_prefix
                delimiter='/'
                storage_client = storage.Client()
                bucket_name = self.input_path
                blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
                for blob in blobs:
                    if (filename in blob.name):
                        fwf_data = blob.download_as_bytes()
                        fwf_data_str = fwf_data.decode("utf-8")
                        f= StringIO(fwf_data_str)
                        df = pd.read_csv(f,dtype=str,keep_default_na=False)
                        break

                    else:
                        df = pd.DataFrame()

                if(len(df)>0):
                    df['CSV File Index'] = df.index
                    df = df[df.Stage1_id == uniqueid]
                    df = df.reset_index()
                else:
                    df['CSV File Index']=''
                yield df
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                logger.log_struct({'severity':'Critical','message':'FATCA_Flag is 0 in FATCA_Read_from_csv'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        except UnboundLocalError as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':filename+' is not present in Bucket',"Error Message":str(exc_obj),"Class name": 'FATCA_Read_from_csv','Error_type':str(exc_type), 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (filename+' is not present in Bucket: '+str(exc_obj))         
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Read_from_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
     
#--------------------------------READING CI (EXCEL) FILE-------------------------------------------------------------------

#Read CI_FILE.xls
class Read_from_xls(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    def process(self, something, filename):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_excel(BytesIO(fwf_data), header=None,engine='openpyxl')
                    rep = {'Reference':'','ClientInstitutionDatascreen' : '','FATCA-SpecificClientInstitutionParameters' : 'FATCA','CRS-SpecificClientInstitutionParameters' : 'CRS','DecisionTree/BatchProcessData(FATCA)' : '','DecisionTree/BatchProcessData(CRS)' : '','ReportDistributiondetails(FATCA)' : '','ReportDistributiondetails(CRS)' : '','UserAccessRequestApproverdetails' : '','SponsorDataScreen' : '','FATCA/CRS-SpecificClientInstitutionParameters-AdditionalData(UK)' : 'UK','FATCA/CRS-SpecificClientInstitutionParameters-AdditionalData(Canada)' : 'CA','FATCA/CRSSpecificClientInstitutionParameters-AdditionalData(Spain)' : 'SQ','FATCA-SpecificClientInstitutionParameters-AdditionalData(Italy)' : 'IT-FATCA','FATCA/CRS-SpecificClientInstitutionParameters-AdditionalData(Luxembourg)' : 'LU','FATCA-SpecificClientInstitutionParameters-AdditionalData(Belgium)' : 'BE','FATCA-SubmissionFileHeader(SouthAfrica)' : 'SA-FATCAFH','FATCA-SubmittingEntityData(SouthAfrica)' : 'SA-FATCAED','FATCA-ReportingFinancialInstitution(SouthAfrica)' : 'SA-FATCAFI','FATCA-SpecificClientInstitutionParameters-AdditionalData(Poland)' : 'PL-FATCA','FATCA/CRS-SpecificClientInstitutionParameters-AdditionalData(India)' : 'IN','CRS-SpecificClientInstitutionParameters(Guernsey)' : 'GG-CRS','FATCA-Contact(OnlyforSingaporeFI)' : 'SG-FATCA','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforSingaporeCRS)' : 'SG-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforMalta)' : 'MT-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforJersey)' : 'JE-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforHK)' : 'HK-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforMacau)' : 'MO-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforNewZealand)' : 'NZ-CRS','FATCASpecificClientInstitutionParameter-AdditionalData(OnlyforNewZealand)' : 'NZ-FATCA','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforSwitzerland)' : 'CH-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforChina)' : 'CN-CRS','CRS/FATCASpecificClientInstitutionParameter-AdditionalData(OnlyforSweden)' : 'SE-CRS','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforRussia)' : 'RU-CRS','SpecificClientInstitutionParameter-AdditionalData(OnlyforIndonesia)' : 'ID','SpecificClientInstitutionParameter-AdditionalData(OnlyforAustralia)' : 'AU','CRSSpecificClientInstitutionParameter-AdditionalData(OnlyforMalaysia)':'MY'}
                    #df = df.fillna(method='ffill', axis=1)
                    df1=df.iloc[0]
                    df1 = df1.fillna(method='ffill')
                    df2 = df.iloc[1]
                    df1 = df1.replace("\n","",regex=True)
                    df2 = df2.replace("\n","",regex=True)
                    df1 = df1.replace(" ","",regex=True)
                    df1 = df1.replace(rep)
                    df.iloc[0] = df1
                    df.iloc[1] = df2                                
                    df.iloc[1] = df.iloc[0] +" "+df.iloc[1]
                    df.columns= df.iloc[1].str.strip()
                    df.drop([0,1],inplace=True)
                    
                    df = df.replace(np.nan,"",regex=True)
                    df = df.reset_index(drop=True)
            yield df      

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_from_xls', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
#--------------------------------------PROCESS ACCOUNT HOLDER AND CONTROL PERSON JOINS---------------------------------------------------------

class Process_AccountHolder_Join(beam.DoFn):
        
    def process(self,element,reporting_type):
        
        try:
            df = element
                                             
            columns_add = ['Accounts_Account_Number_Type','Accounts_Individual_Name_Type',	'Accounts_Middle_Name',	'Accounts_Birth_CountryCode',	'Accounts_Entity_Name_Type',	'Accounts_Address_Type',	'Accounts_Suite_Identifier',	'Accounts_Floor_Identifier',	'Accounts_POB',	'Accounts_Country_Subentity',	'Financials_Aggregate_Amount',	'Financials_Aggregate_Currency',	'Financials_Aggregate_Desc',	'Financials_Depository_Interest_Amount',	'Financials_Depository_Interest_Currency',	'Financials_Depository_Interest_Desc',	'Financials_Income_Amount',	'Financials_Income_Currency',	'Financials_Income_Desc',	'Financials_Dividends_Desc',	'Financials_Gross_Proceeds_Redemptions_Desc',	'Financials_Interest_Desc',	'Financials_Other_Desc',	'Accounts_ItalianTIN',	'Accounts_TIN_MX',	'Accounts_NoTINReason',	'Accounts_NoTINReason2',	'Accounts_NoTINReason3',	'Accounts_NoTINReason4',	'Accounts_Birth_FormerCountryName',	'Accounts_Non_US_TIN',	'Accounts_NoTINReason5',	'Accounts_NoTINReason6',	'Accounts_Gross_Proceeds_From_Sale_Of_Property',	'Accounts_Account_Holder_Type_for_US_Reportable_Person',	'Accounts_Account_Holder_Type_for_Other_Reportable_Person',	'Accounts_Nationality5',	'Accounts_Nationality6',	'Accounts_Nationality7',	'Accounts_Nationality8',	'Accounts_India_Customer_ID',	'Account_Average',	'Account_Average_Currency',	'Accounts_Aadhaar_Number',	'Accounts_Account_Category',	'Accounts_Account_Treatment',	'Accounts_Account_Type',	'Accounts_Accounts_Status',	'Accounts_AddressFreeCN',	'Accounts_Branch_Address',	'Accounts_Branch_City/Town',	'Accounts_Branch_Country_Code',	'Accounts_Branch_Email',	'Accounts_Branch_Fax',	'Accounts_Branch_Fax_STD Code',	'Accounts_Branch_Mobile',	'Accounts_Branch_Name',	'Accounts_Branch_Number_Type',	'Accounts_Branch_Postal_Code',	'Accounts_Branch_Reference_Number',	'Accounts_Branch_State_Code',	'Accounts_Branch_Telephone_Number',	'Accounts_Branch_Telephone_Number_STD Code',	'Accounts_CityCN',	'Accounts_Country_Of_Incorporation',	'Accounts_Date of Account Closure',	'Accounts_Date_Of_Incorporation',	'Accounts_DistrictNameCN',	'Accounts_DueDiligenceInd',	'Accounts_Entity_Constitution_Type',	'Accounts_EntityNameCN',	'Accounts_Fathers_Name',	'Accounts_Gender',	'Accounts_Identification_Issuing_Country',	'Accounts_Identification_Number',	'Accounts_Identification_Type',	'Accounts_IDNumber',	'Accounts_IDType' ,	'Accounts_NameCN',	'Accounts_Nationality',	'Accounts_Nationality2',	'Accounts_Nationality3',	'Accounts_Nationality4',	'Accounts_Nature_Of_Business',	'Accounts_Occupation_Type',	'Accounts_PAN',	'Accounts_PhoneNo',	'Accounts_Place_Of_Incorporation',	'Accounts_Postal_Address_Line_1',	'Accounts_PostCodeCN',	'Accounts_SelfCertification',	'Accounts_Spouses_Name',	'Accounts_TopbalanceAccount',	'Financials_Aggregate_Gross_Amount_Credited',	'Financials_Aggregate_Gross_Amount_Debited',	'Accounts_NoTINReason7',	'Accounts_NoTINReason8',	'Postal_Address_Line_2',	'Account_Preexisting_Undocumented_Indicator',	'Account_State']

            if reporting_type =='FATCA':
                columns_add = columns_add + ['Accounts_UndocumentedAccount','Accounts_Birth_City']

            for newcol in columns_add:
                    df[newcol]= None 
                    
            df['Accounts_FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
            #df = df.rename(columns={"EAG Entity Legal Name (Full)":"Accounts_FFI_Name"})
            df['COTR'] = df['Country of Tax Residence']
            df['Accounts_Date_Of_Birth'] =  pd.to_datetime(df['Accounts_Date_Of_Birth'],errors ='coerce')
            df['Accounts_Date_Of_Birth'] = df['Accounts_Date_Of_Birth'].dt.strftime('%Y-%m-%d')

           
            #for entity, Birth City and Birth Country should be blank and for HK CRS, Birth City and Birth Country are interdependent
            if len(df)>0:                                          
                if df['Accounts_Reporting_Type'][0] =='CRS':

                    df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_Birth_City'] = ''
                    df.loc[df.Accounts_Category_of_Account_Holder == 'Organisation', 'Accounts_Birth_CountryCode'] = ''

                    df.loc[(df.Accounts_Category_of_Account_Holder == 'Individual') & ((df.COTR=='HK')|(df.COTR=='MO')) & ((df.Accounts_Birth_City.str.strip()=='')|(df.Accounts_Birth_City.isna())),'Accounts_Birth_CountryCode'] = ''
                    df.loc[(df.Accounts_Category_of_Account_Holder == 'Individual') & ((df.COTR=='HK')|(df.COTR=='MO')) & ((df.Accounts_Birth_CountryCode.str.strip()=='')|df.Accounts_Birth_CountryCode.isna()),'Accounts_Birth_City'] = ''

                    #df.loc[(df.COTR =='JE') & (df.Accounts_TIN == ''),'Accounts_TIN'] = 'NOTIN'
                    ##df.loc[(df['COTR']=='MX') & (df.Accounts_TIN = ''),'Accounts_TIN'] = 'NOTIN'
                    
                    #QATAR changes
                    
                    if set(['QA Birth Country Code','QA Birth Former Country Name']).issubset(df.columns):
                        df['Accounts_Birth_CountryCode'] = np.where(df.Accounts_Category_of_Account_Holder.eq('Individual'), df['QA Birth Country Code'],df['Accounts_Birth_CountryCode'])
                        #df['Accounts_Birth_FormerCountryName'] = np.where(df.Accounts_Category_of_Account_Holder.eq('Individual'), df['QA Birth Former Country Name'],df['Accounts_Birth_FormerCountryName'])
                    df['Accounts_Birth_CountryCode'] = df['Accounts_Birth_CountryCode'].fillna('').astype(str).replace({'nan':''},regex=True)
                    df['Accounts_Birth_FormerCountryName'] = df['Accounts_Birth_FormerCountryName'].fillna('').astype(str).replace({'nan':''},regex=True)
                   

                    df['Accounts_Res_Country_Code'] = np.where(df.Accounts_UndocumentedAccount.eq('1'), df['COTR'],df['Accounts_Res_Country_Code'])
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code2'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code3'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code4'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code5'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code6'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code7'] = ''
                    df.loc[df.Accounts_UndocumentedAccount == '1', 'Accounts_Res_Country_Code8'] = ''

                    # UndocumentedAccount
                    # 2nd Condition and 4th condition
                    df.loc[((df.COTR == 'FR')|(df.COTR == 'JP')|(df.COTR == 'ID')) & (df.Accounts_UndocumentedAccount == '1'),['Accounts_Address_Free','Accounts_Post_Code','Accounts_City','Accounts_Building_Identifier','Accounts_Street_Name','Accounts_Suite_Identifier','Accounts_Floor_Identifier','Accounts_District_Name','Accounts_POB','Accounts_Country_Subentity']] = ['Undocumented','','','','','','','','','']
                    # 1st Condition and 3rd Condition
                    df.loc[(~((df.COTR == 'FR')|(df.COTR == 'JP')|(df.COTR == 'ID'))) & (df.Accounts_UndocumentedAccount == '1'),['Accounts_Address_Free','Accounts_Post_Code','Accounts_City','Accounts_Building_Identifier','Accounts_Street_Name','Accounts_Suite_Identifier','Accounts_Floor_Identifier','Accounts_District_Name','Accounts_POB','Accounts_Country_Subentity']] = ['','','Undocumented','','','','','','','']

                    #Address Country code

                    df['Accounts_Country_Code'] = np.where(((df['COTR']) != 'DE') & ((df['Accounts_UndocumentedAccount']) == '1'), df['COTR'], df['Accounts_Country_Code'])

                    df.loc[df.Accounts_Account_Closed == 'Yes', 'Financials_Balance'] = 0.00

                    df.loc[(df.COTR == 'TW') & ((df.Entity_Type =='EN01')|(df.Entity_Type =='EN08')|(df.Entity_Type =='EN10')) & (df.Accounts_Category_of_Account_Holder=='Organisation'),'Accounts_Account_Holder_Type'] = 'CRS982' 
                    df.loc[(df.COTR == 'TW') & (df.Entity_Type == 'EN07') & (df.Accounts_Category_of_Account_Holder=='Organisation'),'Accounts_Account_Holder_Type'] = 'CRS981' 
                    df.loc[(df.COTR == 'TW') & (df.Accounts_Category_of_Account_Holder=='Individual'),'Accounts_Account_Holder_Type'] = '' 

                    # Canada
                    df.loc[(df.Accounts_UndocumentedAccount == '1') & (df.COTR =='CA'), 'Accounts_UndocumentedAccount'] = 'Y'
                    df.loc[(df.Accounts_UndocumentedAccount == '0')& (df.COTR =='CA'), 'Accounts_UndocumentedAccount'] = 'N'

                    df.loc[(df.Accounts_DormantAccount == 'Yes') & (df.COTR =='CA'), 'Accounts_DormantAccount'] = 'Y'
                    df.loc[(df.Accounts_DormantAccount == 'No')& (df.COTR =='CA'), 'Accounts_DormantAccount'] = 'N'

                    # GB Changes
                    df.loc[(df.COTR == 'GB') & ((df.Entity_Type =='EN01')|(df.Entity_Type =='EN08')|(df.Entity_Type =='EN10')) & (df.Accounts_Category_of_Account_Holder=='Organisation'),'Accounts_Account_Holder_Type'] = 'Passive Non Financial Entity with Controlling Person(s)'
                    df.loc[(df.COTR == 'GB') & (df.Entity_Type == 'EN07') & (df.Accounts_Category_of_Account_Holder=='Organisation'),'Accounts_Account_Holder_Type'] = 'Reportable Entity (Organisation)'
                    df.loc[(df.COTR == 'GB') & (df.Accounts_Category_of_Account_Holder=='Individual'),'Accounts_Account_Holder_Type'] = 'Reportable Person (Individual)'

                if df['Accounts_Reporting_Type'][0] =='FATCA':
                    df['CanadianIdentificationNumber'] = ''
                    df['Accounts_TIN'] = df['Accounts_TIN'].str.replace(r'\D+', '') #Only digits
                    #df.loc[df.Accounts_TIN == '', 'Accounts_TIN'] = 'AAAAAAAAA' #(Generic for all schema)
                    '''
                    for i in range(len(df)):
                        if df.loc[i,'Accounts_TIN'] == 'AAAAAAAAA':
                            #AU, FR, SG
                            if df.loc[i,'COTR'] in ['AU','FR','SG']:
                                if df.loc[i,'Accounts_Account_Holder_Type'] in ['FATCA101','FATCA102']:
                                    df.loc[i,'Accounts_TIN'] = '000000000'
                    
                            #DE, MU
                            if df.loc[i,'COTR'] in ['DE','MU']:
                                if df.loc[i,'Accounts_Account_Holder_Type'] in ['FATCA101','FATCA102','FATCA103','FATCA105']:
                                    df.loc[i,'Accounts_TIN'] = '000000000'
                                                                           

                        #SG : if TIN field is populated as 00000000 (nine zeros) in case of FATCA101/FATCA102, please leave ResCountryCode as blank.
                        if df.loc[i,'Accounts_TIN'] == '000000000':
                            if df.loc[i,'COTR'] == 'SG':
                                if df.loc[i,'Accounts_Account_Holder_Type'] in ['FATCA101','FATCA102']:
                                    df.loc[i,'Accounts_Res_Country_Code'] = ''
                    '''
                               
                # Schema allignment for GB 
                df.loc[(df.Accounts_DormantAccount == 'Yes') & (df.COTR =='GB'), 'Accounts_DormantAccount'] = '1'
                df.loc[(df.Accounts_DormantAccount == 'No')& (df.COTR =='GB'), 'Accounts_DormantAccount'] = '0'

                df.loc[(df.Accounts_Account_Closed == 'Yes') & (df.COTR =='GB'), 'Accounts_Account_Closed'] = '1'
                df.loc[(df.Accounts_Account_Closed == 'No')& (df.COTR =='GB'), 'Accounts_Account_Closed'] = '0'


                #df.loc[(df.COTR =='GB'), 'Accounts_Reporting_Type'] = 'Combined'

            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process as Process AccountHolder is empty',"Class name": 'Process_AccountHolder_Join'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)
      
            
            
            
            if set(['CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date']).issubset(df.columns):
                df['CanadianIdentificationNumber'] = df['CA_Tax_Id']

            df['CanadianIdentificationNumber'] = df['CanadianIdentificationNumber'].fillna('').astype(str).replace({'nan':''},regex=True)

            # Fix
            df.loc[(df.Accounts_Account_Closed == 'Yes') & (df.COTR =='CA'), 'Accounts_Account_Closed'] = 'Y'
            df.loc[(df.Accounts_Account_Closed == 'No')& (df.COTR =='CA'), 'Accounts_Account_Closed'] = 'N'

                
            #convert all amount fields to float with 2 digits after decimal point 
            cols = ['Financials_Balance','Financials_Dividends_Amount','Financials_Interest_Amount','Financials_Gross_Proceeds_Redemptions_Amount','Financials_Income_Amount','Financials_Depository_Interest_Amount','Financials_Other_Amount','Financials_Aggregate_Amount']
            df[cols] = df[cols].astype(float)
            df[cols] = df[cols].round(2)

            #  Schema allignment for MT

            df.loc[((df['Financials_Balance'].isnull()) & (df['COTR'] =='MT')),'Financials_Balance'] = '0.00'
            df.loc[((df['Financials_Interest_Amount'].isnull()) & (df['COTR'] =='MT')),'Financials_Interest_Amount'] = '0.00'
            df.loc[((df['Financials_Dividends_Amount'].isnull()) & (df['COTR'] =='MT')),'Financials_Dividends_Amount'] = '0.00'
            df.loc[((df['Financials_Gross_Proceeds_Redemptions_Amount'].isnull()) & (df['COTR'] =='MT')),'Financials_Gross_Proceeds_Redemptions_Amount'] = '0.00'
            df.loc[((df['Financials_Other_Amount'].isnull()) & (df['COTR'] =='MT')),'Financials_Other_Amount'] = '0.00'                                                                      
            
            df['Financials_Dividends_Currency'] = df['Financials_Dividends_Currency'].fillna('')
            df['Financials_Interest_Currency'] = df['Financials_Interest_Currency'].fillna('')
            df['Financials_Gross_Proceeds_Redemptions_Currency'] = df['Financials_Gross_Proceeds_Redemptions_Currency'].fillna('')
            df['Financials_Other_Currency'] = df['Financials_Other_Currency'].fillna('')

            df.loc[((df['Financials_Dividends_Currency'] == '') & (df['COTR'] =='MT')),'Financials_Dividends_Currency'] = df['Financials_Account_Currency']
            df.loc[((df['Financials_Interest_Currency']== '') & (df['COTR'] =='MT')),'Financials_Interest_Currency'] = df['Financials_Account_Currency']
            df.loc[((df['Financials_Gross_Proceeds_Redemptions_Currency']== '') & (df['COTR'] =='MT')),'Financials_Gross_Proceeds_Redemptions_Currency'] = df['Financials_Account_Currency']
            df.loc[((df['Financials_Other_Currency']== '') & (df['COTR'] =='MT')),'Financials_Other_Currency'] = df['Financials_Account_Currency']

            df = df[['Accounts_ReportingYear',	'Accounts_Reporting_Type',	'Accounts_Account_Number',	'Accounts_Account_Number_Type',	'Accounts_Account_Closed',	'Accounts_Category_of_Account_Holder',	'Accounts_UndocumentedAccount',	'Accounts_DormantAccount',	'Accounts_Account_Holder_Type',	'Accounts_FFI_Name',	'Accounts_Individual_Name_Type',	'Accounts_First_Name',	'Accounts_Middle_Name',	'Accounts_Last_Name',	'Accounts_Date_Of_Birth',	'Accounts_Birth_City',	'Accounts_Birth_CountryCode',	'Accounts_Entity_Name_Type',	'Accounts_Entity_Name',	'Accounts_TIN',	'Accounts_TIN_issued_by',	'Accounts_TIN_Type',	'Accounts_TIN2',	'Accounts_TIN_issued_by2',	'Accounts_TIN_Type2',	'Accounts_TIN3',	'Accounts_TIN_issued_by3',	'Accounts_TINType3',	'Accounts_TIN4',	'Accounts_TIN_issued_by4',	'Accounts_TINType4',	'Accounts_Res_Country_Code',	'Accounts_Res_Country_Code2',	'Accounts_Res_Country_Code3',	'Accounts_Res_Country_Code4',	'Accounts_Address_Type',	'Accounts_Country_Code',	'Accounts_Street_Name',	'Accounts_Building_Identifier',	'Accounts_Suite_Identifier',	'Accounts_Floor_Identifier',	'Accounts_District_Name',	'Accounts_POB',	'Accounts_Post_Code',	'Accounts_City',	'Accounts_Country_Subentity',	'Accounts_Address_Free',	'Financials_Balance',	'Financials_Account_Currency',	'Financials_Dividends_Amount',	'Financials_Dividends_Currency',	'Financials_Dividends_Desc',	'Financials_Interest_Amount',	'Financials_Interest_Currency',	'Financials_Interest_Desc',	'Financials_Gross_Proceeds_Redemptions_Amount',	'Financials_Gross_Proceeds_Redemptions_Currency',	'Financials_Gross_Proceeds_Redemptions_Desc',	'Financials_Income_Amount',	'Financials_Income_Currency',	'Financials_Income_Desc',	'Financials_Depository_Interest_Amount',	'Financials_Depository_Interest_Currency',	'Financials_Depository_Interest_Desc',	'Financials_Other_Amount',	'Financials_Other_Currency',	'Financials_Other_Desc',	'Financials_Aggregate_Amount',	'Financials_Aggregate_Currency',	'Financials_Aggregate_Desc',	'Accounts_Customer_ID',	'Accounts_Res_Country_Code5',	'Accounts_TIN5',	'Accounts_TIN_issued_by5',	'Accounts_TINType5',	'Accounts_Res_Country_Code6',	'Accounts_TIN6',	'Accounts_TIN_issued_by6',	'Accounts_TINType6',	'Accounts_Res_Country_Code7',	'Accounts_TIN7',	'Accounts_TIN_issued_by7',	'Accounts_TINType7',	'Accounts_Res_Country_Code8',	'Accounts_TIN8',	'Accounts_TIN_issued_by8',	'Accounts_TINType8',	'Accounts_ItalianTIN',	'Accounts_TIN_MX',	'Accounts_BirthInfoYear',	'Accounts_BirthInfoMonth',	'Accounts_BirthInfoDay',	'Accounts_NoTINReason',	'Accounts_NoTINReason2',	'Accounts_NoTINReason3',	'Accounts_NoTINReason4',	'CanadianIdentificationNumber',	'Accounts_Province',	'Accounts_Non_US_TIN',	'Accounts_NoTINReason5',	'Accounts_NoTINReason6',	'Accounts_Gross_Proceeds_From_Sale_Of_Property',	'Accounts_Account_Holder_Type_for_US_Reportable_Person',	'Accounts_Account_Holder_Type_for_Other_Reportable_Person',	'Accounts_Nationality5',	'Accounts_Nationality6',	'Accounts_Nationality7',	'Accounts_Nationality8',	'Accounts_Birth_FormerCountryName',	'Accounts_India_Customer_ID',	'Account_Average',	'Account_Average_Currency',	'Accounts_Aadhaar_Number',	'Accounts_Account_Category',	'Accounts_Account_Treatment',	'Accounts_Account_Type',	'Accounts_Accounts_Status',	'Accounts_AddressFreeCN',	'Accounts_Branch_Address',	'Accounts_Branch_City/Town',	'Accounts_Branch_Country_Code',	'Accounts_Branch_Email',	'Accounts_Branch_Fax',	'Accounts_Branch_Fax_STD Code',	'Accounts_Branch_Mobile',	'Accounts_Branch_Name',	'Accounts_Branch_Number_Type',	'Accounts_Branch_Postal_Code',	'Accounts_Branch_Reference_Number',	'Accounts_Branch_State_Code',	'Accounts_Branch_Telephone_Number',	'Accounts_Branch_Telephone_Number_STD Code',	'Accounts_CityCN',	'Accounts_Country_Of_Incorporation',	'Accounts_Date of Account Closure',	'Accounts_Date_Of_Incorporation',	'Accounts_DistrictNameCN',	'Accounts_DueDiligenceInd',	'Accounts_Entity_Constitution_Type',	'Accounts_EntityNameCN',	'Accounts_Fathers_Name',	'Accounts_Gender',	'Accounts_Identification_Issuing_Country',	'Accounts_Identification_Number',	'Accounts_Identification_Type',	'Accounts_IDNumber',	'Accounts_IDType',	'Accounts_NameCN',	'Accounts_Nationality',	'Accounts_Nationality2',	'Accounts_Nationality3',	'Accounts_Nationality4',	'Accounts_Nature_Of_Business',	'Accounts_Occupation_Type',	'Accounts_PAN',	'Accounts_PhoneNo',	'Accounts_Place_Of_Incorporation',	'Accounts_Postal_Address_Line_1',	'Accounts_PostCodeCN',	'Accounts_SelfCertification',	'Accounts_Spouses_Name',	'Accounts_TopbalanceAccount',	'Financials_Aggregate_Gross_Amount_Credited',	'Financials_Aggregate_Gross_Amount_Debited',	'Accounts_NoTINReason7',	'Accounts_NoTINReason8',	'Postal_Address_Line_2',	'Account_Preexisting_Undocumented_Indicator',	'Account_State','FI_ID','CTCR Process ID_x','Entity_Type']]
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_AccountHolder_Join', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class Process_ControlPerson_Join(beam.DoFn):
        
    def process(self,element):
        try:
            df = element
            columns_add = ['Financials_Other_Currency','SubsOwner_Address_Free','SubsOwner_Address_Type','SubsOwner_Birth_City','SubsOwner_Birth_CountryCode','SubsOwner_Country_Subentity','SubsOwner_Country_Subentity','SubsOwner_Floor_Identifier','SubsOwner_ItalianTIN','SubsOwner_Middle_Name','SubsOwner_Name_Type','SubsOwner_POB','SubsOwner_Suite_Identifier','SubsOwner_Identification_Number','SubsOwner_NoTINReason','SubsOwner_NoTINReason2','SubsOwner_NoTINReason3','SubsOwner_NoTINReason4','SubsOwner_Birth_FormerCountryName','SubsOwner_NoTINReason5',	'SubsOwner_NoTINReason6',	'SubsOwner_NoTINReason7',	'SubsOwner_NoTINReason8',	'SubsOwner_Nationality5',	'SubsOwner_Nationality6',	'SubsOwner_Nationality7',	'SubsOwner_Nationality8',	'SubsOwner_StateCode',	'SubsOwner_TINType5',	'SubsOwner_TINType6',	'SubsOwner_TINType7',	'SubsOwner_TINType8',	'SubsOwner_India_Customer_ID',	'Nationality2',	'Nationality3',	'Nationality4',	'SubsOwner_Aadhaar_Number',	'SubsOwner_AddressFreeCN',	'SubsOwner_CityCN',	'SubsOwner_DistrictNameCN',	'SubsOwner_DueDiligenceInd',	'SubsOwner_Fathers_name',	'SubsOwner_Gender',	'SubsOwner_Identification_Type',	'SubsOwner_NameCN',	'SubsOwner_Nationality',	'SubsOwner_Occupation_Type',	'SubsOwner_PAN',	'SubsOwner_Postal_Address_Line_1',	'SubsOwner_Postal_Address_Line_2',	'SubsOwner_PostCodeCN',	'SubsOwner_Spouses_Name',	'SubsOwner_TIN_Type',	'SubsOwner_TIN_Type2',	'SubsOwner_TIN_Type3',	'SubsOwner_TIN_Type4',	'SubsOwner_US_TIN']
            for newcol in columns_add:
                    df[newcol]= None
                    
            df['SubsOwner_FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
            #df = df.rename(columns={"EAG Entity Legal Name (Full)":"SubsOwner_FFI_Name"})

            df['SubsOwner_Date_Of_Birth'] =  pd.to_datetime(df['SubsOwner_Date_Of_Birth'],errors ='coerce')
            df['SubsOwner_Date_Of_Birth'] = df['SubsOwner_Date_Of_Birth'].dt.strftime('%Y-%m-%d')
            
            if 'CanadianIdentificationNumber' in list(df.columns):
                df['SubsOwner_Identification_Number'] = df['CanadianIdentificationNumber']
            else:
                df['SubsOwner_Identification_Number'] = ''
			
            #QATAR changes
            if set(['QA Birth Country Code','QA Birth Former Country Name']).issubset(df.columns):
                df['SubsOwner_Birth_CountryCode'] = np.where(df.SubsOwner_Indivdual_Organisation_Indicator.eq('Individual'), df['QA Birth Country Code'],df['SubsOwner_Birth_CountryCode'])
                #df['SubsOwner_Birth_FormerCountryName'] = np.where(df.SubsOwner_Indivdual_Organisation_Indicator.eq('Individual'), df['QA Birth Former Country Name'],df['SubsOwner_Birth_FormerCountryName'])
            
            # Canada Changes
            if set(['CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date']).issubset(df.columns):
                df['SubsOwner_Identification_Number'] = df['CA_Tax_Id']

            df['SubsOwner_Birth_CountryCode'] = df['SubsOwner_Birth_CountryCode'].fillna('').astype(str).replace({'nan':''},regex=True)
            df['SubsOwner_Birth_FormerCountryName'] = df['SubsOwner_Birth_FormerCountryName'].fillna('').astype(str).replace({'nan':''},regex=True)
            df['SubsOwner_Identification_Number'] = df['SubsOwner_Identification_Number'].fillna('').astype(str).replace({'nan':''},regex=True)
            

            df = df[['SubsOwner_ReportingYear','SubsOwner_ReportingType','SubsOwner_Account_Number','SubsOwner_Account_Holder_Entity_Name',	'SubsOwner_Customer_ID','SubsOwner_FFI_Name','SubsOwner_Indivdual_Organisation_Indicator','SubsOwner_Substantial_owner_type','SubsOwner_Name_Type',	'SubsOwner_First_Name',	'SubsOwner_Middle_Name','SubsOwner_Last_Name',	'SubsOwner_Date_Of_Birth',	'SubsOwner_Birth_City','SubsOwner_Birth_CountryCode','SubsOwner_Entity_Name','SubsOwner_ResCountryCode','SubsOwner_ResCountryCode2','SubsOwner_ResCountryCode3','SubsOwner_ResCountryCode4','SubsOwner_ResCountryCode5','SubsOwner_ResCountryCode6','SubsOwner_ResCountryCode7','SubsOwner_ResCountryCode8','SubsOwner_TIN','SubsOwner_TINIssuedBy','SubsOwner_TIN2','SubsOwner_TINissuedby2','SubsOwner_TIN3','SubsOwner_TINissuedby3','SubsOwner_TIN4','SubsOwner_TINissuedby4','SubsOwner_TIN5','SubsOwner_TINissuedby5','SubsOwner_TIN6','SubsOwner_TINissuedby6','SubsOwner_TIN7','SubsOwner_TINissuedby7','SubsOwner_TIN8','SubsOwner_TINissuedby8','SubsOwner_Address_Type','SubsOwner_Country_Code','SubsOwner_Street_Name','SubsOwner_Building_Identifier','SubsOwner_Suite_Identifier','SubsOwner_Floor_Identifier','SubsOwner_District_Name','SubsOwner_POB','SubsOwner_Post_Code','SubsOwner_City','SubsOwner_Country_Subentity','SubsOwner_Address_Free','SubsOwner_ItalianTIN','SubsOwner_NoTINReason','SubsOwner_NoTINReason2','SubsOwner_NoTINReason3','SubsOwner_NoTINReason4','SubsOwner_Identification_Number','SubsOwner_BirthInfoYear','SubsOwner_BirthInfoMonth','SubsOwner_BirthInfoDay','SubsOwner_Province','SubsOwner_NoTINReason5','SubsOwner_NoTINReason6','SubsOwner_NoTINReason7','SubsOwner_NoTINReason8','SubsOwner_Nationality5','SubsOwner_Nationality6',	'SubsOwner_Nationality7','SubsOwner_Nationality8','SubsOwner_StateCode','SubsOwner_Birth_FormerCountryName','SubsOwner_TINType5','SubsOwner_TINType6','SubsOwner_TINType7',	'SubsOwner_TINType8',	'SubsOwner_India_Customer_ID','Nationality2','Nationality3','Nationality4','SubsOwner_Aadhaar_Number','SubsOwner_AddressFreeCN','SubsOwner_CityCN',	'SubsOwner_DistrictNameCN',	'SubsOwner_DueDiligenceInd','SubsOwner_Fathers_name','SubsOwner_Gender','SubsOwner_Identification_Type','SubsOwner_NameCN','SubsOwner_Nationality','SubsOwner_Occupation_Type','SubsOwner_PAN','SubsOwner_Postal_Address_Line_1','SubsOwner_Postal_Address_Line_2','SubsOwner_PostCodeCN','SubsOwner_Spouses_Name','SubsOwner_TIN_Type','SubsOwner_TIN_Type2','SubsOwner_TIN_Type3','SubsOwner_TIN_Type4','SubsOwner_US_TIN','FI_ID','CTCR Process ID','Account_Holder_Dormancy_Status_x']]
            
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Process_ControlPerson_Join', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
 
 
#----------------------------------------UNIQUE ID------------------------------------------------------

#Generating unique id every run
class unique_id(beam.DoFn):

    def process(self, element):
        try:
            id = str(uuid.uuid1())
            yield id

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'unique_id', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#--------------------------------USER DEFINED FUNCTIONS---------------------------------------------------

def Left_only(left_df,right_df, result, key):    
    df = left_df[(~left_df[key].isin(result[key]))]
    return df
    
def Right_only(left_df,right_df, result, key):
    df = right_df[(~right_df[key].isin(result[key]))]
    return df

def file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime):
    idx = list()
    #Record Type check
    record_type_check = df[(df['Record_Type'].isnull()) | (df['Record_Type'].astype(str) != '1')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
    message = 'Record Type is either blank or value other than expected(1)'
    label = 'REJECTION'
    error_id = "All_R_0001"
    validation_field = "Record Type"
    current_value = df.loc[record_type_check.index.tolist()]['Record_Type'].tolist()
    index_list = record_type_check.index.tolist()
    primary_key = []
    Institution_Id = " "
    Involved_Party_Id = " "
    Account_Id = " "
    Payment_Id = " "
    Child_Party_Id = " "
    
    for i in index_list:
        Institution_Id = df.loc[i,'Institution_Id']
        if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
            Account_Id = df.loc[i,'Account_Id']
        if 'PAY.csv' in name:
            Payment_Id = df.loc[i,'Payment_Id']
        if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
        if 'IP_IP.csv' in name:
            Child_Party_Id = df.loc[i,'Child_Party_Id']
        com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
        primary_key.append(com_key)
	
    primary_key = str(primary_key)
    current_value = str(current_value)
    idx = idx + record_type_check.index.tolist()
    if ('CRS' in name):
        Record_Log_Info = CRS_record_log(record_type_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    else:
        Record_Log_Info = FATCA_record_log(record_type_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    #Record Action id check
  
    record_action = ['A','U','D']
    record_action_check = df[(df['Record_Action'].isnull()) | (~(df['Record_Action'].isin(record_action)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
    message = 'Record Action either blank or value other than expected(A,U,D)'
    label = 'REJECTION'
    error_id = "All_R_0002"
    validation_field = "Record Action"
    current_value = df.loc[record_action_check.index.tolist()]['Record_Action'].tolist()
    index_list = record_action_check.index.tolist()
    primary_key = []
    Institution_Id = " "
    Involved_Party_Id = " "
    Account_Id = " "
    Payment_Id = " "
    Child_Party_Id = " "

    for i in index_list:
        Institution_Id = df.loc[i,'Institution_Id']
        if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
            Account_Id = df.loc[i,'Account_Id']
        if 'PAY.csv' in name:
            Payment_Id = df.loc[i,'Payment_Id']
        if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
        if 'IP_IP.csv' in name:
            Child_Party_Id = df.loc[i,'Child_Party_Id']
        com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
        primary_key.append(com_key)

    primary_key = str(primary_key)
    current_value = str(current_value)
    idx = idx + record_action_check.index.tolist()
    if ('CRS' in name):
        Record_Log_Info = CRS_record_log(record_action_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    else:
        Record_Log_Info = FATCA_record_log(record_action_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        
    #Institution id check
    inst_id_check = df[(df['Institution_Id'].isnull()) | (df['Institution_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
    message = 'Institution id blank'
    label = 'REJECTION'
    error_id = "All_R_0003"
    validation_field = "Institution id"
    current_value = df.loc[inst_id_check.index.tolist()]['Institution_Id'].tolist()
    index_list = inst_id_check.index.tolist()
    primary_key = []
    Institution_Id = " "
    Involved_Party_Id = " "
    Account_Id = " "
    Payment_Id = " "
    Child_Party_Id = " "

    for i in index_list:
        Institution_Id = df.loc[i,'Institution_Id']
        if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
            Account_Id = df.loc[i,'Account_Id']
        if 'PAY.csv' in name:
            Payment_Id = df.loc[i,'Payment_Id']
        if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
        if 'IP_IP.csv' in name:
            Child_Party_Id = df.loc[i,'Child_Party_Id']
        com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
        primary_key.append(com_key)

    primary_key = str(primary_key)
    current_value = str(current_value)
    idx = idx + inst_id_check.index.tolist()
    if ('CRS' in name):
        Record_Log_Info = CRS_record_log(inst_id_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    else:
        Record_Log_Info = FATCA_record_log(inst_id_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    #cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
    #Record_Log_Info = Record_Log_Info[cols]
    return(Record_Log_Info,df,idx)


def date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime):
    
    df['Effective_From_Date'] = pd.to_datetime(df['Effective_From_Date'], format='%Y-%m-%d', errors='coerce')
    df['Effective_To_Date'] = pd.to_datetime(df['Effective_To_Date'], format='%Y-%m-%d', errors='coerce')

    NZ_date = pd.DataFrame()
    SA_date = pd.DataFrame()

    NZ_date = df[df['MDT_Country'] =='NZ']
    SA_date = df[df['MDT_Country'] =='SA']
    df_date = df[(df['MDT_Country'] !='NZ') | (df['MDT_Country'] !='SA')]
    if len(NZ_date) > 0:
        NZ_date_check = NZ_date[(NZ_date['Effective_From_Date'].dt.month !=4)|(NZ_date['Effective_To_Date'].dt.month !=3) | (NZ_date['Effective_From_Date'].dt.day !=1)|(NZ_date['Effective_To_Date'].dt.day !=31)]
        message = 'Effective_From_Date or Effective_To_Date invalid(YYYY-MM-DD) for New Zealand'
        label = 'REJECTION'
        error_id = "All_R_0004"
        validation_field = "Effective_From_Date or Effective_To_Date"
        current_value = df.loc[NZ_date_check.index.tolist()]['Effective_From_Date'].tolist()
        index_list = NZ_date_check.index.tolist()
        primary_key = []
        Institution_Id = " "
        Involved_Party_Id = " "
        Account_Id = " "
        Payment_Id = " "
        Child_Party_Id = " "

        for i in index_list:
            Institution_Id = df.loc[i,'Institution_Id']
            if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
                Account_Id = df.loc[i,'Account_Id']
            if 'PAY.csv' in name:
                Payment_Id = df.loc[i,'Payment_Id']
            if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
                Involved_Party_Id = df.loc[i,'Involved_Party_Id']
            if 'IP_IP.csv' in name:
                Child_Party_Id = df.loc[i,'Child_Party_Id']
            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
            primary_key.append(com_key)
        primary_key = str(primary_key)
        current_value = str(current_value)
        idx = idx + NZ_date_check.index.tolist()
        if ('CRS' in name):
            Record_Log_Info = CRS_record_log(NZ_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        else:
            Record_Log_Info = FATCA_record_log(NZ_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        #cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
        #Record_Log_Info = Record_Log_Info[cols]
        return(Record_Log_Info,df,idx)


    if len(SA_date)>0:
        SA_date_check = SA_date[(SA_date['Effective_From_Date'].dt.month !=3)|(SA_date['Effective_To_Date'].dt.month !=2) | (SA_date['Effective_From_Date'].dt.day !=1)|(SA_date['Effective_To_Date'].dt.day.isin([28,29]))]
        message = 'Effective_From_Date or Effective_To_Date invalid(YYYY-MM-DD) for South Africa'
        idx = idx + SA_date_check.index.tolist()
        label = 'REJECTION'
        error_id = "All_R_0005"
        validation_field = "Effective_From_Date or Effective_To_Date"
        current_value = df.loc[SA_date_check.index.tolist()]['Effective_From_Date'].tolist()
        index_list = SA_date_check.index.tolist()
        primary_key = []
        Institution_Id = " "
        Involved_Party_Id = " "
        Account_Id = " "
        Payment_Id = " "
        Child_Party_Id = " "

        for i in index_list:
            Institution_Id = df.loc[i,'Institution_Id']
            if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
                Account_Id = df.loc[i,'Account_Id']
            if 'PAY.csv' in name:
                Payment_Id = df.loc[i,'Payment_Id']
            if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
                Involved_Party_Id = df.loc[i,'Involved_Party_Id']
            if 'IP_IP.csv' in name:
                Child_Party_Id = df.loc[i,'Child_Party_Id']
            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
            primary_key.append(com_key)
        primary_key = str(primary_key)
        current_value = str(current_value)
        if ('CRS' in name):
            Record_Log_Info = CRS_record_log(SA_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        else:
            Record_Log_Info = FATCA_record_log(SA_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        #cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
        #Record_Log_Info = Record_Log_Info[cols]    
        return(Record_Log_Info,df,idx)


    if len(df_date) > 0:
        df_date_check = df_date[(df_date['Effective_From_Date'].dt.month !=1)|(df_date['Effective_To_Date'].dt.month !=12) | (df_date['Effective_From_Date'].dt.day !=1)|(df_date['Effective_To_Date'].dt.day !=31)]
        message = 'Effective_From_Date or Effective_To_Date invalid(YYYY-MM-DD)'
        idx = idx + df_date_check.index.tolist()
        label = 'REJECTION'
        error_id = "All_R_0006"
        validation_field = "Effective_From_Date or Effective_To_Date"
        current_value = df.loc[df_date_check.index.tolist()]['Effective_From_Date'].tolist()
        index_list = df_date_check.index.tolist()
        primary_key = []

        Institution_Id = " "
        Involved_Party_Id = " "
        Account_Id = " "
        Payment_Id = " "
        Child_Party_Id = " "

        for i in index_list:
            Institution_Id = df.loc[i,'Institution_Id']
            if 'ACCT.csv' in name or 'PAY.csv' in name or 'ACCT_BAL.csv' in name:
                Account_Id = df.loc[i,'Account_Id']
            if 'PAY.csv' in name:
                Payment_Id = df.loc[i,'Payment_Id']
            if (('IP_ACCT.csv' in name or 'IP_TD.csv' in name or 'IP.csv' in name) and 'IP_IP.csv' not in name):
                Involved_Party_Id = df.loc[i,'Involved_Party_Id']
            if 'IP_IP.csv' in name:
                Child_Party_Id = df.loc[i,'Child_Party_Id']
            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + Child_Party_Id
            primary_key.append(com_key)
		
        primary_key = str(primary_key)
        current_value = str(current_value)
        if ('CRS' in name):
            Record_Log_Info = CRS_record_log(df_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        else:
            Record_Log_Info = FATCA_record_log(df_date_check,Record_Log_Info,message,label,name,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
        #cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
        #Record_Log_Info = Record_Log_Info[cols]    
        return(Record_Log_Info,df,idx)    


#----------------------------------CRS USER DEFINED FUNCTIONS: RECORD LOG---------------------------------------------------

def CRS_record_log(df,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key):

    df_1= df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Source File Index'].apply(lambda x: "".join(str(x.values).strip())).reset_index()
    df_2 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['CSV File Index'].apply(lambda x: "".join(str(x.values).strip())).reset_index()
    df_3 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Stage1_id'].apply(lambda x: "".join(str(x.values).strip())).reset_index()
    df_4 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Key'].apply(lambda x: "".join(str(x.values).strip())).reset_index()
    df = df_1
    df['CSV File Index'] = df_2['CSV File Index']
    df['Key'] = df_4['Key']
    df['Stage1_id'] = df_3['Stage1_id']
    df['Message'] = message
    df['Date'] = date.today()
    df['Source File Index'] = df['Source File Index'].apply(lambda x: x.replace('  ',' '))
    df['Source File Index'] = df['Source File Index'].apply(lambda x: x.replace(' ',','))
    df['CSV File Name'] = filename
    df['CSV File Index'] = df['CSV File Index'].apply(lambda x: x.replace('  ',' '))#','.join([str(elem) for elem in idx])
    df['CSV File Index'] = df['CSV File Index'].apply(lambda x: x.replace(' ',','))
    df['Key'] = df['Key'].apply(lambda x: x.replace(' ',','))
    df['Stage1_id'] = df['Stage1_id'].apply(lambda x: x.replace(' ',','))
    df['Stage2_id'] =''
    df['Message Label'] = label
    df['ETL_ProcessId'] = ETL_ProcessId
    df['ETL_Start_DateTime'] = ETL_Start_DateTime
    df['Regime'] = 'CRS'
    df['ETL_End_DateTime'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    df['Error_ID'] = error_id
    df['Validation field'] = validation_field
    df['Current Value'] = current_value
    df['Primary Key'] = primary_key
	
	
    time =datetime.now()
    df ['Time'] = time.strftime("%H:%M:%S")
    try:
        cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
        df = df[cols]
        df = df.drop_duplicates()
        Record_Log_Info = Record_Log_Info.append(df)
        return(Record_Log_Info)
    except:return(Record_Log_Info)
    
#--------------------------------FATCA USER DEFINED FUNCTION : RECORD LOG---------------------------------------------------

def FATCA_record_log(df,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key):
    df_1= df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Source File Index'].apply(lambda x: "".join(str(x.values))).reset_index()
    df_2 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['CSV File Index'].apply(lambda x: "".join(str(x.values))).reset_index()
    df_3 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Stage1_id'].apply(lambda x: "".join(str(x.values))).reset_index()
    df_4 = df.groupby(['SourceFileName','Source System ID','CTCR Process ID'])['Key'].apply(lambda x: "".join(str(x.values))).reset_index()
    df = df_1
    df['CSV File Index'] = df_2['CSV File Index']
    df['Key'] = df_4['Key']
    df['Stage1_id'] = df_3['Stage1_id']
    df['Message'] = message
    df['Date'] = date.today()
    df['Source File Index'] = df['Source File Index'].apply(lambda x: x.replace(' ',','))
    df['CSV File Name'] = filename
    df['CSV File Index'] = df['CSV File Index'].apply(lambda x: x.replace(' ',','))#','.join([str(elem) for elem in idx])
    df['Key'] = df['Key'].apply(lambda x: x.replace(' ',','))
    df['Stage1_id'] = df['Stage1_id'].apply(lambda x: x.replace(' ',','))
    df['Stage2_id'] ='----'
    df['Message Label'] = label
    df['ETL_ProcessId'] = ETL_ProcessId
    df['ETL_Start_DateTime'] = ETL_Start_DateTime
    df['Regime'] = 'FTR'
    df['ETL_End_DateTime'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    df['Error_ID'] = error_id
    df['Validation field'] = validation_field
    df['Current Value'] = current_value
    df['Primary Key'] = primary_key
    time = datetime.now()
    df ['Time'] = time.strftime("%H:%M:%S")
    try:
        cols = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID','Message Label','Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id','ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key']
        df = df[cols]
        df = df.drop_duplicates()
        Record_Log_Info = Record_Log_Info.append(df)
        return(Record_Log_Info)
    except:return(Record_Log_Info)
    
#---------------------------------------FATCA RECORD VALIDATION--------------------------------------------------------------

#Record Level Validation
class FATCA_Record_level_validation(beam.DoFn):

    OUTPUT_TAG_LOG = 'Log file'

    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,element,df,filename,ETL_ProcessId, ETL_Start_DateTime, exchange_df):
        try:
            #prefix = FATCA_csv_input_prefix
            ETL_ProcessId = ETL_ProcessId
            ETL_Start_DateTime = ETL_Start_DateTime
            cur_list = exchange_df['Category Key'].unique().tolist()
            Record_Log_Info = pd.DataFrame(columns = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID', 'Message Label', 'Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id', 'ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key'])
            if element ==1:
                if(len(df)>0):
                
                    if (filename =='FTR_ACCT.csv'):
                    
                        name = filename
    
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
    
                        #Account Id check
                        Acc_id_check = df[(df['Account_Id'].isnull()) | (df['Account_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Id is blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0001"
                        validation_field = "Account_Id"
                        current_value = df.loc[Acc_id_check.index.tolist()]['Account_Id'].tolist()
                        index_list = Acc_id_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Acc_id_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Acc_id_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        #Account Type check
                        Acc_Type = ['C','D','DI','EI','CV','X']
                        Acc_type_check = df[(df['Account_Type'].isnull()) | (df['Account_Type'] == '') | (~(df['Account_Type'].isin(Acc_Type)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Type is either blank or value other than expected(C,D,DI,EI,CV,X)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0002"
                        validation_field = "Account_Type"
                        current_value = df.loc[Acc_type_check.index.tolist()]['Account_Type'].tolist()
                        index_list = Acc_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Acc_type_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Acc_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Account close date
                        df['Account_close_date'] =  pd.to_datetime(df['Account_close_date'], format='%Y-%m-%d', errors='coerce') 
                        blank_date_acc_close = df[(df['Account_Status']=='C') & (df['Account_close_date'].isnull())][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = "Account Close Date Blank or the format is invalid(YYYY-MM-DD) for Account Status 'C'"
                        idx = idx + blank_date_acc_close.index.tolist()
                        label = 'REJECTION'
                        error_id = "FTR_R_0003"
                        validation_field = "Account_close_date"
                        current_value = df.loc[blank_date_acc_close.index.tolist()]['Account_close_date'].tolist()
                        index_list = blank_date_acc_close.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        Record_Log_Info = FATCA_record_log(blank_date_acc_close,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
    
                    if (filename == 'FTR_PAY.csv'):
                    
                        name = filename
    
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Invalid currency code
                        cur_code_check = df[(df['Payment_Amount_CCY'].isnull()) | (~df['Payment_Amount_CCY'].isin(cur_list))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in currency code with FX Rate file'
                        label = 'REJECTION'
                        error_id = "FTR_R_0036"
                        validation_field = "Payment_Amount_CCY"
                        current_value = df.loc[cur_code_check.index.tolist()]['Payment_Amount_CCY'].tolist()
                        index_list = cur_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + cur_code_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(cur_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

    
                        #Invalid payment date range code
                        index_list=list()
                        df_bkp = df
                        df['Reporting_EndDate']=df['Payment_Date'].astype(str)
                        df['Reporting_EndDate'] = df['Reporting_EndDate'].str[0:4]+"-12-31"
                        #df['Reporting_EndDate'] = pd.to_datetime(df['Payment_Date']).dt.year.astype(str)+"-12-31"
                        df_pay = df
                        df=df[['Payment_Amount_CCY','Reporting_EndDate']].drop_duplicates().reset_index(drop=True)
                        
                        
                        
                        forex_df_columns = list(df.columns)
                        forex_df_vect = df.values
                        
                        unavail_codes = list()
                        for row in forex_df_vect:
                            exchange_df_new = exchange_df.loc[(exchange_df['Category Key'] == row[forex_df_columns.index('Payment_Amount_CCY')]) & (exchange_df['Start Date'] <= row[forex_df_columns.index('Reporting_EndDate')])]
			  
							  
                            exchange_df_new2 = exchange_df_new.loc[exchange_df_new['End Date'] >= row[forex_df_columns.index('Reporting_EndDate')]]
						
                            if len(exchange_df_new2) == 0:
                                unavail_codes.append(row)
                        
                        if len(unavail_codes)>0:
                            for i in unavail_codes:
                                index_list.append(list(df_pay[(df_pay['Payment_Amount_CCY']==i[0]) &(df_pay['Reporting_EndDate']==i[1])].index.values))
                            
                            index_list = sum(index_list,[])
                        
                        df = df_bkp  

                        payment_date_check = df[df.index.isin(index_list)][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in year in FX Rate file'
                        label = 'REJECTION'
                        error_id = "FTR_R_0038"
                        validation_field = "Reporting Year End Date"
                        current_value = df.loc[index_list]['Payment_Date'].tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + index_list
                        Record_Log_Info = FATCA_record_log(payment_date_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        #Blank Payment_id
                        blank_pay_id = df[(df['Payment_Id'].isnull()) | (df['Payment_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment id blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0030"
                        validation_field = "Payment_Id"
                        current_value = df.loc[blank_pay_id.index.tolist()]['Payment_Id'].tolist()
                        index_list = blank_pay_id.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_pay_id.index.tolist()
                        Record_Log_Info = FATCA_record_log(blank_pay_id,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment code check
                        Pay_code = ['DIV','INT','GPR','OTH']
                        Pay_code_check = df[(df['Payment_Code'].isnull()) | (~df['Payment_Code'].isin(Pay_code))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment code is either blank or value other than expected(DIV,INT,GPR,OTH)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0031"
                        validation_field = "Payment_Code"
                        current_value = df.loc[Pay_code_check.index.tolist()]['Payment_Code'].tolist()
                        index_list = Pay_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Pay_code_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Pay_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment Date check
                        pay_df = df
                        pay_df['Payment_Date'] =  pd.to_datetime(pay_df['Payment_Date'], format='%Y-%m-%d', errors='coerce') 
                        invaid_date_pay = pay_df[pay_df['Payment_Date'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment Date is either blank or the format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0032"
                        validation_field = "Payment_Date"
                        current_value = df.loc[invaid_date_pay.index.tolist()]['Payment_Date'].tolist()
                        index_list = invaid_date_pay.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_date_pay.index.tolist()
    
                        Record_Log_Info = FATCA_record_log(invaid_date_pay,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment Amount and Payment Amount USD Check
                        df['Payment_Amount'] = df['Payment_Amount'].apply(lambda x : '' if x.count(' ') == 14 else x)
                        df['Payment_Amount_USD'] = df['Payment_Amount_USD'].apply(lambda x : '' if x.count(' ') == 14 else x)

                        Pay_amt_check = df[((df['Payment_Amount'].isnull()) | (df['Payment_Amount'] == '')) & ((df['Payment_Amount_USD'].isnull()) | (df['Payment_Amount_USD'] == ''))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Both Payment Amount and Payment Amount USD are blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0033"
                        validation_field = "Payment_Amount"
                        current_value = df.loc[Pay_amt_check.index.tolist()]['Payment_Amount'].tolist()
                        index_list = Pay_amt_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Pay_amt_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Pay_amt_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
                    if (filename =='FTR_ACCT_BAL.csv'):
                        name = filename
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        
                        #Invalid currency code
                        cur_code_check = df[(df['Balance_Currency_Code'].isnull()) | (~df['Balance_Currency_Code'].isin(cur_list))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in currency code with FX Rate file'
                        label = 'REJECTION'
                        error_id = "FTR_R_0035"
                        validation_field = "Balance_Currency_Code"
                        current_value = df.loc[cur_code_check.index.tolist()]['Balance_Currency_Code'].tolist()
                        index_list = cur_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + cur_code_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(cur_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Invalid Balance end date range
                        index_list=list()
                        df_bkp = df
                        df['Reporting_EndDate']=df['Balance_End_Date'].astype(str)
                        df['Reporting_EndDate'] = df['Reporting_EndDate'].str[0:4]+"-12-31"																																								 
                        df_acct_bal = df
                        df=df[['Balance_Currency_Code','Reporting_EndDate']].drop_duplicates().reset_index(drop=True)
                        forex_df_columns = list(df.columns)
                        forex_df_vect = df.values
                        unavail_codes = list()
                        for row in forex_df_vect:
	                        exchange_df_new = exchange_df.loc[(exchange_df['Category Key'] == row[forex_df_columns.index('Balance_Currency_Code')]) & (exchange_df['Start Date'] <= row[forex_df_columns.index('Reporting_EndDate')])]
	                        exchange_df_new2 = exchange_df_new.loc[exchange_df_new['End Date'] >= row[forex_df_columns.index('Reporting_EndDate')]]

	                        if len(exchange_df_new2) == 0:
		                        unavail_codes.append(row)
                        if len(unavail_codes)>0:
	                        for i in unavail_codes:
		                        index_list.append(list(df_acct_bal[(df_acct_bal['Balance_Currency_Code']==i[0]) &(df_acct_bal['Reporting_EndDate']==i[1])].index.values))
	
	                        index_list = sum(index_list,[])
                        
                        df = df_bkp 					
						
                        '''
                        for index,row in df.iterrows():
                            s = str(df.loc[index, 'Balance_Currency_Code'])
                            date = df.loc[index, 'Balance_End_Date']
                            date = str(date)
                            reporting_yr = date.split("-")[0]
                            date = reporting_yr + "-12-31"
                            exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == s]
                            if len(exchange_df_new) > 0:
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= date)]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= date)]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
								
							
                                if len(exchange_df_new2) == 0:
                                    index_list.append(index
                        '''

                        balance_end_date_check = df[df.index.isin(index_list)][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in year in FX Rate file'
                        label = 'REJECTION'
                        error_id = "FTR_R_0037"
                        validation_field = "Reporting Year End Date"
                        current_value = df.loc[index_list]['Balance_End_Date'].tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + index_list
                        Record_Log_Info = FATCA_record_log(balance_end_date_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        #Balance End Date Check
                        acct_bal_df = df
                        acct_bal_df['Balance_End_Date'] = acct_bal_df['Balance_End_Date'].apply(lambda x : 'valid' if x=='' else pd.to_datetime(x, format='%Y-%m-%d', errors='coerce'))
                        invaid_balance_end_date = acct_bal_df[acct_bal_df['Balance_End_Date'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Balance End Dateformat is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0004"
                        validation_field = "Balance_End_Date"
                        current_value = df.loc[invaid_balance_end_date.index.tolist()]['Balance_End_Date'].tolist()
                        index_list = invaid_balance_end_date.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_balance_end_date.index.tolist()
    
                        Record_Log_Info = FATCA_record_log(invaid_balance_end_date,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)                       
                        
                        # Balance amount and Balance Amount US Blank check
                        
                        df['Balance_Amount'] = df['Balance_Amount'].apply(lambda x : '' if x.count(' ') == 17 else x)
                        df['USD_Balance_Amount'] = df['USD_Balance_Amount'].apply(lambda x : '' if x.count(' ') == 14 else x)

                        Bal_amt_check = df[((df['Balance_Amount'].isnull()) | (df['Balance_Amount'] == '')) & ((df['USD_Balance_Amount'].isnull()) | (df['USD_Balance_Amount'] == ''))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Both Balance Amount and USD Balance Amount are blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0039"
                        validation_field = "Balance_Amount"
                        current_value = df.loc[Bal_amt_check.index.tolist()]['Balance_Amount'].tolist()
                        index_list = Bal_amt_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Bal_amt_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Bal_amt_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
                    if (filename =='FTR_IP_IP.csv'):
                    
                    
                        name = filename
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Parent Id Check
                        blank_parent_id = df[(df['Parent_Party_Id'].isnull()) | (df['Parent_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Parent Party Id is blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0028"
                        validation_field = "Parent_Party_Id"
                        current_value = df.loc[blank_parent_id.index.tolist()]['Parent_Party_Id'].tolist()
                        index_list = blank_parent_id.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Child_Party_Id = df.loc[i,'Child_Party_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + " " + ';' + " " + ';' + " "+ ';' + Child_Party_Id
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_parent_id.index.tolist()
                        Record_Log_Info = FATCA_record_log(blank_parent_id,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
                    if (filename =='FTR_IP_ACCT.csv'):
                    
                        name = filename
    
                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Account Id check
                        Acc_id_check = df[(df['Account_Id'].isnull()) | (df['Account_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Id is blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0026"
                        validation_field = "Account_Id"
                        current_value = df.loc[Acc_id_check.index.tolist()]['Account_Id'].tolist()
                        index_list = Acc_id_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Acc_id_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Acc_id_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        #Account Relationship Type Check
                        Acc_rel = ['A','J']
                        Acc_rel_type = df[(df['Account_Relationship_Type'].isnull()) | (df['Account_Relationship_Type'] == '') | (~df['Account_Relationship_Type'].isin(Acc_rel))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Relationship Type is either blank or value other than expected(A,J)'
                        idx = idx + Acc_rel_type.index.tolist()
                        label = 'REJECTION'
                        error_id = "FTR_R_0027"
                        validation_field = "Account_Relationship_Type"
                        current_value = df.loc[Acc_rel_type.index.tolist()]['Account_Relationship_Type'].tolist()
                        index_list = Acc_rel_type.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        Record_Log_Info = FATCA_record_log(Acc_rel_type,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
                    if (filename =='FTR_IP.csv'):
                    
                        name = filename
    
                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Involved Party Id Blank
                        blank_invlo_party_id_ip_acc =df[(df['Involved_Party_Id'].isnull()) | (df['Involved_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Involved Party Id is Blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0023"
                        validation_field = "Involved_Party_Id"
                        current_value = df.loc[blank_invlo_party_id_ip_acc.index.tolist()]['Involved_Party_Id'].tolist()
                        index_list = blank_invlo_party_id_ip_acc.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_invlo_party_id_ip_acc.index.tolist()
                        Record_Log_Info = FATCA_record_log(blank_invlo_party_id_ip_acc,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Involved Party Type

                        Inv_party_type = ['I','E']
                        inv_party_type_check = df[(df['Involved_Party_Type'].isnull()) | (df['Involved_Party_Type'] == '') |(~(df['Involved_Party_Type'].isin(Inv_party_type)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Involved Party Type either blank or value other than expected(I,E)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0024"
                        validation_field = "Involved_Party_Type"
                        current_value = df.loc[inv_party_type_check.index.tolist()]['Involved_Party_Type'].tolist()
                        index_list = inv_party_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + inv_party_type_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(inv_party_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Individual DOB Check
                        ip_df = df
                                                                                                                       
                        ip_df['Individual_DOB'] =  pd.to_datetime(ip_df['Individual_DOB'], format='%Y-%m-%d', errors='coerce').fillna("")
                        invaid_date_dob = ip_df[ip_df['Individual_DOB'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Individual DOB is format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0025"
                        validation_field = "Individual_DOB"
                        current_value = df.loc[invaid_date_dob.index.tolist()]['Individual_DOB'].tolist()
                        index_list = invaid_date_dob.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_date_dob.index.tolist()
    
                        Record_Log_Info = FATCA_record_log(invaid_date_dob,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        idx= list(set(idx))
                        df = df.drop(idx)

                    if (filename =='FTR_IP_ADDN_CA.csv'):
                    
                        name = filename
    
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        
                        #Involved Party Id Blank
                        blank_invlo_party_id_ip_ca =df[(df['Involved_Party_Id'].isnull()) | (df['Involved_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        
                        message = 'Involved Party Id is Blank'
                        label = 'REJECTION'
                        error_id = "FTR_R_0023"
                        validation_field = "Involved_Party_Id"
                        current_value = df.loc[blank_invlo_party_id_ip_ca.index.tolist()]['Involved_Party_Id'].tolist()
                        index_list = blank_invlo_party_id_ip_ca.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_invlo_party_id_ip_ca.index.tolist()
                        Record_Log_Info = FATCA_record_log(blank_invlo_party_id_ip_ca,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        
                        '''
                        #CA_Customer_Opening_Date Check
                        ip_CA_df = df
                                                                                                                       
                        ip_CA_df['CA_Customer_Opening_Date'] =  pd.to_datetime(ip_CA_df['CA_Customer_Opening_Date'], format='%Y-%m-%d', errors='coerce').fillna("")
                        invaid_date_dob = ip_CA_df[ip_CA_df['CA_Customer_Opening_Date'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'CA Customer Opening Date format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "FTR_R_0025"
                        validation_field = "CA_Customer_Opening_Date"
                        current_value = df.loc[invaid_date_dob.index.tolist()]['CA_Customer_Opening_Date'].tolist()
                        index_list = invaid_date_dob.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_date_dob.index.tolist()
    
                        Record_Log_Info = FATCA_record_log(invaid_date_dob,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        '''
                        #Tax_Id_type

                        Tax_Id_type_list = ['SIN','BN']
                        Tax_Id_type_check = df[(df['CA_Tax_Id_type'].isnull()) | (~df['CA_Tax_Id_type'].isin(Tax_Id_type_list))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Tax Id type either blank or value other than expected(SIN,BN)'
                        label = 'EXCEPTION'
                        error_id = "FTR_E_0040"
                        validation_field = "CA_Tax_Id_type"
                        current_value = df.loc[Tax_Id_type_check.index.tolist()]['CA_Tax_Id_type'].tolist()
                        index_list = Tax_Id_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Tax_Id_type_check.index.tolist()
                        Record_Log_Info = FATCA_record_log(Tax_Id_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        
                        idx= list(set(idx))
                        df = df.drop(idx)
    
                    df = df.reset_index(drop=True)
                Record_Log_Info = Record_Log_Info.rename(columns = {'Source System ID':'Source System','SourceFileName':'Source_Filename'})
                cols = ['ETL_ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime',  'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key', 'CSV File Index']
                Record_Log_Info = Record_Log_Info[cols]  
                yield df
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_LOG,Record_Log_Info)
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                logger.log_struct({'severity':'Critical','message':'FATCA Flag is 0',"Class name": 'FATCA_Record_level_validation'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Record_level_validation', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
#------------------------------------CRS RECORD VALIDATION--------------------------------------------------------------

#Record Level Validation
class CRS_Record_level_validation(beam.DoFn):

    OUTPUT_TAG_LOG = 'Log file'

    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self, element,df,filename, ETL_ProcessId, ETL_Start_DateTime, exchange_df):
        
        try:
            prefix = CRS_csv_input_prefix
            ETL_ProcessId = ETL_ProcessId
            ETL_Start_DateTime = ETL_Start_DateTime
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            cur_list = exchange_df['Category Key'].unique().tolist()
            Record_Log_Info = pd.DataFrame(columns = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID', 'Message Label', 'Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id', 'ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key'])
            if element == 1:
                
                if(len(df)>0):
                
                    if ('CRS_ACCT.csv' == filename):
                    
                        name = filename
    
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Account Id check
                        Acc_id_check = df[(df['Account_Id'].isnull()) | (df['Account_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Id is blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0001"
                        validation_field = "Account_Id"
                        current_value = df.loc[Acc_id_check.index.tolist()]['Account_Id'].tolist()
                        index_list = Acc_id_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
    
                        idx = idx + Acc_id_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Acc_id_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Account Type check
                        Acc_Type = ['C','D','DI','EI','CV','X']
                        Acc_type_check = df[(df['Account_Type'].isnull()) | (df['Account_Type'] == '') | (~(df['Account_Type'].isin(Acc_Type)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Type is either blank or value other than expected(C,D,DI,EI,CV,X)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0002"
                        validation_field = "Account_Type"
                        current_value = df.loc[Acc_type_check.index.tolist()]['Account_Type'].tolist()
                        index_list = Acc_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Acc_type_check.index.tolist()

                        Record_Log_Info = CRS_record_log(Acc_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Account close date
                        df['Account_close_date'] =  pd.to_datetime(df['Account_close_date'], format='%Y-%m-%d', errors='coerce') 
                        blank_date_acc_close = df[(df['Account_Status']=='C') & (df['Account_close_date'].isnull())][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = "Account Close Date Blank or the format is invalid(YYYY-MM-DD) for Account Status 'C'"
                        idx = idx + blank_date_acc_close.index.tolist()
                        label = 'REJECTION'
                        error_id = "CRS_R_0003"
                        validation_field = "Account_close_date"
                        current_value = df.loc[blank_date_acc_close.index.tolist()]['Account_close_date'].tolist()
                        index_list = blank_date_acc_close.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        Record_Log_Info = CRS_record_log(blank_date_acc_close,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
                    if ('CRS_PAY.csv' == filename):
                    
                        name = filename
    
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Invalid currency code
                        cur_code_check = df[(df['Payment_Amount_CCY'].isnull()) | (~df['Payment_Amount_CCY'].isin(cur_list))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in currency code with FX Rate file'
                        label = 'REJECTION'
                        error_id = "CRS_R_0037"
                        validation_field = "Payment_Amount_CCY"
                        current_value = df.loc[cur_code_check.index.tolist()]['Payment_Amount_CCY'].tolist()
                        index_list = cur_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + cur_code_check.index.tolist()
                        Record_Log_Info = CRS_record_log(cur_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)


                        #Invalid payment date range code
                        index_list=list()
                        df_bkp = df
                        df['Reporting_EndDate']=df['Payment_Date'].astype(str)
                        df['Reporting_EndDate'] = df['Reporting_EndDate'].str[0:4]+"-12-31"
                        #df['Reporting_EndDate'] = pd.to_datetime(df['Payment_Date']).dt.year.astype(str)+"-12-31"
                        df_pay = df
                        df=df[['Payment_Amount_CCY','Reporting_EndDate']].drop_duplicates().reset_index(drop=True)
                        
                        
                        
                        forex_df_columns = list(df.columns)
                        forex_df_vect = df.values
                        
                        unavail_codes = list()
                        for row in forex_df_vect:
                            exchange_df_new = exchange_df.loc[(exchange_df['Category Key'] == row[forex_df_columns.index('Payment_Amount_CCY')]) & (exchange_df['Start Date'] <= row[forex_df_columns.index('Reporting_EndDate')])]
														
																											   
                            exchange_df_new2 = exchange_df_new.loc[exchange_df_new['End Date'] >= row[forex_df_columns.index('Reporting_EndDate')]]
																						  
                            if len(exchange_df_new2) == 0:
                                unavail_codes.append(row)
                        
                        if len(unavail_codes)>0:
                            for i in unavail_codes:
                                index_list.append(list(df_pay[(df_pay['Payment_Amount_CCY']==i[0]) &(df_pay['Reporting_EndDate']==i[1])].index.values))
                            
                            index_list = sum(index_list,[])
                        
                        df = df_bkp	 
                        payment_date_check = df[df.index.isin(index_list)][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in year in FX Rate file'
                        label = 'REJECTION'
                        error_id = "CRS_R_0039"
                        validation_field = "Reporting Year End Date"
                        current_value = df.loc[index_list]['Payment_Date'].tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + index_list
                        Record_Log_Info = CRS_record_log(payment_date_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        #Blank Payment_id
                        blank_pay_id = df[(df['Payment_Id'].isnull()) | (df['Payment_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment id blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0031"
                        validation_field = "Payment_Id"
                        current_value = df.loc[blank_pay_id.index.tolist()]['Payment_Id'].tolist()
                        index_list = blank_pay_id.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_pay_id.index.tolist()
                        Record_Log_Info = CRS_record_log(blank_pay_id,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment code check
                        Pay_code = ['DIV','INT','GPR','OTH']
                        Pay_code_check = df[(df['Payment_Code'].isnull()) | (~(df['Payment_Code'].isin(Pay_code)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment code is either blank or value other than expected(DIV,INT,GPR,OTH)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0032"
                        validation_field = "Payment_Code"
                        current_value = df.loc[Pay_code_check.index.tolist()]['Payment_Code'].tolist()
                        index_list = Pay_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Pay_code_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Pay_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment Date check
                        pay_df = df
                        pay_df['Payment_Date'] =  pd.to_datetime(pay_df['Payment_Date'], format='%Y-%m-%d', errors='coerce') 
                        invaid_date_pay = pay_df[pay_df['Payment_Date'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Payment Date is either blank or the format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0033"
                        validation_field = "Payment_Date"
                        current_value = df.loc[invaid_date_pay.index.tolist()]['Payment_Date'].tolist()
                        index_list = invaid_date_pay.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_date_pay.index.tolist()
    
                        Record_Log_Info = CRS_record_log(invaid_date_pay,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        #Payment Amount and Payment Amount USD Check
                        
                        df['Payment_Amount'] = df['Payment_Amount'].apply(lambda x : '' if x.count(' ') == 14 else x)
                        df['Payment_Amount_USD'] = df['Payment_Amount_USD'].apply(lambda x : '' if x.count(' ') == 14 else x)                        
                        
                        Pay_amt_check = df[((df['Payment_Amount'].isnull()) | (df['Payment_Amount'] == '')) & ((df['Payment_Amount_USD'].isnull()) | (df['Payment_Amount_USD'] == ''))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Both Payment Amount and Payment Amount USD are blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0034"
                        validation_field = "Payment_Amount"
                        current_value = df.loc[Pay_amt_check.index.tolist()]['Payment_Amount'].tolist()
                        index_list = Pay_amt_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Payment_Id = df.loc[i,'Payment_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Pay_amt_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Pay_amt_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
                    if ('CRS_IP.csv' == filename):
                    
                        name = filename
    
                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Involved Party Id Blank
                        blank_invlo_party_id_ip_acc =df[(df['Involved_Party_Id'].isnull()) | (df['Involved_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Involved Party Id is Blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0023"
                        validation_field = "Involved_Party_Id"
                        current_value = df.loc[blank_invlo_party_id_ip_acc.index.tolist()]['Involved_Party_Id'].tolist()
                        index_list = blank_invlo_party_id_ip_acc.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_invlo_party_id_ip_acc.index.tolist()
                        Record_Log_Info = CRS_record_log(blank_invlo_party_id_ip_acc,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    

                        #Sc Status Check
                        CRS_SC_Status = ['000001','000002','000003','000004','000005','000006','000007','000008']
                        #df['CRS_SC_Status_']= df['CRS_SC_Status_'].fillna(0)
                        #df['CRS_SC_Status_']= df['CRS_SC_Status_'].astype(int).astype(str)
                        df['CRS_SC_Status_']= df['CRS_SC_Status_'].astype(str)
                        CRS_SC_Status_check = df[(df['CRS_SC_Status_']=='000000')|(df['CRS_SC_Status_'].isnull())|(df['CRS_SC_Status_']=='')|(~df['CRS_SC_Status_'].isin(CRS_SC_Status))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'CRS SC Status either blank or value other than expected'
                        label = 'EXCEPTION'
                        error_id = "CRS_E_0022"
                        validation_field = "CRS_SC_Status_"
                        current_value = df.loc[CRS_SC_Status_check.index.tolist()]['CRS_SC_Status_'].tolist()
                        index_list = CRS_SC_Status_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        Record_Log_Info = CRS_record_log(CRS_SC_Status_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Involved Party Type

                        Inv_party_type = ['I','E']
                        inv_party_type_check = df[(df['Involved_Party_Type'].isnull()) | (df['Involved_Party_Type'] == '') | (~(df['Involved_Party_Type'].isin(Inv_party_type)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Involved Party Type either blank or value other than expected(I,E)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0024"
                        validation_field = "Involved_Party_Type"
                        current_value = df.loc[inv_party_type_check.index.tolist()]['Involved_Party_Type'].tolist()
                        index_list = inv_party_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + inv_party_type_check.index.tolist()
                        Record_Log_Info = CRS_record_log(inv_party_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Individual DOB Check

                        
                        ip_df = df
                                                                                                                       
                        ip_df['Individual_DOB'] =  pd.to_datetime(ip_df['Individual_DOB'], format='%Y-%m-%d', errors='coerce').fillna("")
                        invaid_date_dob = ip_df[ip_df['Individual_DOB'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Individual DOB is format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0025"
                        validation_field = "Individual_DOB"
                        current_value = df.loc[invaid_date_dob.index.tolist()]['Individual_DOB'].tolist()
                        index_list = invaid_date_dob.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_date_dob.index.tolist()
    
                        Record_Log_Info = CRS_record_log(invaid_date_dob,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value,primary_key)
                        idx= list(set(idx))
                        df = df.drop(idx)
    
                    if ('CRS_IP_ADDN_QA.csv' == filename):

                        name = filename

                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        #Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
                        
                        
                        #Involved Party Id Blank
                        blank_invlo_party_id_ip_acc =df[(df['Involved_Party_Id'].isnull()) | (df['Involved_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        
                        message = 'Involved Party Id is Blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0023"
                        validation_field = "Involved_Party_Id"
                        current_value = df.loc[blank_invlo_party_id_ip_acc.index.tolist()]['Involved_Party_Id'].tolist()
                        index_list = blank_invlo_party_id_ip_acc.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_invlo_party_id_ip_acc.index.tolist()
                        Record_Log_Info = CRS_record_log(blank_invlo_party_id_ip_acc,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        
                        #Involved Party Type

                        Inv_party_type = ['I','E']
                        inv_party_type_check = df[(df['Involved_Party_Type'].isnull()) | (df['Involved_Party_Type'] == '') | (~(df['Involved_Party_Type'].isin(Inv_party_type)))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        
                        message = 'Involved Party Type either blank or value other than expected(I,E)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0024"
                        validation_field = "Involved_Party_Type"
                        current_value = df.loc[inv_party_type_check.index.tolist()]['Involved_Party_Type'].tolist()
                        index_list = inv_party_type_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + inv_party_type_check.index.tolist()
                        Record_Log_Info = CRS_record_log(inv_party_type_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        '''
                        #Birth Country Code
                        BCcode_check = df[(df['Birth Country Code'].isnull()) | (df['Birth Country Code'] == '') | (df['Birth Country Code']) >= 2][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                       
                        message = 'Birth Country Code must be a valid 2 char ISO Country Code'
                        label = 'EXCEPTION'
                        error_id = ""
                        validation_field = "Birth Country Code"
                        current_value = df.loc[BCcode_check.index.tolist()]['Birth Country Code'].tolist()
                        index_list = BCcode_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + BCcode_check.index.tolist()
                        Record_Log_Info = CRS_record_log(BCcode_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        #Birth Former Country Name
                        BCName_check = df[(df['Birth Former Country Name'].isnull()) | (df['Birth Former Country Name'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        
                        message = 'Birth Former Country Name must be Birth Former jurisdiction Name'
                        label = 'EXCEPTION'
                        error_id = ""
                        validation_field = "Birth Former Country Name"
                        current_value = df.loc[BCName_check.index.tolist()]['Birth Former Country Name'].tolist()
                        index_list = BCName_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + BCName_check.index.tolist()
                        Record_Log_Info = CRS_record_log(BCName_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        '''
                        idx= list(set(idx))
                        
                        df = df.drop(idx)
    
    
                    if ('CRS_IP_ACCT.csv' == filename):
                    
                        name = filename
    
                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
                        #Account Id check
                        Acc_id_check = df[(df['Account_Id'].isnull()) | (df['Account_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Id is blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0026"
                        validation_field = "Account_Id"
                        current_value = df.loc[Acc_id_check.index.tolist()]['Account_Id'].tolist()
                        index_list = Acc_id_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Acc_id_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Acc_id_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
    
                        #Account Relationship Type Check
                        Acc_rel = ['A','J']
                        Acc_rel_type = df[(df['Account_Relationship_Type'].isnull()) | (df['Account_Relationship_Type'] == '') | (~df['Account_Relationship_Type'].isin(Acc_rel))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Account Relationship Type is either blank or value other than expected(A,J)'
                        idx = idx + Acc_rel_type.index.tolist()
                        label = 'REJECTION'
                        error_id = "CRS_R_0027"
                        validation_field = "Account_Relationship_Type"
                        current_value = df.loc[Acc_rel_type.index.tolist()]['Account_Relationship_Type'].tolist()
                        index_list = Acc_rel_type.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        Record_Log_Info = CRS_record_log(Acc_rel_type,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)


                    if ('CRS_IP_TD.csv' == filename):
                    
                        name = filename
    
                        #df.rename(columns={'Effective_from_date': 'Effective_From_Date', 'Effective_to_date': 'Effective_To_Date'}, inplace=True)
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
                        #Involved Party Id Blank
                        blank_invlo_party_id_ip_acc =df[(df['Involved_Party_Id'].isnull()) | (df['Involved_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Involved Party Id is Blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0023"
                        validation_field = "Involved_Party_Id"
                        current_value = df.loc[blank_invlo_party_id_ip_acc.index.tolist()]['Involved_Party_Id'].tolist()
                        index_list = blank_invlo_party_id_ip_acc.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Involved_Party_Id = df.loc[i,'Involved_Party_Id']
                            com_key = Institution_Id + ';'+ Involved_Party_Id + ';' + " " + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
                        
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_invlo_party_id_ip_acc.index.tolist()
                        Record_Log_Info = CRS_record_log(blank_invlo_party_id_ip_acc,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
    
    
                    if ('CRS_IP_IP.csv' == filename):
                    
    
                        name = filename
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)
                        Record_Log_Info,df,idx = date_validation(df,Record_Log_Info,name,idx,ETL_ProcessId,ETL_Start_DateTime)
    
    
    
                        #Parent Id Check
                        blank_parent_id = df[(df['Parent_Party_Id'].isnull()) | (df['Parent_Party_Id'] == '')][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Parent Party Id is blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0028"
                        validation_field = "Parent_Party_Id"
                        current_value = df.loc[blank_parent_id.index.tolist()]['Parent_Party_Id'].tolist()
                        index_list = blank_parent_id.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Child_Party_Id = df.loc[i,'Child_Party_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + " " + ';' + " " + ';' + " "+ ';' + Child_Party_Id
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + blank_parent_id.index.tolist()
                        Record_Log_Info = CRS_record_log(blank_parent_id,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)
    
                    if ('CRS_ACCT_BAL.csv' == filename):
                    
                        name = filename
                        Record_Log_Info,df,idx = file_validation(df,Record_Log_Info,name,ETL_ProcessId,ETL_Start_DateTime)

                        #Invalid balance currency code
                        cur_code_check = df[(df['Balance_Currency_Code'].isnull()) | (~df['Balance_Currency_Code'].isin(cur_list))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in currency code with FX Rate file'
                        label = 'REJECTION'
                        error_id = "CRS_R_0036"
                        validation_field = "Balance_Currency_Code"
                        current_value = df.loc[cur_code_check.index.tolist()]['Balance_Currency_Code'].tolist()
                        index_list = cur_code_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + cur_code_check.index.tolist()
                        Record_Log_Info = CRS_record_log(cur_code_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)

                        
                        #Invalid Balance end date range
                        index_list=list()
                        df_bkp = df
                        df['Reporting_EndDate']=df['Balance_End_Date'].astype(str)
                        df['Reporting_EndDate'] = df['Reporting_EndDate'].str[0:4]+"-12-31"
                        #df['Reporting_EndDate'] = pd.to_datetime(df['Balance_End_Date']).dt.year.astype(str)+"-12-31"
                        df_acct_bal = df
                        df=df[['Balance_Currency_Code','Reporting_EndDate']].drop_duplicates().reset_index(drop=True)
                        
                        #exchange_df = pd.read_excel('USDMS.xls',engine='xlrd')
                        #exchange_df=exchange_df.replace('\x01',"", regex=True)
                        
                        forex_df_columns = list(df.columns)
                        forex_df_vect = df.values
                        
                        unavail_codes = list()
                        for row in forex_df_vect:
                            exchange_df_new = exchange_df.loc[(exchange_df['Category Key'] == row[forex_df_columns.index('Balance_Currency_Code')]) & (exchange_df['Start Date'] <= row[forex_df_columns.index('Reporting_EndDate')])]
														
																											   
                            exchange_df_new2 = exchange_df_new.loc[exchange_df_new['End Date'] >= row[forex_df_columns.index('Reporting_EndDate')]]
																						  
                            if len(exchange_df_new2) == 0:
                                unavail_codes.append(row)
                        
                        if len(unavail_codes)>0:
                            for i in unavail_codes:
                                index_list.append(list(df_acct_bal[(df_acct_bal['Balance_Currency_Code']==i[0]) &(df_acct_bal['Reporting_EndDate']==i[1])].index.values))
                            
                            index_list = sum(index_list,[])
                        
                        df = df_bkp 
                        balance_end_date_check = df[df.index.isin(index_list)][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Mismatch in year in FX Rate file'
                        label = 'REJECTION'
                        error_id = "CRS_R_0038"
                        validation_field = "Reporting Year End Date"
                        current_value = df.loc[index_list]['Balance_End_Date'].tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)

                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + index_list
                        Record_Log_Info = CRS_record_log(balance_end_date_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
            
                 
                        #Balance End Date Check
                        acct_bal_df = df
                        acct_bal_df['Balance_End_Date'] = acct_bal_df['Balance_End_Date'].apply(lambda x : 'valid' if x=='' else pd.to_datetime(x, format='%Y-%m-%d', errors='coerce'))
                        invaid_balance_end_date = acct_bal_df[acct_bal_df['Balance_End_Date'].isnull()][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Balance End Date format is invalid(YYYY-MM-DD)'
                        label = 'REJECTION'
                        error_id = "CRS_R_0004"
                        validation_field = "Balance_End_Date"
                        current_value = df.loc[invaid_balance_end_date.index.tolist()]['Balance_End_Date'].tolist()
                        index_list = invaid_balance_end_date.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + invaid_balance_end_date.index.tolist()
    
                        Record_Log_Info = CRS_record_log(invaid_balance_end_date,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
                        
                        # Balance amount and Balance Amount US Blank check
                        
                        df['Balance_Amount'] = df['Balance_Amount'].apply(lambda x : '' if x.count(' ') == 17 else x)
                        df['USD_Balance_Amount'] = df['USD_Balance_Amount'].apply(lambda x : '' if x.count(' ') == 14 else x)

                        Bal_amt_check = df[((df['Balance_Amount'].isnull()) | (df['Balance_Amount'] == '')) & ((df['USD_Balance_Amount'].isnull()) | (df['USD_Balance_Amount'] == ''))][['SourceFileName','Source File Index','Source System ID','CTCR Process ID','CSV File Index','Key','Stage1_id']]
                        message = 'Both Balance Amount and USD Balance Amount are blank'
                        label = 'REJECTION'
                        error_id = "CRS_R_0040"
                        validation_field = "Balance_Amount"
                        current_value = df.loc[Bal_amt_check.index.tolist()]['Balance_Amount'].tolist()
                        index_list = Bal_amt_check.index.tolist()
                        primary_key = []
                        for i in index_list:
                            Institution_Id = df.loc[i,'Institution_Id']
                            Account_Id = df.loc[i,'Account_Id']
                            com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + " " + ';' + " "+ ';' + " "
                            primary_key.append(com_key)
						
                        primary_key = str(primary_key)
                        current_value = str(current_value)
                        idx = idx + Bal_amt_check.index.tolist()
                        Record_Log_Info = CRS_record_log(Bal_amt_check,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
    
                        idx= list(set(idx))
                        df = df.drop(idx)

                    df = df.reset_index(drop=True)
                Record_Log_Info = Record_Log_Info.rename(columns = {'Source System ID':'Source System','SourceFileName':'Source_Filename'})    
                cols = ['ETL_ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime',  'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key', 'CSV File Index']
                Record_Log_Info = Record_Log_Info[cols]

                yield df
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_LOG,Record_Log_Info)
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                              
                logger.log_struct({'severity':'Critical','message':'CRS Flag is 0',"Class name": 'CRS_Record_level_validation'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Record_level_validation', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
    
	
# CRS and FATCA Payment date and Account Date validation

class CRS_Date_validation_pay_acct(beam.DoFn):
    def process(self, element, join_with,key,filename,ETL_ProcessId,ETL_Start_DateTime):
        try:
            idx = list()
            ETL_ProcessId = ETL_ProcessId
            ETL_Start_DateTime = ETL_Start_DateTime
			
            Record_Log_Info = pd.DataFrame(columns = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID', 'Message Label', 'Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id', 'ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key'])
            
            
            df_list = [element, join_with]
            result = reduce(lambda  left,right: pd.merge(left,right, on=key,how='inner'), df_list)
            
            #result = pd.merge(element, join_with, how='inner', on=key)
            result = result[result['Account_Status']=='C']

            if len(result) > 0:
                except_date = result[result['Payment_Date'] > result['Account_close_date']][['SourceFileName_y','Source File Index_y','Source System ID_y','CTCR Process ID_y','CSV File Index_y','Key_y','Stage1_id_y']]
                except_date = except_date.rename(columns = {'SourceFileName_y':'SourceFileName','Source File Index_y':'Source File Index','Source System ID_y':'Source System ID','CTCR Process ID_y':'CTCR Process ID','CSV File Index_y':'CSV File Index','Key_y':'Key','Stage1_id_y':'Stage1_id'})
                idx = idx + except_date.index.tolist()
                message = 'Payment Date is post Account Closure date'
                label = 'EXCEPTION'
                error_id = "CRS_E_0030"
                validation_field = "Payment_Date"
                current_value = result.loc[except_date.index.tolist()]['Payment_Date'].tolist()
                index_list = except_date.index.tolist()
                primary_key = []
                for i in index_list:
                    Institution_Id = result.loc[i,'Institution_Id']
                    Account_Id = result.loc[i,'Account_Id']
                    Payment_Id = result.loc[i,'Payment_Id']
                    com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                    primary_key.append(com_key)
				
                primary_key = str(primary_key)
                current_value = str(current_value)
                Record_Log_Info = CRS_record_log(except_date,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
            Record_Log_Info = Record_Log_Info.rename(columns = {'Source System ID':'Source System','SourceFileName':'Source_Filename'})     
            cols = ['ETL_ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime',  'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key', 'CSV File Index']
            Record_Log_Info = Record_Log_Info[cols]
            yield Record_Log_Info 
        except Exception as e:
            logger_client = logging.Client()
            log_name = filename.split('_')
            logger = logger_client.logger("Eyfirst_Stack_logs")
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Date_validation_pay_acct', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))           

class FATCA_Date_validation_pay_acct(beam.DoFn):
    def process(self, element, join_with,key,filename,ETL_ProcessId,ETL_Start_DateTime):
        try:
            idx = list()
            ETL_ProcessId = ETL_ProcessId
            ETL_Start_DateTime = ETL_Start_DateTime
            Record_Log_Info = pd.DataFrame(columns = ['Date','Time','SourceFileName','Source System ID','CTCR Process ID', 'Message Label', 'Message','Source File Index','CSV File Name','CSV File Index','Key','Stage1_id','Stage2_id', 'ETL_ProcessId', 'ETL_Start_DateTime', 'Regime', 'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key'])
            
            df_list = [element, join_with]
            result = reduce(lambda  left,right: pd.merge(left,right,on=key,how='inner'), df_list)
            #result = pd.merge(element, join_with, how='inner', on=key)
            result = result[result['Account_Status']=='C']
            if len(result) > 0:
                except_date = result[result['Payment_Date'] > result['Account_close_date']][['SourceFileName_y','Source File Index_y','Source System ID_y','CTCR Process ID_y','CSV File Index_y','Key_y','Stage1_id_y']]
                except_date = except_date.rename(columns = {'SourceFileName_y':'SourceFileName','Source File Index_y':'Source File Index','Source System ID_y':'Source System ID','CTCR Process ID_y':'CTCR Process ID','CSV File Index_y':'CSV File Index','Key_y':'Key','Stage1_id_y':'Stage1_id'})
                idx = idx + except_date.index.tolist()
                message = 'Payment Date is post Account Closure date'
                label = 'EXCEPTION'
                error_id = "FTR_E_0029"
                validation_field = "Payment_Date"
                current_value = result.loc[except_date.index.tolist()]['Payment_Date'].tolist()
                index_list = except_date.index.tolist()
                primary_key = []
                for i in index_list:
                    Institution_Id = result.loc[i,'Institution_Id']
                    Account_Id = result.loc[i,'Account_Id']
                    Payment_Id = result.loc[i,'Payment_Id']
                    com_key = Institution_Id + ';'+ " " + ';' + Account_Id + ';' + Payment_Id + ';' + " "+ ';' + " "
                    primary_key.append(com_key)
                primary_key = str(primary_key)
                current_value = str(current_value)
                Record_Log_Info = FATCA_record_log(except_date,Record_Log_Info,message,label,filename,idx,ETL_ProcessId,ETL_Start_DateTime,error_id, validation_field, current_value, primary_key)
            Record_Log_Info = Record_Log_Info.rename(columns = {'Source System ID':'Source System','SourceFileName':'Source_Filename'})        
            cols = ['ETL_ProcessId', 'CTCR Process ID', 'Regime', 'Source System', 'Source_Filename', 'ETL_Start_DateTime',  'ETL_End_DateTime', 'Error_ID', 'Validation field', 'Current Value', 'Primary Key', 'CSV File Index']
            Record_Log_Info = Record_Log_Info[cols]
            yield Record_Log_Info 
        except Exception as e:
            logger_client = logging.Client()
            log_name = filename.split('_')
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Date_validation_pay_acct', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger('Eyfirst_Stack_logs')
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))     

			
#----------------------------------------FATCA APPEND LOGS TO CSV-------------------------------------------------------

class FATCA_Append_logs_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path
	
    def process(self,element,e2,e3,e4,e5,e6,e7,e8):
        
        try:
            #storage_client = storage.Client()
            #bucket_name = self.input_path
            #bucket = storage_client.get_bucket(bucket_name)
            #log_prefix = log_prefix
            #destination_blob_name = 'RECORD_LEVEL_LOG'+ '.csv'
            #blob = bucket.blob(log_prefix + destination_blob_name)
            df1 = pd.DataFrame()
            #crs_log = e8
            #prev_log = e9
            
            #df1 = df1.append(pd.DataFrame(prev_log))
            #df1 = df1.append(pd.DataFrame(crs_log))
            #u_id = str(uid)
            df1 = df1.append(pd.DataFrame(element))
            df1 = df1.append(pd.DataFrame(e2))
            df1 = df1.append(pd.DataFrame(e3))
            df1 = df1.append(pd.DataFrame(e4))
            df1 = df1.append(pd.DataFrame(e5))
            df1 = df1.append(pd.DataFrame(e6))
            df1 = df1.append(pd.DataFrame(e7))
            df1 = df1.append(pd.DataFrame(e8))
            df1 = df1.reindex(df1.columns, axis=1)
            #df1['Stage2_id'] = u_id
            #df1.to_csv(destination_blob_name, index = False)	
            #blob.upload_from_filename(destination_blob_name)
            yield df1
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Append_logs_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#-------------------------------CRS APPEND LOG TO CSV-------------------------------------------------------

#Appending record level logs to a single csv aand writing back in storage
class CRS_Append_logs_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path
	
    def process(self,element,e2,e3,e4,e5,e6,e7,e8,e9):
        
        try:
            #storage_client = storage.Client()
            #bucket_name = self.input_path
            #bucket = storage_client.get_bucket(bucket_name)
            #destination_blob_name = 'CRS_RECORDLEVEL_LOG_'+str(date.today())+'.csv'
            #blob = bucket.blob(log_prefix + destination_blob_name)
            df1 = pd.DataFrame()
            #u_id = str(uid)
            df1 = df1.append(pd.DataFrame(element))
            df1 = df1.append(pd.DataFrame(e2))
            df1 = df1.append(pd.DataFrame(e3))
            df1 = df1.append(pd.DataFrame(e4))
            df1 = df1.append(pd.DataFrame(e5))
            df1 = df1.append(pd.DataFrame(e6))
            df1 = df1.append(pd.DataFrame(e7))
            df1 = df1.append(pd.DataFrame(e8))
            df1 = df1.append(pd.DataFrame(e9))
            df1 = df1.reindex(df1.columns, axis=1)
            #df1['Stage2_id'] = u_id
            #df1.to_csv(destination_blob_name, index = False)	
            #blob.upload_from_filename(destination_blob_name)
            yield df1
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Append_logs_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
			
#--------------------------------APPEND DF TO CSV AND API CALL-----------------------------------------------
   
class CRS_Append_df_to_zip(beam.DoFn):

    def __init__(self, input_path,business_secrets,landing):
        self.input_path = input_path
        self.business_secrets = business_secrets
        self.landing = landing

    def process(self,ControlPerson,AccountHolder,FI_Final,Header_Mapping,unique_id,etl_start_datetime,Thresholdvalue,elog):
        
        try:
            delimiter = '/'
            storage_client = storage.Client()
            '''
            bucket_cert = storage_client.bucket(self.business_secrets)
            file = bucket_cert.blob("DataFlow.cer")
            cert_file = file.download_to_filename('DataFlow.cer')
            '''
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)

            secrets_bucket_name = self.business_secrets
            secrets_bucket = storage_client.get_bucket(secrets_bucket_name)
            api_key_blobs = storage_client.list_blobs(secrets_bucket_name,prefix='', delimiter=delimiter)

            Api_Log_bucket_name = self.landing
            Api_log_bucket = storage_client.get_bucket(Api_Log_bucket_name)
            blobs_api_log = storage_client.list_blobs(Api_log_bucket,prefix=log_prefix+Api_folder, delimiter=delimiter)

            data_key =''
            for blob in api_key_blobs:
                if(blob.name == 'secrets_business.json'):
                    data_key = json.loads(blob.download_as_bytes(client=None))

            api_log_columns = ['ETL_Start_DateTime','CTCR Process ID','ETL_Process ID','Regime','Reporting Year','Institution ID','Zip File Name','Request Type','Status Code','EYFIRST_Import Id']
            Api_log = pd.DataFrame(columns = api_log_columns)

            df1 = ControlPerson.reset_index(drop=True)
            df2 = AccountHolder.reset_index(drop=True)
            df3 = FI_Final.reset_index(drop=True)
            df4 = Header_Mapping.reset_index(drop=True)
            df5 = pd.DataFrame()
            columns_add = ['Pooled_ReportingYear','Pooled_FFI_Name','Pooled_Reporting_Type','Pooled_Number_of_Accounts','Pooled_Aggregate_Account_Balance','Pooled_Currency_Code']

            for newcol in columns_add:
                    df5[newcol]= None

            df2['Accounts_TIN_Type']  = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TIN_Type']))
            df2['Accounts_TIN_Type2'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TIN_Type2']))
            df2['Accounts_TINType3'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType3']))
            df2['Accounts_TINType4'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType4']))
            df2['Accounts_TINType5'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType5']))
            df2['Accounts_TINType6'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType6']))
            df2['Accounts_TINType7'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType7']))
            df2['Accounts_TINType8'] = (np.select(condlist=[df2['Accounts_Category_of_Account_Holder'] == 'Individual'],choicelist=[''],default=df2['Accounts_TINType8']))
            
            InstitutionID_df = df3[['Service Institution ID','Abbrev. (Insitution ID)                 (8 char limit)']]
            InstitutionID_df = InstitutionID_df.drop_duplicates('Abbrev. (Insitution ID)                 (8 char limit)')

            InstitutionID_df['Service Institution ID'] = InstitutionID_df['Service Institution ID'].fillna('UNIQ ID').astype(str)
            InstitutionID_df.loc[InstitutionID_df['Service Institution ID'].isin(['UNIQ ID','']),'Service Institution ID'] = InstitutionID_df['Abbrev. (Insitution ID)                 (8 char limit)']
            Final_group_InsID = InstitutionID_df.groupby('Service Institution ID')

            df3 = df3.drop('Service Institution ID', axis= 1)
            
            if len(InstitutionID_df)<=0:

                                             
                logger_client = logging.Client() 
                logger = logger_client.logger("Eyfirst_Stack_logs")
                logger.log_struct({'severity':'Critical','message':'Either BU Name from the resultant dataset is not present in the CI File or No result created for input dataset'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 

                                                                     
            elogfinal = pd.DataFrame(columns = ['Filename','ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id','Customer_Id','Error_ID'])
            exceptions = pd.DataFrame(columns = ['Filename','Regime','CTCR_Process_ID','GDF_ProcessId','Reporting_CI','Account_Id','Customer_Id','Error_ID'])
            Regime = 'CRS'



                                                       
                                                  

            for filename, filegroup in Final_group_InsID:
                BU = filename
                BU_list = list(filegroup['Abbrev. (Insitution ID)                 (8 char limit)'])
                BU_list_join = ','.join(BU_list)
                
                df6 = df1.loc[df1['FI_ID'].isin(BU_list)].reset_index(drop=True)
                df7 = df2.loc[df2['FI_ID'].isin(BU_list)].reset_index(drop=True)
                df8 = df3.loc[df3['Abbrev. (Insitution ID)                 (8 char limit)'].isin(BU_list)].reset_index(drop=True)
				
                df9 = df4.loc[df4['Abbrev.(InsitutionID)(8charlimit)'].isin(BU_list)]
                df9 = df9[df9['CountryofTaxResidence'].isin(["LU","CA"])].reset_index(drop=True)
                df9 = df9.drop(['Abbrev.(InsitutionID)(8charlimit)', 'CountryofTaxResidence'], 1)
																		

                if len(df8) > 0:
                    COTR = df8['FFI_ResCountryCode'][0]

                    #if COTR == 'GB':
                        #df6['SubsOwner_ReportingType'] = 'Combined'               
                                                       

                                    
                                                                   
    
                #Get CTCR processID value here
                if len(df7) > 0:
                    ctcrid = df7['CTCR Process ID_x'].unique()[0]
                else:
                    ctcrid = ''
                df6 = df6.drop('FI_ID', 1)
                df7 = df7.drop('FI_ID', 1)
                df8 = df8.drop('Abbrev. (Insitution ID)                 (8 char limit)', 1)

                #Remove CTCR ProcessID column
                df7 = df7.drop('CTCR Process ID_x', 1)
                             
                #Remove FFI data with Reporting year blank
                
                repyearcheck = df8[(df8['FFI_ReportingYear'] == '') | (df8['FFI_ReportingYear'].isnull())]
                idx = repyearcheck.index.tolist()
                idx = list(set(idx))
                df8 = df8.drop(idx)   

                zip_file_list = []
                ### Volumn split

                if len(df7) > Thresholdvalue:

                    AH_df_ind = df7[df7['Accounts_Category_of_Account_Holder'] == 'Individual'].sort_values(['Accounts_Customer_ID','Accounts_Account_Number']).reset_index(drop=True)
                    AH_df_org = df7[df7['Accounts_Category_of_Account_Holder'] == 'Organisation'].sort_values(['Accounts_Customer_ID','Accounts_Account_Number']).reset_index(drop=True)

                    # Category of AH is 'Individual'
                    Ind_counter = 0
                    r_keys=['Accounts_ReportingYear','Accounts_Account_Number','Accounts_Entity_Name','Accounts_FFI_Name'] 
                    l_keys=['SubsOwner_ReportingYear','SubsOwner_Account_Number','SubsOwner_Account_Holder_Entity_Name','SubsOwner_FFI_Name']

                    # cp_df = pd.DataFrame(columns=list(df6.columns))
                    split_count = 1
                    while len(AH_df_ind) > Thresholdvalue:
                        last = AH_df_ind['Accounts_Customer_ID'][Ind_counter+(Thresholdvalue - 1)]
                        idx = AH_df_ind['Accounts_Customer_ID'].where(AH_df_ind['Accounts_Customer_ID']==last).last_valid_index()
                        AH_split_df = AH_df_ind.loc[Ind_counter:idx]

                        
                        df_list = [df6,AH_split_df]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                        #'*********************************************'
                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_split_df).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df5).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')

                        now = datetime.now()
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)
                            
                        #Exception Log:
                        AH_split_data = AH_split_df[['Accounts_Account_Number','Accounts_Customer_ID']]

                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]

       
                        elogfinal = elogfinal.append(log)   

                                                     
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(CRS_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)

                        zip_file_list.append((destination_blob_name,zip_file))
                        #'*********************************************'
                        idx= idx+1
                        AH_df_ind = AH_df_ind.loc[idx:]
                        Ind_counter  = idx
                        split_count += 1

                    else:
                        if len(AH_df_ind) > 0 :
                            df_list = [df6,AH_df_ind]
                            CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                            pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(AH_df_ind).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df5).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')

                            now = datetime.now()
                            dt_day = now.strftime("%d")
                            dt_month = now.strftime("%m")
                            dt_year = now.strftime("%Y")
                            dt_hour = now.strftime("%H")
                            dt_min = now.strftime("%M")
                            # dt_sec = now.strftime("%S")
                            no_split = str(split_count).zfill(2)

                        
                            compression = zipfile.ZIP_DEFLATED
                            destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                            zip_file = destination_blob_name
                            with ZipFile(destination_blob_name, 'w') as zip_obj:
                                zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                                zip_obj.write("Account_Holder.csv",compress_type=compression)
                                zip_obj.write("FFI.csv",compress_type=compression)
                                zip_obj.write("Header.csv",compress_type=compression)
                                zip_obj.write("Pooled_Report.csv",compress_type=compression)
                                

                            #Exception Log:
                            AH_split_data = AH_df_ind[['Accounts_Account_Number','Accounts_Customer_ID']]
                                        
                                                                                                                                                                                          
                                        
                                                                   
                                                                                  
                                                            
                                                               
                            df_list = [elog, AH_split_data]
                            log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)





                                                                       
                            log['Filename'] = destination_blob_name
                            log = log[log['Institution_Id'].isin(BU_list)]

                                                                 

                                                            
                            elogfinal = elogfinal.append(log)
                            
                            
                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(CRS_api_output_prefix + destination_blob_name)
                            blob.upload_from_filename(zip_file)

                            zip_file_list.append((destination_blob_name,zip_file))
                            split_count += 1

                    Org_counter = 0

                    while len(AH_df_org) > Thresholdvalue:
                        last = AH_df_org['Accounts_Customer_ID'][Org_counter+(Thresholdvalue -1)]
                        idx = AH_df_org['Accounts_Customer_ID'].where(AH_df_org['Accounts_Customer_ID']==last).last_valid_index()

                        AH_split_df = AH_df_org.loc[Org_counter:idx]

                        df_list = [df6,AH_split_df]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)
                        #'********************************************************************************'
                        
                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_split_df).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df5).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')

                        now = datetime.now()
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)
                            

                        #Exception Log:
                        AH_split_data = AH_split_df[['Accounts_Account_Number','Accounts_Customer_ID']]
                                       
                                                                                                                                                                                      
                                       
          
                                                                                 
                                                                                  
                                                            
                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                                                                                                                                                                                                                                                                                                                                          

                                                                                    
                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]

                                                             

                                                                                    
                        elogfinal = elogfinal.append(log)
   
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(CRS_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)

                        zip_file_list.append((destination_blob_name,zip_file))
                        #'********************************************************************************'
                        idx= idx+1
                        AH_df_org = AH_df_org.loc[idx:]
                        Org_counter  = idx
                        split_count += 1

                                        

                    else:
                        if len(AH_df_org) > 0 :

                            df_list = [df6,AH_df_org]
                            CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                            pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(AH_df_org).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                            pd.DataFrame(df5).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')

                            now = datetime.now()
                            dt_day = now.strftime("%d")
                            dt_month = now.strftime("%m")
                            dt_year = now.strftime("%Y")
                            dt_hour = now.strftime("%H")
                            dt_min = now.strftime("%M")
                            # dt_sec = now.strftime("%S")
                            no_split = str(split_count).zfill(2)

                            
                            compression = zipfile.ZIP_DEFLATED
                            destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                            zip_file = destination_blob_name
                            with ZipFile(destination_blob_name, 'w') as zip_obj:
                                zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                                zip_obj.write("Account_Holder.csv",compress_type=compression)
                                zip_obj.write("FFI.csv",compress_type=compression)
                                zip_obj.write("Header.csv",compress_type=compression)
                                zip_obj.write("Pooled_Report.csv",compress_type=compression)
                                


                            #Exception Log:
                            AH_split_data = AH_df_org[['Accounts_Account_Number','Accounts_Customer_ID']]
                                            
                                                                                                                                                                                          
                                            
                                                                   
                                                                                  
                                                            
                                                               
                            df_list = [elog, AH_split_data]
                            log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)
                                    

                                    


                            log['Filename'] = destination_blob_name
                            log = log[log['Institution_Id'].isin(BU_list)]

                            elogfinal = elogfinal.append(log)
                            
                            
                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(CRS_api_output_prefix + destination_blob_name)
                            blob.upload_from_filename(zip_file)

                            zip_file_list.append((destination_blob_name,zip_file))
                            split_count += 1

                else:
                                                 
                    pd.DataFrame(df6).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df7).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df5).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')

                    now = datetime.now()
                    dt_day = now.strftime("%d")
                    dt_month = now.strftime("%m")
                    dt_year = now.strftime("%Y")
                    dt_hour = now.strftime("%H")
                    dt_min = now.strftime("%M")
                    dt_sec = now.strftime("%S")

                    
                    compression = zipfile.ZIP_DEFLATED
                    destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+dt_sec+'.zip'
                    zip_file = destination_blob_name
                    with ZipFile(destination_blob_name, 'w') as zip_obj:
                        zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                        zip_obj.write("Account_Holder.csv",compress_type=compression)
                        zip_obj.write("FFI.csv",compress_type=compression)
                        zip_obj.write("Header.csv",compress_type=compression)
                        zip_obj.write("Pooled_Report.csv",compress_type=compression)
                        
                    bucket_name = self.input_path
                    bucket = storage_client.get_bucket(bucket_name)
                    blob = bucket.blob(CRS_api_output_prefix + destination_blob_name)
                    blob.upload_from_filename(zip_file)

                    zip_file_list.append((destination_blob_name,zip_file))
                 
                if len(df8) > 0:
                    ReportingYear = df8['FFI_ReportingYear'].unique()[0]
                else:
                    ReportingYear = ''
							  


                
                import_headers = {"X-ApiKey": data_key['EyFirst_API_Key'], "Accept": "application/json"}
                url_link = data_key["EyFirst_API_GETClients"]
                import_request = requests.get(url_link,headers = import_headers)#,verify="DataFlow.cer")
                client_content = json.loads(import_request.content)

                client_content1 = dict((k.title(), v) for k, v in client_content.items())
                client_content2 = {'Clients':[]}

                for i in client_content1['Clients']:
                    client_content2['Clients'].append(dict((k.title(), v) for k, v in i.items()))

                bu_id = list()
                client_bu = list()

                for c in client_content2['Clients']:
                    bu_id.append(c['Id'])
                    client_bu.append(c['Name'])

                client_details = pd.DataFrame([bu_id,client_bu]).T
                client_details =client_details.rename(columns = {0 :'Id', 1:'Name'})
                client_details.reset_index(drop=True, inplace=True)
                
                for zip_data in zip_file_list:
                    destination_blob_name = zip_data[0]
                    zip_file = zip_data[1]

                    try:
                        client_id = client_details[client_details['Name']==BU]['Id'].values[0]

                        file_z = open(destination_blob_name,'rb')
                        
                                                                 
                        #files = {"X-File-Name": file_z}
                        files = {"file": (zip_file, file_z, 'application/zip')}
                        headers = {"X-ApiKey": data_key["EyFirst_API_Key"], "Accept": "application/json", "type":"formData"}
								
                        # r = requests.post(data_key["EyFirst_API_Import_CSV_Template"]+str(client_id), headers=headers, files=files)#,verify="DataFlow.cer")
                        #RETRY (max 3 times)
                        counter = 0
                        status = 0

                                  
                        while (status<200 or status>=300) and counter<3:
                            r = requests.post(data_key["EyFirst_API_Import_CSV_Template"]+str(client_id), headers=headers, files=files)#,verify="DataFlow.cer")
                            status = r.status_code
                            counter = counter + 1					

                        if r.status_code >=200 and r.status_code < 300:

                            # import_id = json.loads(r.content)["ImportId"]
                            import_id1 = json.loads(r.content)
                            import_id = dict((k.title(), v) for k, v in import_id1.items())["Importid"]
                            Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'Import Master Data Csv Template','Status Code':r.status_code,'EYFIRST_Import Id':import_id},ignore_index=True)
                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(api_eyfirst_processed+ destination_blob_name)
                            blob.upload_from_filename(zip_file)

                        else:
                            Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'Import Master Data Csv Template','Status Code':r.status_code,'EYFIRST_Import Id':json.loads(r.content)},ignore_index=True)                                                                                                                                                                                                                                                                                                                    
                                                                                                                                                                                                                                                                                                                                                
                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(api_eyfirst_failed+ destination_blob_name)
                            blob.upload_from_filename(zip_file)

                        #result = r.status_code 
                    except Exception as e:
                        Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'BU not configured in EYFIRST ' + str(e),'Status Code':'0','EYFIRST_Import Id':'0'},ignore_index=True)
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(api_eyfirst_failed+ destination_blob_name)
                        blob.upload_from_filename(zip_file)

                        logger_client = logging.Client() 
                        logger = logger_client.logger('Eyfirst_Stack_logs')
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Append_df_to_zip', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
                        handler = CloudLoggingHandler(logger_client)
                        cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                        cloud_logger.setLevel(logg.INFO)
                        cloud_logger.addHandler(handler)
                        cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
                        # raise TypeError (str(exc_obj))
                
            #Exception log changes

            #delimiter='/'
            bucket_name = self.landing
            bucket = storage_client.get_bucket(bucket_name)
            blobs = storage_client.list_blobs(bucket,prefix=log_prefix)
            
            for b in blobs:
                if ('ETLVolumeSplitLog_CRS.csv' in b.name):
                    elog_data = b.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exceptions = pd.read_csv(BytesIO(elog_data), header=[0])

            elogfinal = elogfinal.rename(columns = {'CTCR Process ID':'CTCR_Process_ID','ETL_ProcessId':'GDF_ProcessId','Institution_Id':'Reporting_CI'})

                                                                                                                                                                         
            elogfinal = elogfinal[['Filename','CTCR_Process_ID','GDF_ProcessId','Reporting_CI','Account_Id','Customer_Id','Error_ID']]
            elogfinal['Regime'] = Regime
            									 
            exceptions = exceptions.append(elogfinal)
            exceptions = exceptions.drop_duplicates()
            exceptions = exceptions.reset_index(drop=True)
																				

                                                                                                    


            blob = bucket.blob(log_prefix +'ETLVolumeSplitLog_CRS.csv')
            exceptions.to_csv('ETLVolumeSplitLog_CRS.csv',index=False)
            blob.upload_from_filename('ETLVolumeSplitLog_CRS.csv')

            yield Api_log
                                                         
                         
                                                         
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Append_df_to_zip', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
 #--------------------------------APPEND DF TO CSV AND API CALL-----------------------------------------------

class FATCA_Append_df_to_zip(beam.DoFn):

    def __init__(self, input_path,business_secrets,landing):
        self.input_path = input_path
        self.business_secrets = business_secrets
        self.landing = landing

    def process(self,ControlPerson,AccountHolder,FI_Final,Header_Mapping,poolreporting,unique_id,etl_start_datetime,Thresholdvalue,elog):
        
        try:
            delimiter = '/'
            storage_client = storage.Client()
            
            '''
            bucket_cert = storage_client.bucket(self.business_secrets)
            file = bucket_cert.blob("DataFlow.cer")
            cert_file = file.download_to_filename('DataFlow.cer')
            '''
            
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)

            secrets_bucket_name = self.business_secrets
            secrets_bucket = storage_client.get_bucket(secrets_bucket_name)
            api_key_blobs = storage_client.list_blobs(secrets_bucket_name,prefix='', delimiter=delimiter)

            Api_Log_bucket_name = self.landing
            Api_log_bucket = storage_client.get_bucket(Api_Log_bucket_name)
            blobs_api_log = storage_client.list_blobs(Api_log_bucket,prefix=log_prefix+Api_folder, delimiter=delimiter)

            data_key =''
            for blob in api_key_blobs:
                if(blob.name == 'secrets_business.json'):
                    data_key = json.loads(blob.download_as_bytes(client=None))
            api_log_columns = ['ETL_Start_DateTime','CTCR Process ID','ETL_Process ID','Regime','Reporting Year','Institution ID','Zip File Name','Request Type','Status Code','EYFIRST_Import Id']
            Api_log = pd.DataFrame(columns = api_log_columns)                                                               

            AccountHolder = AccountHolder.drop(columns=['Entity_Type'])  
            ControlPerson = ControlPerson.drop(columns=['CTCR Process ID','Account_Holder_Dormancy_Status_x'])                                                              
             
             
            df1 = ControlPerson.reset_index(drop=True)
            df2 = AccountHolder.reset_index(drop=True)
            df3 = FI_Final.reset_index(drop=True)
            df4 = Header_Mapping.reset_index(drop=True)
                                 
            df5 = poolreporting.reset_index(drop=True)
            '''
            columns_add = ['Pooled_ReportingYear','Pooled_FFI_Name','Pooled_Reporting_Type','Pooled_Number_of_Accounts','Pooled_Aggregate_Account_Balance','Pooled_Currency_Code']

            for newcol in columns_add:
                    df5[newcol]= None

            '''
            InstitutionID_df = df3[['Service Institution ID','Abbrev. (Insitution ID)                 (8 char limit)']]
            InstitutionID_df = InstitutionID_df.drop_duplicates('Abbrev. (Insitution ID)                 (8 char limit)')

            InstitutionID_df['Service Institution ID'] = InstitutionID_df['Service Institution ID'].fillna('UNIQ ID').astype(str)
            InstitutionID_df.loc[InstitutionID_df['Service Institution ID'].isin(['UNIQ ID','']),'Service Institution ID'] = InstitutionID_df['Abbrev. (Insitution ID)                 (8 char limit)']
            Final_group_InsID = InstitutionID_df.groupby('Service Institution ID')

            df3 = df3.drop('Service Institution ID', axis= 1)
            
            
            if len(InstitutionID_df)<=0:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                logger.log_struct({'severity':'Critical','message':'Either BU Name from the resultant dataset is not present in the CI File or No result created for input dataset'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO)

            elogfinal = pd.DataFrame(columns = ['Filename','ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id','Customer_Id','Error_ID'])
            exceptions = pd.DataFrame(columns = ['Filename','Regime','CTCR_Process_ID','GDF_ProcessId','Reporting_CI','Account_Id','Customer_Id','Error_ID'])
            Regime = 'FTR'
            
            for filename, filegroup in Final_group_InsID:
                # BU = str(unique_Institution_Id[i])
                BU = filename
                BU_list = list(filegroup['Abbrev. (Insitution ID)                 (8 char limit)'])
                BU_list_join = ','.join(BU_list)
                
                df6 = df1.loc[df1['FI_ID'].isin(BU_list)].reset_index(drop=True)
                df7 = df2.loc[df2['FI_ID'].isin(BU_list)].reset_index(drop=True)
                df8 = df3.loc[df3['Abbrev. (Insitution ID)                 (8 char limit)'].isin(BU_list)].reset_index(drop=True)
                dfp = df5.loc[df5['FI_ID'].isin(BU_list)].reset_index(drop=True)
                df9 = df4.loc[df4['Abbrev.(InsitutionID)(8charlimit)'].isin(BU_list)]
                df9 = df9[df9['CountryofTaxResidence'].isin(["LU","CA"])].reset_index(drop=True)
                df9 = df9.drop(['Abbrev.(InsitutionID)(8charlimit)', 'CountryofTaxResidence'], 1)
																					 

                if len(df8) > 0:
                    COTR = df8['FFI_ResCountryCode'][0]

                    if COTR == 'GB':
                        #df6['SubsOwner_ReportingType'] = 'Combined'
                        df6['SubsOwner_Substantial_owner_type'] = ''                


                #Get CTCR processID value here
                if len(df7) > 0:
                    ctcrid = df7['CTCR Process ID_x'].unique()[0]
                else:
                    ctcrid = ''


                df6 = df6.drop('FI_ID', 1)
                df7 = df7.drop('FI_ID', 1)
                df8 = df8.drop('Abbrev. (Insitution ID)                 (8 char limit)', 1)
                dfp = dfp.drop('FI_ID', 1)
                #Remove CTCR ProcessID column
                df7 = df7.drop('CTCR Process ID_x', 1)

                #Remove FFI reporting year blank
                repyearcheck = df8[(df8['FFI_ReportingYear'] == '') | (df8['FFI_ReportingYear'].isnull())]
                idx = repyearcheck.index.tolist()
                idx = list(set(idx))
                df8 = df8.drop(idx)   

                                                                             
                                                   
                zip_file_list = []

                pool_flag = 0
                pool_org_flag = 0

                pool_empty_df = pd.DataFrame(columns=list(dfp.columns))
            
                #Volumn split

                if len(df7) > Thresholdvalue:

                    AH_df_ind = df7[df7['Accounts_Category_of_Account_Holder'] == 'Individual'].sort_values(['Accounts_Customer_ID','Accounts_Account_Number']).reset_index(drop=True)
                    AH_df_org = df7[df7['Accounts_Category_of_Account_Holder'] == 'Organisation'].sort_values(['Accounts_Customer_ID','Accounts_Account_Number']).reset_index(drop=True)
                    
                    if len(AH_df_org) != 0:
                        pool_org_flag = 1

                    # Category of AH is 'Individual'
                    Ind_counter = 0
                    r_keys=['Accounts_ReportingYear','Accounts_Account_Number','Accounts_Entity_Name','Accounts_FFI_Name'] 
                    l_keys=['SubsOwner_ReportingYear','SubsOwner_Account_Number','SubsOwner_Account_Holder_Entity_Name','SubsOwner_FFI_Name']
																									   
                    # cp_df = pd.DataFrame(columns=list(df6.columns))
                    split_count = 1
                    while len(AH_df_ind) > Thresholdvalue:
                        last = AH_df_ind['Accounts_Customer_ID'][Ind_counter+(Thresholdvalue - 1)]
                        idx = AH_df_ind['Accounts_Customer_ID'].where(AH_df_ind['Accounts_Customer_ID']==last).last_valid_index()
                        AH_split_df = AH_df_ind.loc[Ind_counter:idx]
                        
                        df_list = [df6,AH_split_df]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                        if pool_org_flag == 0 and pool_flag == 0:
                            pool_df = dfp
                            pool_flag = 1
                        else:
                            pool_df = pool_empty_df

                        #'*********************************************'
                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_split_df).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(pool_df).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')
                        time.sleep(1)

                        now = datetime.now() + timedelta(seconds=3)
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)

                                                                                                                                                            

                        #Exception Log:
                        AH_split_data = AH_split_df[['Accounts_Account_Number','Accounts_Customer_ID']]
                                  
                                                                                                                                                                                      
                                  
          
                                                                                 
                                                                                  
                                                            
                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                                                                                                                                                                                                                                                                                                                                          

                                                                                    
                        '''
                        if len(log)>0:
                            log['Is_Exception']= log.apply(lambda x: '1' if (x['Institution_Id'] == unique_Institution_Id and x['CTCR Process ID'] == ctcrid and x['Accounts_Account_Number'] == x['Account_Id'] and x['Accounts_Customer_ID'] == x['Customer_Id']) else '0', axis=1)
                        else:
                            log['Is_Exception'] = '0'
                        '''

                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]
                                                                 
                                                                  
                                                        

                        elogfinal = elogfinal.append(log)
                            
                        bucket_name = self.input_path
                            
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(FATCA_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)

                        zip_file_list.append((destination_blob_name,zip_file))
                        #'*********************************************'
                        idx= idx+1
                        AH_df_ind = AH_df_ind.loc[idx:]
                        Ind_counter  = idx

                        split_count += 1

                    else:

                        if pool_org_flag == 0 and pool_flag == 0:
                            pool_df = dfp
                            pool_flag = 1
                        else:
                            pool_df = pool_empty_df

                        df_list = [df6,AH_df_ind]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_df_ind).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(pool_df).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')
                        time.sleep(1)

                        now = datetime.now() + timedelta(seconds=3)
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)
                            
                        #Exception Log:
                        AH_split_data = AH_df_ind[['Accounts_Account_Number','Accounts_Customer_ID']]
				
                                                                         
    
                                                                         
                                                                                 
                                                                                  
                                                            
                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                                                                                                                                                                                                                                                                                                                                          

                                                                                    
                        
                                                                         
                                                          
                                                                 
                                                                  
                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]
                        elogfinal = elogfinal.append(log)
                            
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(FATCA_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)
                        
                        zip_file_list.append((destination_blob_name,zip_file))
                        split_count += 1


                    Org_counter = 0

                    r_keys=['Accounts_ReportingYear','Accounts_Account_Number','Accounts_Entity_Name','Accounts_FFI_Name'] 
                    l_keys=['SubsOwner_ReportingYear','SubsOwner_Account_Number','SubsOwner_Account_Holder_Entity_Name','SubsOwner_FFI_Name']

                    while len(AH_df_org) > Thresholdvalue:
                        last = AH_df_org['Accounts_Customer_ID'][Org_counter+ (Thresholdvalue - 1)]
                        idx = AH_df_org['Accounts_Customer_ID'].where(AH_df_org['Accounts_Customer_ID']==last).last_valid_index()

                        AH_split_df = AH_df_org.loc[Org_counter:idx]

                        df_list = [df6,AH_split_df]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)
                        
                        if pool_org_flag == 1 and pool_flag == 0:
                            pool_df = dfp
                            pool_flag = 1
                        else:
                            pool_df = pool_empty_df
                        #'********************************************************************************'

                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_split_df).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(pool_df).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')
                        time.sleep(1)

                        now = datetime.now() + timedelta(seconds=3)
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)
                            
                        #Exception Log:
                        AH_split_data = AH_split_df[['Accounts_Account_Number','Accounts_Customer_ID']]
                                   
                                                                                                                                                                                      
                                   
          
                                                                                 
                                                                                  
                                                            
                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                                                                                                                                                                                                                                                                                                                                          

                                                                                    

                                                                         
                                                          
                                                                 
                                                                  
                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]
                        elogfinal = elogfinal.append(log)
                            
                            
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(FATCA_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)
                        
                        zip_file_list.append((destination_blob_name,zip_file))
                        #'********************************************************************************'
                        idx= idx+1
                        AH_df_org = AH_df_org.loc[idx:]
                        Org_counter  = idx

                        split_count += 1

                    else:

                        if pool_org_flag == 1 and pool_flag == 0:
                            pool_df = dfp
                            pool_flag = 1
                        else:
                            pool_df = pool_empty_df

                        df_list = [df6,AH_df_org]
                        CP_split_df = reduce(lambda  left,right: pd.merge(left, right, left_on=l_keys, right_on=r_keys,how='inner'),df_list)

                        pd.DataFrame(CP_split_df[list(df6.columns)]).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(AH_df_org).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                        pd.DataFrame(pool_df).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')
                        time.sleep(1)

                        now = datetime.now() + timedelta(seconds=3)
                        dt_day = now.strftime("%d")
                        dt_month = now.strftime("%m")
                        dt_year = now.strftime("%Y")
                        dt_hour = now.strftime("%H")
                        dt_min = now.strftime("%M")
                        # dt_sec = now.strftime("%S")
                        no_split = str(split_count).zfill(2)

                        
                        compression = zipfile.ZIP_DEFLATED
                        destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+no_split+'.zip'
                        zip_file = destination_blob_name
                        with ZipFile(destination_blob_name, 'w') as zip_obj:
                            zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                            zip_obj.write("Account_Holder.csv",compress_type=compression)
                            zip_obj.write("FFI.csv",compress_type=compression)
                            zip_obj.write("Header.csv",compress_type=compression)
                            zip_obj.write("Pooled_Report.csv",compress_type=compression)
                            
                        #Exception Log:
                        AH_split_data = AH_df_org[['Accounts_Account_Number','Accounts_Customer_ID']]
                                    
                                                                                                                                                                                  
                                    
          
                                                                                 
                                                                                  
                                                            
                        df_list = [elog, AH_split_data]
                        log = reduce(lambda  left,right: pd.merge(left,right,left_on=['Account_Id','Customer_Id'], right_on=['Accounts_Account_Number','Accounts_Customer_ID'],how='inner'), df_list)

                                                                                                                                                                                                                                                                                                                                          

                                                                                    
                        
                                                                   
                        log['Filename'] = destination_blob_name
                        log = log[log['Institution_Id'].isin(BU_list)]

                                                             

                                                                  
                        elogfinal = elogfinal.append(log)
                            
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(FATCA_api_output_prefix + destination_blob_name)
                        blob.upload_from_filename(zip_file)
                        
                        zip_file_list.append((destination_blob_name,zip_file))
                        split_count += 1

                else:
                                                 
                    pd.DataFrame(df6).to_csv("Substantional_Owner_Controlling_Persons.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df7).to_csv("Account_Holder.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df8).to_csv("FFI.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(df9).to_csv("Header.csv", index = False, encoding='utf-8-sig', sep = '|')
                    pd.DataFrame(dfp).to_csv("Pooled_Report.csv", index = False, encoding='utf-8-sig', sep = '|')
                    
                    time.sleep(1)

                    now = datetime.now() + timedelta(seconds=3)
                    dt_day = now.strftime("%d")
                    dt_month = now.strftime("%m")
                    dt_year = now.strftime("%Y")
                    dt_hour = now.strftime("%H")
                    dt_min = now.strftime("%M")
                    dt_sec = now.strftime("%S")

                    
                    compression = zipfile.ZIP_DEFLATED
                    destination_blob_name = 'ITG_'+BU+'_'+dt_year+dt_month+dt_day+dt_hour+dt_min+dt_sec+'.zip'
                    zip_file = destination_blob_name
                    with ZipFile(destination_blob_name, 'w') as zip_obj:
                        zip_obj.write("Substantional_Owner_Controlling_Persons.csv",compress_type=compression)
                        zip_obj.write("Account_Holder.csv",compress_type=compression)
                        zip_obj.write("FFI.csv",compress_type=compression)
                        zip_obj.write("Header.csv",compress_type=compression)
                        zip_obj.write("Pooled_Report.csv",compress_type=compression)
                        
                  
                        
                    bucket_name = self.input_path
                    bucket = storage_client.get_bucket(bucket_name)
                    blob = bucket.blob(FATCA_api_output_prefix + destination_blob_name)
                    blob.upload_from_filename(zip_file)
                    
                    zip_file_list.append((destination_blob_name,zip_file))

                if len(df8) > 0:
                    ReportingYear = df8['FFI_ReportingYear'].unique()[0]
                else:
                    ReportingYear = ''
                
                Regime = 'FTR'
                
                
                import_headers = {"X-ApiKey": data_key['EyFirst_API_Key'], "Accept": "application/json"}
                url_link = data_key["EyFirst_API_GETClients"]
                import_request = requests.get(url_link,headers = import_headers)#,verify="DataFlow.cer")

                client_content = json.loads(import_request.content)
				
                client_content1 = dict((k.title(), v) for k, v in client_content.items())
                client_content2 = {'Clients':[]}

                for i in client_content1['Clients']:
                    client_content2['Clients'].append(dict((k.title(), v) for k, v in i.items()))

                bu_id = list()
                client_bu = list()
                
                for c in client_content2['Clients']:
                    bu_id.append(c['Id'])
                    client_bu.append(c['Name'])

                client_details = pd.DataFrame([bu_id,client_bu]).T
                client_details =client_details.rename(columns = {0 :'Id', 1:'Name'})
                client_details.reset_index(drop=True, inplace=True)
                
                for zip_data in zip_file_list:
                    destination_blob_name = zip_data[0]
                    zip_file = zip_data[1]
                    try:
                        client_id = client_details[client_details['Name']==BU]['Id'].values[0]

                        file_z = open(destination_blob_name,'rb')
                        time.sleep(1)

                        #files = {"X-File-Name": file_z}
                        files = {"file": (zip_file, file_z, 'application/zip')}
                        headers = {"X-ApiKey": data_key["EyFirst_API_Key"], "Accept": "application/json", "type":"formData"}

                        # r = requests.post(data_key["EyFirst_API_Import_CSV_Template"]+str(client_id), headers=headers, files=files)#,verify="DataFlow.cer")
                        #RETRY (max 3 times)
                        counter = 0
                        status = 0

                        while (status<200 or status>=300) and counter<3:
                            r = requests.post(data_key["EyFirst_API_Import_CSV_Template"]+str(client_id), headers=headers, files=files)#,verify="DataFlow.cer")
                            status = r.status_code
                            counter = counter + 1						  

                        if r.status_code >=200 and r.status_code < 300:
                            # import_id = json.loads(r.content)["Importid"]
                            import_id1 = json.loads(r.content)
                            import_id = dict((k.title(), v) for k, v in import_id1.items())["Importid"]
                            Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'Import Master Data Csv Template','Status Code':r.status_code,'EYFIRST_Import Id':import_id},ignore_index=True)

                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(api_eyfirst_processed+ destination_blob_name)
                            blob.upload_from_filename(zip_file)


                        else:
                            Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'Import Master Data Csv Template','Status Code':r.status_code,'EYFIRST_Import Id':json.loads(r.content)},ignore_index=True)
                                                                                                                                                                                                                                                                                                                                                
                            bucket_name = self.input_path
                            bucket = storage_client.get_bucket(bucket_name)
                            blob = bucket.blob(api_eyfirst_failed+ destination_blob_name)
                            blob.upload_from_filename(zip_file)
                        
                    #result = r.status_code 
                    except:
                        Api_log = Api_log.append({'ETL_Start_DateTime':etl_start_datetime,'CTCR Process ID':ctcrid,'ETL_Process ID':unique_id,'Regime':Regime,'Reporting Year':ReportingYear,'Institution ID':BU_list_join,'Zip File Name':destination_blob_name,'Request Type':'BU not configured in EYFIRST','Status Code':'0','EYFIRST_Import Id':'0'},ignore_index=True) 
                        bucket_name = self.input_path
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(api_eyfirst_failed+ destination_blob_name)
                        blob.upload_from_filename(zip_file)
                
            
            #Exception log changes

            #delimiter='/'
            bucket_name = self.landing
            bucket = storage_client.get_bucket(bucket_name)
            blobs = storage_client.list_blobs(bucket,prefix=log_prefix)
            
            for b in blobs:
                if ('ETLVolumeSplitLog_FTR.csv' in b.name):
                    elog_data = b.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exceptions = pd.read_csv(BytesIO(elog_data), header=[0])

            elogfinal = elogfinal.rename(columns = {'CTCR Process ID':'CTCR_Process_ID','ETL_ProcessId':'GDF_ProcessId','Institution_Id':'Reporting_CI'})

            elogfinal = elogfinal[['Filename','CTCR_Process_ID','GDF_ProcessId','Reporting_CI','Account_Id','Customer_Id','Error_ID']]
            elogfinal['Regime'] = Regime
            									 
            exceptions = exceptions.append(elogfinal)
            exceptions = exceptions.drop_duplicates()
            exceptions = exceptions.reset_index(drop=True)
                                                                                                                                                                                                                                                                                                                                                          

                                                                                                                                                                                                                                                                                                                                                          

            

            blob = bucket.blob(log_prefix +'ETLVolumeSplitLog_FTR.csv')
            exceptions.to_csv('ETLVolumeSplitLog_FTR.csv',index=False)
            blob.upload_from_filename('ETLVolumeSplitLog_FTR.csv')


            yield Api_log
                                                                 
                                                                      
                                                                 
                                                                      
                                                             
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Append_df_to_zip', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
                       
#----------------------------------------------------------HEADER MAPPING-------------------------------------------------
class header_mapping(beam.DoFn):
	
    def process(self,element, FI):
        try:
            df1 = FI
            df2 = element
            df_list = [df1, df2]
            df = reduce(lambda  left,right: pd.merge(left,right,left_on="Abbrev. (Insitution ID)                 (8 char limit)", right_on="Abbrev. (Insitution ID)                 (8 char limit)",how='inner'), df_list)
            
            #df = pd.merge(df1, df2, how='inner', left_on="Abbrev. (Insitution ID)                 (8 char limit)", right_on="Abbrev. (Insitution ID)                 (8 char limit)")
            if len(df) > 0 :
                columns_add = ['Extension','Declarer_Person_Telephone_Direct']
                for newcol in columns_add:
                        df[newcol]= None
                df["Language_Indicator"] = "E"
                df["Transmitter_Type_Indicator"] = 1
                df = df.rename(columns={"FFI_Name":"HeaderTitle"})
                df.columns = df.columns.str.replace('\\n','', regex=True)
                df.columns = df.columns.str.replace(' ', '') 
                # df['Telephone']= df.apply(lambda x: x['CAContactTelephoneNumber(RequiredforCanada)35charlimit'] if x['Country'] == 'CA' else x['Telephone'], axis=1)
                #Telephone
                df['Telephone'] = df['FFI_ContactPerson_Telephone_Number']
                df['ContactAreaCode'] = df['FFI_ContactPerson_Telephone_Number_STDCode']

                cols = {"LUDepositorNameDepositor(100charlimit)":"Depositor_Name","LUDepositorPersonalIdentificationNumber(13digits)":"Depositor_Personal_Identification_Number","LUDepositorStreetPhysical(100charlimit)":"Depositor_Street","LUDepositorNumberPhysical(100charlimit)":"Depositor_Address_Number","LUDepositorCityPhysical(75charlimit)":"Depositor_City","LUDepositorCountryPhysical":"Depositor_Country","LUDepositorPOBox(16charlimit)":"Depositor_POB","LUDepositorPostalCodePostal(16charlimit)":"Depositor_Postal_Code_Postal","LUDepositorCityPostal(75charlimit)":"Depositor_City_Postal","LUDepositorCountryPostal":"Depositor_Country_Postal","LUDepositorName(100charlimit)":"Depositor_Person_Name","LUDepositorFirstName(100charlimit)":"Depositor_Person_First_Name","LUDepositorEmailOrganisation":"Depositor_Person_Email_Organisation","LUDepositorEmailPersonal":"Depositor_Person_Email_Personal","LUDepositorTelephoneDirect":"Depositor_Person_Telephone_Direct","LUDeclarerNameDeclarer":"Declarer_Name","LUDeclarerPersonalIdentificationNumber(13digits)":"Declarer_Personal_Identification_Number","LUDeclarerStreetPhysical(100charlimit)":"Declarer_Street","LUDeclarerNumberPhysical(100charlimit)":"Declarer_Address_Number","LUDeclarerPostalCodePhysical(16charlimit)":"Declarer_Postal_Code","LUDeclarerCityPhysical(75charlimit)":"Declarer_City","LUDeclarerCountryPhysical":"Declarer_Country","LUDeclarerPOBox(16charlimit)":"Declarer_POB","LUDeclarerPostalCodePostal(16charlimit)":"Declarer_Postal_Code_Postal","LUDeclarerCityPostal(75charlimit)":"Declarer_City_Postal","LUDeclarerCountryPostal":"Declarer_Country_Postal","LUDeclarerName(100charlimit)":"Declarer_Person_Name","LUDeclarerFirstName(100charlimit)":"Declarer_Person_First_Name","LUDeclarerEmailPersonal":"Declarer_Person_Email_Personal","LUDeclarerEmailOrganisation":"Declarer_Person_Email_Organisation","CATransmittername-line1(RequiredforCanada)35charlimit":"TransmitterNameLine","CATransmitteraddress-line1(RequiredforCanada)35charlimit":"TransmitterAddressLine","CATransmittercity(RequiredforCanada)35charlimit":"TransmitterCity","CATransmittercountrycode(RequiredforCanada)35charlimit":"TransmitterCountryCode","CATransmitterpostalcode(RequiredforCanada)35charlimit":"TransmitterPostalCode","CAContactName(RequiredforCanada)35charlimit":"ContactName","CAContactEmailID(RequiredforCanada)35charlimit":"Email", "CATransmitternumber(RequiredforCanada)35charlimit":"Transmitter_Number", "CATransmitteraddress-line2(RequiredforCanada)35charlimit":"TransmitterAddressLine2", "CATransmitterprovinceorterritorycode(RequiredforCanada)35charlimit":"TransmitterProvinceOrCode","LUDepositorPostalCodePhysical(16charlimit)":"Depositor_Postal_Code","LUCanal":"Transmitter"}
                
                for i in cols.keys():
                    for j in df.columns:
                        if i in j:
                            df = df.rename(columns={j:cols[i]})
                df = df[['CountryofTaxResidence', 'Abbrev.(InsitutionID)(8charlimit)','FFI_ReportingYear',	'HeaderTitle',	'Depositor_Name',	'Depositor_Personal_Identification_Number',	'Depositor_Street',	'Depositor_Address_Number',	'Depositor_Postal_Code',	'Depositor_City',	'Depositor_Country',	'Depositor_POB',	'Depositor_Postal_Code_Postal',	'Depositor_City_Postal',	'Depositor_Country_Postal',	'Depositor_Person_Name',	'Depositor_Person_First_Name',	'Depositor_Person_Email_Organisation',	'Depositor_Person_Email_Personal',	'Depositor_Person_Telephone_Direct',	'Declarer_Name',	'Declarer_Personal_Identification_Number',	'Declarer_Street',	'Declarer_Address_Number',	'Declarer_Postal_Code',	'Declarer_City',	'Declarer_Country',	'Declarer_POB',	'Declarer_Postal_Code_Postal',	'Declarer_City_Postal',	'Declarer_Country_Postal',	'Declarer_Person_Name',	'Declarer_Person_First_Name',	'Declarer_Person_Email_Personal',	'Declarer_Person_Email_Organisation',	'Declarer_Person_Telephone_Direct',	'Transmitter',	'Transmitter_Number',	'Transmitter_Type_Indicator',	'Language_Indicator',	'TransmitterNameLine',	'TransmitterAddressLine',	'TransmitterCity',	'TransmitterProvinceOrCode',	'TransmitterCountryCode',	'TransmitterPostalCode',	'ContactName',	'ContactAreaCode',	'Telephone',	'Extension',	'Email',	'TransmitterAddressLine2']]
                
                yield df
            else:
                yield df
                
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'header_mapping', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

 
#---------------------------------------------------------WRITE FILE TO CSV---------------------------------------------------------------------------------------

#Write file to csv 
class Write_file_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename):
        try:
            delimiter='/'
            df = element
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)


            nr_log_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nrlog = pd.DataFrame(columns = nr_log_columns)

            for b in blobs:
                if (filename in b.name):
                    nr_data = b.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    nrlog = pd.read_csv(BytesIO(nr_data), header=[0])

            nrlog = nrlog.append(element)
            
            nrlog = nrlog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
            nrlog.dropna(how="all", inplace=True)
            nrlog = nrlog.reset_index(drop=True)                                                                                                                                                                                                                                                                                                                                                                    
            nrlog.to_csv(filename,index=False)                                                                                     
            blob = bucket.blob(log_prefix + destination_blob_name)

            blob.upload_from_filename(filename)
            yield nrlog
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_file_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
          
#-------------------------------------------------CRS_CURRENCY CONVERSION---------------------------------------------------------------------------------------------------------
#Currency Conversion 
class CRS_Currency_Conversion(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,element, FI):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("USDMS" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exchange_df = pd.read_excel(BytesIO(fwf_data),engine='xlrd')    
            crs_acct_bal = element
            CI = FI
            

            #Merging CI and Account Balance and getting "Reporting in account currency" and "Standard Reporting Currency" from CI
            FI = CI[['Abbrev. (Insitution ID)                 (8 char limit)','CRS Report in account Currency','CRS Standard Reporting Currency']]
            
            
            df_list = [crs_acct_bal, FI]
            account_bal_FI = reduce(lambda  left,right: pd.merge(left,right,left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)",how='inner'), df_list)
            #account_bal_FI = pd.merge(crs_acct_bal, FI, how='inner', left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)")


            if len(account_bal_FI) > 0:
                #Checking conditions in CI file to convert currency for Account Balance Amount
                account_bal_FI['CRS Standard Reporting Currency'] = account_bal_FI['CRS Standard Reporting Currency'].astype(str)

                account_bal_FI['original_cur'] = account_bal_FI['Balance_Currency_Code'].tolist()
                deleted_index = []
                
                accbal_instid = account_bal_FI[['Institution_Id','CRS Report in account Currency','CRS Standard Reporting Currency']]
                accbal_instid.drop_duplicates(keep='first', inplace=True)
                accbal_instid = accbal_instid.reset_index(drop=True)
                #account_bal_FI_cols = list(account_bal_FI.columns)
                #account_bal_FI = account_bal_FI.values
                account_bal_FI_Final = pd.DataFrame()
                for i in range(len(accbal_instid)): 

                    if  ((accbal_instid.loc[i,'CRS Report in account Currency'] == 'No' or accbal_instid.loc[i,'CRS Report in account Currency'] == " " or accbal_instid.loc[i,'CRS Report in account Currency'] == "N") and accbal_instid.loc[i,'CRS Standard Reporting Currency'] == "USD"):
                        
                        acc_bal_FI_conv_N = account_bal_FI[account_bal_FI.Institution_Id == accbal_instid.loc[i,'Institution_Id']]
                        acc_bal_FI_conv_N.reset_index(drop=True,inplace =True)
                        account_bal_FI_cols = list(acc_bal_FI_conv_N.columns)
                        #account_bal_FI_N = acc_bal_FI_conv_N.values

                        # Get year end date for each balance end date
                        acc_bal_FI_conv_N['Conv_End_Date']=  pd.to_datetime(acc_bal_FI_conv_N['Balance_End_Date']).dt.year.astype(str)+"-12-31"
    
                        accbal_forex_df = acc_bal_FI_conv_N[['Balance_Currency_Code','Conv_End_Date']]
                        accbal_forex_df = accbal_forex_df.drop_duplicates()
                        accbal_forex_df = accbal_forex_df.reset_index(drop=True)
                        accbal_forex_df['Forex'] = 1
                        accbal_forex_df['Updated_Bal_CCY'] = accbal_forex_df['Balance_Currency_Code']
                                                                    

                        accbal_forex_df_columns = list(accbal_forex_df.columns)
                        accbal_forex_df_vect = accbal_forex_df.values
                    
                        for row in accbal_forex_df_vect:
                                                                        
                                                                                          

                            if (row[accbal_forex_df_columns.index('Balance_Currency_Code')] != "USD"):
                                
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('Balance_Currency_Code')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex')] = forex
                                    row[accbal_forex_df_columns.index('Updated_Bal_CCY')] = 'USD'
                                else:
                                    # deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex')] = 0
                        
                        accbal_forex_df = pd.DataFrame(accbal_forex_df_vect, columns=accbal_forex_df_columns)
                        
                        df_list = [acc_bal_FI_conv_N, accbal_forex_df]
                        acc_bal_FI_conv_N = reduce(lambda  left,right: pd.merge(left,right,on=['Conv_End_Date','Balance_Currency_Code'],how='left'), df_list)
                        
                        #acc_bal_FI_conv_N = pd.merge(acc_bal_FI_conv_N, accbal_forex_df, how='left', on=['Conv_End_Date','Balance_Currency_Code'])
                        acc_bal_FI_conv_N['Balance_Amount'] = (acc_bal_FI_conv_N['Balance_Amount'].astype(float) / acc_bal_FI_conv_N['Forex'])
                        acc_bal_FI_conv_N['Balance_Amount'] = acc_bal_FI_conv_N['Balance_Amount'].astype(float)
    
                        acc_bal_FI_conv_N['Balance_Amount'] = acc_bal_FI_conv_N['Balance_Amount'].round(2)
                                                                                                                                                                                                                                                                
    
                        acc_bal_FI_conv_N['Balance_Currency_Code'] = acc_bal_FI_conv_N['Updated_Bal_CCY']            
                        acc_bal_FI_conv_N.drop(acc_bal_FI_conv_N.index[acc_bal_FI_conv_N['Forex'] == 0], inplace = True)
                        acc_bal_FI_conv_N = pd.DataFrame(acc_bal_FI_conv_N, columns=account_bal_FI_cols)
                        
                        #acc_bal_FI_conv_N = acc_bal_FI_conv_N.drop(deleted_index)
                        account_bal_FI_Final = account_bal_FI_Final.append(acc_bal_FI_conv_N)
                                                                                                
    
                    else:
                        acc_bal_FI_conv_Y= account_bal_FI[account_bal_FI.Institution_Id == accbal_instid.loc[i,'Institution_Id']]
                                        
                        account_bal_FI_Final = account_bal_FI_Final.append(acc_bal_FI_conv_Y)

                account_bal_FI = account_bal_FI_Final.drop(columns=['Abbrev. (Insitution ID)                 (8 char limit)','CRS Report in account Currency','CRS Standard Reporting Currency'])
                account_bal_FI = account_bal_FI.reset_index(drop=True)
                yield account_bal_FI
            else:
                crs_acct_bal['original_cur'] = crs_acct_bal['Balance_Currency_Code'].tolist()
                yield crs_acct_bal


        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Currency_Conversion', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
         
#-------------------------------------------------FATCA_CURRENCY CONVERSION---------------------------------------------------------------------------------------------------------
class FATCA_Currency_Conversion(beam.DoFn):
    
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,element, FI):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("USDMS" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exchange_df = pd.read_excel(BytesIO(fwf_data),engine='xlrd')
            
            fatca_acct_bal = element
            CI = FI
            #Merging CI and Account Balance and getting "Reporting in account currency" and "Standard Reporting Currency" from CI
            FI = CI[['Abbrev. (Insitution ID)                 (8 char limit)','FATCA Report in account ccy','FATCA Standard reporting ccy for 8966']]
            
            df_list = [fatca_acct_bal, FI]
            account_bal_FI = reduce(lambda  left,right: pd.merge(left,right, left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)",how='inner'), df_list)
            #account_bal_FI = pd.merge(fatca_acct_bal, FI, how='inner', left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)")
 
            if len(account_bal_FI)>0:
            
                #Checking conditions in CI file to convert currency for Account Balance Amount
                account_bal_FI['FATCA Standard reporting ccy for 8966'] = account_bal_FI['FATCA Standard reporting ccy for 8966'].astype(str)
                account_bal_FI['original_cur'] = account_bal_FI['Balance_Currency_Code'].tolist()
                deleted_index = []
                accbal_instid = account_bal_FI[['Institution_Id','FATCA Report in account ccy','FATCA Standard reporting ccy for 8966']]
                accbal_instid.drop_duplicates(keep='first', inplace=True)
                accbal_instid = accbal_instid.reset_index(drop=True)
                account_bal_FI_Final = pd.DataFrame()
                for i in range(len(accbal_instid)):
                    if ((accbal_instid.loc[i,'FATCA Report in account ccy'] == 'No' or accbal_instid.loc[i,'FATCA Report in account ccy'] == " " or accbal_instid.loc[i,'FATCA Report in account ccy'] == "N") and accbal_instid.loc[i,'FATCA Standard reporting ccy for 8966'] == 'USD'):
                        acc_bal_FI_conv_N = account_bal_FI[account_bal_FI.Institution_Id == accbal_instid.loc[i,'Institution_Id']]
                        acc_bal_FI_conv_N.reset_index(drop=True,inplace =True)
                        account_bal_FI_cols = list(acc_bal_FI_conv_N.columns)
                        # Get year end date for each balance end date
                        acc_bal_FI_conv_N['Conv_End_Date']=  pd.to_datetime(acc_bal_FI_conv_N['Balance_End_Date']).dt.year.astype(str)+"-12-31"
    
                        accbal_forex_df = acc_bal_FI_conv_N[['Balance_Currency_Code','Conv_End_Date']]
                        accbal_forex_df = accbal_forex_df.drop_duplicates()
                        accbal_forex_df = accbal_forex_df.reset_index(drop=True)
                        accbal_forex_df['Forex'] = 1
                        accbal_forex_df['Updated_Bal_CCY'] = accbal_forex_df['Balance_Currency_Code']
                        accbal_forex_df_columns = list(accbal_forex_df.columns)
                        accbal_forex_df_vect = accbal_forex_df.values
                        for row in accbal_forex_df_vect:
                            if (row[accbal_forex_df_columns.index('Balance_Currency_Code')] != "USD"):
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('Balance_Currency_Code')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex')] = forex
                                    row[accbal_forex_df_columns.index('Updated_Bal_CCY')] = 'USD'
                                else:
                                    #deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex')] = 0
    
                        accbal_forex_df = pd.DataFrame(accbal_forex_df_vect, columns=accbal_forex_df_columns)
                        
                        df_list = [acc_bal_FI_conv_N, accbal_forex_df]
                        acc_bal_FI_conv_N = reduce(lambda  left,right: pd.merge(left,right,on=['Conv_End_Date','Balance_Currency_Code'],how='left'), df_list)
                        
                        
                        #acc_bal_FI_conv_N = pd.merge(acc_bal_FI_conv_N, accbal_forex_df, how='left', on=['Conv_End_Date','Balance_Currency_Code'])
                        acc_bal_FI_conv_N['Balance_Amount'] = (acc_bal_FI_conv_N['Balance_Amount'].astype(float) / acc_bal_FI_conv_N['Forex'])
                        acc_bal_FI_conv_N['Balance_Amount'] = acc_bal_FI_conv_N['Balance_Amount'].astype(float)
                        acc_bal_FI_conv_N['Balance_Amount'] = acc_bal_FI_conv_N['Balance_Amount'].round(2)
                        acc_bal_FI_conv_N['Balance_Currency_Code'] = acc_bal_FI_conv_N['Updated_Bal_CCY']            
                        acc_bal_FI_conv_N.drop(acc_bal_FI_conv_N.index[acc_bal_FI_conv_N['Forex'] == 0], inplace = True)
                        acc_bal_FI_conv_N = pd.DataFrame(acc_bal_FI_conv_N, columns=account_bal_FI_cols)
                        #acc_bal_FI_conv_N = acc_bal_FI_conv_N.drop(deleted_index)
                        account_bal_FI_Final = account_bal_FI_Final.append(acc_bal_FI_conv_N)
                    
                    else:
                        acc_bal_FI_conv_Y= account_bal_FI[account_bal_FI.Institution_Id == accbal_instid.loc[i,'Institution_Id']]
                      
                        account_bal_FI_Final = account_bal_FI_Final.append(acc_bal_FI_conv_Y)

    
                account_bal_FI = account_bal_FI_Final.drop(columns=['Abbrev. (Insitution ID)                 (8 char limit)','FATCA Report in account ccy','FATCA Standard reporting ccy for 8966'])
                account_bal_FI = account_bal_FI.reset_index(drop=True)

                yield account_bal_FI
            else:
                fatca_acct_bal['original_cur'] = fatca_acct_bal['Balance_Currency_Code'].tolist()
                yield fatca_acct_bal
 
 
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Currency_Conversion', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#-------------------------------------------------CRS_CURRENCY CONVERSION1---------------------------------------------------------------------------------------------------------


class CRS_Currency_Conversion1(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,element, Payments_df, FI):

        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("USDMS" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exchange_df = pd.read_excel(BytesIO(fwf_data),engine='xlrd')    

            account_bal_FI = element
            crs_pay = Payments_df
            CI = FI        

            if len(crs_pay) > 0:
                #Merging Payment and Converted Account Balance and getting "Balance currency code" from Converted Account Balance
                
                crs_acct_bal_new = account_bal_FI[["Institution_Id","Balance_Currency_Code","Account_Id","original_cur"]]
                
                df_list = [crs_pay, crs_acct_bal_new]
                account_bal_pay = reduce(lambda  left,right: pd.merge(left,right,left_on=["Institution_Id","Account_Id"], right_on=["Institution_Id","Account_Id"],how='inner'), df_list)

                #account_bal_pay = pd.merge(crs_pay, crs_acct_bal_new, how='inner', left_on=["Institution_Id","Account_Id"], right_on=["Institution_Id","Account_Id"])
                
                #Merging CI Files with above merged Payment - Account Balance and getting "Transaction Reporting Currency" from CI Files
                FI = CI[['Abbrev. (Insitution ID)                 (8 char limit)','CRS Transaction Reporting Currency', 'CRS Report in account Currency', 'CRS Standard Reporting Currency']]
                
                df_list = [account_bal_pay, FI]
                account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right,left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)",how='inner'), df_list)
                    
                #account_bal_pay_FI = pd.merge(account_bal_pay, FI, how='inner', left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)")     

                #Filtering Payment_Amount_CCY groupby Account_Id and Payment_Code
                account_bal_pay_FI_Gb = account_bal_pay_FI.groupby(["Payment_Code", "Account_Id"])['Payment_Amount_CCY'].unique().reset_index()
                deleted_index = []
                
                #Keep the source currency to a different column
                account_bal_pay_FI['cur_CCY'] = account_bal_pay_FI['Payment_Amount_CCY']
                
                #Keep the unique currency list for that account id, payment code in a different column Curr_List
                account_bal_pay_FI_Gb = account_bal_pay_FI_Gb.rename(columns={"Payment_Amount_CCY":"Curr_List"})
                if len(account_bal_pay_FI_Gb) > 0:
                
                    df_list = [account_bal_pay_FI, account_bal_pay_FI_Gb]
                    account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right,on=["Payment_Code","Account_Id"],how='left'), df_list)
                    
                    #account_bal_pay_FI = pd.merge(account_bal_pay_FI, account_bal_pay_FI_Gb, how='left', on=["Payment_Code","Account_Id"])
                    #Data cleansing on 'CRS Report in account Currency'
                    blank_list = ['No'," "]
                    account_bal_pay_FI['CRS Report in account Currency'] = account_bal_pay_FI['CRS Report in account Currency'].apply(lambda x: 'N' if x in blank_list else x)
                    
                    #Update the Target Currency for single payment and Multiple payment of Case 2 
                    account_bal_pay_FI['Updated_Pay_CCY']= account_bal_pay_FI.apply(lambda x: x['original_cur'] if (x['CRS Transaction Reporting Currency'] == 'Original Currency' and x['CRS Report in account Currency'] == 'N' and len(x['Curr_List'])>1) else x['Payment_Amount_CCY'], axis=1)

                    #Update the Target Currency for single payment and Multiple payment of Case 1 , Case 3 and Case 4
                    account_bal_pay_FI['Updated_Pay_CCY']= account_bal_pay_FI.apply(lambda x: x['Balance_Currency_Code'] if ((x['CRS Transaction Reporting Currency'] == 'Original Currency' and x['CRS Report in account Currency'] != 'N' and len(x['Curr_List'])>1) or (x['CRS Transaction Reporting Currency'] != 'Original Currency')) else x['Updated_Pay_CCY'], axis=1)

                    # Get year end date for each payment date
                    account_bal_pay_FI['Conv_End_Date']=  pd.to_datetime(account_bal_pay_FI['Payment_Date']).dt.year.astype(str)+"-12-31"

                    accbal_forex_df = account_bal_pay_FI[['Conv_End_Date','cur_CCY','Updated_Pay_CCY']]
                    accbal_forex_df = accbal_forex_df.drop_duplicates()
                    accbal_forex_df = accbal_forex_df.reset_index(drop=True)
                    accbal_forex_df['Forex1'] = 1
                    accbal_forex_df['Forex2'] = 1

                    accbal_forex_df_columns = list(accbal_forex_df.columns)
                    accbal_forex_df_vect = accbal_forex_df.values
                    
                    for row in accbal_forex_df_vect:

                        if (row[accbal_forex_df_columns.index('Updated_Pay_CCY')] == "USD" and row[accbal_forex_df_columns.index('cur_CCY')] != "USD"):
                            
                            exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('cur_CCY')]]
                            exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                            exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                            exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                            
                            if len(exchange_df_new2) > 0:
                                forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                row[accbal_forex_df_columns.index('Forex1')] = forex
                            else:
                                # deleted_index.append(index)
                                row[accbal_forex_df_columns.index('Forex1')] = 0

                        else:
                            if (row[accbal_forex_df_columns.index('Updated_Pay_CCY')] != 'USD' and row[accbal_forex_df_columns.index('Updated_Pay_CCY')] != row[accbal_forex_df_columns.index('cur_CCY')]):
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('cur_CCY')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex1')] = forex
                                else:
                                    #deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex1')] = 0
                                        
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('Updated_Pay_CCY')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex2')] = forex
                                else:
                                    #deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex2')] = 0


                    accbal_forex_df = pd.DataFrame(accbal_forex_df_vect, columns=accbal_forex_df_columns)
                    df_list = [account_bal_pay_FI, accbal_forex_df]
                    account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right,on=['Conv_End_Date','cur_CCY','Updated_Pay_CCY'],how='left'), df_list)
                    
                    #account_bal_pay_FI = pd.merge(account_bal_pay_FI, accbal_forex_df, how='left', on=['Conv_End_Date','cur_CCY','Updated_Pay_CCY'])
                    account_bal_pay_FI['Payment_Amount'] = (account_bal_pay_FI['Payment_Amount'] / account_bal_pay_FI['Forex1']) * account_bal_pay_FI['Forex2']
                    account_bal_pay_FI['Payment_Amount_CCY'] = account_bal_pay_FI['Updated_Pay_CCY']
                    account_bal_pay_FI = account_bal_pay_FI.reset_index(drop=True)
                    account_bal_pay_FI = account_bal_pay_FI[['index','Record_Type','Record_Action','Institution_Id','Account_Id','Payment_Id','Payment_Type','Payment_Code','Payment_Date','Payment_Amount','Payment_Amount_CCY','Payment_Amount_USD','USD_Cross_Rate','USD_Cross_Rate_Date','Effective_From_Date','Effective_To_Date','CompositeKey','CTCR Process ID','Source System ID','Source File Index','MDT_Country','Key','SourceFileName','Stage1_id','Stage1_date_time','CSV File Index','original_cur','CRS Report in account Currency','CRS Standard Reporting Currency']] 
                    
                    yield account_bal_pay_FI
                else:
                    yield crs_pay

            else:
                yield crs_pay
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Currency_Conversion1', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#-------------------------------------------------FATCA_CURRENCY CONVERSION1---------------------------------------------------------------------------------------------------------
class FATCA_Currency_Conversion1(beam.DoFn):
    
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,element, Payments_df, FI):
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("USDMS" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    exchange_df = pd.read_excel(BytesIO(fwf_data),engine='xlrd')

            account_bal_FI = element
            fatca_pay = Payments_df
            CI = FI        
            if len(fatca_pay) > 0:
                #Merging Payment and Converted Account Balance and getting "Balance currency code" from Converted Account Balance
                fatca_acct_bal_new = account_bal_FI[["Institution_Id","Balance_Currency_Code","Account_Id","original_cur"]]
                
                df_list = [fatca_pay, fatca_acct_bal_new]
                account_bal_pay = reduce(lambda  left,right: pd.merge(left,right,  left_on=["Institution_Id","Account_Id"], right_on=["Institution_Id","Account_Id"],how='inner'), df_list)
                
                #account_bal_pay = pd.merge(fatca_pay, fatca_acct_bal_new, how='inner', left_on=["Institution_Id","Account_Id"], right_on=["Institution_Id","Account_Id"])

                #Merging CI Files with above merged Payment - Account Balance and getting "Transaction Reporting Currency" from CI Files
                FI = CI[['Abbrev. (Insitution ID)                 (8 char limit)','FATCA Transaction Reporting Currency', 'FATCA Report in account ccy', 'FATCA Standard reporting ccy for 8966']]
                df_list = [account_bal_pay, FI]
                account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right, left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)",how='inner'), df_list)
                #account_bal_pay_FI = pd.merge(account_bal_pay, FI, how='inner', left_on="Institution_Id", right_on="Abbrev. (Insitution ID)                 (8 char limit)")        

                #Filtering Payment_Amount_CCY groupby Account_Id and Payment_Code
                account_bal_pay_FI_Gb = account_bal_pay_FI.groupby(["Payment_Code", "Account_Id"])['Payment_Amount_CCY'].unique().reset_index()
                deleted_index = []
                
                # New changes
                #Keep the source currency to a different column
                account_bal_pay_FI['cur_CCY'] = account_bal_pay_FI['Payment_Amount_CCY']
                
                #Keep the unique currency list for that account id, payment code in a different column Curr_List
                account_bal_pay_FI_Gb = account_bal_pay_FI_Gb.rename(columns={"Payment_Amount_CCY":"Curr_List"})
                if len(account_bal_pay_FI_Gb) >0:
                    
                    df_list = [account_bal_pay_FI, account_bal_pay_FI_Gb]
                    account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right,on=["Payment_Code","Account_Id"],how='left'), df_list)
                
                    #account_bal_pay_FI = pd.merge(account_bal_pay_FI, account_bal_pay_FI_Gb, how='left', on=["Payment_Code","Account_Id"])
                    
                    #Data cleansing on 'FATCA Report in account Currency'
                    blank_list = ['No'," "]
                    account_bal_pay_FI['FATCA Report in account ccy'] = account_bal_pay_FI['FATCA Report in account ccy'].apply(lambda x: 'N' if x in blank_list else x)
                    
                    #Update the Target Currency for single payment and Multiple payment of Case 2 
                    account_bal_pay_FI['Updated_Pay_CCY']= account_bal_pay_FI.apply(lambda x: x['original_cur'] if (x['FATCA Transaction Reporting Currency'] == 'Original Currency' and x['FATCA Report in account ccy'] == 'N' and len(x['Curr_List'])>1) else x['Payment_Amount_CCY'], axis=1)
    
                    #Update the Target Currency for single payment and Multiple payment of Case 1 , Case 3 and Case 4
                    account_bal_pay_FI['Updated_Pay_CCY']= account_bal_pay_FI.apply(lambda x: x['Balance_Currency_Code'] if ((x['FATCA Transaction Reporting Currency'] == 'Original Currency' and x['FATCA Report in account ccy'] != 'N' and len(x['Curr_List'])>1) or (x['FATCA Transaction Reporting Currency'] != 'Original Currency')) else x['Updated_Pay_CCY'], axis=1)
    
                    # Get year end date for each payment date
                    account_bal_pay_FI['Conv_End_Date']=  pd.to_datetime(account_bal_pay_FI['Payment_Date']).dt.year.astype(str)+"-12-31"
    
                    accbal_forex_df = account_bal_pay_FI[['Conv_End_Date','cur_CCY','Updated_Pay_CCY']]
                    accbal_forex_df = accbal_forex_df.drop_duplicates()
                    accbal_forex_df = accbal_forex_df.reset_index(drop=True)
                    accbal_forex_df['Forex1'] = 1
                    accbal_forex_df['Forex2'] = 1
    
                    accbal_forex_df_columns = list(accbal_forex_df.columns)
                    accbal_forex_df_vect = accbal_forex_df.values
    
                    for row in accbal_forex_df_vect:
    
                        if (row[accbal_forex_df_columns.index('Updated_Pay_CCY')] == "USD" and row[accbal_forex_df_columns.index('cur_CCY')] != "USD"):
                            
                            exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('cur_CCY')]]
                            exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                            exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                            exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                            
                            if len(exchange_df_new2) > 0:
                                forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                row[accbal_forex_df_columns.index('Forex1')] = forex
                            else:
                                # deleted_index.append(index)
                                row[accbal_forex_df_columns.index('Forex1')] = 0
    
                        else:
                            if (row[accbal_forex_df_columns.index('Updated_Pay_CCY')] != 'USD' and row[accbal_forex_df_columns.index('Updated_Pay_CCY')] != row[accbal_forex_df_columns.index('cur_CCY')]):
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('cur_CCY')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex1')] = forex
                                else:
                                    #deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex1')] = 0
                                        
                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == row[accbal_forex_df_columns.index('Updated_Pay_CCY')]]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= row[accbal_forex_df_columns.index('Conv_End_Date')])]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)
                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.loc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    row[accbal_forex_df_columns.index('Forex2')] = forex
                                else:
                                    #deleted_index.append(index)
                                    row[accbal_forex_df_columns.index('Forex2')] = 0
                    
                    accbal_forex_df = pd.DataFrame(accbal_forex_df_vect, columns=accbal_forex_df_columns)
                        
                        
                    df_list = [account_bal_pay_FI, accbal_forex_df]
                    account_bal_pay_FI = reduce(lambda  left,right: pd.merge(left,right,on=['Conv_End_Date','cur_CCY','Updated_Pay_CCY'],how='left'), df_list)    
                    #account_bal_pay_FI = pd.merge(account_bal_pay_FI, accbal_forex_df, how='left', on=['Conv_End_Date','cur_CCY','Updated_Pay_CCY'])
    
                    account_bal_pay_FI['Payment_Amount'] = (account_bal_pay_FI['Payment_Amount'] / account_bal_pay_FI['Forex1']) * account_bal_pay_FI['Forex2']
                    account_bal_pay_FI['Payment_Amount_CCY'] = account_bal_pay_FI['Updated_Pay_CCY']
                    account_bal_pay_FI = account_bal_pay_FI.reset_index(drop=True)
                    account_bal_pay_FI = account_bal_pay_FI[['index','Record_Type','Record_Action','Institution_Id','Account_Id','Payment_Id','Payment_Type','Payment_Code','Payment_Date','Payment_Amount','Payment_Amount_CCY','Payment_Amount_USD','USD_Cross_Rate','USD_Cross_Rate_Date','Effective_From_Date','Effective_To_Date','CompositeKey','CTCR Process ID','Source System ID','Source File Index','MDT_Country','Key','SourceFileName','Stage1_id','Stage1_date_time','CSV File Index','Balance_Currency_Code','original_cur','Abbrev. (Insitution ID)                 (8 char limit)','FATCA Transaction Reporting Currency','FATCA Report in account ccy','FATCA Standard reporting ccy for 8966']] 
                    
                    yield account_bal_pay_FI
                else:
                    yield fatca_pay
            else:
                yield fatca_pay
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Currency_Conversion1', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#----------------------------------------------------------------------------------------CRS_PROCESS NONREPORTABLE FINAL DF-----------------------------------------------------------

#Filter the non-reportable set of data 
class CRS_ProcessNonReportableFinalDF(beam.DoFn):
    def process(self,element,ahnr,cpnr,uniqueid,etl_start_datetime):
        try:
            df = element
            nonreplog_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nonreplog = pd.DataFrame(columns = nonreplog_columns)

            if len(df)>0:
                idx = list()
                df['Non-Reporting Reason']=''

                allowed_accounttype = ['C','D','DI','EI','CV']
                allowed_accounttype_check = df[(~df['Account_Type'].isin(allowed_accounttype))]
                idx = idx + allowed_accounttype_check.index.tolist()
                idx= list(set(idx))

                df.loc[idx,'Non-Reporting Reason'] = 'Non reportable due to Account Type'
                df.loc[idx,'Reportable']='N'
                df.loc[idx,'Decision Branch']='Account Type'

                idx = list()
                allowed_entitytype = ['EN01','EN07','EN08','EN10']
                allowed_entitytype_check = df[((~df['Entity_Type'].isin(allowed_entitytype)) & (df['Category_of_Account_Holder'] == 'Organisation'))]

                idx = idx + allowed_entitytype_check.index.tolist()
                idx= list(set(idx))

                df.loc[idx,'Non-Reporting Reason'] = 'Non reportable due to Entity Type'
                df.loc[idx,'Reportable']='N'
                df.loc[idx,'Decision Branch']='Entity Type'

                df['Regime'] = 'CRS'
                df['Person Type']=df['Category_of_Account_Holder']
                df = df[df['Reportable']=='N'] 

                #df = df.rename(columns={"MDT_Country_x":"MDT_Country"})
                #df = df[['Institution_Id','Account_Id','Account_Type','MDT_Country','DormantAccount','Account_Closed','Balance_Currency_Code','Balance_Amount','Financials_Dividends_Amount','Financials_Gross_Proceeds_Redemptions_Amount','Financials_Interest_Amount','Financials_Dividends_Currency','Financials_Gross_Proceeds_Redemptions_Currency','Financials_Interest_Currency','Financials_Other_Amount','Financials_Other_Currency','Involved_Party_Id','Account_Relationship_Type','Individual_Forenames','Individual_Surname','Individual_DOB','Entity_Name','Place_of_Birth','Entity_Type','CRS_SC_Status_','LOB_Indicator','Customer_ID','Category_of_Account_Holder','Account_Holder_Type','Accounts_UndocumentedAccount','ReportingYear','Building_Identifier','Street_Name','District_Name','City','Post_Code','Country_Code','ProvinceStateCodeOrName','Accounts_Address_Free','OrganizationAccountHolderTypeCode','BirthInfoYear','BirthInfoMonth','BirthInfoDay','Accounts_TIN_issued_by','Accounts_Res_Country_Code','Accounts_TIN','Accounts_TIN_Type','Accounts_Res_Country_Code2','Accounts_Res_Country_Code3','Accounts_Res_Country_Code4','Accounts_Res_Country_Code5','Accounts_Res_Country_Code6','Accounts_Res_Country_Code7','Accounts_Res_Country_Code8','Accounts_TIN2','Accounts_TIN3','Accounts_TIN4','Accounts_TIN5','Accounts_TIN6','Accounts_TIN7','Accounts_TIN8','Accounts_TIN_Type2','Accounts_TINType3','Accounts_TINType4','Accounts_TINType5','Accounts_TINType6','Accounts_TINType7','Accounts_TINType8','Accounts_TIN_issued_by2','Accounts_TIN_issued_by3','Accounts_TIN_issued_by4','Accounts_TIN_issued_by5','Accounts_TIN_issued_by6','Accounts_TIN_issued_by7','Accounts_TIN_issued_by8','Non-Reporting Reason','Reportable','Decision Branch','Regime','Person Type']]
              
                df = df.rename(columns= {'CTCR Process ID_x':'CTCR Process ID','Regime':'Regime','ReportingYear':'Reporting Year','Institution_Id':'Institution ID','Customer_ID':'Customer ID','Entity_Name':'Customer Entity Name','Individual_Forenames':'Customer First Name','Individual_Surname':'Customer Last Name','Account_Id':'Account Id','Account_Closed':'Closed_Account','Account_Type':'Account Type','Entity_Type':'Entity Type','Non-Reporting Reason':'Reason for Non-Reporting','DormantAccount':'Dormant','Category_of_Account_Holder':'Category of Account Holder'})

                nonreplog = nonreplog.append(df,sort=False)
                '''
                for i in range(len(nonreplog)):
                    nonreplog['ETL ProcessId'].values[i] = uniqueid
                    #nonreplog['CTCR Process ID'].values[i] = ''
                    nonreplog['Classification'] = ''
                    nonreplog['Sub Classification'] = ''
                    nonreplog['ETL_Start_DateTime'].values[i] = etl_start_datetime
                '''
                nonreplog['ETL ProcessId'] = uniqueid
                nonreplog['Classification'] = ''
                nonreplog['Sub Classification'] = ''
                nonreplog['ETL_Start_DateTime'] = etl_start_datetime
                                                               
                                                              
                nonreplog = nonreplog.append([ahnr,cpnr],sort=False)

                nonreplog = nonreplog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
                nonreplog = nonreplog.reset_index(drop=True)

            yield nonreplog

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessNonReportableFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
      
      
#-------------------------------------------------------------FATCA_PROCESSS NONREPORTABLE FINAL DF----------------------------------------------------------------------
class FATCA_ProcessNonReportableFinalDF(beam.DoFn):
    def process(self,element,uniqueid,etl_start_datetime, res_depository_non_reportable_df):
        try:
            df = element
            deMinimis_nonReportable = res_depository_non_reportable_df
            nonreplog_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nonreplog = pd.DataFrame(columns = nonreplog_columns)
            if len(df)>0:
                
                df = df[df['Reportable']=='N']
                df['Regime'] = 'FATCA'
                df['Person Type']=df['Category_of_Account_Holder']
                #df = df.rename(columns={"MDT_Country_x":"MDT_Country","Institution_Id_x":"Institution_Id"})
                df = df.rename(columns= {'CTCR Process ID_x':'CTCR Process ID','Regime':'Regime','ReportingYear':'Reporting Year','Institution_Id':'Institution ID','Customer_ID':'Customer ID','Entity_Name':'Customer Entity Name','Individual_Forenames':'Customer First Name','Individual_Surname':'Customer Last Name','Account_Id':'Account Id','Account_Closed':'Closed_Account','Account_Type':'Account Type','Non-Reporting Reason':'Reason for Non-Reporting','FATCA_Classification':'Classification','FATCA_Sub-Classification':'Sub Classification','DormantAccount':'Dormant','Category_of_Account_Holder':'Category of Account Holder'})

                nonreplog = nonreplog.append(df,sort=False)
                '''
                for i in range(len(nonreplog)):
                    nonreplog['ETL ProcessId'].values[i] = uniqueid
                    nonreplog['Regime'].values[i] = 'FTR'
                    nonreplog['Entity Type'].values[i] = ''
                    nonreplog['ETL_Start_DateTime'].values[i] = etl_start_datetime
                '''
                nonreplog['ETL ProcessId'] = uniqueid
                nonreplog['Regime'] = 'FTR'
                nonreplog['Entity Type'] = ''
                nonreplog['ETL_Start_DateTime'] = etl_start_datetime
                
                nonreplog = nonreplog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
                nonreplog = nonreplog.reset_index(drop=True)
				
            # ##======== DeMinimis part ==========================
            deMinimis_nonReportable['ETL ProcessId'] = uniqueid
            deMinimis_nonReportable['Entity Type'] = ''
            deMinimis_nonReportable['ETL_Start_DateTime'] = etl_start_datetime
            deMinimis_nonReportable = deMinimis_nonReportable[nonreplog_columns]
            
            nonreplog = nonreplog.append(deMinimis_nonReportable)

            yield nonreplog
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessNonReportableFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#---------------------------------------------------------------------CRS_PROCESS REPORTABLE FINAL DF---------------------------------------------------------------

#Filter the reportable set of data
class CRS_ProcessReportableFinalDF(beam.DoFn):
    def process(self,element):
        try:
            df = element
            if len(df)>0:
                
                idx = list()
                allowed_accounttype = ['C','D','DI','EI','CV']
                allowed_accounttype_check = df[(~df['Account_Type'].isin(allowed_accounttype))]

                idx = idx + allowed_accounttype_check.index.tolist()
                idx= list(set(idx))
                df.loc[idx,'Reportable']='N'

                idx = list()
                allowed_entitytype = ['EN01','EN07','EN08','EN10']
                allowed_entitytype_check = df[((~df['Entity_Type'].isin(allowed_entitytype)) & (df['Category_of_Account_Holder'] == 'Organisation'))]

                idx = idx + allowed_entitytype_check.index.tolist()
                idx= list(set(idx))
                df.loc[idx,'Reportable']='N'

                df = df[df['Reportable']!='N']

                df = df.reset_index(drop=True)

 
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessReportableFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
                       
#---------------------------------------------------------------------FATCA_PROCESS REPORTABLE FINAL DF---------------------------------------------------------------            
class FATCA_ProcessReportableFinalDF(beam.DoFn):
    def process(self,element):
        try:
            df = element
            df = df[df['Reportable']!='N']
            df = df.reset_index(drop=True)
            df['Account_Holder_Type'] = df['Classification_Code']
            df['Account_Holder_Type'] = df['Account_Holder_Type'].apply(lambda x: x.replace('No reporting code for Individuals (Reportable)','').replace('(for 2015 and 2016 only)','') if ('No reporting code for Individuals (Reportable)' in x or '(for 2015 and 2016 only)' in x) else x)
            df = df.drop(columns=['Country of Tax Residence','FATCA Regime (FFI/IGA 1/IGA 2)','Abbrev. (Insitution ID)                 (8 char limit)'])

            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportableFinalDF', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
#---------------------------------------------------------------------FATCA_PROCESS REPORTABILITY---------------------------------------------------------------                  
class FATCA_ProcessReportability(beam.DoFn):
    
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,fi_df):
        
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("FATCA_classification_codes_lookup_file" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    fatca_lookup = pd.read_excel(BytesIO(fwf_data),header = [7],keep_default_na=False,engine='xlrd')


            df_ci = fi_df[['Abbrev. (Insitution ID)                 (8 char limit)','Country of Tax Residence','FATCA Regime (FFI/IGA 1/IGA 2)']]
            df = element

            no_repo  = {'non-reportable':'Non-reportable as per classification logic','N/A':'Not Applicable','???':'Not Applicable','Could not classify':'Non-reportable as unable to classify'}
            no_repo_list = list(no_repo.keys())
        
            df_list = [df, df_ci]
            df = reduce(lambda  left,right: pd.merge(left,right,left_on=['Institution_Id'], right_on=['Abbrev. (Insitution ID)                 (8 char limit)'],how='inner'), df_list)
            #df= pd.merge(df, df_ci, how='inner', left_on=['Institution_Id'], right_on=['Abbrev. (Insitution ID)                 (8 char limit)'])
            
            idx_non_repo = list()

            
                                           
                                         
                                      
            Ip_Type_list = {'Individual':'I',"Organisation":'E'}
            CustomerDormancy_Type_list = {'Yes':'Y',"No":'N OR [Spaces]'}


            #category 1:
            allowed_accounttype = ['C','D','DI','EI','CV']
            not_allowed_account_type = df[(~df['Account_Type'].isin(allowed_accounttype))]
  
            allowed_account_type = df[(df['Account_Type'].isin(allowed_accounttype))]
            df_opt = allowed_account_type[['FATCA_Classification','FATCA_Sub-Classification','FATCA Regime (FFI/IGA 1/IGA 2)','Category_of_Account_Holder','Account_Holder_Dormancy_Status','Country of Tax Residence','ReportingYear']].drop_duplicates()
            df_opt['Reportable'] = ''
            df_opt['Non-Reporting Reason'] = ''
            df_opt["Classification_Code"] =""
            df_opt['Decision Branch'] = ""
            for idx, row in df_opt.iterrows():
                class_df1 = fatca_lookup[fatca_lookup['HSBC Class Code']=='[Other]']
                class_df = fatca_lookup[fatca_lookup['HSBC Class Code'].str.strip() == str(row['FATCA_Classification']).strip()]
                if len(class_df)>=1:
                    class_df1 = class_df

                if len(class_df1)>=1:
                    subclass_df1 = class_df1[class_df1['HSBC Sub-Class Code'].isin(['[All]','[none] or [Other]',''])]
                    subclass_df = class_df1[class_df1['HSBC Sub-Class Code'].str.strip() == str(row["FATCA_Sub-Classification"]).strip()]
                    if len(subclass_df)>=1:
                        subclass_df1 = subclass_df

                    

                if len(subclass_df1)>=1:
                    taxregime_df1 = subclass_df1[subclass_df1['Tax Regime'].isin(['[All]'])]
                    taxregime_df = subclass_df1[subclass_df1['Tax Regime'].replace(" ",'')== str(row['FATCA Regime (FFI/IGA 1/IGA 2)']).replace(" ",'')]
                    if len(taxregime_df)>=1:
                        taxregime_df1 = taxregime_df

                    

                if len(taxregime_df1)>=1:
                    eylegalcategory_df1 = taxregime_df1[taxregime_df1['EY Legal Category'].isin(['[All]'])]
                                                                           
                    if str(row['Category_of_Account_Holder']) in ['Organisation','Individual']:
                        eylegalcategory_df = taxregime_df1[taxregime_df1['EY Legal Category'] == Ip_Type_list[row['Category_of_Account_Holder']]]
                    else:
                        eylegalcategory_df = eylegalcategory_df1
                         
                                                                

                    if len(eylegalcategory_df)>=1:
                        eylegalcategory_df1 = eylegalcategory_df    

                    

                if len(eylegalcategory_df1)>=1:
                    custdormancy_df1 = eylegalcategory_df1[eylegalcategory_df1['Customer Dormancy flag'].isin(['[All]'])]
                    
                                                                  
                    if str(row['Account_Holder_Dormancy_Status']) in ['Yes','No']:
                        custdormancy_df = eylegalcategory_df1[eylegalcategory_df1['Customer Dormancy flag'] == CustomerDormancy_Type_list[row['Account_Holder_Dormancy_Status']]]
                    else:
                        custdormancy_df = custdormancy_df1     
                                                                


                    if len(custdormancy_df)>=1:
                        custdormancy_df1 = custdormancy_df  
                    
                    try:
                        x = custdormancy_df1[row['Country of Tax Residence']].values[0]
                        df_opt.at[idx,'Classification_Code'] = custdormancy_df1[row['Country of Tax Residence']].values[0]
                    except:
                        df_opt.at[idx,'Classification_Code'] = 'Could not classify as country is not present in FATCA Classification file'
                           
                                                                                                                                      
                        
                     
                else:
                    df_opt.at[idx,'Classification_Code'] = 'Could not classify'

            df_opt['Reportable'] = df_opt.Classification_Code.apply(lambda x : 'N' if x in no_repo_list else '')
            df_opt['Non-Reporting Reason'] = df_opt.Classification_Code.apply(lambda x : no_repo[x] if x in no_repo_list else '')
            df_temp= df_opt[(df_opt['Classification_Code'].str.contains('for 2015 and 2016 only')) & (~df_opt['ReportingYear'].isin(['2015','2016']))]
            l1 = df_temp.index.to_list()
            df_opt.at[l1,'Reportable'] = 'N'

            for i in l1:
                df_opt.at[i,'Non-Reporting Reason'] = df_opt.at[i,'Classification_Code'].replace('(for 2015 and 2016 only)','is only allowed to be reported in 2015 or 2016') 

            df_opt['Decision Branch'] = df_opt.Reportable.apply(lambda x: 'Classification Logic' if x == 'N' else '' )



            df_list_opt = [df, df_opt]
            df = reduce(lambda  left,right: pd.merge(left,right,how='left'), df_list_opt)
            
            #non reporting due to Account sub-type not equal to IDL for other accounts :
            not_allowed_account_sub_type = not_allowed_account_type[(not_allowed_account_type['Account_Sub-Type'] != 'IDL')]
            #--------------
            idx_non_repo = set(not_allowed_account_sub_type.index.tolist())
            df.at[idx_non_repo,'Reportable'] = 'N'
            for i in idx_non_repo:
                df.at[i,'Non-Reporting Reason'] = 'Non reporting due to Account Sub-Type'
                df.at[i,'Decision Branch'] = 'Account Sub-Type'
            df[['Reportable','Non-Reporting Reason','Classification_Code','Decision Branch']] = df[['Reportable','Non-Reporting Reason','Classification_Code','Decision Branch']].astype(str).replace('nan','')


                                                                
                                                           
                                        
                                            
                                                                  
                                           
                                               


                                                                  
                                           
                                               
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportability', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

                 
#------------------------------------Conditional function to start Stage 2----------------------------------------------

class CRS_find_csv(beam.DoFn):
                              

    def process(self,element,account,accountbalance,payment,ipaccount,ipip,ip,iptd,ipaddn):
        
        try:
            if len(account) != 0 and len(ipaccount) != 0 and len(ip) != 0:
                flag = 1
            else:
                flag = 0

            yield flag
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_find_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            

class FATCA_find_csv(beam.DoFn):
    

    def process(self,element,account,accountbalance,payment,ipaccount,ipip,ip,ipaddca):
        
        try:
            
            if len(account) != 0 and len(ipaccount) != 0 and len(ip) != 0:
                flag = 1
            else:
                flag = 0

            yield flag
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_find_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class Write_API_log(beam.DoFn):
    def __init__(self, input_path,business_secrets,landing):
        self.input_path = input_path
        self.business_secrets = business_secrets
        self.landing = landing

    def process(self, element):
        try:

            delimiter = '/'
            storage_client = storage.Client()
            Api_Log_bucket_name = self.landing
            Api_log_bucket = storage_client.get_bucket(Api_Log_bucket_name)
            blobs_api_log = storage_client.list_blobs(Api_log_bucket,prefix=log_prefix+Api_folder, delimiter=delimiter)

            api_log_columns = ['ETL_Start_DateTime','CTCR Process ID','ETL_Process ID','Regime','Reporting Year','Institution ID','Zip File Name','Request Type','Status Code','EYFIRST_Import Id']
            Api_log = pd.DataFrame(columns = api_log_columns)
            for blob in blobs_api_log:
                if 'EYFIRST_API_LOG' in blob.name:
                    Api_log_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    Api_log = pd.read_csv(BytesIO(Api_log_data), header=[0])
                    
        

            Api_log = Api_log.append(element)
            
               
            Api_log.to_csv('EYFIRST_API_LOG.csv',index=False)
            api_blob = Api_log_bucket.blob(log_prefix+Api_folder+"EYFIRST_API_LOG.csv")
            api_blob.upload_from_filename('EYFIRST_API_LOG.csv') 
            yield Api_log
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_API_log', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#------------------------------------------------ Match and Merge for IP-------------------------------------
class MatchMergeIPIP(beam.DoFn):
    def process(self, element, IPIP, IPAccount, filetype):
        try:
            if element == 1 and len(IPIP) > 0:
                
                if (len(IPAccount)>0):
                    IPAccount = IPAccount[['Institution_Id','Involved_Party_Id','CTCR Process ID']]
                IPIP = IPIP.drop_duplicates(subset= ['Parent_Party_Id','Child_Party_Id','Relationship_Type','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                actual_IPIP = IPIP
                
                cols = list(actual_IPIP.columns.values) 
                df_list = [IPIP, IPAccount]
                df = reduce(lambda  left,right: pd.merge(left,right,left_on=['Parent_Party_Id','CTCR Process ID'], right_on=['Involved_Party_Id','CTCR Process ID'],how='left'), df_list)
                #df = pd.merge(IPIP, IPAccount, how='left', left_on=['Parent_Party_Id','CTCR Process ID'], right_on=['Involved_Party_Id','CTCR Process ID'])
                              
                df['Institution_Id']= df.apply(lambda x: x['Institution_Id_y'] if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else x['Institution_Id_x'], axis=1)
                     
                                           
                df = df.drop(columns=['Institution_Id_x','Institution_Id_y','Involved_Party_Id','Key','CompositeKey'])
                third_column = df.pop('Institution_Id')
                df.insert(2, 'Institution_Id',third_column)
                df1 = df.drop_duplicates(subset= ['Institution_Id', 'Parent_Party_Id','Child_Party_Id','Relationship_Type','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                
                if(filetype == 'IPIP'):
                    df1['Key'] = df1['Institution_Id'].astype(str)+"_"+df1['Parent_Party_Id'].astype(str)+"_"+df1['Child_Party_Id'].astype(str)+"_"+df1['Relationship_Type'].astype(str)
                    df1["CompositeKey"] = df1['CTCR Process ID'].astype(str)+"_"+ df1['Source System ID'].astype(str)+"_"+ df1['Key'].astype(str)
                    
                df1 = df1.reindex(columns=cols)
                df1 = df1.drop_duplicates(keep='first')
                df1 = df1.reset_index(drop= True)
                yield df1
            else:
                yield IPIP
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MatchMergeIPIP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))        

#------------------------------------------------ Match and Merge for IP-------------------------------------
class MatchMergeIP(beam.DoFn):
    def process(self, element, IP, IPAccount, filetype):
        try:

            if element == 1 and len(IP) > 0:
                if (len(IPAccount)>0):
                    IPAccount = IPAccount[['Institution_Id','Involved_Party_Id','CTCR Process ID']]
                if(filetype == 'IP'):
                    IP = IP.drop_duplicates(subset= ['Involved_Party_Id','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                
                actual_IP = IP
                actual_IP['MM_Flag'] = ''
                cols = list(actual_IP.columns.values)   
                df_list = [IP, IPAccount]
                df = reduce(lambda  left,right: pd.merge(left,right,on=['Involved_Party_Id','CTCR Process ID'],how='left'), df_list)
                #df = pd.merge(IP, IPAccount, how='left', on=['Involved_Party_Id','CTCR Process ID'])
                if len(df)>0:
                    df['Institution_Id']= df.apply(lambda x: x['Institution_Id_y'] if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else x['Institution_Id_x'], axis=1)
                    df['MM_Flag']= df.apply(lambda x: 1 if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else 0, axis=1)
                else:
                    df['Institution_Id']= ''
                    df['MM_Flag']= ''
                
                df = df.drop(columns=['Institution_Id_x','Institution_Id_y','Key','CompositeKey'])

                third_column = df.pop('Institution_Id')
                df.insert(2, 'Institution_Id',third_column)
                
                                
                if(filetype == 'IP'):
                    df1 = df.drop_duplicates(subset= ['Institution_Id', 'Involved_Party_Id','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                    df1['Key'] = df1['Institution_Id'].astype(str)+"_"+df1['Involved_Party_Id'].astype(str)
                    df1['CompositeKey'] = df1['CTCR Process ID'].astype(str)+"_"+ df1['Source System ID'].astype(str)+"_"+ df1['Key'].astype(str)
                                
                df1 = df1.reindex(columns=cols)
                df1 = df1.drop_duplicates(keep='first')
                df1 = df1.reset_index(drop= True)
                yield df1
            else:
                yield IP
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MatchMergeIP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#------------------------------------------------ Match and Merge for Child IP-------------------------------------
class MatchMergeIPChild(beam.DoFn):
    def process(self, element, IP, IPIP, filetype):
        try:
            if element == 1 and len(IP) > 0:
                if (len(IPIP)>0):
                    IPIP = IPIP[['Institution_Id','Child_Party_Id','CTCR Process ID']]
                    IP1 = IP[IP['MM_Flag']==0]
                    IP = IP[IP['MM_Flag']==1]
                    IP = IP.reset_index(drop= True)
                    
                    if(filetype == 'IP'):
                        IP1 = IP1.drop_duplicates(subset= ['Involved_Party_Id','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                                        
                    actual_IP = IP1
                    actual_IP['MM_Flag'] = ''
                    cols = list(actual_IP.columns.values) 
                    
                    df_list = [IP1,IPIP]
                    df = reduce(lambda  left,right: pd.merge(left,right,left_on=['Involved_Party_Id','CTCR Process ID'], right_on=['Child_Party_Id','CTCR Process ID'],how='left'), df_list)
                    
                    #df = pd.merge(IP1, IPIP, how='left', left_on=['Involved_Party_Id','CTCR Process ID'], right_on=['Child_Party_Id','CTCR Process ID'])
                    if len(df)>0:
                        df['Institution_Id']= df.apply(lambda x: x['Institution_Id_y'] if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else x['Institution_Id_x'], axis=1)
                    else:
                        df['Institution_Id'] = ''
                        
                    df = df.drop(columns=['Institution_Id_x','Institution_Id_y','Child_Party_Id','Key','CompositeKey'])
    
                    third_column = df.pop('Institution_Id')
                    df.insert(2, 'Institution_Id',third_column)
                    
                    if(filetype == 'IP'):
                        df1 = df.drop_duplicates(subset= ['Institution_Id', 'Involved_Party_Id','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                        df1['Key'] = df1['Institution_Id'].astype(str)+"_"+df1['Involved_Party_Id'].astype(str)
                        df1['CompositeKey'] = df1['CTCR Process ID'].astype(str)+"_"+ df1['Source System ID'].astype(str)+"_"+ df1['Key'].astype(str)
                                        
                    df1 = df1.reindex(columns=cols)
                    df1 = df1.drop_duplicates(keep='first')
                    df1 = df1.append(IP)
                    df1.reset_index(inplace = True,drop=True)   
                    
                    df1 = df1.drop(columns=['MM_Flag'])
    
                    yield df1
                else:
                    IP = IP.drop(columns=['MM_Flag'])
                    yield IP
            else:
                yield IP
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MatchMergeIPChild', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#------------------------------------------------ Match and Merge for IPTD-------------------------------------
class MatchMergeIPTD(beam.DoFn):
    def process(self, element, IPTD, IPAccount, filetype):
        try:

            if element == 1 and len(IPTD) > 0:
                if (len(IPAccount)>0):
                    IPAccount = IPAccount[['Institution_Id','Involved_Party_Id','CTCR Process ID']]
                    IPAccount['CompareKey'] = IPAccount['Institution_Id'].astype(str)+IPAccount['Involved_Party_Id'].astype(str)+IPAccount['CTCR Process ID'].astype(str)
                
                IPTD['CompareKey'] = IPTD['Institution_Id'].astype(str)+IPTD['Involved_Party_Id'].astype(str)+IPTD['CTCR Process ID'].astype(str)
                
                IPTD = IPTD.assign(MM_Flag=IPTD['CompareKey'].isin(IPAccount['CompareKey']).astype(int))
                
                IPTD = IPTD.drop(columns=['CompareKey'])
                
                IPTD1 = IPTD[IPTD['MM_Flag']==0]
                IPTD = IPTD[IPTD['MM_Flag']==1]
                
                IPTD = IPTD.reset_index(drop= True)
                
                if (filetype == 'IPTD'):
                    IPTD1 = IPTD1.drop_duplicates(subset= ['Involved_Party_Id','Country_of_Tax_Residence','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                
                actual_IP = IPTD
                
                #actual_IP['MM_Flag'] = ''
                
                cols = list(actual_IP.columns.values)                                    
                
                df_list = [IPTD1,IPAccount]
                df = reduce(lambda  left,right: pd.merge(left,right,on=['Involved_Party_Id','CTCR Process ID'],how='left'), df_list)
                
                #df = pd.merge(IPTD1, IPAccount, how='left', on=['Involved_Party_Id','CTCR Process ID'])
                if len(df)>0:
                    df['Institution_Id']= df.apply(lambda x: x['Institution_Id_y'] if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else x['Institution_Id_x'], axis=1)
                    df['MM_Flag']= df.apply(lambda x: 1 if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else 0, axis=1)
                else:
                    df['Institution_Id']= ''
                    df['MM_Flag']= ''
                
                df = df.drop(columns=['Institution_Id_x','Institution_Id_y','Key','CompositeKey'])

                third_column = df.pop('Institution_Id')
                df.insert(2, 'Institution_Id',third_column)
                
                if (filetype == 'IPTD'):
                    df1 = df.drop_duplicates(subset= ['Institution_Id', 'Involved_Party_Id','Country_of_Tax_Residence','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                    df1['Key'] = df1['Institution_Id'].astype(str)+"_"+df1['Involved_Party_Id'].astype(str)+"_"+df1['Country_of_Tax_Residence'].astype(str)
                    df1['CompositeKey'] = df1['CTCR Process ID'].astype(str)+"_"+ df1['Source System ID'].astype(str)+"_"+ df1['Key'].astype(str)
                
                df1 = df1.reindex(columns=cols)
                df1 = df1.drop_duplicates(keep='first')
                
                IPTD = IPTD.append(df1)
                IPTD = IPTD.reset_index(drop= True)
                yield IPTD
            else:
                yield IPTD
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MatchMergeIPTD', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#------------------------------------------------ Match and Merge for Child IPTD-------------------------------------
class MatchMergeIPTDChild(beam.DoFn):
    def process(self, element, IPTD, IPIP, filetype):
        try:
            if element == 1 and len(IPTD) > 0:
                if (len(IPIP)>0):
                    IPIP = IPIP[['Institution_Id','Child_Party_Id','CTCR Process ID']]
                    IPIP['CompareKey'] = IPIP['Institution_Id'].astype(str)+IPIP['Child_Party_Id'].astype(str)+IPIP['CTCR Process ID'].astype(str)
                    
                    IPTD0 = IPTD[IPTD['MM_Flag']==0]
                    IPTD0['CompareKey'] = IPTD0['Institution_Id'].astype(str)+IPTD0['Involved_Party_Id'].astype(str)+IPTD0['CTCR Process ID'].astype(str)
                    IPTD0 = IPTD0.assign(MM_Flag=IPTD0['CompareKey'].isin(IPIP['CompareKey']).astype(int))
                    IPTD0 = IPTD0.drop(columns=['CompareKey'])
                    
                    IPTD00 = IPTD0[IPTD0['MM_Flag']==0]
                    
                    IPTD01 = IPTD0[IPTD0['MM_Flag']==1]
                    IPTD1 = IPTD[IPTD['MM_Flag']==1]
                    
                    
                    if (filetype == 'IPTD'):
                        IPTD00 = IPTD00.drop_duplicates(subset= ['Involved_Party_Id','Country_of_Tax_Residence','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                    
                    actual_IP = IPTD1
                    actual_IP['MM_Flag'] = ''
                    cols = list(actual_IP.columns.values) 
                    # Fix
                    df_list = [IPTD00,IPIP]
                    df = reduce(lambda  left,right: pd.merge(left,right,left_on=['Involved_Party_Id','CTCR Process ID'], right_on=['Child_Party_Id','CTCR Process ID'],how='left'), df_list)
                
                    #df = pd.merge(IPTD1, IPAccount, how='left', on=['Involved_Party_Id','CTCR Process ID'])
                    if len(df)>0:
                        df['Institution_Id']= df.apply(lambda x: x['Institution_Id_y'] if (len(str(x['Institution_Id_y'])) > 0 and str(x['Institution_Id_y']).upper()!='NAN') else x['Institution_Id_x'], axis=1)
                    else:
                        df['Institution_Id'] = ''
                        
                    df = df.drop(columns=['Institution_Id_x','Institution_Id_y','Child_Party_Id','Key','CompositeKey'])
    
                    third_column = df.pop('Institution_Id')
                    df.insert(2, 'Institution_Id',third_column)
                    
                    if (filetype == 'IPTD'):
                        df1 = df.drop_duplicates(subset= ['Institution_Id', 'Involved_Party_Id','Country_of_Tax_Residence','CTCR Process ID'],keep= 'first').reset_index(drop= True)
                        df1['Key'] = df1['Institution_Id'].astype(str)+"_"+df1['Involved_Party_Id'].astype(str)+"_"+df1['Country_of_Tax_Residence'].astype(str)
                        df1['CompositeKey'] = df1['CTCR Process ID'].astype(str)+"_"+ df1['Source System ID'].astype(str)+"_"+ df1['Key'].astype(str)
                    
                    df1 = df1.reindex(columns=cols)
                    IPTD1 = IPTD1.reindex(columns=cols)
                    IPTD01 = IPTD01.reindex(columns=cols)
                    IPTD1 = IPTD1.append(IPTD01)
                    IPTD1 = IPTD1.reset_index(drop= True)
                    df1 = df1.drop_duplicates(keep='first')
                    df1 = df1.append(IPTD1)
                    df1.reset_index(inplace = True,drop=True)   
                    
                    df1 = df1.drop(columns=['MM_Flag'])
                    
                    yield df1
                else:
                    IPTD = IPTD.drop(columns=['MM_Flag'])
                    yield IPTD
            else:
                yield IPTD
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'MatchMergeIPTDChild', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))            
#------------------------------------------------ Update Zip Flag to Process_Log file -------------------------------------
class Update_Zipflag_ProcessLog(beam.DoFn):
    def process(self, element, unique_id, APIdf, regime, flag):
        try:
            df = element
            if flag == 1:
                if(len(APIdf)>0):
                    APIdf['ETL_Process_ID']=APIdf["ETL_Process ID"]
                    APIdf = APIdf[APIdf.ETL_Process_ID == unique_id]
                    APIdf = APIdf[APIdf.Regime == regime]
                    APIdf = APIdf.reset_index(drop = True)
                    for i in range(len(df)):
                        if (df.at[i,'GDF_ProcessId'] == unique_id and df.at[i,'Regime'] == regime and (df.at[i,'Load_Status']!= 'Fail' or df.at[i,'Load_Status']!= 'Insufficient File')):
                            if(len(APIdf)>0):
                                for j in range(len(APIdf)):
                                    if(df.at[i,'CTCR_ProcessId'] == APIdf.at[j,"CTCR Process ID"] and APIdf.at[j,"Status Code"]== 200):
                                        df.at[i,'Load_Status'] = 'Success'
                                        df.at[i,'Zip_File'] = 'Y'
                                        now = datetime.now() # current date and time
                                        End_DateTime = now.strftime("%Y-%m-%d %H:%M:%S")
                                        df.at[i,'End_DateTime'] = End_DateTime
                                    
            
            yield df                           
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Update_Zipflag_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))             
                    
#------------------------------------------------ Update Consolidation Flag to Process_Log file -------------------------------------
class Update_Consolidation_ProcessLog(beam.DoFn):    
    def process(self, element, unique_id, filedf, regime):
        try:
            df = element
            consolidated_file_list = []
            if(len(filedf)>0):
                consolidated_file_array = filedf["SourceFileName"].unique()
                consolidated_file_list = consolidated_file_array.tolist()
            for i in range(len(df)):
                if (df.at[i,'GDF_ProcessId'] == unique_id and df.at[i,'Regime'] == regime and df.at[i,'Consolidation']!= 'Y'):
                    if(df.at[i,'Source_File_Name'] in consolidated_file_list):
                        df.at[i,'Consolidation'] = 'Y'
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Update_Consolidation_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#------------------------------------------------ Identify invalid file set to Process_Log file -------------------------------------
class Update_Valid_File_ProcessLog(beam.DoFn):
    def process(self, element, unique_id, processdf, logdf, regime):
        try:
            df = element
            Valid_Process_Array = processdf["CTCR Process ID"].unique()
            Valid_Process_List = Valid_Process_Array.tolist()
            
            Invalid_File_Array = logdf["Source_Filename"].unique()
            Invalid_File_List = Invalid_File_Array.tolist()
            
            for i in range(len(df)):
                if (df.at[i,'GDF_ProcessId'] == unique_id and df.at[i,'Regime'] == regime):
                    
                    if(df.at[i,'CTCR_ProcessId'] in Valid_Process_List and df.at[i,'Source_File_Name'] not in Invalid_File_List):
                        df.at[i,'File_Validation'] = 'Y'
                    elif(df.at[i,'CTCR_ProcessId'] not in Valid_Process_List):
                        if(str(df.at[i,'Load_Status']).upper()=='NAN'):
                            df.at[i,'Load_Status'] = 'Fail'
                            now = datetime.now() # current date and time
                            End_DateTime = now.strftime("%Y-%m-%d %H:%M:%S")
                            df.at[i,'End_DateTime'] = End_DateTime
                            if(df.at[i,'Source_File_Name'] in Invalid_File_List):
                                df.at[i,'File_Validation'] = 'N'   
                            else:
                                df.at[i,'File_Validation'] = 'Y'
            yield df    
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Update_Valid_File_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#------------------------------------------------ Identify insufficient file set to Process_Log file -------------------------------------
class Update_Valid_CTCR_ProcessLog(beam.DoFn):
    def process(self, element, unique_id, processdf, regime):
        try:
            df = element
            Valid_Process_Array = processdf["CTCR Process ID"].unique()
            Valid_Process_List = Valid_Process_Array.tolist()
    
            for i in range(len(df)):
                if (df.at[i,'GDF_ProcessId'] == unique_id and df.at[i,'Regime'] == regime):
                    if(len(Valid_Process_List)>0):
                        if(df.at[i,'CTCR_ProcessId'] in Valid_Process_List):
                            continue
                        else:
                            if (df.at[i,'Load_Status'] != 'Fail' and df.at[i,'File_Found_FIL'] != 'N'):
                                df.at[i,'Load_Status'] = 'Insufficient File'
                                now = datetime.now() # current date and time
                                End_DateTime = now.strftime("%Y-%m-%d %H:%M:%S")
                                df.at[i,'End_DateTime'] = End_DateTime
                    else:
                        if (df.at[i,'Load_Status'] != 'Fail' and df.at[i,'File_Found_FIL'] != 'N'):
                            df.at[i,'Load_Status'] = 'Insufficient File'
                            now = datetime.now() # current date and time
                            End_DateTime = now.strftime("%Y-%m-%d %H:%M:%S")
                            df.at[i,'End_DateTime'] = End_DateTime
            yield df    
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Update_Valid_CTCR_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#------------------------------------------------ Update FIL Reference to Process_Log file -------------------------------------
class UpdateFIL_Reference_ProcessLog(beam.DoFn):
   
    def process(self, element, unique_id, fil):
        try:
            df = element
    
            for i in range(len(df)):
                if (df.at[i,'GDF_ProcessId'] == unique_id):
                    blob_name = df.at[i,'Source_File_Name'] 
                    TrimmedFileName = blob_name[:-7]
                                
                    fil_rec = fil[fil["MDT_Filename"]== TrimmedFileName]
                    fil_rec = fil_rec.reset_index(drop = True)
                    if(len(fil_rec)>0):
                        df.at[i,'File_Found_FIL'] = 'Y'
                        df.at[i,'CTCR_ProcessId'] = fil_rec.at[0,'CTCR Process ID']
                        if(fil_rec.at[0,'File_Received']==1):
                            df.at[i,'Inventory_Lookup'] = 'Y'
                        if(fil_rec.at[0,'File_Received']!=1):
                            df.at[i,'Inventory_Lookup'] = 'N'
                                                
                    else:
    
                        df.at[i,'File_Found_FIL'] = 'N'
                        now = datetime.now() # current date and time
                        End_DateTime = now.strftime("%Y-%m-%d %H:%M:%S")
                        df.at[i,'End_DateTime'] = End_DateTime
                        df.at[i,'Load_Status'] = 'Fail'
                        
            yield df    
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'UpdateFIL_Reference_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#------------------------------------------------ Add filenames,gdf_process_id to Process_Log file -------------------------------------
class Initialise_ProcessLog(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self, element, unique_id, etl_start_datetime):
        try:    
            df = element
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=input_prefix, delimiter=delimiter)
            
            fileseq = 1
            if(len(df)!= 0): #If process_log is empty, start from 0, else start from next row
                i = len(df)
            else:
                i = 0        
            
            for blob in blobs:
            
                blobpath = blob.name
                blob_path = blobpath.split('/')
                blob_name = blob_path[-1]
                TrimmedFileName = blob_name[:-7]
                now = datetime.now() # current date and time
                date_time = now.strftime("%Y%m%d%H%M%S")
                fileseq_str = str(fileseq)
                File_Process_Id = date_time + fileseq_str
                if(blob_name!=''):
                    df.at[i,'Source_File_Name'] = blob_name
                    df.at[i,'Source_Key_Name'] = TrimmedFileName
                    df.at[i,'GDF_ProcessId'] = unique_id
                    df.at[i,'File_Process_Id'] = File_Process_Id
                    df.at[i,'Start_DateTime'] = etl_start_datetime
                    df.at[i,'Regime'] = blob_name[0:3]
                    i = i+1
                    fileseq = fileseq + 1
            yield df    
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Initialise_ProcessLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#------------------------------------------------ Read or intialise Recon_Log file -------------------------------------
class Read_ReconLog_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self, something, filename):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=recon_prefix, delimiter=delimiter)
        recon_log = pd.DataFrame()
        try:
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    recon_log = pd.read_csv(BytesIO(fwf_data), header=[0])
                    recon_log = recon_log.fillna('')
                    recon_log = recon_log.applymap(str)

            if len(recon_log)<1:
                column_names = ['ReportingYear','Regime','Institution_Id',"CTCR Process ID","GDF Process Id","Process_Start_Run_Time",'File_Type','Count_Desc','Count']

                recon_log = pd.DataFrame(columns = column_names)
                recon_log = recon_log.applymap(str)
            yield recon_log
          

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_ReconLog_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 

#------------------------------------Update Recon log and write back to storage----------------------------       
class ReconciliationUpd(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    def process(self,element,IPAccount,IPIP,Unique_ID,regime,StartTime,recon_log):
        try:
            if element == 1:
                Final_Recon_DF = pd.DataFrame()
                            
                if (len(IPAccount)>0):
                    IPAccount_df = IPAccount.astype(str)
                    IPAccount_df['Effective_From_Date'] =  pd.to_datetime(IPAccount_df['Effective_From_Date'], format='%Y-%m-%d', errors='coerce').fillna("")
                    IPAccount_df = IPAccount_df[IPAccount_df["Effective_From_Date"]!= ""]
                    IPAccount_df['ReportingYear'] = pd.DatetimeIndex(IPAccount_df['Effective_From_Date']).year.astype(int).astype(str)
                    ReportingYear_Array = IPAccount_df['ReportingYear'].unique()
                    ReportingYear_List = ReportingYear_Array.tolist()
                    if(len(ReportingYear_List)>0):
                        ReportingYear = ReportingYear_List[0]
                    else:
                        ReportingYear = ''
                    IPAccount_NR = IPAccount.groupby(["CTCR Process ID",'Institution_Id']).size().reset_index(name='Count')
                    IPAccount_NR['Count_Desc'] = '# of Records'
                    IPAccount_NR['File_Type'] = 'IP_Acct'
                    
                    IPAccount_CA = IPAccount.groupby(["CTCR Process ID",'Institution_Id']).Account_Id.nunique().reset_index(name='Count')
                    IPAccount_CA['Count_Desc'] = '# of Account'
                    IPAccount_CA['File_Type'] = 'IP_Acct'
                    
                    IPAccount_CI = IPAccount.groupby(["CTCR Process ID",'Institution_Id']).Involved_Party_Id.nunique().reset_index(name='Count')
                    IPAccount_CI['Count_Desc'] = '# of IP'
                    IPAccount_CI['File_Type'] = 'IP_Acct'
                    
                    IPAccount_Recon_df = pd.DataFrame()
                    IPAccount_Recon_df = IPAccount_Recon_df.append(IPAccount_NR)
                    IPAccount_Recon_df = IPAccount_Recon_df.append(IPAccount_CA)
                    IPAccount_Recon_df = IPAccount_Recon_df.append(IPAccount_CI)
                    IPAccount_Recon_df = IPAccount_Recon_df.reset_index(drop = True)
                    Final_Recon_DF = Final_Recon_DF.append(IPAccount_Recon_df)
                    Final_Recon_DF['ReportingYear'] = ReportingYear
                    Final_Recon_DF = Final_Recon_DF.reset_index(drop = True)
                    
                if (len(IPIP)>0):
                    
                    IPIP_df = IPIP.astype(str)
                    IPIP_df['Effective_From_Date'] =  pd.to_datetime(IPIP_df['Effective_From_Date'], format='%Y-%m-%d', errors='coerce').fillna("")
                    IPIP_df = IPIP_df[IPIP_df["Effective_From_Date"]!= ""]
                    IPIP_df['ReportingYear'] = pd.DatetimeIndex(IPIP_df['Effective_From_Date']).year.astype(int).astype(str)
                    ReportingYear_Array = IPIP_df['ReportingYear'].unique()
                    ReportingYear_List = ReportingYear_Array.tolist()
                    if(len(ReportingYear_List)>0):
                        ReportingYear = ReportingYear_List[0]
                    else:
                        ReportingYear = ''
                    
                    IPIP_NR = IPIP.groupby(["CTCR Process ID",'Institution_Id']).size().reset_index(name='Count')
                    IPIP_NR['Count_Desc'] = '# of Records'
                    IPIP_NR['File_Type'] = 'IP_IP'
                    
                    IPIP_PP = IPIP.groupby(["CTCR Process ID",'Institution_Id']).Parent_Party_Id.nunique().reset_index(name='Count')
                    IPIP_PP['Count_Desc'] = '# of Parent_Id'
                    IPIP_PP['File_Type'] = 'IP_IP'
                    
                    IPIP_CP = IPIP.groupby(["CTCR Process ID",'Institution_Id']).Child_Party_Id.nunique().reset_index(name='Count')
                    IPIP_CP['Count_Desc'] = '# of Child_Id'
                    IPIP_CP['File_Type'] = 'IP_IP'
                    
                    IPIP_Recon_df = pd.DataFrame()
                    IPIP_Recon_df = IPIP_Recon_df.append(IPIP_NR)
                    IPIP_Recon_df = IPIP_Recon_df.append(IPIP_PP)
                    IPIP_Recon_df = IPIP_Recon_df.append(IPIP_CP)
                    IPIP_Recon_df = IPIP_Recon_df.reset_index(drop = True)
    
                    Final_Recon_DF = Final_Recon_DF.append(IPIP_Recon_df)
                    Final_Recon_DF['ReportingYear'] = ReportingYear
                    Final_Recon_DF = Final_Recon_DF.reset_index(drop = True)
                
                Final_Recon_DF['Regime'] = regime
                Final_Recon_DF["GDF Process Id"] = Unique_ID
                Final_Recon_DF["Process_Start_Run_Time"] = StartTime
                
                Final_Recon_DF = Final_Recon_DF[['ReportingYear','Regime','Institution_Id',"CTCR Process ID","GDF Process Id","Process_Start_Run_Time",'File_Type','Count_Desc','Count']]
                recon_log = recon_log.append(Final_Recon_DF)
                recon_log = recon_log.reset_index(drop = True)
    
            if len(recon_log)>0:        
                storage_client = storage.Client()
                bucket_name = self.input_path
                bucket = storage_client.get_bucket(bucket_name)
                filename1 = 'Reconciliation_Log.csv'
                destination_blob_name = filename1
                blob = bucket.blob(recon_prefix + destination_blob_name)
                recon_log.to_csv(filename1, index=False)
                blob.upload_from_filename(filename1)
    
            yield recon_log 

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'ReconciliationUpd', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))     

#------------------------------------------------ Read Process_Log file -------------------------------------
class Read_ProcessLog_csv(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self, something, filename):
        delimiter='/'
        storage_client = storage.Client()
        bucket_name = self.input_path
        blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)
        process_log = pd.DataFrame()
        try:
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    process_log = pd.read_csv(BytesIO(fwf_data), header=[0])
                    process_log = process_log.fillna('')
                    process_log = process_log.applymap(str)

            if len(process_log)<1:
                column_names = ['Regime', 'GDF_ProcessId', 'CTCR_ProcessId','File_Process_Id','Start_DateTime','End_DateTime','Source_File_Name','Source_Key_Name','File_Found_FIL','Inventory_Lookup','File_Validation','Consolidation','Zip_File','Load_Status']

                process_log = pd.DataFrame(columns = column_names)
                process_log = process_log.applymap(str)
            yield process_log
          

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_ProcessLog_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj)) 
			
			
class Write_Record_log(beam.DoFn):
    def __init__(self, landing):
        self.landing = landing

    def process(self, element):
        try:

            delimiter = '/'
            storage_client = storage.Client()
            Record_Log_bucket_name = self.landing
            Record_log_bucket = storage_client.get_bucket(Record_Log_bucket_name)
            blobs_record_level_log = storage_client.list_blobs(Record_log_bucket,prefix=log_prefix, delimiter=delimiter)
            
            record_level_log_columns = ['ETL_ProcessId','CTCR Process ID','Regime','Source System','Source_Filename','ETL_Start_DateTime','ETL_End_DateTime','Error_ID','Validation field','Current Value','Primary Key','CSV File Index'] 
            Record_log = pd.DataFrame(columns = record_level_log_columns)
            for blob in blobs_record_level_log:
                if 'RECORD_LEVEL_LOG.csv' in blob.name:
                    Record_log_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    Record_log = pd.read_csv(BytesIO(Record_log_data), header=[0])
                    
        

            Record_log = Record_log.append(element)
            
               
            Record_log.to_csv('RECORD_LEVEL_LOG.csv',index=False)
            record_blob = Record_log_bucket.blob(log_prefix+"RECORD_LEVEL_LOG.csv")
            record_blob.upload_from_filename('RECORD_LEVEL_LOG.csv') 
            yield Record_log
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_Record_log', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


#---------------------------Split and merge FIxes--------------------------------------------
class CRS_ProcessAHLength_Flag(beam.DoFn):
    

    def process(self,element):
        
        try:
            
            if len(element) >= 1000:
                flag = 1
            else:
                flag = 0

            yield flag
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_ProcessAHLength_Flag', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FATCA_ProcessAHLength_Flag(beam.DoFn):
    

    def process(self,element):
        
        try:
            
            if len(element) >= 1000:
                flag = 1
            else:
                flag = 0

            yield flag
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_AHLength_Flag', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
class Splitandjoin_AHData(beam.DoFn):
    def process(self, element, join_with,leftkey,rightkey,flag,serial_no):
    
        numberOfRows = len(element)
        try:
            if flag == 1:
    
                if serial_no == 1:
        
                    df1 = element[0:numberOfRows//10]
                    
        
                elif serial_no == 2:
        
                    df1 = element[numberOfRows//10:(numberOfRows//10)*2]
        
                elif serial_no == 3:
        
                    df1 = element[(numberOfRows//10)*2:(numberOfRows//10)*3]
                
                elif serial_no == 4:
        
                    df1 = element[(numberOfRows//10)*3:(numberOfRows//10)*4]
        
                elif serial_no == 5:
        
                    df1 = element[(numberOfRows//10)*4:(numberOfRows//10)*5]
        
                elif serial_no == 6:
        
                    df1 = element[(numberOfRows//10)*5:(numberOfRows//10)*6]
                elif serial_no == 7:
        
                    df1 = element[(numberOfRows//10)*6:(numberOfRows//10)*7]
                    
                elif serial_no == 8:
        
                    df1 = element[(numberOfRows//10)*7:(numberOfRows//10)*8]
        
                elif serial_no == 9:
        
                    df1 = element[(numberOfRows//10)*8:(numberOfRows//10)*9]
        
                elif serial_no == 10:
        
                    df1 = element[(numberOfRows//10)*9:(numberOfRows//10)*10]
                elif serial_no == 11:
        
                    df1 = element[(numberOfRows//10)*10:]
        
                df_list = [df1, join_with]  
                df = reduce(lambda  left,right: pd.merge(left,right,left_on=leftkey, right_on=rightkey,how='inner'), df_list)
        
            else:
                df = pd.DataFrame()
        
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Splitandjoin_AHData', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
            
class SplitandLeftjoin_RData(beam.DoFn):
    def process(self, element, join_with,key,flag,serial_no):
    
        numberOfRows = len(element)
        try:
            if flag == 1:
    
                if serial_no == 1:
        
                    df1 = element[0:numberOfRows//10]
                    
        
                elif serial_no == 2:
        
                    df1 = element[numberOfRows//10:(numberOfRows//10)*2]
        
                elif serial_no == 3:
        
                    df1 = element[(numberOfRows//10)*2:(numberOfRows//10)*3]
                
                elif serial_no == 4:
        
                    df1 = element[(numberOfRows//10)*3:(numberOfRows//10)*4]
        
                elif serial_no == 5:
        
                    df1 = element[(numberOfRows//10)*4:(numberOfRows//10)*5]
        
                elif serial_no == 6:
        
                    df1 = element[(numberOfRows//10)*5:(numberOfRows//10)*6]
                elif serial_no == 7:
        
                    df1 = element[(numberOfRows//10)*6:(numberOfRows//10)*7]
                    
                elif serial_no == 8:
        
                    df1 = element[(numberOfRows//10)*7:(numberOfRows//10)*8]
        
                elif serial_no == 9:
        
                    df1 = element[(numberOfRows//10)*8:(numberOfRows//10)*9]
        
                elif serial_no == 10:
        
                    df1 = element[(numberOfRows//10)*9:(numberOfRows//10)*10]
                elif serial_no == 11:
        
                    df1 = element[(numberOfRows//10)*10:]
        
                df_list = [df1, join_with]  
                df = reduce(lambda  left,right: pd.merge(left,right,on=key,how='left'), df_list)
        
            else:
                df = pd.DataFrame()
        
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'SplitandLeftjoin_RData', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
class NoSplitandjoin_AHData(beam.DoFn):
    def process(self, element, join_with,leftkey,rightkey,flag):
    
        numberOfRows = len(element)
        df = pd.DataFrame()
        try:
            if flag == 0:
        
                df_list = [element, join_with]  
                df = reduce(lambda  left,right: pd.merge(left,right,left_on=leftkey, right_on=rightkey,how='inner'), df_list)

            yield df

                
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'NoSplitandjoin_AHData', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
class NoSplitandleftjoin_RData(beam.DoFn):
    def process(self, element, join_with,key,flag):
    
        numberOfRows = len(element)
        df = pd.DataFrame()
        try:
            if flag == 0:
        
                df_list = [element, join_with]  
                df = reduce(lambda  left,right: pd.merge(left,right,on=key,how='left'), df_list)

            yield df

                
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'NoSplitandleftjoin_RData', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
            
            
            
class CRS_Process_SplitMerge(beam.DoFn):
    def process(self, element, df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,flag):

        
        try:
            if flag == 1:
        
                df_list = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11]
                df = pd.concat(df_list, ignore_index=True)

            else:
                df = element

            yield df
  
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Process_SplitMerge', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class FATCA_Process_SplitMerge(beam.DoFn):
    def process(self, element, df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,flag):

        
        try:
            if flag == 1:
        
                df_list = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11]
                df = reduce(lambda left,right: pd.merge(left,right,how='outer'), df_list)

            else:
                df = element

            yield df
  
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Process_SplitMerge', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))																							 




#------------------------- Orphan Logs ------------------------------------------
def Left_only(left_df,right_df, key):
    df= pd.DataFrame() 
    try:  
        index1 = pd.MultiIndex.from_arrays([left_df[col] for col in key])
        index2 = pd.MultiIndex.from_arrays([right_df[col] for col in key])
        df = left_df[~index1.isin(index2)]
    except:{}
    
    return df
    
def Right_only(left_df,right_df, key):
    df= pd.DataFrame()
    try:
        index1 = pd.MultiIndex.from_arrays([left_df[col] for col in key])
        index2 = pd.MultiIndex.from_arrays([right_df[col] for col in key])
        df = right_df[~index2.isin(index1)]
    except:{}
    return df

class Orphan_rows_join(beam.DoFn):
    def process(self,df1,df2,key,join_type,uid,etl_datetime,regime,link_type):
        try:
            orphan_log = pd.DataFrame(columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID'])
            columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']
            if join_type=='left':
                right_orphan = Right_only(df1,df2,key)
                right_orphan = right_orphan.reset_index()
                try:
                    if len(df1)>0:
                        year = pd.DatetimeIndex(df1['Effective_From_Date']).year[0]
                    else:
                        year = ''
                except: 
                    if len(df2)>0:
                        year = pd.DatetimeIndex(df2['Effective_From_Date']).year[0]
                    else:
                        year = ''

                if len(right_orphan)>0:
                    #'CTCR Process ID'		
                    right_orphan['ETL_Process ID'] = uid
                    right_orphan['ETL_Start_time'] = etl_datetime
                    right_orphan['Regime'] = regime
                    right_orphan['Reporting Year'] = year
                    right_orphan['Linkage Type'] = link_type
                    #'Institution_Id'
                    if 'Involved_Party_Id' in right_orphan.columns:
                        right_orphan['Customer ID'] = right_orphan['Involved_Party_Id']
                    else:
                        right_orphan['Customer ID'] = ''
                    
                    if 'Account_Id' not in right_orphan.columns:
                        right_orphan['Account_Id'] = ''
                        
                    if 'Payment_Id' in right_orphan.columns:
                        right_orphan['Payment ID'] = right_orphan['Payment_Id']
                    else:
                        right_orphan['Payment ID'] = ''
                        
                    if 'Parent_Party_Id' in right_orphan.columns:
                        right_orphan['Parent ID'] = right_orphan['Parent_Party_Id']
                    else:
                        right_orphan['Parent ID'] = ''
                    
                    if 'Child_Party_Id' in right_orphan.columns:
                        right_orphan['Child ID'] = right_orphan['Child_Party_Id']
                    else:
                        right_orphan['Child ID'] = ''
                    yield right_orphan[columns]
            else:
                yield orphan_log
                '''
                if len(right_orphan)>0:
                    for i in range(len(right_orphan)):
                        log = list()
                        log.append(right_orphan.at[i,'CTCR Process ID'])
                        log.append(uid)
                        log.append(etl_datetime)
                        log.append(regime)
                        log.append(year)
                        log.append(link_type)
                        log.append(right_orphan.at[i,'Institution_Id'])
                        if 'Involved_Party_Id' in right_orphan.columns  :
                            log.append(right_orphan.at[i,'Involved_Party_Id'])
                        else:
                            log.append('')

                        if 'Account_Id' in right_orphan.columns :    
                            log.append(right_orphan.at[i,'Account_Id'])
                        else:
                            log.append('')

                        if 'Payment_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Payment_Id'])
                        else:
                            log.append('')

                        if 'Parent_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Parent_Party_Id'])
                        else:
                            log.append('')

                        if 'Child_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Child_Party_Id'])
                        else:
                            log.append('')

                        log_series = pd.Series(log, index = orphan_log.columns)
                        orphan_log = orphan_log.append(log_series, ignore_index=True)
                '''
            
                            

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Orphan_rows_join', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))



def join_diff_key(left_df,right_df, key1,key2):    
    df= pd.DataFrame() 
    try:
        index1 = pd.MultiIndex.from_arrays([left_df[col] for col in key1])
        index2 = pd.MultiIndex.from_arrays([right_df[col] for col in key2])
        df = left_df[~index1.isin(index2)]
    except:{}

    return df

class Orphan_rows_inner_join(beam.DoFn):
    def process(self,df1,df2,key1,key2,join_type,uid,etl_datetime,regime,link_type,updated_type):
        try:
            log = list()
            orphan_log = pd.DataFrame(columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID'])
            columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']
            if join_type=='inner':
                left_orphan = join_diff_key(df1,df2,key1,key2)
                right_orphan = join_diff_key(df2,df1,key2,key1)
                right_orphan = right_orphan.reset_index()
                left_orphan =  left_orphan.reset_index()   

                try:
                    year = pd.DatetimeIndex(df1['Effective_From_Date']).year[0]
                except: 
                    try:
                        year = pd.DatetimeIndex(df2['Effective_From_Date']).year[0]
                    except:
                        year = ''

                if len(right_orphan)>0 and (updated_type == 'None' or updated_type == 'Left'):
                    #'CTCR Process ID'		
                    right_orphan['ETL_Process ID'] = uid
                    right_orphan['ETL_Start_time'] = etl_datetime
                    right_orphan['Regime'] = regime
                    right_orphan['Reporting Year'] = year
                    right_orphan['Linkage Type'] = link_type
                    #'Institution_Id'
                    if 'Involved_Party_Id' in right_orphan.columns:
                        right_orphan['Customer ID'] = right_orphan['Involved_Party_Id']
                    else:
                        right_orphan['Customer ID'] = ''
                    
                    if 'Account_Id' not in right_orphan.columns:
                        right_orphan['Account_Id'] = ''
                        
                    if 'Payment_Id' in right_orphan.columns:
                        right_orphan['Payment ID'] = right_orphan['Payment_Id']
                    else:
                        right_orphan['Payment ID'] = ''
                        
                    if 'Parent_Party_Id' in right_orphan.columns:
                        right_orphan['Parent ID'] = right_orphan['Parent_Party_Id']
                    else:
                        right_orphan['Parent ID'] = ''
                    
                    if 'Child_Party_Id' in right_orphan.columns:
                        right_orphan['Child ID'] = right_orphan['Child_Party_Id']
                    else:
                        right_orphan['Child ID'] = ''
                    orphan_log = orphan_log.append(right_orphan[columns])

                    '''
                    for i in range(len(right_orphan)):
                        log = list()
                        log.append(right_orphan.at[i,'CTCR Process ID'])
                        log.append(uid)
                        log.append(etl_datetime)
                        log.append(regime)
                        log.append(year)
                        log.append(link_type)
                        log.append(right_orphan.at[i,'Institution_Id'])
                        if 'Involved_Party_Id' in right_orphan.columns  :
                            log.append(right_orphan.at[i,'Involved_Party_Id'])
                        else:
                            log.append('')

                        if 'Account_Id' in right_orphan.columns :    
                            log.append(right_orphan.at[i,'Account_Id'])
                        else:
                            log.append('')

                        if 'Payment_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Payment_Id'])
                        else:
                            log.append('')

                        if 'Parent_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Parent_Party_Id'])
                        else:
                            log.append('')

                        if 'Child_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Child_Party_Id'])
                        else:
                            log.append('')

                        
                        log_series = pd.Series(log, index = orphan_log.columns)
                        orphan_log = orphan_log.append(log_series, ignore_index=True)
                    '''

                if len(left_orphan)>0 and (updated_type == 'None' or updated_type == 'Right'):
                    #'CTCR Process ID'		
                    left_orphan['ETL_Process ID'] = uid
                    left_orphan['ETL_Start_time'] = etl_datetime
                    left_orphan['Regime'] = regime
                    left_orphan['Reporting Year'] = year
                    left_orphan['Linkage Type'] = link_type
                    #'Institution_Id'
                    if 'Involved_Party_Id' in left_orphan.columns:
                        left_orphan['Customer ID'] = left_orphan['Involved_Party_Id']
                    else:
                        left_orphan['Customer ID'] = ''
                    
                    if 'Account_Id' not in left_orphan.columns:
                        left_orphan['Account_Id'] = ''
                        
                    if 'Payment_Id' in left_orphan.columns:
                        left_orphan['Payment ID'] = left_orphan['Payment_Id']
                    else:
                        left_orphan['Payment ID'] = ''
                        
                    if 'Parent_Party_Id' in left_orphan.columns:
                        left_orphan['Parent ID'] = left_orphan['Parent_Party_Id']
                    else:
                        left_orphan['Parent ID'] = ''
                    
                    if 'Child_Party_Id' in left_orphan.columns:
                        left_orphan['Child ID'] = left_orphan['Child_Party_Id']
                    else:
                        left_orphan['Child ID'] = ''
                    orphan_log = orphan_log.append(left_orphan[columns])

                    '''
                    for i in range(len(left_orphan)):
                        log = list()
                        log.append(left_orphan.at[i,'CTCR Process ID'])
                        log.append(uid)
                        log.append(etl_datetime)
                        log.append(regime)
                        log.append(year)
                        log.append(link_type)
                        log.append(left_orphan.at[i,'Institution_Id'])
                        if 'Involved_Party_Id' in left_orphan.columns  :
                            log.append(df1.at[i,'Involved_Party_Id'])
                        else:
                            log.append('')

                        if 'Account_Id' in left_orphan.columns :    
                            log.append(left_orphan.at[i,'Account_Id'])
                        else:
                            log.append('')

                        if 'Payment_Id' in left_orphan.columns :
                            log.append(left_orphan.at[i,'Payment_Id'])
                        else:
                            log.append('')

                        if 'Parent_Party_Id' in left_orphan.columns :
                            log.append(left_orphan.at[i,'Parent_Party_Id'])
                        else:
                            log.append('')

                        if 'Child_Party_Id' in left_orphan.columns :
                            log.append(left_orphan.at[i,'Child_Party_Id'])
                        else:
                            log.append('')

                        log_series = pd.Series(log, index = orphan_log.columns)
                        orphan_log = orphan_log.append(log_series, ignore_index=True)
                    '''

            yield orphan_log
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Orphan_rows_inner_join', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            


class Orphan_Acct_holder_Fi(beam.DoFn):
    def process(self,fi,acct_hld,key1,key2,join_type,uid,etl_datetime,regime,link_type):
        try:
            orphan_log = pd.DataFrame(columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID'])
            columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']
            if join_type=='left':
                right_orphan = join_diff_key(acct_hld,fi,key2,key1)
                right_orphan = right_orphan.reset_index()
                if len(right_orphan)>0:
                    right_orphan['CTCR Process ID'] = right_orphan['CTCR Process ID_x']		
                    right_orphan['ETL_Process ID'] = uid
                    right_orphan['ETL_Start_time'] = etl_datetime
                    right_orphan['Regime'] = regime
                    try:
                        right_orphan['Reporting Year'] = pd.DatetimeIndex(right_orphan['Accounts_ReportingYear']).year[0]
                    except:
                        right_orphan['Reporting Year'] = ''
                    right_orphan['Linkage Type'] = link_type
                    right_orphan['Institution_Id'] = right_orphan['FI_ID']
                    if 'Accounts_Customer_ID' in right_orphan.columns:
                        right_orphan['Customer ID'] = right_orphan['Accounts_Customer_ID']
                    else:
                        right_orphan['Customer ID'] = ''
                    
                    if 'Accounts_Account_Number' in right_orphan.columns:
                        right_orphan['Account_Id'] = right_orphan['Accounts_Account_Number']
                    else:
                        right_orphan['Account_Id'] = ''
                        
                    if 'Payment_Id' in right_orphan.columns:
                        right_orphan['Payment ID'] = right_orphan['Payment_Id']
                    else:
                        right_orphan['Payment ID'] = ''
                        
                    if 'Parent_Party_Id' in right_orphan.columns:
                        right_orphan['Parent ID'] = right_orphan['Parent_Party_Id']
                    else:
                        right_orphan['Parent ID'] = ''
                    
                    if 'Child_Party_Id' in right_orphan.columns:
                        right_orphan['Child ID'] = right_orphan['Child_Party_Id']
                    else:
                        right_orphan['Child ID'] = ''
                    orphan_log = orphan_log.append(right_orphan[columns])

                    '''
                    for i in range(len(right_orphan)):
                        log = list()
                        log.append(right_orphan.at[i,'CTCR Process ID_x'])
                        log.append(uid)
                        log.append(etl_datetime)
                        log.append(regime)
                        try:
                            log.append(pd.DatetimeIndex(right_orphan['Accounts_ReportingYear']).year[0])
                        except:
                            log.append('')
                        log.append(link_type)
                        log.append(right_orphan.at[i,'FI_ID'])
                        if 'Accounts_Customer_ID' in right_orphan.columns  :
                            log.append(right_orphan.at[i,'Accounts_Customer_ID'])
                        else:
                            log.append('')

                        if 'Accounts_Account_Number' in right_orphan.columns :    
                            log.append(right_orphan.at[i,'Accounts_Account_Number'])
                        else:
                            log.append('')

                        if 'Payment_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Payment_Id'])
                        else:
                            log.append('')

                        if 'Parent_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Parent_Party_Id'])
                        else:
                            log.append('')

                        if 'Child_Party_Id' in right_orphan.columns :
                            log.append(right_orphan.at[i,'Child_Party_Id'])
                        else:
                            log.append('')

                        log_series = pd.Series(log, index = orphan_log.columns)
                        orphan_log = orphan_log.append(log_series, ignore_index=True)
                        '''

            yield orphan_log

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Orphan_Acct_holder_Fi', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class Ip_Ipip_IpAcct_Orphan(beam.DoFn):
    def process(self,df1,df2,df3,uid,etl_datetime,regime,link_type):
        try:
            
            orphan_log = pd.DataFrame(columns =['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID'])
            columns = ['CTCR Process ID','ETL_Process ID','ETL_Start_time','Regime','Reporting Year','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']
            if len(df1)>0 and len(df2)>0 and len(df3)>0:
                orp= df1[~(((df1['Involved_Party_Id']).isin(df2['Parent_Party_Id']))|((df1['Involved_Party_Id']).isin(df2['Child_Party_Id'])))]
                orp= orp[~((orp['Involved_Party_Id']).isin(df3['Involved_Party_Id']))]
                orp = orp.reset_index()
                try:
                    year = pd.DatetimeIndex(df1['Effective_From_Date']).year[0]
                except:
                    year = ''

                if len(orp)>0:
                    orp['ETL_Process ID'] = uid
                    orp['ETL_Start_time'] = etl_datetime
                    orp['Regime'] = regime
                    orp['Reporting Year'] = year
                    orp['Linkage Type'] = link_type
                    
                    if 'Involved_Party_Id' in orp.columns:
                        orp['Customer ID'] = orp['Involved_Party_Id']
                    else:
                        orp['Customer ID'] = ''
                    
                    if 'Account_Id' not in orp.columns:
                        orp['Account_Id'] = ''
                        
                    if 'Payment_Id' in orp.columns:
                        orp['Payment ID'] = orp['Payment_Id']
                    else:
                        orp['Payment ID'] = ''
                        
                    if 'Parent_Party_Id' in orp.columns:
                        orp['Parent ID'] = orp['Parent_Party_Id']
                    else:
                        orp['Parent ID'] = ''
                    
                    if 'Child_Party_Id' in orp.columns:
                        orp['Child ID'] = orp['Child_Party_Id']
                    else:
                        orp['Child ID'] = ''
                    
                    orphan_log = orphan_log.append(orp[columns])

                    

            yield orphan_log

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Ip_Ipip_IpAcct_Orphan', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class CRS_Orphan_logs_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path
	
    def process(self,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11):
        try:
            delimiter='/'
            filename = 'Orphan_LOG.csv'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)
            df = pd.DataFrame()
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_csv(BytesIO(fwf_data), header=[0])
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blob = bucket.blob(log_prefix + destination_blob_name)
            try:
                df = df.append(pd.DataFrame(e1))
                df = df.append(pd.DataFrame(e2))
                df = df.append(pd.DataFrame(e3))
                df = df.append(pd.DataFrame(e4))
                df = df.append(pd.DataFrame(e5))
                df = df.append(pd.DataFrame(e6))
                df = df.append(pd.DataFrame(e7))
                df = df.append(pd.DataFrame(e8))
                df = df.append(pd.DataFrame(e9))
                df = df.append(pd.DataFrame(e10))
                df = df.append(pd.DataFrame(e11))
            except:{}    
   

            pd.DataFrame(df).to_csv(destination_blob_name, index = False)	
            blob.upload_from_filename(destination_blob_name)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Orphan_logs_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))



class FATCA_Orphan_logs_to_csv(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path
	
    def process(self,e9,e10,e11,e12,e13,e14,e15,e16,e17,e18):
        try:
            delimiter='/'
            filename = 'Orphan_LOG.csv'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)
            df = pd.DataFrame()
            for blob in blobs:
                if (filename in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_csv(BytesIO(fwf_data), header=[0])
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blob = bucket.blob(log_prefix + destination_blob_name)
   
            try:
                df = df.append(pd.DataFrame(e9))
                df = df.append(pd.DataFrame(e10))
                df = df.append(pd.DataFrame(e11))
                df = df.append(pd.DataFrame(e12))
                df = df.append(pd.DataFrame(e13))
                df = df.append(pd.DataFrame(e14))
                df = df.append(pd.DataFrame(e15))
                df = df.append(pd.DataFrame(e16))
                df = df.append(pd.DataFrame(e17))
                df = df.append(pd.DataFrame(e18))
            except:{}    

            pd.DataFrame(df).to_csv(destination_blob_name, index = False)	
            blob.upload_from_filename(destination_blob_name)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Orphan_logs_to_csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))





#-------------------------------------------------------Orphan log end---------------------------------



#-------------------------------------------FATCA REPORTABILITY CHECK FOR AH AND CP-------------------------------------------

class FATCA_Reportable_CP(beam.DoFn):
    def process(self,element):
        try:
            df = element
            AHtypecheck = ['FATCA101','F101','FATCA102','F102','Passive Non Financial Entity with Controlling Person(s)','Owner documented Financial Institution']
            df = df[df['Account_Holder_Type'].isin(AHtypecheck)]
            #df = df[['Institution_Id','Account_Id']]
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Reportable_CP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
            
class FATCA_Reportable_PR(beam.DoFn):
    def process(self,element):
        try:
            df = element
            AHtypecheck = ['FATCA201','FATCA202','FATCA203','FATCA204','FATCA205','FATCA206','FATCA204 (for 2015 and 2016 only)']
            df = df[df['Account_Holder_Type'].isin(AHtypecheck)]
            #df = df[['Institution_Id','Account_Id']]
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Reportable_PR', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FATCA_Reportable_AH(beam.DoFn):
    def process(self,element):
        try:
            df = element
            AHtypecheck = ['FATCA101','F101','FATCA102','F102','Passive Non Financial Entity with Controlling Person(s)','Owner documented Financial Institution','FATCA201','FATCA202','FATCA203','FATCA204','FATCA205','FATCA206','FATCA204 (for 2015 and 2016 only)']
            df = df[(~df['Account_Holder_Type'].isin(AHtypecheck))]
            #df = df[['Institution_Id','Account_Id']]
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Reportable_AH', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FATCA_Rel_type(beam.DoFn):
    def process(self,element):
        try:
            df = element
            df['BO_Flag'] = df['Relationship_Type'].apply(lambda x : 'Y' if x == 'BO' else 'N')
            yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_Rel_type', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FATCA_ProcessReportability_CP(beam.DoFn):
    
    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,fi_df):
        
        try:
            prefix = CI_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            for blob in blobs:
                if ("FATCA_classification_codes_lookup_file" in blob.name):
                    fwf_data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    fatca_lookup = pd.read_excel(BytesIO(fwf_data),header = [7],keep_default_na=False,engine='xlrd')


            df_ci = fi_df[['Abbrev. (Insitution ID)                 (8 char limit)','Country of Tax Residence','FATCA Regime (FFI/IGA 1/IGA 2)']]
            df = element

            #no_repo  = {'non-reportable':'Non-reportable as per classification logic','N/A':'Not Applicable','???':'Not Applicable','Could not classify':'Non-reportable as unable to classify'}
            #no_repo_list = list(no_repo.keys())
            #no_repo_list

            repo_cp  = {'F104':'F104','FATCA104':'FATCA104','No reporting code for Individuals (Reportable)':'No reporting code for Individuals (Reportable)','Reportable Person (Individual)':'Reportable Person (Individual)'}
            repo_cp_list = list(repo_cp.keys())
            
            df_list = [df, df_ci]
            df = reduce(lambda  left,right: pd.merge(left,right,left_on=['Institution_Id'], right_on=['Abbrev. (Insitution ID)                 (8 char limit)'],how='inner'), df_list)

            #df= pd.merge(df, df_ci, how='inner', left_on=['Institution_Id'], right_on=['Abbrev. (Insitution ID)                 (8 char limit)'])
            #idx_non_repo = list()
            
            
            df_opt = df[['FATCA_Classification','FATCA_Sub-Classification','FATCA Regime (FFI/IGA 1/IGA 2)','SubsOwner_Indivdual_Organisation_Indicator','Account_Holder_Dormancy_Status','Country of Tax Residence','ReportingYear','BO_Flag']].drop_duplicates()
            df_opt['Reportable'] = ''
            df_opt['Non-Reporting Reason'] = ''
            df_opt["Classification_Code"] =""
            df_opt['Decision Branch'] = ""

            Ip_Type_list = {'Individual':'I',"Organisation":'E'}
            CustomerDormancy_Type_list = {'Yes':'Y',"No":'N OR [Spaces]'}

            #category 2:

            for idx, row in df_opt.iterrows():

                class_df1 = fatca_lookup[fatca_lookup['HSBC Class Code']=='[Other]']
                class_df = fatca_lookup[fatca_lookup['HSBC Class Code'].str.strip() == str(row['FATCA_Classification']).strip()]
                if len(class_df)>=1:
                    class_df1 = class_df

                if len(class_df1)>=1:
                    subclass_df1 = class_df1[class_df1['HSBC Sub-Class Code'].isin(['[All]','[none] or [Other]',''])]
                    subclass_df = class_df1[class_df1['HSBC Sub-Class Code'].str.strip() == str(row["FATCA_Sub-Classification"]).strip()]
                    if len(subclass_df)>=1:
                        subclass_df1 = subclass_df

                    

                if len(subclass_df1)>=1:
                    taxregime_df1 = subclass_df1[subclass_df1['Tax Regime'].isin(['[All]'])]
                    taxregime_df = subclass_df1[subclass_df1['Tax Regime'].replace(" ",'')== str(row['FATCA Regime (FFI/IGA 1/IGA 2)']).replace(" ",'')]
                    if len(taxregime_df)>=1:
                        taxregime_df1 = taxregime_df

                    

                if len(taxregime_df1)>=1:
                    eylegalcategory_df1 = taxregime_df1[taxregime_df1['EY Legal Category'].isin(['[All]'])]
                                                                           
                    if str(row['SubsOwner_Indivdual_Organisation_Indicator']) in ['Organisation','Individual']:
                        eylegalcategory_df = taxregime_df1[taxregime_df1['EY Legal Category'] == Ip_Type_list[row['SubsOwner_Indivdual_Organisation_Indicator']]]
                    else:
                        eylegalcategory_df = eylegalcategory_df1
                         
                                                                

                    if len(eylegalcategory_df)>=1:
                        eylegalcategory_df1 = eylegalcategory_df    

                    

                if len(eylegalcategory_df1)>=1:
                    custdormancy_df1 = eylegalcategory_df1[eylegalcategory_df1['Customer Dormancy flag'].isin(['[All]'])]
                    
                                                                  
                    if str(row['Account_Holder_Dormancy_Status']) in ['Yes','No']:
                        custdormancy_df = eylegalcategory_df1[eylegalcategory_df1['Customer Dormancy flag'] == CustomerDormancy_Type_list[row['Account_Holder_Dormancy_Status']]]
                    else:
                        custdormancy_df = custdormancy_df1     
                                                                


                    if len(custdormancy_df)>=1:
                        custdormancy_df1 = custdormancy_df  
                    
                    try:
                        x = custdormancy_df1[row['Country of Tax Residence']].values[0]
                        df_opt.at[idx,'Classification_Code'] = custdormancy_df1[row['Country of Tax Residence']].values[0]
                    except:
                        df_opt.at[idx,'Classification_Code'] = 'Could not classify as country is not present in FATCA Classification file'
                           
                                                                                                                                      
                        
                     
                else:
                    df_opt.at[idx,'Classification_Code'] = 'Could not classify'

            df_opt['Reportable'] = df_opt.BO_Flag.apply(lambda x : 'Y' if x == 'Y' else 'N')
            df_opt['Non-Reporting Reason'] = df_opt.BO_Flag.apply(lambda x : 'Relationship Type is not BO' if x == 'N' else '')

            for idx, row in df_opt.iterrows():
                    if  df_opt.loc[idx,'BO_Flag'] == 'Y':
                        if df_opt.loc[idx,'Classification_Code'] in repo_cp_list:
                            df_opt.loc[idx,'Reportable'] = 'Y'
                            df_opt.loc[idx,'Non-Reporting Reason'] = ''
                        else:
                            df_opt.loc[idx,'Reportable'] = 'N'
                            df_opt.loc[idx,'Non-Reporting Reason'] = 'Non Reportable CP due to Classification Logic'
           
            df_opt['Decision Branch'] = df_opt.Reportable.apply(lambda x: 'Classification Logic' if x == 'N' else '' )

            df_list_opt = [df, df_opt]
            df = reduce(lambda  left,right: pd.merge(left,right,how='left'), df_list_opt)
            
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportability_CP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class FATCA_ProcessReportableFinalDF_CP(beam.DoFn):
    def process(self,element):
        try:
            df = element
            if len(df)>0:
                df = df[df['Reportable']!='N']
                df = df.reset_index(drop=True)
                df['Account_Holder_Type'] = df['Classification_Code']
                df['Account_Holder_Type'] = df['Account_Holder_Type'].apply(lambda x: x.replace('No reporting code for Individuals (Reportable)','').replace('(for 2015 and 2016 only)','') if ('No reporting code for Individuals (Reportable)' in x or '(for 2015 and 2016 only)' in x) else x)																									 
   
            df = df.drop(columns=['Country of Tax Residence','FATCA Regime (FFI/IGA 1/IGA 2)','Abbrev. (Insitution ID)                 (8 char limit)','BO_Flag','Classification_Code','Decision Branch','Non-Reporting Reason','Reportable'])

            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportableFinalDF_CP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class FATCA_ProcessNonReportableFinalDF_CP(beam.DoFn):
    def process(self,element,uniqueid,etl_start_datetime):
        try:
            df = element
            
            nonreplog_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nonreplog = pd.DataFrame(columns = nonreplog_columns)

            if len(df)>0:
            
                df = df[df['Reportable']=='N']
                df['Regime'] = 'FTR'
                df['ETL ProcessId'] = uniqueid
                df['ETL_Start_DateTime'] = etl_start_datetime
                df['Account Id'] = ''
                df['Closed_Account'] =''
                df['Account Type'] = ''
                df['Entity Type'] = ''

                df = df.rename(columns= {'ReportingYear':'Reporting Year','Institution_Id':'Institution ID','Customer_ID':'Customer ID','Entity_Name_y':'Customer Entity Name','SubsOwner_First_Name':'Customer First Name','SubsOwner_Last_Name':'Customer Last Name','Non-Reporting Reason':'Reason for Non-Reporting','FATCA_Classification':'Classification','FATCA_Sub-Classification':'Sub Classification','Account_Holder_Dormancy_Status':'Dormant','SubsOwner_Indivdual_Organisation_Indicator':'Category of Account Holder'})
                df = df.reset_index(drop=True)

            nonreplog = nonreplog.append(df,sort=False)
            nonreplog = nonreplog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
            nonreplog = nonreplog.reset_index(drop=True)

            yield nonreplog

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessNonReportableFinalDF_CP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))



class FATCA_ProcessReportability_AHonCP(beam.DoFn):
    def process(self,element,reportablefinalcp):
        try:
            df = element
            if len(df)>0:
                df1 = df
                df2 = reportablefinalcp['Parent_Party_Id'].unique().tolist()

                for idx, row in df1.iterrows():
                    if  df1.loc[idx,'Customer_ID'] in df2:
                        df1.loc[idx,'Reportable'] = 'Y'
                        df1.loc[idx,'Non-Reporting Reason'] = ''
                    else:
                        df1.loc[idx,'Reportable'] = 'N'
                        df1.loc[idx,'Non-Reporting Reason'] = 'No Reportable CP for this account holder'
                
                
            else:
                df1=df
                df1['Reportable'] = ''
                df1['Non-Reporting Reason'] = ''
            
            #df = pd.merge(df1, reportablefinalcp, how='inner', left_on=['Customer_ID'], right_on=['Parent_Party_Id'])
           
            yield df1

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportability_AHonCP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))



class FATCA_ProcessReportability_AHwithCP(beam.DoFn):
    def process(self,element,reportableAH):
        try:
            df = element
            df1 = reportableAH
            if ((len(df) > 0) & ('Y' in df['Reportable'].unique())):
                df = df[df['Reportable'] == 'Y']
                ahcp = df.append(df1)
                ahcp['Reportable'] = 'Y'

                #df = pd.merge(df, df1, how='inner', left_on=['Customer_ID'], right_on=['Parent_Party_Id'])
            elif (len(df1)> 0):
                ahcp = df1
                ahcp['Reportable'] = 'Y'
            else:
                ahcp = df1

            yield ahcp

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportability_AHwithCP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FATCA_ProcessReportability_AHwoCP(beam.DoFn):
    def process(self,element,nrcpdf,nrahdf,uniqueid,etl_start_datetime):
        try:
            df = element
            nonreplog_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nonreplog = pd.DataFrame(columns = nonreplog_columns)

            if ((len(df) > 0) & ('N' in df['Reportable'].unique())):
                df = df[df['Reportable'] == 'N']
                df['Regime'] = 'FTR'
                df['ETL ProcessId'] = uniqueid
                df['ETL_Start_DateTime'] = etl_start_datetime
                df['Entity Type'] = ''
                df = df.rename(columns= {'CTCR Process ID_x':'CTCR Process ID','Account_Id':'Account Id','Account_Closed':'Closed_Account','Account_Type':'Account Type','ReportingYear':'Reporting Year','Institution_Id':'Institution ID','Customer_ID':'Customer ID','Entity_Name':'Customer Entity Name','Individual_Forenames':'Customer First Name','Individual_Surname':'Customer Last Name','Non-Reporting Reason':'Reason for Non-Reporting','FATCA_Classification':'Classification','FATCA_Sub-Classification':'Sub Classification','DormantAccount':'Dormant','Category_of_Account_Holder':'Category of Account Holder'})
                df = df.reset_index(drop=True)
                nonreplog = nonreplog.append(df,sort=False)

                nonreplog = nonreplog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
                nonreplog = nonreplog.reset_index(drop=True)

            log1 = nonreplog.append(nrcpdf)  
                     
            log =  log1.append(nrahdf)

            yield log

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessReportability_AHwoCP', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

                      
class Reportable_Data(beam.DoFn):
    def process(self,element):
        try:

            df = element
            if (len(df)>0):
            #AccountHolder_Reportable
                if 'Accounts_ReportingYear' in df:
                
                    df['Accounts_ReportingYear'] = df['Accounts_ReportingYear'].replace(np.nan,'',regex=True)
                    df['Accounts_Customer_ID'] = df['Accounts_Customer_ID'].replace(np.nan,'',regex=True)
                    df['Accounts_FFI_Name'] = df['Accounts_FFI_Name'].replace(np.nan,'',regex=True)
                    df['Accounts_Account_Number'] = df['Accounts_Account_Number'].replace(np.nan,'',regex=True)

                    rdf = df[(df['Accounts_ReportingYear'] != '') & (df['Accounts_Customer_ID'] != '') & (df['Accounts_Account_Number'] != '') & (df['Accounts_FFI_Name'] != '')]   
                    rdf = rdf.drop(columns=['Entity_Type'])  
                    

			#ControlPerson_Reportable
                elif 'SubsOwner_ReportingYear' in df:
                    df['SubsOwner_ReportingYear'] = df['SubsOwner_ReportingYear'].replace(np.nan,'',regex=True)
                    df['SubsOwner_Customer_ID'] = df['SubsOwner_Customer_ID'].replace(np.nan,'',regex=True)
                    df['SubsOwner_Account_Number'] = df['SubsOwner_Account_Number'].replace(np.nan,'',regex=True)
                    df['SubsOwner_FFI_Name'] = df['SubsOwner_FFI_Name'].replace(np.nan,'',regex=True)

                    rdf = df[(df['SubsOwner_ReportingYear'] != '') & (df['SubsOwner_Customer_ID'] != '') & (df['SubsOwner_Account_Number'] != '') & (df['SubsOwner_FFI_Name'] != '')]
                    rdf = rdf.drop(columns=['CTCR Process ID','Account_Holder_Dormancy_Status_x'])  


            else:
                if 'Accounts_ReportingYear' in df:
                    rdf = df
                    rdf = rdf.drop(columns=['Entity_Type'])  
                if 'SubsOwner_ReportingYear' in df:
                    rdf = df
                    rdf = rdf.drop(columns=['CTCR Process ID','Account_Holder_Dormancy_Status_x'])  

            
            rdf = rdf.reset_index(drop=True)
           
            yield rdf

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Reportable_Data', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class NonReportable_Data(beam.DoFn):
    def process(self,element,uniqueid,etl_start_datetime):
        try:
            nrdf = element
            nonreplog_columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']
            nonreplog = pd.DataFrame(columns = nonreplog_columns)

            nrdf = nrdf.replace(np.nan,'',regex=True)

            if (len(nrdf)>0):

                nrdf['ETL ProcessId'] = uniqueid
                nrdf['Regime'] = 'CRS'
                
                nrdf['ETL_Start_DateTime'] = etl_start_datetime
                nrdf['Classification'] = ''
                nrdf['Sub Classification'] = ''
                
                if 'Accounts_ReportingYear' in nrdf:
                    nrdf['Accounts_ReportingYear'] = nrdf['Accounts_ReportingYear'].replace(np.nan,'',regex=True)
                    nrdf['Accounts_Customer_ID'] = nrdf['Accounts_Customer_ID'].replace(np.nan,'',regex=True)
																											 
                    nrdf = nrdf[(nrdf['Accounts_ReportingYear'] == '') | (nrdf['Accounts_Customer_ID'] == '') | (nrdf['Accounts_Account_Number'] == '') | (nrdf['Accounts_FFI_Name'] == '')]
					
                    nrdf['Reason for Non-Reporting'] = 'Account Holder NonReportable due to blank Reporting year/Customer id/FFI Name/Account Number'																						  

                    nrdf = nrdf.rename(columns= {'Entity_Type':'Entity Type','CTCR Process ID_x':'CTCR Process ID','Accounts_Account_Number':'Account Id','Accounts_Account_Closed':'Closed_Account','Accounts_Account_Holder_Type':'Account Type','Accounts_ReportingYear':'Reporting Year','FI_ID':'Institution ID','Accounts_Customer_ID':'Customer ID','Accounts_Entity_Name':'Customer Entity Name','Accounts_First_Name':'Customer First Name','Accounts_Last_Name':'Customer Last Name','Accounts_DormantAccount':'Dormant','Accounts_Category_of_Account_Holder':'Category of Account Holder'})
                    nrdf = nrdf.reset_index(drop=True)

                elif 'SubsOwner_ReportingYear' in nrdf:
                    nrdf['SubsOwner_ReportingYear'] = nrdf['SubsOwner_ReportingYear'].replace(np.nan,'',regex=True)
                    nrdf['SubsOwner_Customer_ID'] = nrdf['SubsOwner_Customer_ID'].replace(np.nan,'',regex=True)

                    nrdf = nrdf[(nrdf['SubsOwner_ReportingYear'] == '') | (nrdf['SubsOwner_Customer_ID'] == '') | (nrdf['SubsOwner_Account_Number'] == '') | (nrdf['SubsOwner_FFI_Name'] == '')]
                    nrdf['Reason for Non-Reporting'] = 'Substantional Owner NonReportable due to blank Reporting year/Customer id/FFI Name/Account Number'

                    nrdf['Entity Type'] = ''
                    nrdf['Account Type'] = ''
                    nrdf['Closed_Account'] = ''
                    

                    nrdf = nrdf.rename(columns= {'SubsOwner_Account_Number':'Account Id','SubsOwner_ReportingYear':'Reporting Year','FI_ID':'Institution ID','SubsOwner_Customer_ID':'Customer ID','SubsOwner_Account_Holder_Entity_Name':'Customer Entity Name','SubsOwner_First_Name':'Customer First Name','SubsOwner_Last_Name':'Customer Last Name','SubsOwner_Indivdual_Organisation_Indicator':'Category of Account Holder','Account_Holder_Dormancy_Status_x':'Dormant'})
                    nrdf = nrdf.reset_index(drop=True)
                else:
                    pass

            else:
                nrdf = nonreplog
 

            nonreplog = nonreplog.append(nrdf,sort=False)
            nonreplog = nonreplog[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','ETL_Start_DateTime','Classification','Sub Classification','Dormant','Category of Account Holder']]
            nonreplog = nonreplog.reset_index(drop=True)

            yield nonreplog

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'NonReportable_Data', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))                                          

class Data_Format(beam.DoFn):
    def process(self,Acc_df, Pay_df, key):
        try:
            
            df_list = [Pay_df, Acc_df]
            result = reduce(lambda  left,right: pd.merge(left,right,on=key,how='left'), df_list)
            #result = pd.merge(Pay_df, Acc_df, how='left', on=key)
         
            # Fix
            result.loc[result.Account_Type.isin(['DI','EI','CV']), 'Payment_Code'] = 'OTH'

            columns = list(Pay_df.columns)
            result = result.rename(columns= {'index_x':'index','Record_Type_x':'Record_Type','Record_Action_x':'Record_Action','Effective_From_Date_x':'Effective_From_Date', 'Effective_To_Date_x':'Effective_To_Date', 'CompositeKey_x':'CompositeKey', 'CTCR Process ID_x':'CTCR Process ID', 'Source System ID_x':'Source System ID','Source File Index_x':'Source File Index', 'MDT_Country_x':'MDT_Country', 'Key_x':'Key', 'SourceFileName_x':'SourceFileName', 'Stage1_id_x':'Stage1_id', 'Stage1_date_time_x':'Stage1_date_time', 'CSV File Index_x':'CSV File Index'})
            result = result[columns]

            yield result

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Data_Format', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
			
			
class UK_DeMinimus(beam.DoFn):

    OUTPUT_TAG_LOG = 'Log file'

    def process(self,FATCA_AccountwithBalance, FATCA_InvolvedParty_df, FI, BQ_table_id):
        try:
            client = BG.Client()
            name_group_query = """SELECT * FROM `{}ETLDeminimisActive_list`""".format(BQ_table_id)
            query_results = client.query(name_group_query).to_dataframe()
            mask = query_results.applymap(type) != bool
            d = {True: 'TRUE', False: 'FALSE'}
            query_results = query_results.where(mask, query_results.replace(d))
            
            deMinimis_countries = list(query_results.loc[query_results['DeminimisFlag'] == 'TRUE', 'CountryCode'])
            
            CI = FI[['Abbrev. (Insitution ID)                 (8 char limit)', 'Country of Tax Residence']]
            CI = CI.rename(columns= {'Abbrev. (Insitution ID)                 (8 char limit)':'Institution_Id',
                                                'Country of Tax Residence':'Country'})
            
            df_list = [FATCA_AccountwithBalance, CI]
            key = ['Institution_Id']
            result = reduce(lambda  left,right: pd.merge(left,right,on=key,how='left'), df_list)
            
            #===Separate Deminimis and NOT deminimis========
            result1 = result.loc[result['Country'].isin(deMinimis_countries)]
            result_NoMinimis = result.loc[~result['Country'].isin(deMinimis_countries)]  ##Reportable
            
            res_NonD = result1.loc[(result1['Account_Type'] != 'D')]   ##Reportable
            
            res_D = result1.loc[(result1['Account_Type'] == 'D')]
            
            idx_with_USD = res_D.index[res_D['Balance_Currency_Code'] == 'USD'].tolist()
            res_D_with_USD = res_D[res_D.index.isin(idx_with_USD)]   #Further proceed for Deminimis calculation
            
            res_D_non_USD = res_D[~res_D.index.isin(idx_with_USD)]   ##reportable
			
            
            
            ##Non reportable log columns for deMinimis=============
            nonRepoCol = ['CTCR Process ID','Institution ID', 'Customer ID', 'Customer Entity Name','Customer First Name', 'Regime', 
            'Reporting Year', 'Customer Last Name', 'Account Id','Closed_Account', 'Account Type','Reason for Non-Reporting', 'Classification','Sub Classification', 'Dormant', 'Category of Account Holder']
            res_depository_non_reportable = pd.DataFrame(columns=nonRepoCol)
            
            if len(res_D_with_USD) > 0:
                ##=======Joining with IP file=======
                list2 = [FATCA_InvolvedParty_df, res_D_with_USD]
                key2 = ['Institution_Id']
                result2 = reduce(lambda  left,right: pd.merge(left,right,on=key2,how='left'), list2)
                
                #resGB['Balance_Amount'] = resGB['Balance_Amount'].astype('float64')
                
                result2 = result2.rename(columns= {'index_x':'index','Record_Type_x':'Record_Type',
                                        'Record_Action_x':'Record_Action',
                                        'CompositeKey_x':'CompositeKey', 
                                        'CTCR Process ID_x':'CTCR Process ID', 
                                        'Source System ID_x':'Source System ID',
                                        'Source File Index_x':'Source File Index', 
                                        'MDT_Country_x':'MDT_Country', 'Key_x':'Key', 
                                        'SourceFileName_x':'SourceFileName', 
                                        'Stage1_id_x':'Stage1_id', 
                                        'Stage1_date_time_x':'Stage1_date_time', 
                                        'CSV File Index_x':'CSV File Index'})
                
                
                ## ============1st step======================================= 
                
                res_depository = result2
                
                res_depository['count_max'] = res_depository.groupby(['Institution_Id','Involved_Party_Id'])['Balance_Amount'].transform(sum)
                res_depository['count_max'] = res_depository['count_max'].astype('float64')
                res_depository_reportable = res_depository.loc[(res_depository['count_max'] >= 50000)]
                res_depository_non_reportable_data = res_depository.loc[(res_depository['count_max'] < 50000)]
                
                cols = ['index', 'Record_Type', 'Record_Action', 'Institution_Id', 'Account_Id',
                        'Account_Type', 'Account_Sub-Type', 'CompositeKey', 'CTCR Process ID',
                        'Source System ID', 'Source File Index', 'MDT_Country', 'Key',
                        'SourceFileName', 'Stage1_id', 'Stage1_date_time', 'CSV File Index',
                        'DormantAccount', 'Account_Closed', 'Balance_Currency_Code',
                        'Balance_Amount']
						
                outOfScope_deminimis1 = result_NoMinimis.append(res_NonD)
                outOfScope_deminimis2 = outOfScope_deminimis1.append(res_D_non_USD)
                outOfScope_deminimis2 = outOfScope_deminimis2[cols]
				
                res_depository_reportable = res_depository_reportable[cols]
                res_final_reportable = outOfScope_deminimis2.append(res_depository_reportable)
                
                ###====Non reportable log part ====================================
                
                res_depository_non_reportable_data = res_depository_non_reportable_data.rename(columns= {'Institution_Id':'Institution ID',
                                                                                    'Account_Id':'Account Id',
                                                                                    'Account_Type':'Account Type',
                                                                                    'Involved_Party_Id':'Customer ID',
                                                                                    'Entity_Name':'Customer Entity Name',
                                                                                    'Individual_Forenames':'Customer First Name',
                                                                                    'Individual_Surname':'Customer Last Name',
                                                                                    'Account_Closed':'Closed_Account',
                                                                                    'FATCA_Classification':'Classification',
                                                                                    'FATCA_Sub-Classification':'Sub Classification'})
                                                                                    
                res_depository_non_reportable_data['Reason for Non-Reporting'] = "As per Deminimis logic"
                res_depository_non_reportable_data['Regime'] = "FTR"
                res_depository_non_reportable_data['Category of Account Holder'] = res_depository_non_reportable_data.Involved_Party_Type.apply(lambda x: 'Individual' if x == 'I' else ('Organisation' if 'E' else x))
                res_depository_non_reportable_data['Dormant']= res_depository_non_reportable_data.Account_Holder_Dormancy_Status.apply(lambda x: 'Yes' if x == 'D' else 'No')
				
                ##Reporting year 
                res_depository_non_reportable_data['Effective_From_Date'] = pd.DatetimeIndex(res_depository_non_reportable_data['Effective_From_Date'])
                res_depository_non_reportable_data['Reporting Year'] = pd.DatetimeIndex(res_depository_non_reportable_data['Effective_From_Date']).year
                res_depository_non_reportable_data = res_depository_non_reportable_data[nonRepoCol]
                
                yield res_final_reportable
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_LOG,res_depository_non_reportable_data)
                
            else:
                outOfScope_deminimis1 = result_NoMinimis.append(res_NonD)
                outOfScope_deminimis2 = outOfScope_deminimis1.append(res_D_non_USD)
                outOfScope_deminimis2 = outOfScope_deminimis2.drop('Country', 1)
                res_final_reportable = outOfScope_deminimis2
                yield res_final_reportable
                yield pvalue.TaggedOutput(self.OUTPUT_TAG_LOG,res_depository_non_reportable)
            
			
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'UK_DeMinimus', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

			
			
			

class Get_Col(beam.DoFn):

    def process(self,ip_add, key):
        try:
            df = pd.DataFrame()

            if key == 'CA':
                if len(ip_add.columns) > 3:

                    df = ip_add[['Institution_Id','Involved_Party_Id','CA_Tax_Id_type','CA_Tax_Id','CA_Customer_Opening_Date','CTCR Process ID']]
                else:
                    df = pd.DataFrame(columns=['Institution_Id','Involved_Party_Id','CTCR Process ID'])

            elif key == 'QA':
                if len(ip_add.columns) > 3:
                    df = ip_add[['Institution_Id','Involved_Party_Id','Involved_Party_Type','QA Birth Country Code','QA Birth Former Country Name','CTCR Process ID']]
                else:
                    df = pd.DataFrame(columns = ['Institution_Id','Involved_Party_Id','Involved_Party_Type','CTCR Process ID'])

            
            else:
                pass
            
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Get_Col', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class CTCR_rename(beam.DoFn):

    def process(self,element):
        try:
            df = element
            df = df.rename(columns= {'CTCR Process ID_x':'CTCR Process ID'})
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CTCR_rename', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class FTR_FieldRename(beam.DoFn):

    def process(self,element):
        try:
            df = element
            df = df.rename(columns= {'CTCR Process ID':'CTCR Process ID_x','Institution_Id':'FI_ID','Account_Id':'Accounts_Account_Number','ReportingYear':'Accounts_ReportingYear','Customer_ID':'Accounts_Customer_ID'})
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CTCR_rename', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))


class FI_Process_Subset(beam.DoFn):

    def process(self,element):
        try:
            df = element
            df = df[['Abbrev. (Insitution ID)                 (8 char limit)','EAG Entity Legal Name (Full)', 'Name 1 (35 char limit)', 'Name 2  (35 char limit)', 'Country of Tax Residence']]
            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FI_Process_Subset', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
        
                                     
class ProcessExceptionLog(beam.DoFn):

    def process(self,element,ipacct):
        try:
            df = element
            df = df[['ETL_ProcessId','CTCR Process ID','Error_ID','Primary Key','Validation field']]
            df = df[df['Error_ID'].str.contains('_E_', na=False)]
            datalist = []
            dfr = pd.DataFrame(columns = ['ETL_ProcessId','CTCR Process ID','Error_ID','Institution_Id','Account_Id','Customer_Id'])
            if len(df) > 0:
                for index ,line in df.iterrows():
                  line = dict(line)


                  for ele in line:
                  
                    if ele == 'Primary Key':
                      arr0 = line[ele]

                      txt = arr0.replace('[','').replace(']','')
                      x = txt.split(',')

                      for i in x:
                        arr = i.split(';')
                        if line['Validation field'] == 'Payment_Date':
                            datalist.append(
                            {'ETL_ProcessId':line['ETL_ProcessId'],
                            'CTCR Process ID':line['CTCR Process ID'],
                            'Error_ID':line['Error_ID'],
                            'Institution_Id':arr[0].replace("'","").strip()
                            ,'Account_Id':arr[2].strip()
                            #'Payment_Id':arr[3].strip()
                            })
                        else:
                            datalist.append(
                            {'ETL_ProcessId':line['ETL_ProcessId'],
                            'CTCR Process ID':line['CTCR Process ID'],
                            'Error_ID':line['Error_ID'],
                            'Institution_Id':arr[0].replace("'","").strip()
                            ,'Customer_Id':arr[1].strip()
                            #'Payment_Id':arr[3].strip()
                            })

            dfs = pd.DataFrame(datalist)
            dfr = dfr.append(dfs)
            dfr = dfr[['ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id','Customer_Id','Error_ID']].reset_index()
            ipacct = ipacct.rename(columns={"Stage1_id":"ETL_ProcessId","Involved_Party_Id":"Customer_Id"})
            ipacct = ipacct[['ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id','Customer_Id']].reset_index()
                        
																																																											
                                                                                                                                                                                             
																																																											 
                                                                                                                                                                                                                                            
                                                                                                                                                                                              

                                                                                                                                                                                                                                             
           
            df_list1 = [dfr, ipacct]
            log = reduce(lambda  left,right: pd.merge(left,right,on=['ETL_ProcessId','CTCR Process ID','Institution_Id','Customer_Id'],how='left'), df_list1)
            
            if len(log)>0:
                log['Account_Id']= log.apply(lambda x: x['Account_Id_y'] if (len(str(x['Account_Id_y'])) > 0 and str(x['Account_Id_y']).upper()!='NAN') else x['Account_Id_x'], axis=1)
            else:
                log['Account_Id'] = ''
								 
									
																																							 

						   
																																															  
				 
										

            df_list2 = [log, ipacct]
            log1 = reduce(lambda  left,right: pd.merge(left,right,on=['ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id'],how='left'), df_list2)
            
            if len(log1)>0:
                log1['Customer_Id']= log1.apply(lambda x: x['Customer_Id_y'] if (len(str(x['Customer_Id_y'])) > 0 and str(x['Customer_Id_y']).upper()!='NAN') else x['Customer_Id_x'], axis=1)
            else:
                log1['Customer_Id'] = ''

            log2 = log1[['ETL_ProcessId','CTCR Process ID','Institution_Id','Account_Id','Customer_Id','Error_ID']].reset_index(drop=True)
            log2 = log2.drop_duplicates(keep='first')
            yield log2

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'ProcessExceptionLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
                                                                                    
class CRS_Filter_IPTD(beam.DoFn):

    def process(self,ip_td, key):
        try:
            

            if key == 'CA':

                df = ip_td[(ip_td['Tax_Id_type'] == 'SIN')| (ip_td['Tax_Id_type'] == 'BN')]

                df['CanadianIdentificationNumber'] = df['Tax_ID']

                df = df[['Institution_Id','Involved_Party_Id','CanadianIdentificationNumber']]

            else:

                df = ip_td[(ip_td['Tax_Id_type'] != 'SIN') & (ip_td['Tax_Id_type'] != 'BN')]

            yield df

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'CRS_Filter_IPTD', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))                                                                           
#---------------------------Big Query Pipeline Functions to run pipelines--------------------------------------------

def column_mapping(filename):
    column_structure = {'FILE_LEVEL_LOG.csv':['GDF_ProcessId','CTCR_ProcessId','Regime','Source_System','Source_File_Name','GDF_Start_DateTime','GDF_End_DateTime','ErrorId','Reporting_Year','Error_Message'],
                        'EYFIRST_API_LOG.csv':['GDF_Start_DateTime','CTCR_ProcessId','GDF_ProcessId','Regime','Reporting_Year','InstitutionId','Zip_File_Name','Request_Type','Status_Code','EYFIRST_ImportId'],
                        'Reconciliation_Log.csv':['Reporting_Year','Regime','InstitutionId','CTCR_ProcessId','GDF_ProcessId','Process_Start_Run_Time','File_Type','Count_Desc','Count'],
                        'Non-Reportable Log.csv': ['GDF_ProcessId','CTCR_ProcessId','Regime','Reporting_Year','InstitutionId','CustomerId','Customer_Entity_Name','Customer_First_Name','Customer_Last_Name','AccountId','Closed_Account','Account_Type','Entity_Type','Reason_For_NonReporting','GDF_Start_DateTime','Classification','Sub_Classification','Dormant','Category_of_Account_Holder'],
                        'Orphan_LOG.csv':['CTCR_ProcessId','GDF_ProcessId','GDF_Start_time','Regime','Reporting_Year','Linkage_Type','Institution_Id','Customer_ID','Account_Id','Payment_ID','Parent_ID','Child_ID'],
                        'RECORD_LEVEL_LOG.csv' :['GDF_ProcessId','CTCR_ProcessId','Regime','Source_System','Source_File_Name','GDF_Start_DateTime','GDF_End_DateTime','ErrorId','Validation_Field','Current_Value','PrimaryKey','CSV_File_Index','Reporting_Year','Error_Message'],
                        'Process_Log.csv':['Regime','GDF_ProcessId','CTCR_ProcessId','File_Process_Id','Start_DateTime','End_DateTime','Source_File_Name','Source_Key_Name','File_Found_FIL','Inventory_Lookup','File_Validation','Consolidation','Zip_File','Load_Status','Reporting_Year'],
                        'File_Inventory':['Record_Action','Country','Country_ITContact1','Country_ITContact2','Source_System','Source_SystemId','File_Type','File_SequenceNo','CTCR_ProcessId','InstitutionId','Destination_File_Name','Remarks','Instance','Destination_File_Name_Instance','MDT_File_Name','PrimaryKey','Active_Flag','File_Received','File_Count','DateTime','Regime','Reporting_Year'],
                        'Pool Report Details.csv':['ReportingYear','Account_Number','Account_Closed','Category_of_Account_Holder','Account_Holder_Type','FFI_Name','First_Name','Middle_Name','Last_Name','Date_Of_Birth','Entity_Name','TIN','Res_Country_Code','Country_Code','Street_Name','Building_Identifier','Suite_Identifier','Floor_Identifier','District_Name','POB','Post_Code','City','Country_Subentity','Address_Free','Account_Balance','Account_Currency','Dividends_Amount','Dividends_Currency','Interest_Amount','Interest_Currency','Gross_Proceeds_Redemptions_Amount','Gross_Proceeds_Redemptions_Currency','Other_Amount','Other_Currency','Classification','Sub_Classification','GDF_ProcessId','CTCR_Process_ID','Regime','Institution_ID','Entity_Type','GDF_Start_DateTime'],
                        'Error_Master.csv':['ErrorId','Regime','File_Type','Level','Error_Type','Error_Category','Validation_Field','Message']
        }
    
    return column_structure[filename]

class Error_log(beam.DoFn):
    def __init__(self, input_path):
        self.input_path = input_path
    
    def process(self,something,filename,csv_prefix,bq_tab):
       
        try:
            prefix = csv_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.input_path
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            df = pd.DataFrame()
            for blob in blobs:
   
                if (filename in blob.name):
                    data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_csv(BytesIO(data), header=[0])
                    table_id = bq_tab
                    df[['Error Id','Regime','File Type','Level','Error Type','Error category','Validated field','Message']] = \
                        df[['Error Id','Regime','File Type','Level','Error Type','Error category','Validated field','Message']].astype(str)
                       
                    df = df.replace('\n','', regex=True)
                    df = df.replace('nan','', regex=True)
                    df.columns = column_mapping(filename)
                    client  = BG.Client()
                    job_config = BG.LoadJobConfig(source_format=BG.SourceFormat.CSV,write_disposition="WRITE_TRUNCATE",)
                    job = client.load_table_from_dataframe(
                    df, table_id,job_config=job_config)  # Make an API request.
                    job.result()
            yield df
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Error log: '+str(filename), 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class Read_from_logs(beam.DoFn):
    def __init__(self, landing):
        self.landing = landing
    
    def process(self,something,filename,csv_prefix,bq_tab):
        
        try:
            prefix = csv_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.landing
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            df = pd.DataFrame()
            for blob in blobs:
                if (filename in blob.name):
                    data = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df = pd.read_csv(BytesIO(data), header=[0])

                    table_id = bq_tab
                    if filename=='FILE_LEVEL_LOG.csv':
                        
                        df['ETL_End_DateTime']=pd.to_datetime(df['ETL_End_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df['ETL_Start_DateTime']=pd.to_datetime(df['ETL_Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df['Reporting_Year'] =np.nan
                        df['Error_Message'] = ''
                        df[['ETL ProcessId','CTCR Process ID','Regime','Source System','Source_Filename','Error_ID']] = \
                            df[['ETL ProcessId','CTCR Process ID','Regime','Source System','Source_Filename','Error_ID']].fillna('').astype(str)
                        
                        df.columns = column_mapping(filename)
                        
                    
                    if filename=='Non-Reportable Log.csv':
                        
                        df['ETL_Start_DateTime']=pd.to_datetime(df['ETL_Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df[['ETL ProcessId','CTCR Process ID','Regime','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','Classification','Sub Classification','Dormant','Category of Account Holder']] = \
                            df[['ETL ProcessId','CTCR Process ID','Regime','Institution ID','Customer ID','Customer Entity Name','Customer First Name','Customer Last Name','Account Id','Closed_Account','Account Type','Entity Type','Reason for Non-Reporting','Classification','Sub Classification','Dormant','Category of Account Holder']].fillna('').astype(str)
                        
                        df.columns = column_mapping(filename)

                    if filename=='Orphan_LOG.csv':
                        
                        df['ETL_Start_time']=pd.to_datetime(df['ETL_Start_time'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df[['CTCR Process ID','ETL_Process ID','Regime','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']] = \
                            df[['CTCR Process ID','ETL_Process ID','Regime','Linkage Type','Institution_Id','Customer ID','Account_Id','Payment ID','Parent ID','Child ID']].fillna('').astype(str)
                        df.columns = column_mapping(filename)
                    
                    if filename=='RECORD_LEVEL_LOG.csv':
                        df['ETL_Start_DateTime']=pd.to_datetime(df['ETL_Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df['ETL_End_DateTime']=pd.to_datetime(df['ETL_End_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df[['ETL_ProcessId','CTCR Process ID','Regime','Source System','Source_Filename','Error_ID','Validation field','Current Value','Primary Key','CSV File Index']] = \
                            df[['ETL_ProcessId','CTCR Process ID','Regime','Source System','Source_Filename','Error_ID','Validation field','Current Value','Primary Key','CSV File Index']]
                        
                        df['Reporting_Year'] =np.nan
                        df['Error_Message'] = ''
         
                        df.columns = column_mapping(filename)
                        
                    
                    if filename=='Process_Log.csv':
                        df['Start_DateTime']=pd.to_datetime(df['Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df['End_DateTime']=pd.to_datetime(df['End_DateTime']).dt.tz_localize(None)

                        df['File_Found_FIL'] = df['File_Found_FIL'].map(lambda x: True if x=='Y' else False )

                        df['Inventory_Lookup'] = df['Inventory_Lookup'].map(lambda x: True if x=='Y' else False )

                        df['File_Validation'] = df['File_Validation'].map(lambda x: True if x=='Y' else False )

                        df['Zip_File'] =df['Zip_File'].map(lambda x: True if x=='Y' else False )
                        df['Consolidation'] =df['Consolidation'].map(lambda x: True if x=='Y' else False )
                        df['Reporting_Year'] =np.nan

                        df[['Regime','GDF_ProcessId','CTCR_ProcessId','File_Process_Id','Source_File_Name','Source_Key_Name','Load_Status']] = \
                            df[['Regime','GDF_ProcessId','CTCR_ProcessId','File_Process_Id','Source_File_Name','Source_Key_Name','Load_Status']].fillna('').astype(str)
                        
                        df.columns = column_mapping(filename)
                        
                    if filename =="Reconciliation_Log.csv":
                        df['Process_Start_Run_Time'] = pd.to_datetime(df['Process_Start_Run_Time'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df[['Regime','Institution_Id','CTCR Process ID','GDF Process Id','File_Type','Count_Desc']] = \
                            df[['Regime','Institution_Id','CTCR Process ID','GDF Process Id','File_Type','Count_Desc']].fillna('').astype(str)
                        df.columns = column_mapping(filename)



                    if filename =="EYFIRST_API_LOG.csv":
                        df['ETL_Start_DateTime']=pd.to_datetime(df['ETL_Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df[['CTCR Process ID','ETL_Process ID','Regime','Institution ID','Zip File Name','Request Type','Status Code','EYFIRST_Import Id']] = \
                            df[['CTCR Process ID','ETL_Process ID','Regime','Institution ID','Zip File Name','Request Type','Status Code','EYFIRST_Import Id']].fillna('').astype(str)
                        
                        df.columns = column_mapping(filename)
                    
                    if filename == 'Pool Report Details.csv':
                        
                        df['ETL_Start_DateTime']=pd.to_datetime(df['ETL_Start_DateTime'],format = "%d-%m-%Y %H:%M:%S").dt.tz_localize(None)
                        df['Date_Of_Birth'] = pd.to_datetime(df['Date_Of_Birth']).dt.tz_localize(None)
                        df['ReportingYear'] = df['ReportingYear'].astype(int)
                        df['Account_Balance'] = df['Account_Balance'].astype(float)
                        df['Dividends_Amount'] = df['Dividends_Amount'].astype(float)
                        df['Interest_Amount'] = df['Interest_Amount'].astype(float)
                        df['Gross_Proceeds_Redemptions_Amount'] = df['Gross_Proceeds_Redemptions_Amount'].astype(float)
                        df['Other_Amount'] = df['Other_Amount'].astype(float)

                        df[['Account_Number','Account_Closed','Category_of_Account_Holder','Account_Holder_Type','FFI_Name','First_Name','Middle_Name','Last_Name','Entity_Name','TIN','Res_Country_Code','Country_Code','Street_Name','Building_Identifier','Suite_Identifier','Floor_Identifier','District_Name','POB','Post_Code','City','Country_Subentity','Address_Free','Account_Currency','Dividends_Currency','Interest_Currency','Gross_Proceeds_Redemptions_Currency','Other_Currency','Classification','Sub Classification','ETL ProcessId','CTCR Process ID','Regime','Institution ID','Entity Type']] = \
                            df[['Account_Number','Account_Closed','Category_of_Account_Holder','Account_Holder_Type','FFI_Name','First_Name','Middle_Name','Last_Name','Entity_Name','TIN','Res_Country_Code','Country_Code','Street_Name','Building_Identifier','Suite_Identifier','Floor_Identifier','District_Name','POB','Post_Code','City','Country_Subentity','Address_Free','Account_Currency','Dividends_Currency','Interest_Currency','Gross_Proceeds_Redemptions_Currency','Other_Currency','Classification','Sub Classification','ETL ProcessId','CTCR Process ID','Regime','Institution ID','Entity Type']].fillna('').astype(str)
                        logg.info("columns: %s",df.dtypes)
                        df.columns = column_mapping(filename) 


                    df = df.replace('\n','', regex=True)
                    client  = BG.Client()
                    job_config = BG.LoadJobConfig(source_format=BG.SourceFormat.CSV)
                    job = client.load_table_from_dataframe(
                    df, table_id, job_config=job_config)  # Make an API request.
                    job.result()
                    
                    yield df

                    
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_from_logs for log: '+str(filename), 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

                                          

class move_etlvolumnesplitlog(beam.DoFn):
    def __init__(self, landing):
        self.landing = landing
    
    def process(self,something,filename,csv_prefix,bq_tab):
        
        try:
            client = BG.Client()
            table_id = bq_tab
            storage_client = storage.Client()

            job_config = BG.LoadJobConfig(
                schema=[
                    BG.SchemaField("Filename", "STRING"),
                    BG.SchemaField("Regime", "STRING"),
                    BG.SchemaField("CTCR_Process_ID", "STRING"),
                    BG.SchemaField("GDF_ProcessId", "STRING"),
                    BG.SchemaField("Reporting_CI", "STRING"),
                    BG.SchemaField("Account_Id", "STRING"),
                    BG.SchemaField("Customer_Id", "STRING"),
                    BG.SchemaField("Error_ID", "STRING"),
                ],
                skip_leading_rows=1,
                # The source format defaults to CSV, so the line below is optional.
                source_format=BG.SourceFormat.CSV,
            )

            bucket_name = self.landing
            bucket = storage_client.get_bucket(bucket_name)
            blobs = storage_client.list_blobs(bucket,prefix=log_prefix)
            flag = 0
            for b in blobs:
                if (filename in b.name):
                    
                    uri = "gs://" + bucket_name + "/" + csv_prefix + filename

                    load_job = client.load_table_from_uri(
                        uri, table_id, job_config=job_config
                    ) 

                    load_job.result() 

                    destination_table = client.get_table(table_id) 

                    flag = 1
            yield flag

        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'move_etlvolumnesplitlog: '+str(filename), 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class move_volsplitlogs(beam.DoFn):
    def __init__(self, landing,processed):
        self.landing =landing
        self.processed = processed
    
    def process(self,flag,filename,csv_prefix):
        
        try:
            if flag == 1:

                prefix = csv_prefix
                delimiter='/'
                storage_client = storage.Client()
                bucket_name = self.landing
                dest_bucket_name = self.processed
                source_bucket = storage_client.get_bucket(bucket_name)
                destination_bucket = storage_client.get_bucket(dest_bucket_name)
                blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)

                for blob in blobs:
                    if filename in blob.name:
                        source_blob = blob
                        source_blob_name = blob.name.split('/')[-1]
                        now = datetime.now()
                        dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                        dest_blob_name = bq_log_folder + source_blob_name.strip('.csv') +'_('+dt_string+').csv'
                        new_blob = source_bucket.copy_blob(source_blob, destination_bucket, dest_blob_name)
                        source_blob.delete()
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'move_volsplitlogs', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

                                          
                                                             
                                          
class Read_file_inventory_log(beam.DoFn):
    def __init__(self, landing):
        self.landing = landing
    
    def process(self,something,filename1,filename2,csv_prefix,bq_tab):
        
        try:
            prefix = csv_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.landing
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)
            df1 = pd.DataFrame()
            df2 = pd.DataFrame()
            table_id = bq_tab
            for blob in blobs:
                if (filename1 in blob.name):
                    data1 = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df1 = pd.read_excel(BytesIO(data1), header=[0],engine='xlrd')

                if (filename2 in blob.name):
                    data2 = blob.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    df2 = pd.read_excel(BytesIO(data2), header=[0],engine='xlrd')
            
            if len(df1)>0 and len(df2)>0:
                df= df1.append(df2)
                df['Active_Flag'] =df['Active_Flag'].map({1:True ,0:False})
                df['File_Received'] =df['File_Received'].map({1:True ,0:False})
                df['Reporting_Year'] =np.nan
                df['DateTime'] = pd.to_datetime(df['DateTime']).dt.tz_localize(None)
                df[['Record Action','Country','Country IT Contact - 1','Country IT Contact - 2','Source System','Source System ID','File Type','File Sequence No.','CTCR Process ID','Institution IDs','Destination File Name','Remarks','Instance','Destination File Name Instance','MDT_Filename','PrimaryKey','Regime']] = \
                    df[['Record Action','Country','Country IT Contact - 1','Country IT Contact - 2','Source System','Source System ID','File Type','File Sequence No.','CTCR Process ID','Institution IDs','Destination File Name','Remarks','Instance','Destination File Name Instance','MDT_Filename','PrimaryKey','Regime']].fillna('').astype(str)

                try:
                    df = df.drop(['Status'], axis = 1)
                except:{}
                
                df = df.replace('\n','', regex=True)
                df.columns = column_mapping('File_Inventory')
                client  = BG.Client()
                job_config = BG.LoadJobConfig(source_format=BG.SourceFormat.CSV,write_disposition="WRITE_TRUNCATE",)
                job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config)  # Make an API request.
                job.result()

                yield df
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Read_file_inventory_log', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class bq_error_message(beam.DoFn):
    def process(self,element1,element2,tab_id):
        try:
            client = BG.Client()
            #sql = "CREATE TABLE gp-ct-sbox-tax-globalbts.HSBC_DataSet.Test(ADDRESS_ID STRING,INDIVIDUAL_ID STRING,FIRST_NAME STRING,LAST_NAME STRING,);"
            sql1 = """update {0}ETLFileValidation 
                      set ETLFileValidation.Error_Message = (select Message from {0}ErrorMaster
                      where ETLFileValidation.ErrorId = ErrorMaster.ErrorId)
                      where ETLFileValidation.Error_Message ='' """.format(tab_id)
            #query_job = client.query(sql)
            #query_job.result()
            query_job1 = client.query(sql1)
            query_job1.result()

            sql2 = """update {0}ETLRecordValidation 
                      set ETLRecordValidation.Error_Message = (select Message from {0}ErrorMaster
                      where ETLRecordValidation.ErrorId = ErrorMaster.ErrorId)
                      where ETLRecordValidation.Error_Message = '' """.format(tab_id)
            #query_job = client.query(sql)
            #query_job.result()
            query_job2 = client.query(sql2)
            query_job2.result()
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'bq_error_message', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

class bq_token_value(beam.DoFn):
    
    def __init__(self, landing):
        self.landing = landing

    def process(self,e1,e2,e3,e4,e5,e6,e7,e8,e9,e10,e11,e12,e13,e14):
        
        flag=1
        yield flag

class move_logs(beam.DoFn):
    def __init__(self, landing,processed):
        self.landing =landing
        self.processed = processed
    
    def process(self,something,filename,csv_prefix):
        
        try:

            prefix = csv_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.landing
            dest_bucket_name = self.processed
            source_bucket = storage_client.get_bucket(bucket_name)
            destination_bucket = storage_client.get_bucket(dest_bucket_name)
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)

            for blob in blobs:
                if filename in blob.name:
                    source_blob = blob
                    source_blob_name = blob.name.split('/')[-1]
                    now = datetime.now()
                    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                    dest_blob_name = bq_log_folder + source_blob_name.strip('.csv') +'_('+dt_string+').csv'
                    new_blob = source_bucket.copy_blob(source_blob, destination_bucket, dest_blob_name)
                    source_blob.delete()

        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'move_logs', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

# Volumn split
class Vol_BQ_Threshold(beam.DoFn):

    def process(self,something,project_id,dataset,schema_tag,table):
        
        try:

            table_id = project_id +'.'+ dataset + table


            # Adding new logic for duplicates
            query = "SELECT * FROM `"+table_id+"`"

            project_id = project_id
            client = BG.Client(project=project_id)
            query_job = client.query(query)
            results = query_job.result()
            df = results.to_dataframe(create_bqstorage_client=True,)

            # threshold_value = df['Thresholdvalue'][0]
            threshold_value = df['Thresholdvalue'].max()

            # print(threshold_value)
                                     
                                                                        
            yield threshold_value
            
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Vol_BQ_Threshold', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
                                                                 
#---------------------------------------------------Pool Reporting---------------------------------------

class FATCA_ProcessPoolReportable(beam.DoFn):
    def process(self,element,usdms,uniqueid,etl_start_datetime,flag):
        try:
            if flag == 1:
                df = element
                #df = df[df['Accounts_Category_of_Account_Holder']=='Organisation']
                exchange_df = usdms
                df['Pooled_FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
                poolrep = pd.DataFrame(columns = ['FI_ID','Pooled_ReportingYear','Pooled_FFI_Name','Pooled_Reporting_Type','Pooled_Number_of_Accounts','Pooled_Aggregate_Account_Balance','Pooled_Currency_Code'])
                                                                                                                                                                                                                                                                                                                                                                                                                                    

                                                                                     
                                                                                                                                 

                schemacheck=['US','AM','BD','BM','EG','HK','JP','MO','OM','LK','CH','TW']
                PLtypecheck = ['FATCA201','FATCA202','FATCA203','FATCA204','FATCA205','FATCA206','FATCA204 (for 2015 and 2016 only)']
                                                                          
                                                                                
         
                                
                    
                                           

                if len(df)>0:
                                             

                    dfs = df[df['Country of Tax Residence'].isin(schemacheck)]
                    dfr = dfs[dfs['Accounts_Account_Holder_Type'].isin(PLtypecheck)]
    
                    if (len(dfr)>0):

                        dfr = dfr.reset_index()

                        #BalanceAmount Formatting
                        for i in range(len(dfr)):

                            if len(str(dfr.at[i,'Financials_Balance']).strip()) != 0:
                                dfr.at[i,'BalanceAmount_decimal'] = Decimal(dfr.at[i,'Financials_Balance'])
                                                                                                                                                                 

                        #Currency Conversion
                                                                                                                                                                                                      
                                                                                                                                                                                                     
                                                                                                                                                        

                        for index, row in dfr.iterrows():

                                                         
                                                                                                                                             
                            if dfr.loc[index, 'Financials_Account_Currency'] != 'USD':
                                s = str(dfr.loc[index, 'Financials_Account_Currency'])
                                date = str(dfr.loc[index, 'Accounts_ReportingYear']) + "-12-31"

                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == s]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= date)]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= date)]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)


                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.iloc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    dfr.loc[index, 'Converted_Financials_Account_Currency'] = 'USD'
                                    dfr.loc[index, 'BalanceAmount_in_USD'] = dfr.loc[index, 'BalanceAmount_decimal'] / Decimal(forex)

                            else:
                                dfr.loc[index, 'Converted_Financials_Account_Currency'] = 'USD' 
                                dfr.loc[index, 'BalanceAmount_in_USD'] = dfr.at[index,'BalanceAmount_decimal']

    
                                         
                                              
                    
                        
                        dfr = dfr[['FI_ID','Accounts_Account_Number','Pooled_FFI_Name','Accounts_ReportingYear','Accounts_Account_Holder_Type','EAG Entity Legal Name (Full)','Financials_Balance','Financials_Account_Currency','Converted_Financials_Account_Currency','BalanceAmount_in_USD','Involved_Party_Id']]

                        dfr = dfr.drop_duplicates(subset= ['FI_ID','Accounts_Account_Number','Pooled_FFI_Name', 'Accounts_ReportingYear','Accounts_Account_Holder_Type','EAG Entity Legal Name (Full)','Financials_Balance','Financials_Account_Currency','Converted_Financials_Account_Currency','BalanceAmount_in_USD'],keep= 'first').reset_index(drop= True)
                        dfr['BalanceAmount_in_USD'] = dfr['BalanceAmount_in_USD'].fillna(Decimal(0.00))

                                                                                                       
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                        grouped = dfr.groupby(['FI_ID','Accounts_Account_Holder_Type'])['BalanceAmount_in_USD'].agg(['sum','count']).reset_index()

                        df_list = [dfr, grouped]
                        df1 = reduce(lambda  left,right: pd.merge(left,right,on=['Accounts_Account_Holder_Type'],how='left'), df_list)
                        
                        #df1 = pd.merge(dfr, grouped, how='left', on=['Accounts_Account_Holder_Type'])
                        df1 = df1.reset_index()


                        for i in range(len(df1)):
                            if len(str(df1.at[i,'Financials_Balance']))>15:
                                split_num = str(df1.at[i,'sum']).split('.')
                                df1.at[i,'Aggsum'] = str(split_num[0]) + "." + str(split_num[1])[0:2] 
                            else:
                                df1.at[i,'Aggsum'] = str(round(float(df1.at[i,'sum']),2))

                        df2 = df1.rename(columns = {'FI_ID_x':'FI_ID','Accounts_ReportingYear':'Pooled_ReportingYear','Accounts_Account_Holder_Type':'Pooled_Reporting_Type','count':'Pooled_Number_of_Accounts','Aggsum':'Pooled_Aggregate_Account_Balance','Converted_Financials_Account_Currency':'Pooled_Currency_Code'})
                        #df2['Pooled_FFI_Name'] = df1['Name 1 (35 char limit)'] + " " + df1['Name 2  (35 char limit)']
                        
                                                                                                                                                                                                                                                                                                                                                                              
                        df2 = df2[['FI_ID','Pooled_ReportingYear','Pooled_FFI_Name','Pooled_Reporting_Type','Pooled_Number_of_Accounts','Pooled_Aggregate_Account_Balance','Pooled_Currency_Code']]
                        df2 = df2[df2['Pooled_Currency_Code'] == 'USD']
                        df2 = df2.drop_duplicates().reset_index(drop= True)

                        poolrep = poolrep.append(df2,sort=False)
                        yield poolrep
                    else:
                    
                        yield poolrep

                else:
                                        

                 
                                          
                    yield poolrep
            else:
                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process Pool Reportable',"Class name": 'FATCA_ProcessPoolReportable'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 

        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessPoolReportable', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
			
################Pool Log#####################
#---------------------------------------------------Pool Reporting---------------------------------------

class FATCA_ProcessPoolLog(beam.DoFn):
    def process(self,element,usdms,uniqueid,etl_start_datetime,flag):
        try:
            if flag == 1:
                df = element
                #df = df[df['Accounts_Category_of_Account_Holder']=='Organisation']
                df = df.assign(Accounts_Middle_Name ='',Accounts_Country_Subentity='', Accounts_Floor_Identifier='', Accounts_POB='', Accounts_Suite_Identifier='')

                exchange_df = usdms

                #poolreplog = pd.DataFrame(columns = ['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Account Id','Closed_Account','Account Type','Entity Type','ETL_Start_DateTime','Classification','Sub Classification','Dormant',	'Category of Account Holder'])

                df = df.rename(columns = {'CTCR Process ID_x':'CTCR Process ID','FI_ID':'Institution ID','Entity_Type':'Entity Type','FATCA_Classification':'Classification','FATCA_Sub-Classification':'Sub Classification'})
                ##'EAG Entity Legal Name (Full)':'Accounts_FFI_Name'
                df['Accounts_FFI_Name'] = df['Name 1 (35 char limit)'] + " " + df['Name 2  (35 char limit)']
                df['Regime'] = 'FTR'
                df['ETL ProcessId'] = uniqueid
                df['ETL_Start_DateTime'] = etl_start_datetime
                
                df1 = df[['Accounts_ReportingYear','Accounts_Account_Number','Accounts_Account_Closed','Accounts_Category_of_Account_Holder','Accounts_Account_Holder_Type','Accounts_FFI_Name','Accounts_First_Name','Accounts_Middle_Name','Accounts_Last_Name','Accounts_Date_Of_Birth','Accounts_Entity_Name','Accounts_TIN','Accounts_Res_Country_Code','Accounts_Country_Code','Accounts_Street_Name','Accounts_Building_Identifier','Accounts_Suite_Identifier','Accounts_Floor_Identifier','Accounts_District_Name','Accounts_POB','Accounts_Post_Code','Accounts_City','Accounts_Country_Subentity','Accounts_Address_Free','Financials_Balance','Financials_Account_Currency','Financials_Dividends_Amount','Financials_Dividends_Currency','Financials_Interest_Amount','Financials_Interest_Currency','Financials_Gross_Proceeds_Redemptions_Amount','Financials_Gross_Proceeds_Redemptions_Currency','Financials_Other_Amount','Financials_Other_Currency','Classification','Sub Classification','ETL ProcessId','CTCR Process ID','Regime','Institution ID','Entity Type','ETL_Start_DateTime','Country of Tax Residence']]
                df1 = df1.reset_index(drop=True)
                schemacheck=['US','AM','BD','BM','EG','HK','JP','MO','OM','LK','CH','TW']
                PLtypecheck = ['FATCA201','FATCA202','FATCA203','FATCA204','FATCA205','FATCA206','FATCA204 (for 2015 and 2016 only)']

                if len(df1)>0:

                    dfs = df1[df1['Country of Tax Residence'].isin(schemacheck)]
                    dfr = dfs[dfs['Accounts_Account_Holder_Type'].isin(PLtypecheck)]
                    dfr = dfr.reset_index(drop=True)
                    
                    if (len(dfr)>0):

                                               

                        #BalanceAmount Formatting
                        for i in range(len(dfr)):

                            if len(str(dfr.at[i,'Financials_Balance']).strip()) != 0:
                                dfr.at[i,'BalanceAmount_decimal'] = Decimal(dfr.at[i,'Financials_Balance'])

                        #Currency Conversion

                        for index, row in dfr.iterrows():

                            if dfr.loc[index, 'Financials_Account_Currency'] != 'USD':
                                s = str(dfr.loc[index, 'Financials_Account_Currency'])
                                date = str(dfr.loc[index, 'Accounts_ReportingYear']) + "-12-31"

                                exchange_df_new = exchange_df.loc[exchange_df['Category Key'] == s]
                                exchange_df_new1 = exchange_df_new.loc[(exchange_df_new['Start Date'] <= date)]
                                exchange_df_new2 = exchange_df_new1.loc[(exchange_df_new['End Date'] >= date)]
                                exchange_df_new2 = exchange_df_new2.reset_index(drop=True)


                                if len(exchange_df_new2) > 0:
                                    forex = exchange_df_new2.iloc[0]['US Dollar Daily Mid-spot Exchange Rate']
                                    dfr.loc[index, 'Converted_Financials_Account_Currency'] = 'USD'
                                    dfr.loc[index, 'BalanceAmount_in_USD'] = dfr.loc[index, 'BalanceAmount_decimal'] / Decimal(forex)

                            else:
                                dfr.loc[index, 'Converted_Financials_Account_Currency'] = 'USD' 
                                dfr.loc[index, 'BalanceAmount_in_USD'] = dfr.at[i,'BalanceAmount_decimal']
                        #Generate log



                        dfr = dfr.drop(columns=['Converted_Financials_Account_Currency','BalanceAmount_in_USD','BalanceAmount_decimal','Country of Tax Residence'])  
                                             
                                                       
                                                                      
                                                                                                                                                                                                                                                                                                                    
                                                                      
                                             
                                                       
                                                                      
                        #df3 = df3[['ETL ProcessId','CTCR Process ID','Regime','Reporting Year','Institution ID','Customer ID','Customer Entity Name','Account Id','Closed_Account','Account Type','Entity Type','ETL_Start_DateTime','Classification','Sub Classification','Dormant',	'Category of Account Holder']]
                        #poolreplog = poolreplog.append(df3,sort=False)
                        
                        yield dfr        

                    else:
                        dfr = dfr.drop(columns=['Country of Tax Residence'])
                        
                        yield dfr

                else:
                    df1 = df1.drop(columns=['Country of Tax Residence'])
                    
                    yield df1
            else:

                logger_client = logging.Client() 
                logger = logger_client.logger('Eyfirst_Stack_logs')
                
                logger.log_struct({'severity':'Critical','message':'Unable to process Pool Reportable',"Class name": 'FATCA_ProcessPoolLog'})
                handler = CloudLoggingHandler(logger_client)
                cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
                cloud_logger.setLevel(logg.INFO)
                cloud_logger.addHandler(handler)
                cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'FATCA_ProcessPoolLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))
#############################################
			
			
class Write_PoolReportLog(beam.DoFn):
								
							  

    def __init__(self, input_path):
        self.input_path = input_path

    def process(self,element,filename):
        try:
							   
            delimiter='/'
            df = element
            storage_client = storage.Client()
            bucket_name = self.input_path
            bucket = storage_client.get_bucket(bucket_name)
            destination_blob_name = filename
            blobs = storage_client.list_blobs(bucket_name,prefix=log_prefix, delimiter=delimiter)

            nrlog = pd.DataFrame(columns = ['ReportingYear','Account_Number','Account_Closed','Category_of_Account_Holder','Account_Holder_Type','FFI_Name','First_Name','Middle_Name','Last_Name','Date_Of_Birth','Entity_Name','TIN','Res_Country_Code','Country_Code','Street_Name','Building_Identifier','Suite_Identifier','Floor_Identifier','District_Name','POB','Post_Code','City','Country_Subentity','Address_Free','Account_Balance','Account_Currency','Dividends_Amount','Dividends_Currency','Interest_Amount','Interest_Currency','Gross_Proceeds_Redemptions_Amount','Gross_Proceeds_Redemptions_Currency','Other_Amount','Other_Currency','Classification','Sub Classification','ETL ProcessId','CTCR Process ID','Regime','Institution ID','Entity Type','ETL_Start_DateTime'])

 
            element = element.rename(columns={'Accounts_ReportingYear':'ReportingYear','Accounts_Account_Number':'Account_Number','Accounts_Account_Closed':'Account_Closed','Accounts_Category_of_Account_Holder':'Category_of_Account_Holder','Accounts_Account_Holder_Type':'Account_Holder_Type','Accounts_FFI_Name':'FFI_Name','Accounts_First_Name':'First_Name','Accounts_Middle_Name':'Middle_Name','Accounts_Last_Name':'Last_Name','Accounts_Date_Of_Birth':'Date_Of_Birth','Accounts_Entity_Name':'Entity_Name','Accounts_TIN':'TIN','Accounts_Res_Country_Code':'Res_Country_Code','Accounts_Country_Code':'Country_Code','Accounts_Street_Name':'Street_Name','Accounts_Building_Identifier':'Building_Identifier','Accounts_Suite_Identifier':'Suite_Identifier','Accounts_Floor_Identifier':'Floor_Identifier','Accounts_District_Name':'District_Name','Accounts_POB':'POB','Accounts_Post_Code':'Post_Code','Accounts_City':'City','Accounts_Country_Subentity':'Country_Subentity','Accounts_Address_Free':'Address_Free','Financials_Balance':'Account_Balance','Financials_Account_Currency':'Account_Currency','Financials_Dividends_Amount':'Dividends_Amount','Financials_Dividends_Currency':'Dividends_Currency','Financials_Interest_Amount':'Interest_Amount','Financials_Interest_Currency':'Interest_Currency','Financials_Gross_Proceeds_Redemptions_Amount':'Gross_Proceeds_Redemptions_Amount','Financials_Gross_Proceeds_Redemptions_Currency':'Gross_Proceeds_Redemptions_Currency','Financials_Other_Amount':'Other_Amount','Financials_Other_Currency':'Other_Currency'})
            element = element.reset_index(drop=True)
            for b in blobs:
                if (filename in b.name):
                    nr_data = b.download_as_bytes()
                    BytesIO = pd.io.common.BytesIO
                    nrlog = pd.read_csv(BytesIO(nr_data), header=[0])

										 
            nrlog = nrlog.append(element)
            nrlog = nrlog.reset_index(drop=True)                                                                                                                                                                                                                                                                                                                                                                    
																					   

            nrlog.to_csv(filename,index=False)                                                                                     
            blob = bucket.blob(log_prefix + destination_blob_name)
								
						 

            blob.upload_from_filename(filename)
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
			
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'Write_PoolReportLog', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))

#Move CSV'S to Cold storage

class move_Csv(beam.DoFn):
    def __init__(self,output,processed):
        self.output = output
        self.processed = processed
    
    def process(self,something,csv_prefix):
        
        try:
            prefix = csv_prefix
            delimiter='/'
            storage_client = storage.Client()
            bucket_name = self.output
            dest_bucket_name = self.processed
            source_bucket = storage_client.get_bucket(bucket_name)
            destination_bucket = storage_client.get_bucket(dest_bucket_name)
            blobs = storage_client.list_blobs(bucket_name,prefix=prefix, delimiter=delimiter)

            for blob in blobs:
                if '.csv' in blob.name:
                    source_blob = blob
                    source_blob_name = blob.name.split('/')[-1]
                    now = datetime.now()
                    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                    dest_blob_name = move_csv_storage +csv_prefix + source_blob_name.strip('.csv') +'_('+dt_string+').csv'
                    new_blob = source_bucket.copy_blob(source_blob, destination_bucket, dest_blob_name)
                    source_blob.delete()
        
        except Exception as e:
            logger_client = logging.Client() 
            logger = logger_client.logger('Eyfirst_Stack_logs')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            logger.log_struct({'severity':'Critical','message':str(exc_obj),"Class name": 'move_Csv', 'Error_type':str(exc_type),'File Name':fname, 'At line number':exc_tb.tb_lineno})
            handler = CloudLoggingHandler(logger_client)
            cloud_logger = logg.getLogger("Eyfirst_Stack_logs")
            cloud_logger.setLevel(logg.INFO)
            cloud_logger.addHandler(handler)
            cloud_logger.log(msg="Eyfirst_log", level=logg.INFO) 
            raise TypeError (str(exc_obj))



#-------------------------------------Main function to run pipelines ----------------------------------------------------
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--landing', required=True, type=str)
    parser.add_argument('--output',required=True, type=str)
    parser.add_argument('--input',required=True, type=str)
    parser.add_argument('--processed',required=True, type=str)
    parser.add_argument('--business_secrets',required=True, type=str)
    parser.add_argument('--BQ_project',required=True, type=str)
    parser.add_argument('--BQ_dataset',required=True, type=str)
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    


    with beam.Pipeline(options=pipeline_options) as pipeline1:
        BQ_project = str(known_args.BQ_project)
        BQ_dataset = str(known_args.BQ_dataset) + "."
        BQ_table_id = BQ_project+"."+BQ_dataset

                                               
#-----------------------------------------------------Stage 1 Pipelines----------------------------------------------------------#
        #----------------------------------CRS Initiale Pipeline ----------------------------
        
        CRS_MDT = (pipeline1 | 'Start Reading CRS MDT' >> beam.Create([None])
                            | 'Read CRS MDT Xls' >> beam.ParDo(Stage1_Read_from_xls(known_args.landing),'MDT_CRS.xls')) 
                            
        CRS_FileInventory = (pipeline1 | 'Start Reading CRS FileInventory' >> beam.Create([None])
                            | 'Read CRS FileInventory Xls' >> beam.ParDo(Read_FileInventory_xls(known_args.landing),'CRS_FileInventory.xls',beam.pvalue.AsSingleton(CRS_MDT)))
        
        CRS_MDTvalidated = (CRS_FileInventory | 'Generate CRSInventory validation df' >> beam.ParDo(Validate_from_FileInventory(known_args.landing),'CRS_FileInventory.xls'))

        CRS_File_to_process = (pipeline1 | 'Start gouping CRS file to process' >> beam.Create([None])
                            | 'Generate CRS Process List' >> beam.ParDo(Process_Validation(),beam.pvalue.AsSingleton(CRS_MDTvalidated)))
        
        Unique_ID = (pipeline1 | 'Initiating UUID for log' >> beam.Create([None])
                                | 'Initiate UUID for log' >> beam.ParDo(unique_id()))
                                
        Read_usdms = (pipeline1 | 'Initiating read usdms' >> beam.Create([None])
                                | 'Initiate read usdms' >> beam.ParDo(read_usdms(known_args.landing)))
                                
        Read_deMinimis = (pipeline1 | 'Initiating read deminimis' >> beam.Create([None])
                                | 'Initiate read deminimis' >> beam.ParDo(read_deMinimis(known_args.landing)))
								
							 
        ETL_start_datetime = (pipeline1 | 'Initiating ETL start datetime' >> beam.Create([None])
                                        | 'Initiate ETL start datetime' >> beam.ParDo(etl_start_datetime()))
                                        
        CRS_File_Level_Validation = (CRS_File_to_process | 'validation CRS file' >> beam.ParDo(CRS_Validation_File(known_args.landing),beam.pvalue.AsSingleton(CRS_MDTvalidated),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))
        
        CRS_File_creation_process = (CRS_File_to_process | 'process CRS detail' >> beam.ParDo(File_creation(),beam.pvalue.AsSingleton(CRS_File_Level_Validation)))
                                                                                                                                                     
        Write_CRS_FileInventory = (CRS_MDTvalidated| 'Generate CRS_FileInventory' >> beam.ParDo(Write_FileInventoryMDT_to_csv(known_args.landing),'CRS_FileInventory.xls'))
        
		# Volumn split threshold
        Vol_Split_Thresold = (pipeline1 | 'Pick threshold Value' >> beam.Create([None])| 'BQ threshold value' >> beam.ParDo(Vol_BQ_Threshold(known_args.landing),BQ_project,BQ_dataset,'EYFirst','VolumeSplit'))
        
                                                                                                                                                                                                                                                                    
        #---------------------------------FATCA Initiale Pipeline-----------------------------
        FATCA_MDT = (pipeline1 | 'Start Reading FATCA MDT' >> beam.Create([None])
                            | 'Read FATCA MDT Xls' >> beam.ParDo(Stage1_Read_from_xls(known_args.landing),'MDT_FATCA.xls')) 
                            
        FATCA_FileInventory = (pipeline1 | 'Start Reading FATCA FileInventory' >> beam.Create([None])
                            | 'Read FATCA FileInventory Xls' >> beam.ParDo(Read_FileInventory_xls(known_args.landing),'FATCA_FileInventory.xls',beam.pvalue.AsSingleton(FATCA_MDT)))
        
        FATCA_MDTvalidated = (FATCA_FileInventory | 'Generate FATCA Inventory validation df' >> beam.ParDo(Validate_from_FileInventory(known_args.landing),'FATCA_FileInventory.xls'))
                                                                                                                                                     
        FATCA_File_to_process = (pipeline1 | 'Start gouping FATCA file to process' >> beam.Create([None])
                            | 'Generate FATCA Process List' >> beam.ParDo(Process_Validation(),beam.pvalue.AsSingleton(FATCA_MDTvalidated)))

        FATCA_File_Level_Validation = (FATCA_File_to_process | 'validation FATCA file' >> beam.ParDo(FATCA_Validation_File(known_args.landing),beam.pvalue.AsSingleton(FATCA_MDTvalidated),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime))) 


        CRS_FATCA_current_date_log = 'FILE_LEVEL_LOG' +'.csv'
		
		##Read if existing log is present in bucket
		
        Read_file_level_log_csv = (pipeline1 | 'reading file level log' >> beam.Create([None])
                                        | 'Initiate reading file level log' >> beam.ParDo(read_file_level_log_csv(known_args.landing),'FILE_LEVEL_LOG.csv'))
										
        Merge_file_level_log = (Read_file_level_log_csv| 'merge CRS and FATCA file level log with previous log' >> beam.ParDo(merge_file_level_log(),beam.pvalue.AsSingleton(CRS_File_Level_Validation),beam.pvalue.AsSingleton(FATCA_File_Level_Validation)))
		
        CRS_FATCA_WriteValidation = (Merge_file_level_log | 'Generate CRS FATCA Validation log' >> beam.ParDo(Write_log_to_csv(known_args.landing),CRS_FATCA_current_date_log))

     
        FATCA_File_creation_process = (FATCA_File_to_process | 'process FATCA detail' >> beam.ParDo(File_creation(),beam.pvalue.AsSingleton(FATCA_File_Level_Validation)))
                                                                                                                                                                                         
        Write_FATCA_FileInventory = (FATCA_MDTvalidated| 'Generate FATCA_FileInventory' >> beam.ParDo(Write_FileInventoryMDT_to_csv(known_args.landing),'FATCA_FileInventory.xls'))
        
        #--------------------------------Create Common Inventory---------------------------------#
        
        FileInventory = (FATCA_MDTvalidated| 'Merge Inventory' >> beam.ParDo(MergeInventory(),beam.pvalue.AsSingleton(CRS_MDTvalidated)))

        #----Read Latest Process Log and put it in a dataframe, if not present initialise the dataframe and write it back--------------#
        Read_ProcessLog = (pipeline1 | 'Start Reading ProcessLog' >> beam.Create([None])
                            | 'Create or Initialise Process_Log' >> beam.ParDo(Read_ProcessLog_csv(known_args.landing),'Process_Log.csv'))
        
        Write_ProcessLog = (Read_ProcessLog | 'Write Initialised Process Log' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #----Add filenames and few auto generated columns in the process log and write it back--------------#
        AddFiles_ProcessLog = (Read_ProcessLog | 'Add filenames to ProcessLog' >> beam.ParDo(Initialise_ProcessLog(known_args.landing),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))
        
        Writefilenames_ProcessLog = (AddFiles_ProcessLog | 'Write Filenames to Process Log' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #----Update Process Log with respect to File Inventory List--------------#
        UpdateFIL_ProcessLog = (AddFiles_ProcessLog | 'Update FIL reference to ProcessLog' >> beam.ParDo(UpdateFIL_Reference_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FileInventory)))
        
        WriteFIL_ProcessLog = (UpdateFIL_ProcessLog | 'Write FIL reference to Process Log' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        #-----Update Process Log for Valid and Invalid Processes for CRS & FATCA due to insufficient file and write back to storage--------#
                
        UpdateValidCTCRCRS_ProcessLog = (UpdateFIL_ProcessLog | 'Update CRS Insufficient fileset to ProcessLog' >> beam.ParDo(Update_Valid_CTCR_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_File_to_process),'CRS'))
        
        UpdateValidCTCRFTR_ProcessLog = (UpdateValidCTCRCRS_ProcessLog | 'Update FTR Insufficient fileset to ProcessLog' >> beam.ParDo(Update_Valid_CTCR_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_File_to_process),'FTR'))
        
        WriteValidCTCR_ProcessLog = (UpdateValidCTCRFTR_ProcessLog | 'Write Insufficient fileset to Process Log' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #-----Update Process Log for Valid and Invalid Processes for CRS & FATCA due to one or more invalid files and write back to storage--------#
        UpdateFileValidCRS_ProcessLog = (UpdateValidCTCRFTR_ProcessLog | 'Update CRS Invalid fileset to ProcessLog' >> beam.ParDo(Update_Valid_File_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_File_creation_process),beam.pvalue.AsSingleton(CRS_File_Level_Validation),'CRS'))
        
        UpdateFileValidFTR_ProcessLog = (UpdateFileValidCRS_ProcessLog | 'Update FTR Invalid fileset to ProcessLog' >> beam.ParDo(Update_Valid_File_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_File_creation_process),beam.pvalue.AsSingleton(FATCA_File_Level_Validation),'FTR'))
        
        WriteValidFile_ProcessLog = (UpdateFileValidFTR_ProcessLog | 'Write Invalid fileset to Process Log' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #--------------------------------CRS Pipeline--------------------------------------------
        CRS_Account1 = (pipeline1 | 'Start Reading CRS Account' >> beam.Create([None])
                             | 'Read CRS Account fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'Account',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        
        CRS_AccountBalance1 = (pipeline1 | 'Start Reading CRS AccountBalance' >> beam.Create([None])
                             | 'Read CRS AccountBalance fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'AccountBalance',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        
        
        CRS_Payment1 = (pipeline1 | 'Start CRS Reading Payment' >> beam.Create([None])
                             | 'Read CRS Payment fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'Payment',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        CRS_IPAccount1 = (pipeline1 | 'Start CRS Reading IPAccount' >> beam.Create([None])
                             | 'Read CRS IPAccount fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'IPAccount',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        CRS_IPIP1 = (pipeline1 | 'Start CRS Reading IPIP' >> beam.Create([None])
                             | 'Read CRS IPIP fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'IPIP',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        CRS_IP1 = (pipeline1 | 'Start CRS Reading IP' >> beam.Create([None])
                             | 'Read CRS IP fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'IP',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        CRS_IPTD1 = (pipeline1 | 'Start CRS Reading IPTD' >> beam.Create([None])
                            | 'Read CRS IPTD fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'IPTD',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        CRS_IP_ADDN_QA1 = (pipeline1 | 'Start CRS Reading IP ADDN QA' >> beam.Create([None])
                            | 'Read CRS IP ADDN QA fwf' >> beam.ParDo(CRS_Read_from_fwf(known_args.landing),'IP_ADDN_QA',beam.pvalue.AsSingleton(CRS_File_creation_process)))
        #-------------------------------------------------Conditional Check CRS--------------------------------------------------------#
        CRS_FLAG = (CRS_Account1 | 'Check if CRS Files are present' >> beam.ParDo(CRS_find_csv(),beam.pvalue.AsSingleton(CRS_Account1),beam.pvalue.AsSingleton(CRS_AccountBalance1),beam.pvalue.AsSingleton(CRS_Payment1),beam.pvalue.AsSingleton(CRS_IPAccount1),beam.pvalue.AsSingleton(CRS_IPIP1),beam.pvalue.AsSingleton(CRS_IP1),beam.pvalue.AsSingleton(CRS_IPTD1),beam.pvalue.AsSingleton(CRS_IP_ADDN_QA1)))
        
        #---------------------------------------Handling Dummy Institution ID (Match and Merge for CRS)--------------------------------#
        CRS_IP_MM = (CRS_FLAG| 'Match and Merge CRS IP' >> beam.ParDo(MatchMergeIP(),beam.pvalue.AsSingleton(CRS_IP1),beam.pvalue.AsSingleton(CRS_IPAccount1),'IP'))
        
        CRS_IPTD_MM = (CRS_FLAG| 'Match and Merge CRS IPTD' >> beam.ParDo(MatchMergeIPTD(),beam.pvalue.AsSingleton(CRS_IPTD1),beam.pvalue.AsSingleton(CRS_IPAccount1),'IPTD'))
        
        CRS_IPIP_MM = (CRS_FLAG| 'Match and Merge CRS IPIP' >> beam.ParDo(MatchMergeIPIP(),beam.pvalue.AsSingleton(CRS_IPIP1),beam.pvalue.AsSingleton(CRS_IPAccount1),'IPIP'))
        
        CRS_IP_MM_Child = (CRS_FLAG| 'Match and Merge CRS Child IP' >> beam.ParDo(MatchMergeIPChild(),beam.pvalue.AsSingleton(CRS_IP_MM),beam.pvalue.AsSingleton(CRS_IPIP_MM),'IP'))
        
        CRS_IPTD_MM_Child = (CRS_FLAG| 'Match and Merge CRS Child IPTD' >> beam.ParDo(MatchMergeIPTDChild(),beam.pvalue.AsSingleton(CRS_IPTD_MM),beam.pvalue.AsSingleton(CRS_IPIP_MM),'IPTD'))
        

        #QATAR Changes
        CRS_IPADDN_MM_QA = (CRS_FLAG| 'Match and Merge CRS IP ADDN' >> beam.ParDo(MatchMergeIP(),beam.pvalue.AsSingleton(CRS_IP_ADDN_QA1),beam.pvalue.AsSingleton(CRS_IPAccount1),'IP'))
        CRS_IPADDN_MM_QA_Child = (CRS_FLAG| 'Match and Merge CRS Child ADDN IP' >> beam.ParDo(MatchMergeIPChild(),beam.pvalue.AsSingleton(CRS_IPADDN_MM_QA),beam.pvalue.AsSingleton(CRS_IPIP_MM),'IP'))


        #----------------------------------Reconciliation Pipeline---------------------------------------------------------------------#
        Read_ReconLog = (pipeline1 | 'Start Reading ReconLog' >> beam.Create([None])
                            | 'Create or Initialise Recon_Log' >> beam.ParDo(Read_ReconLog_csv(known_args.landing),'Reconciliation_Log.csv'))
        
        CRS_Reconciliation = (CRS_FLAG | 'Create CRS Recon Dataframe' >> beam.ParDo(ReconciliationUpd(known_args.landing),beam.pvalue.AsSingleton(CRS_IPAccount1),beam.pvalue.AsSingleton(CRS_IPIP_MM),beam.pvalue.AsSingleton(Unique_ID),'CRS',beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_ReconLog)))                                                                                                                            
        #------------------------Writing Stage1 CSVs for CRS and updating consolidation flag in Process Log -------------------------
        
        CRS_WriteAccount = (CRS_Account1 | 'Generate CRS Account csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_ACCT.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        CRS_ConsoleAccount = (UpdateFileValidFTR_ProcessLog | 'Update Account Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_Account1),'CRS'))
        
        CRS_WriteAccountBalance = (CRS_AccountBalance1 | 'Generate CRS AccountBalance csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_ACCT_BAL.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        CRS_ConsoleAccountBalance = (CRS_ConsoleAccount | 'Update AccountBalance Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_AccountBalance1),'CRS'))
        
        CRS_WritePayment = (CRS_Payment1 | 'Generate CRS Payment csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_PAY.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        CRS_ConsolePayment = (CRS_ConsoleAccountBalance | 'Update Payment Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_Payment1),'CRS'))
        
        CRS_WriteIPAccount = (CRS_IPAccount1 | 'Generate CRS IPAccount csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_IP_ACCT.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        CRS_ConsoleIPAccount = (CRS_ConsolePayment | 'Update IPAccount Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPAccount1),'CRS'))
        
        CRS_WriteIP = (CRS_IP_MM_Child | 'Generate CRS IP csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_IP.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        CRS_ConsoleIP = (CRS_ConsoleIPAccount | 'Update IP Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IP_MM_Child),'CRS'))
        
        CRS_WriteIPTD = (CRS_IPTD_MM_Child | 'Generate CRS IPTD csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_IP_TD.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        CRS_ConsoleIPTD = (CRS_ConsoleIP | 'Update IPTD Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPTD_MM_Child),'CRS'))
        
        CRS_WriteIPIP = (CRS_IPIP_MM | 'Generate CRS IPIP csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_IP_IP.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))      
        CRS_ConsoleIPIP = (CRS_ConsoleIPTD | 'Update IPIP Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPIP_MM),'CRS'))
        
        #QATAR Changes
        CRS_WriteIPADDNQA = (CRS_IPADDN_MM_QA_Child | 'Generate CRS QA IPADDN csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"CRS_IP_ADDN_QA.csv",CRS_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))      
        CRS_ConsoleIPADDNQA = (CRS_ConsoleIPIP | 'Update IPADDN Consoleflag CRS to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPADDN_MM_QA_Child),'CRS'))
       
        #CRS_MoveAndDelete = (CRS_ConsoleIPIP | 'Move file and delete CRS from source' >> beam.ParDo(Move_and_delete(known_args.landing,known_args.processed),beam.pvalue.AsSingleton(Unique_ID),'CRS'))
        
        
        #--------------------------------FATCA Pipeline----------------------------------------------------------
        
        FATCA_Account1 = (pipeline1 | 'Start Reading FATCA Account' >> beam.Create([None])
                             | 'Read Account FATCA fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'Account',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        FATCA_AccountBalance1 = (pipeline1 | 'Start Reading FATCA AccountBalance' >> beam.Create([None])
                             | 'Read FATCA AccountBalance fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'AccountBalance',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        FATCA_Payment1 = (pipeline1 | 'Start Reading FATCA Payment' >> beam.Create([None])
                             | 'Read FATCA Payment fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'Payment',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        FATCA_IPAccount1 = (pipeline1 | 'Start Reading IPAccount' >> beam.Create([None])
                             | 'Read FATCA IPAccount fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'IPAccount',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        FATCA_IPIP1 = (pipeline1 | 'Start Reading FATCA IPIP' >> beam.Create([None])
                             | 'Read FATCA IPIP fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'IPIP',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        FATCA_IP1 = (pipeline1 | 'Start Reading FATCA IP' >> beam.Create([None])
                             | 'Read FATCA IP fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'IP',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        
        # New pipline
        FATCA_IPAddCA1 = (pipeline1 | 'Start Reading FATCA IPAddCA' >> beam.Create([None])
                             | 'Read FATCA IPAddCA fwf' >> beam.ParDo(FATCA_Read_from_fwf(known_args.landing),'IPAddCA',beam.pvalue.AsSingleton(FATCA_File_creation_process)))
        #-------------------------------------------------Conditional Check FATCA--------------------------------------------------------#
        FATCA_FLAG = (FATCA_Account1 | 'Check if FATCA Files are present' >> beam.ParDo(FATCA_find_csv(),beam.pvalue.AsSingleton(FATCA_Account1),beam.pvalue.AsSingleton(FATCA_AccountBalance1),beam.pvalue.AsSingleton(FATCA_Payment1),beam.pvalue.AsSingleton(FATCA_IPAccount1),beam.pvalue.AsSingleton(FATCA_IPIP1),beam.pvalue.AsSingleton(FATCA_IP1),beam.pvalue.AsSingleton(FATCA_IPAddCA1))) 
        
        #---------------------------------------Handling Dummy Institution ID (Match and Merge for FATCA)--------------------------------#
        FATCA_IP_MM = (FATCA_FLAG| 'Match and Merge FTR IP' >> beam.ParDo(MatchMergeIP(),beam.pvalue.AsSingleton(FATCA_IP1),beam.pvalue.AsSingleton(FATCA_IPAccount1),'IP'))
        
        # New pipeline
        FATCA_IPCA_MM = (FATCA_FLAG| 'Match and Merge FTR IPCA' >> beam.ParDo(MatchMergeIP(),beam.pvalue.AsSingleton(FATCA_IPAddCA1),beam.pvalue.AsSingleton(FATCA_IPAccount1),'IP'))

        FATCA_IPIP_MM = (FATCA_FLAG| 'Match and Merge FTR IPIP' >> beam.ParDo(MatchMergeIPIP(),beam.pvalue.AsSingleton(FATCA_IPIP1),beam.pvalue.AsSingleton(FATCA_IPAccount1),'IPIP'))
        
        FATCA_IP_MM_Child = (FATCA_FLAG| 'Match and Merge FTR Child IP' >> beam.ParDo(MatchMergeIPChild(),beam.pvalue.AsSingleton(FATCA_IP_MM),beam.pvalue.AsSingleton(FATCA_IPIP_MM),'IP'))
        
        # New pipeline
        FATCA_IPCA_MM_Child = (FATCA_FLAG| 'Match and Merge FTR Child IPCA' >> beam.ParDo(MatchMergeIPChild(),beam.pvalue.AsSingleton(FATCA_IPCA_MM),beam.pvalue.AsSingleton(FATCA_IPIP_MM),'IP'))
        
        
        #----------------------------------Reconciliation Pipeline FATCA---------------------------------
        FATCA_Reconciliation = (FATCA_FLAG | 'Create FATCA Recon Dataframe' >> beam.ParDo(ReconciliationUpd(known_args.landing),beam.pvalue.AsSingleton(FATCA_IPAccount1),beam.pvalue.AsSingleton(FATCA_IPIP_MM),beam.pvalue.AsSingleton(Unique_ID),'FTR',beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(CRS_Reconciliation)))
        
        #---------------------------------------Write FATCA CSVs------------------------------------------
        
        FATCA_WriteAccount = (FATCA_Account1 | 'Generate Account csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_ACCT.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))   
        FTR_ConsoleAccount = (CRS_ConsoleIPIP | 'Update Account Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_Account1),'FTR'))
        
        FATCA_WriteAccountBalance = (FATCA_AccountBalance1 | 'Generate AccountBalance csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_ACCT_BAL.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        FTR_ConsoleAccountBalance = (FTR_ConsoleAccount | 'Update AccountBalance Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_AccountBalance1),'FTR'))
        
        FATCA_WritePayment = (FATCA_Payment1 | 'Generate Payment csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_PAY.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        FTR_ConsolePayment = (FTR_ConsoleAccountBalance | 'Update Payment Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_Payment1),'FTR'))
        
        FATCA_WriteIPAccount = (FATCA_IPAccount1 | 'Generate IPAccount csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_IP_ACCT.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))  
        FTR_ConsoleIPAccount = (FTR_ConsolePayment | 'Update IPAccount Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPAccount1),'FTR'))
        
        FATCA_WriteIP = (FATCA_IP_MM_Child | 'Generate IP csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_IP.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        FTR_ConsoleIP = (FTR_ConsoleIPAccount | 'Update IP Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IP_MM_Child),'FTR'))
        
        FATCA_WriteIPIP = (FATCA_IPIP_MM | 'Generate IPIP csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_IP_IP.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        FTR_ConsoleIPIP = (FTR_ConsoleIP | 'Update IPIP Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPIP_MM),'FTR'))
        
        # New pipeline
        FATCA_WriteIPCA = (FATCA_IPCA_MM_Child | 'Generate IPAddCA csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FTR_IP_ADDN_CA.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID))) 
        FTR_ConsoleIPCA = (FTR_ConsoleIP | 'Update IPAddCA Consoleflag FTR to ProcessLog' >> beam.ParDo(Update_Consolidation_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPCA_MM),'FTR'))

        
        WriteFATCA_Consolidation = (FTR_ConsoleIPIP | 'Write Consolidation to Process Log for FATCA' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
              
        #FATCA_MoveAndDelete = (FTR_ConsoleIPIP | 'Move file and delete FATCA from source' >> beam.ParDo(Move_and_delete(known_args.landing,known_args.processed),beam.pvalue.AsSingleton(Unique_ID),'FTR'))

        #-------------------------------------------------Stage 2 Pipelines--------------------------------------------------------#
        #Read CI & Header mapping_FILE--------------------------------------------------#
        FI = (pipeline1 | 'Start Reading FI' >> beam.Create([None])
                    | 'Read FI xls' >> beam.ParDo(Read_from_xls(known_args.landing),'Centurion_CI_Detail_form.xlsx'))
       
        #Header_Mapping = (pipeline1 | 'Mapping header' >> beam.Create([None])
                    #| 'Map Header' >> beam.ParDo(header_mapping(),beam.pvalue.AsSingleton(FI)))
        
        #------------------------------------------------CRS Staging Pipeline---------------------------------------------------------------------#  
        #CRS - Read files from staging bucket and filter delta records----------------------------------#
        CRS_Account = (CRS_FLAG | 'Read Account CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_Account1)))

        CRS_AccountBalance = (CRS_FLAG | 'Read AccountBalance CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_ACCT_BAL.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_AccountBalance1)))
        
        CRS_InvolvedParty = (CRS_FLAG | 'Read InvolvedParty CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IP_MM_Child)))
        
        CRS_IP_IP = (CRS_FLAG | 'Read IP-IP CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_IP_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPIP_MM)))
        
        CRS_IP_Account = (CRS_FLAG | 'Read IP-Account CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_IP_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPAccount1)))
        
        CRS_Payments = (CRS_FLAG | 'Read Payments CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_Payment1)))
        
        CRS_IP_TD = (CRS_FLAG | 'Read IP-ID CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_IP_TD.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPTD_MM_Child)))
                       
        #QATAR Changes        
        CRS_IP_ADDN_QA = (CRS_FLAG | 'Read IP ADDN QA CRS csv' >> beam.ParDo(CRS_Read_from_csv(known_args.input),'/CRS_IP_ADDN_QA.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRS_IPADDN_MM_QA_Child)))
               
        
        #CRS_Record Level Validation : Filter invalid records and generate individual logs-----------------------------------#
        CRS_Account_validate = (CRS_FLAG | 'Read Account csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_Account),'CRS_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
    
        CRS_Account_df = CRS_Account_validate['df']
        CRS_Account_log = CRS_Account_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_AccountBalance_validate = (CRS_FLAG | 'Read AccountBalance csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_AccountBalance),'CRS_ACCT_BAL.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_AccountBalance_df = CRS_AccountBalance_validate['df']
        CRS_AccountBalance_log = CRS_AccountBalance_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_InvolvedParty_validate = (CRS_FLAG | 'Read InvolvedParty csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_InvolvedParty),'CRS_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_InvolvedParty_df = CRS_InvolvedParty_validate['df']
        CRS_InvolvedParty_log = CRS_InvolvedParty_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_IP_IP_validate = (CRS_FLAG | 'Read IP-IP csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_IP_IP),'CRS_IP_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_IP_IP_df = CRS_IP_IP_validate['df']
        CRS_IP_IP_log = CRS_IP_IP_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_IP_Account_validate = (CRS_FLAG | 'Read IP-Account csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_IP_Account),'CRS_IP_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_IP_Account_df = CRS_IP_Account_validate['df']
        CRS_IP_Account_log = CRS_IP_Account_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_Payments_validate = (CRS_FLAG | 'Read Payments csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_Payments),'CRS_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_Payments_df = CRS_Payments_validate['df']
        CRS_Payments_log = CRS_Payments_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        CRS_IP_TD_validate = (CRS_FLAG | 'Read IP-TD csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_IP_TD),'CRS_IP_TD.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_IP_TD_df = CRS_IP_TD_validate['df']
        CRS_IP_TD_log = CRS_IP_TD_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]
        
        #QATAR Changes
        CRS_IP_ADDN_QA_validate = (CRS_FLAG | 'Read IP-ADDN QA csv for CRS validation' >> beam.ParDo(CRS_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(CRS_IP_ADDN_QA),'CRS_IP_ADDN_QA.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            CRS_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        CRS_IP_ADDN_QA_df = CRS_IP_ADDN_QA_validate['df']
        CRS_IP_ADDN_QA_log = CRS_IP_ADDN_QA_validate[CRS_Record_level_validation.OUTPUT_TAG_LOG]


        #-----------------------------------------------------------------------Merge IP with IPADDN-------------------------------------------
        #QATAR changes

       
        CRS_IP_Addn_QA_df1 = (CRS_IP_ADDN_QA_df | 'Select CRS columns ADD_QA' >> beam.ParDo(Get_Col(known_args.input),'QA'))
        
        CRS_IP_IPADDN_QA_df = (CRS_InvolvedParty_df | 'CRS Merge IP with IP ADDN QA' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_IP_Addn_QA_df1),['Institution_Id','Involved_Party_Id','CTCR Process ID','Involved_Party_Type'],beam.pvalue.AsSingleton(CRS_FLAG)))
        CRS_InvolvedParty_df1_new = (CRS_IP_IPADDN_QA_df | 'Rename CRS CTCR Process ID_X to CTCR Process ID' >> beam.ParDo(CTCR_rename()))


        CRS_payment_date_validation = (CRS_Account_df | 'CRS Payment Date validation' >> beam.ParDo(CRS_Date_validation_pay_acct(),beam.pvalue.AsSingleton(CRS_Payments_df),['Institution_Id','Account_Id'],'CRS_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))
        
        #CRS_Append logs and write to storage-------------------------------------------------------#
        #CRS_write_log = (CRS_Account_log | 'Writing CRS Log' >> beam.ParDo(CRS_Append_logs_to_csv(known_args.landing),beam.pvalue.AsSingleton(CRS_AccountBalance_log),beam.pvalue.AsSingleton(CRS_InvolvedParty_log),beam.pvalue.AsSingleton(CRS_IP_IP_log),beam.pvalue.AsSingleton(CRS_IP_Account_log),beam.pvalue.AsSingleton(CRS_Payments_log),beam.pvalue.AsSingleton(CRS_IP_TD_log),beam.pvalue.AsSingleton(CRS_payment_date_validation),beam.pvalue.AsSingleton(Unique_ID)))
		
        CRS_append_log = (CRS_Account_log | 'Writing CRS Log' >> beam.ParDo(CRS_Append_logs_to_csv(known_args.landing),beam.pvalue.AsSingleton(CRS_AccountBalance_log),beam.pvalue.AsSingleton(CRS_InvolvedParty_log),beam.pvalue.AsSingleton(CRS_IP_IP_log),beam.pvalue.AsSingleton(CRS_IP_Account_log),beam.pvalue.AsSingleton(CRS_Payments_log),beam.pvalue.AsSingleton(CRS_IP_TD_log),beam.pvalue.AsSingleton(CRS_IP_ADDN_QA_log),beam.pvalue.AsSingleton(CRS_payment_date_validation)))
		
        CRSWriteRecordLog = (CRS_append_log | 'CRS Record Log File' >> beam.ParDo(Write_Record_log(known_args.landing)))
        
        #Formatting Account Balance Amount and Payment Amount
        CRS_AccountBalanceFormat_df = (CRS_AccountBalance_df | 'Formatting CRS Account Balance Currency' >>  beam.ParDo(CRS_AccountBalanceAmount_Format()))

        CRS_PaymentAmount_df = (CRS_Account_df | 'CRS Data Formating' >> beam.ParDo(Data_Format(),beam.pvalue.AsSingleton(CRS_Payments_df),['Institution_Id','Account_Id']))

        CRS_PaymentFormat_df = (CRS_PaymentAmount_df | 'Formatting CRS Payment Amount' >> beam.ParDo(CRS_PaymentAmount_Format()))

        #CURRENCY CONVERSION--------------------------------------------------------------------------#
        CRS_Curency_Conversion = (CRS_AccountBalanceFormat_df | 'Converting CRS Account Balance Currency' >>  beam.ParDo(CRS_Currency_Conversion(known_args.landing),beam.pvalue.AsSingleton(FI)))
        CRS_Curency_Conversion1 = (CRS_Curency_Conversion | 'Converting CRS Payment Currency' >> beam.ParDo(CRS_Currency_Conversion1(known_args.landing),beam.pvalue.AsSingleton(CRS_PaymentFormat_df),beam.pvalue.AsSingleton(FI))) 
        
        CRS_VALID_FLAG = (CRS_Account_df | 'Check if CRS Files are Validated at record level' >> beam.ParDo(CRS_find_csv(),beam.pvalue.AsSingleton(CRS_Account_df),beam.pvalue.AsSingleton(CRS_AccountBalance_df),beam.pvalue.AsSingleton(CRS_Payments_df),beam.pvalue.AsSingleton(CRS_IP_Account_df),beam.pvalue.AsSingleton(CRS_IP_IP_df),beam.pvalue.AsSingleton(CRS_InvolvedParty_df),beam.pvalue.AsSingleton(CRS_IP_TD_df),beam.pvalue.AsSingleton(CRS_InvolvedParty_df1_new)))
        #---------------------------------------------------------------------CRS DATA Transformations---------------------------------------------------------------#
                                                                                                                                                                                      
        #Individual transformation--------------------------------------------------------------#
        CRS_ProcessedAccount = (CRS_Account_df | 'Process CRS Account' >> beam.ParDo(CRS_Process_Accounts(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_ProcessedAccountBalance = (CRS_Curency_Conversion | 'Process CRS Account Balance' >> beam.ParDo(Process_AccountBalance(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_ProcessedPayments = (CRS_Curency_Conversion1 | 'Process CRS Payments' >> beam.ParDo(CRS_Process_Payments(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        
        CRS_ProcessedIPAccountRel = (CRS_IP_Account_df | 'Process CRS IPAcct' >> beam.ParDo(Process_IPAccountRel(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        #CA changes
        CRS_IP_TD_CA_df = (CRS_IP_TD_df | 'Filter CA data from IPTD' >> beam.ParDo(CRS_Filter_IPTD(),'CA'))
        CRS_IP_TD_NonCA_df = (CRS_IP_TD_df | 'Filter Non CA data from IPTD' >> beam.ParDo(CRS_Filter_IPTD(),'NonCA'))
        
        CRS_ProcessedIPTD = (CRS_IP_TD_NonCA_df | 'Process CRS IPTD' >> beam.ParDo(CRS_Process_IPTD(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_ProcessedIP = (CRS_InvolvedParty_df1_new | 'Process CRS IP' >> beam.ParDo(CRS_Process_IP(),beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
                
                                                                                                                                                                                                 
        
        #JOINS----------------------------------------------------------------------------------#
        CRS_AccountwithBalance = (CRS_ProcessedAccount | 'CRS Account and Balance Join' >> beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_ProcessedAccountBalance),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_AccountDetails = (CRS_AccountwithBalance | 'CRS AccountDetails' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_ProcessedPayments),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_AccountDetails = (CRS_AccountDetails | 'CRS Process AccountDetails' >> beam.ParDo(Process_AccountDetails()))
        CRS_Account_IPRel = (CRS_AccountDetails | 'CRS Account_IP_Rel' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(CRS_ProcessedIPAccountRel),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        #CA changes
        CRS_IPDetails1 = (CRS_ProcessedIP | 'CRS IP Join TD CA' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_IP_TD_CA_df),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_IPDetails = (CRS_IPDetails1 | 'CRS IP Join TD' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_ProcessedIPTD),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_FinalDF = (CRS_Account_IPRel | 'CRS Account+IP' >> beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(CRS_IPDetails),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_ReportableFinalDF = (CRS_FinalDF | 'CRS Filter Reportable from FinalDF' >> beam.ParDo(CRS_ProcessReportableFinalDF()))
    
        CRS_AccountHolder = (CRS_ReportableFinalDF | 'CRS AccountHolder' >> beam.ParDo(CRS_ProcessFinalAccount()))
        #CRS_AccountHolder_Join = (FI | 'CRS Join Account Holder and FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(CRS_AccountHolder),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
                                                                                                                                           
                                                                                                                                          
        
        FI_Subset = (FI| 'FI Subset' >>beam.ParDo(FI_Process_Subset()))
        
        CRS_AHLength_Flag = (CRS_AccountHolder | 'Account count check' >> beam.ParDo(CRS_ProcessAHLength_Flag()))
        
        CRS_AccountHolder0 = (CRS_AccountHolder | 'No Split' >> beam.ParDo(NoSplitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag)))

        CRS_AccountHolder1 = (CRS_AccountHolder | 'Split1' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),1))
        CRS_AccountHolder2 = (CRS_AccountHolder | 'Split2' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),2))
        CRS_AccountHolder3 = (CRS_AccountHolder | 'Split3' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),3))
        CRS_AccountHolder4 = (CRS_AccountHolder | 'Split4' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),4))
        CRS_AccountHolder5 = (CRS_AccountHolder | 'Split5' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),5))
        CRS_AccountHolder6 = (CRS_AccountHolder | 'Split6' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),6))
        CRS_AccountHolder7 = (CRS_AccountHolder | 'Split7' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),7))
        CRS_AccountHolder8 = (CRS_AccountHolder | 'Split8' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),8))
        CRS_AccountHolder9 = (CRS_AccountHolder | 'Split9' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),9))
        CRS_AccountHolder10 = (CRS_AccountHolder | 'Split10' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),10))
        CRS_AccountHolder11 = (CRS_AccountHolder | 'Split11' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_AHLength_Flag),11))





        CRS_AccountHolder_SplitMerge =  (CRS_AccountHolder0 | 'Split and Merge AH Data' >> beam.ParDo(CRS_Process_SplitMerge(),beam.pvalue.AsSingleton(CRS_AccountHolder1),beam.pvalue.AsSingleton(CRS_AccountHolder2),beam.pvalue.AsSingleton(CRS_AccountHolder3),beam.pvalue.AsSingleton(CRS_AccountHolder4),beam.pvalue.AsSingleton(CRS_AccountHolder5),beam.pvalue.AsSingleton(CRS_AccountHolder6),beam.pvalue.AsSingleton(CRS_AccountHolder7),beam.pvalue.AsSingleton(CRS_AccountHolder8),beam.pvalue.AsSingleton(CRS_AccountHolder9),beam.pvalue.AsSingleton(CRS_AccountHolder10),beam.pvalue.AsSingleton(CRS_AccountHolder11),beam.pvalue.AsSingleton(CRS_AHLength_Flag)))

       
        CRS_AccountHolder_New = (CRS_AccountHolder_SplitMerge | 'CRS Process Account Holder Join' >>  beam.ParDo(Process_AccountHolder_Join(),'CRS'))

        CRS_AccountHolder_Reportable =  (CRS_AccountHolder_New | 'CRS Process Account Holder Reportable' >>  beam.ParDo(Reportable_Data()))

                                                                                                                                                                                         
        CRS_AccountHolder_NonReportable =  (CRS_AccountHolder_New | 'CRS Process Account Holder Non Reportable' >>  beam.ParDo(NonReportable_Data(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))

    
        #Orphan Log for CRS
        #1
        crs_orphan_acct_bal = (CRS_Account_df|"CRS ACCT-BAL" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(CRS_AccountBalance_df),['Institution_Id','Account_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking Account with Balance'))
        #2
        crs_orphan_acct_pay = (CRS_Account_df|"CRS ACCT and Pay" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(CRS_Payments_df),['Institution_Id','Account_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking Account Payment'))
        #3
        crs_orphan_acct_ip_acct = (CRS_Account_df|"CRS Account and IP Account" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(CRS_IP_Account_df),['Institution_Id','Account_Id'],['Institution_Id','Account_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking Account AND IP Account','None'))
        #4
        crs_orphan_ip_iptd = (CRS_InvolvedParty_df1_new|"CRS ip and iptd" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(CRS_IP_TD_df),['Institution_Id','Involved_Party_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IP AND IPTD'))
        #5
        crs_orphan_ipacct_ip = (CRS_InvolvedParty_df1_new|"CRS ip acct  and ip" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(CRS_IP_Account_df),['Institution_Id','Involved_Party_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IP Account AND IP'))
        #6
        crs_orphan_ip_ipip_child = (CRS_InvolvedParty_df1_new|"CRS IP AND IPIP on child" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(CRS_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Child_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IP AND IPIP on child party id','Left')) 
        #7
        crs_orphan_ipip_ip_parent = (CRS_InvolvedParty_df1_new|"CRS IPIP AND IP on parent" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(CRS_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Parent_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IPIP AND IP on parent party id','Left'))
        #8
        crs_fi_acct_orphan = (FI|"CRS FI AND Accountholder" >> beam.ParDo(Orphan_Acct_holder_Fi(),beam.pvalue.AsSingleton(CRS_AccountHolder),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking FI and AccountHolder'))
		#9
        crs_orphan_ipip_ipacct_parent = (CRS_IP_Account_df|"CRS IPIP AND IP ACC on parent" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(CRS_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Parent_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IPIP and IP Account on parent party id','Left'))
        
        # ip,ipip and ip account orphan's
        crs_ip_orphan = (CRS_InvolvedParty_df1_new|"Crs ip orphan rows" >> beam.ParDo(Ip_Ipip_IpAcct_Orphan(),beam.pvalue.AsSingleton(CRS_IP_IP_df),beam.pvalue.AsSingleton(CRS_IP_Account_df),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IP, IPIP and IP Account'))
        
        #QATAR changes
        #10
        crs_orphan_ip_ipaddn = (CRS_InvolvedParty_df1_new|"CRS IPAADDN AND IP ACC on parent" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(CRS_IP_Addn_QA_df1),['Institution_Id','Involved_Party_Id'],['Institution_Id','Involved_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'CRS','Linking IP and IP Additional on parent party id','Left'))
        
        CRS_RFinalDF = (CRS_ReportableFinalDF | 'CRS Required columns for CP from reportable Dataset' >> beam.ParDo(CRS_ProcessRFinalDF()))
        CRS_RFinalDF_A = (CRS_RFinalDF | 'CRS Get reportable accounts for IP Acct' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(CRS_ProcessedIPAccountRel),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_INST_IP = (CRS_ProcessedIP | 'CRS Get Inst ID for IP' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(CRS_RFinalDF_A),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        

        CRS_INST_IP = (CRS_INST_IP | 'CRS Process INST_IP' >> beam.ParDo(CRS_Process_Inst_IP()))
        CRS_ParentIP = CRS_INST_IP 
        CRS_ChildIP = CRS_IPDetails
        CRS_ProcessedIPIP = (CRS_IP_IP_df | 'CRS Process IP-IP' >> beam.ParDo(CRS_Process_IPIP()))

        CRS_ProcessParentIP = (CRS_ParentIP | 'CRS Process Parent IP' >> beam.ParDo(CRS_Process_Parent_IP()))

        CRS_Process_ChildIP = (CRS_ChildIP | 'CRS Process Child IP' >> beam.ParDo(CRS_Process_Child_IP()))
        CRS_ChildIP_IPRel = (CRS_Process_ChildIP | 'CRS ChildIP with IPRel' >> beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(CRS_ProcessedIPIP),['Institution_Id','Involved_Party_Id'],['Institution_Id','Child_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        CRS_ChildIP_IPRel_Parent = (CRS_ChildIP_IPRel | 'CRS ChildIP with ParentIP' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(CRS_ProcessParentIP),['Institution_Id','Parent_Party_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        
       
        CRS_ControlPerson = (CRS_ChildIP_IPRel_Parent | 'CRS Controlling Person' >> beam.ParDo(CRS_ProcessControlPerson()))
        #CRS_ControlPerson_Join = (FI | 'CRS Control Person and FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(CRS_ControlPerson),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
                
        
        CRS_CPLength_Flag = (CRS_ControlPerson | 'CP Account count check' >> beam.ParDo(CRS_ProcessAHLength_Flag()))
        
        CRS_ControlPerson0 = (CRS_ControlPerson | 'CP No Split' >> beam.ParDo(NoSplitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag)))

        CRS_ControlPerson1 = (CRS_ControlPerson | 'CP Split1' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),1))
        CRS_ControlPerson2 = (CRS_ControlPerson | 'CP Split2' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),2))
        CRS_ControlPerson3 = (CRS_ControlPerson | 'CP Split3' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),3))
        CRS_ControlPerson4 = (CRS_ControlPerson | 'CP Split4' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),4))
        CRS_ControlPerson5 = (CRS_ControlPerson | 'CP Split5' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),5))
        CRS_ControlPerson6 = (CRS_ControlPerson | 'CP Split6' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),6))
        CRS_ControlPerson7 = (CRS_ControlPerson | 'CP Split7' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),7))
        CRS_ControlPerson8 = (CRS_ControlPerson | 'CP Split8' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),8))
        CRS_ControlPerson9 = (CRS_ControlPerson | 'CP Split9' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),9))
        CRS_ControlPerson10 = (CRS_ControlPerson | 'CP Split10' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),10))
        CRS_ControlPerson11 = (CRS_ControlPerson | 'CP Split11' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(CRS_CPLength_Flag),11))

       

        
        CRS_CP_SplitMerge =  (CRS_ControlPerson0 | 'CP Split and Merge AH Data' >> beam.ParDo(CRS_Process_SplitMerge(),beam.pvalue.AsSingleton(CRS_ControlPerson1),beam.pvalue.AsSingleton(CRS_ControlPerson2),beam.pvalue.AsSingleton(CRS_ControlPerson3),beam.pvalue.AsSingleton(CRS_ControlPerson4),beam.pvalue.AsSingleton(CRS_ControlPerson5),beam.pvalue.AsSingleton(CRS_ControlPerson6),beam.pvalue.AsSingleton(CRS_ControlPerson7),beam.pvalue.AsSingleton(CRS_ControlPerson8),beam.pvalue.AsSingleton(CRS_ControlPerson9),beam.pvalue.AsSingleton(CRS_ControlPerson10),beam.pvalue.AsSingleton(CRS_ControlPerson11),beam.pvalue.AsSingleton(CRS_CPLength_Flag)))
                                                                                                             

       
        CRS_ControlPerson_New = (CRS_CP_SplitMerge | 'CRS Process Control Person Join' >>  beam.ParDo(Process_ControlPerson_Join()))
        
        CRS_ControlPerson_Reportable =  (CRS_ControlPerson_New | 'CRS Process Control person Reportable' >>  beam.ParDo(Reportable_Data()))
        CRS_ControlPerson_NonReportable =  (CRS_ControlPerson_New | 'CRS Process Control person Non Reportable' >>  beam.ParDo(NonReportable_Data(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))

        #FIcontrolPerson
        CRS_FI_CP_Acct = (FI | 'CRS IP and Account for FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(CRS_ReportableFinalDF),['Abbrev. (Insitution ID)                 (8 char limit)'],['Institution_Id'],beam.pvalue.AsSingleton(CRS_VALID_FLAG)))
        

        CRS_FI_Final = (CRS_FI_CP_Acct | 'CRS FFI and SE' >> beam.ParDo(CRS_ProcessFI_CP_Acct()))

	    
        CRS_Header_Mapping = (CRS_FI_Final | 'CRS Mapping header' >> beam.ParDo(header_mapping(),beam.pvalue.AsSingleton(FI)))
                
        #------------------------------CRS Exception Log-----------------------------------------------------------#
        CRS_Exceptionlog = (CRS_append_log | 'CRS Exception Logs' >>  beam.ParDo(ProcessExceptionLog(),beam.pvalue.AsSingleton(CRS_IP_Account)))
																																											



        #CRS Write Zip file----------------------------------------------------------------------------------#
        CRS_Writezip = (CRS_ControlPerson_Reportable | 'CRS Append Sheets to zip' >> beam.ParDo(CRS_Append_df_to_zip(known_args.input,known_args.business_secrets,known_args.landing),beam.pvalue.AsSingleton(CRS_AccountHolder_Reportable),beam.pvalue.AsSingleton(CRS_FI_Final),beam.pvalue.AsSingleton(CRS_Header_Mapping),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Vol_Split_Thresold),beam.pvalue.AsSingleton(CRS_Exceptionlog)))                 
       
        
        
        #CRS Write API Log
        CRSWriteAPILog = (CRS_Writezip | 'CRS API LOG File' >> beam.ParDo(Write_API_log(known_args.input,known_args.business_secrets,known_args.landing)))
        
        #CRS Update Zip Flag, Load Status and write back to storage
        CRS_UpdateZip = (FTR_ConsoleIP | 'Update Zipflag CRS to ProcessLog' >> beam.ParDo(Update_Zipflag_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(CRSWriteAPILog),'CRS',beam.pvalue.AsSingleton(CRS_VALID_FLAG)))               
        
        WriteCRS_ZipFlag = (CRS_UpdateZip | 'Write Zip Flag to Process Log for CRS' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #CRS Write DF to excel-------------------------------------------------------------------------------#
        
        CRS_NonReportableFinalDF = (CRS_FinalDF | 'Filter Non-Reportable from CRS FinalDF' >> beam.ParDo(CRS_ProcessNonReportableFinalDF(),beam.pvalue.AsSingleton(CRS_AccountHolder_NonReportable),beam.pvalue.AsSingleton(CRS_ControlPerson_NonReportable),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))

        #CRS NonReportableLog
        CRS_WriteNRFinalDF = (CRS_NonReportableFinalDF | 'Write CRS Non-Reported file to csv' >> beam.ParDo(Write_file_to_csv(known_args.landing),'Non-Reportable Log.csv'))        
        
        #------------------------------------------------FATCA Staging Pipeline-------------------------------------------------------------------#  
        
        #FATCA - Read files from staging bucket and filter delta records----------------------------------#
        FATCA_Account = (FATCA_FLAG | 'Read Account FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_Account1) ))
        
        FATCA_AccountBalance = (FATCA_FLAG | 'Read AccountBalance FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_ACCT_BAL.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_AccountBalance1)))
        
        FATCA_InvolvedParty = (FATCA_FLAG | 'Read InvolvedParty FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IP_MM_Child)))
                        
        FATCA_IP_IP = (FATCA_FLAG | 'Read IP-IP FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_IP_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPIP_MM)))
        
        FATCA_IP_Account = (FATCA_FLAG | 'Read IP-Account FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_IP_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPAccount1)))
        
        FATCA_Payments = (FATCA_FLAG | 'Read Payments FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_Payment1)))
        
        # New pipeline
        FATCA_IP_ADD_CA = (FATCA_FLAG | 'Read IP_ADD_CA FATCA csv' >> beam.ParDo(FATCA_Read_from_csv(known_args.input),'/FTR_IP_ADDN_CA.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCA_IPCA_MM_Child)))
        
        #FATCA_Record Level Validation : Filter invalid records and generate individual logs-----------------------------------#
        FATCA_Account_validate = (FATCA_FLAG | 'Read Account csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_Account),'FTR_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        
        FATCA_Account_df = FATCA_Account_validate['df']
        FATCA_Account_log = FATCA_Account_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]
    
        FATCA_AccountBalance_validate = (FATCA_FLAG | 'Read AccountBalance csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_AccountBalance),'FTR_ACCT_BAL.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
    
        FATCA_AccountBalance_df = FATCA_AccountBalance_validate['df']
        FATCA_AccountBalance_log = FATCA_AccountBalance_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]
    
        FATCA_InvolvedParty_validate = (FATCA_FLAG | 'Read InvolvedParty csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_InvolvedParty),'FTR_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        
        FATCA_InvolvedParty_df = FATCA_InvolvedParty_validate['df']
        FATCA_InvolvedParty_log = FATCA_InvolvedParty_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]
        
                            
        FATCA_IP_IP_validate = (FATCA_FLAG | 'Read IP-IP csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_IP_IP),'FTR_IP_IP.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        
        FATCA_IP_IP_df = FATCA_IP_IP_validate['df']
        FATCA_IP_IP_log = FATCA_IP_IP_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]
        
        FATCA_IP_Account_validate = (FATCA_FLAG | 'Read IP-Account csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_IP_Account),'FTR_IP_ACCT.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        FATCA_IP_Account_df = FATCA_IP_Account_validate['df']
        FATCA_IP_Account_log = FATCA_IP_Account_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]
    
        FATCA_Payments_validate = (FATCA_FLAG | 'Read Payments csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_Payments),'FTR_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        FATCA_Payments_df = FATCA_Payments_validate['df']
        FATCA_Payments_log = FATCA_Payments_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]  
        
        FATCA_payment_date_validation = (FATCA_Account_df | 'FATCA Payment Date validation' >> beam.ParDo(FATCA_Date_validation_pay_acct(),beam.pvalue.AsSingleton(FATCA_Payments_df),['Institution_Id','Account_Id'],'FATCA_PAY.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))    
        
        # New pipeline
        FATCA_IP_ADD_CA_validate = (FATCA_FLAG | 'Read IP-ADD_CA csv for FATCA validation' >> beam.ParDo(FATCA_Record_level_validation(known_args.input),beam.pvalue.AsSingleton(FATCA_IP_ADD_CA),'FTR_IP_ADDN_CA.csv',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Read_usdms)).with_outputs(
                            FATCA_Record_level_validation.OUTPUT_TAG_LOG,
                            main='df'))
        # New pipeline
        FATCA_IP_ADD_CA_df = FATCA_IP_ADD_CA_validate['df']
        # New pipeline
        FATCA_IP_ADD_CA_log = FATCA_IP_ADD_CA_validate[FATCA_Record_level_validation.OUTPUT_TAG_LOG]

        CRS_FATCA_record_current_date_log = 'RECORD_LEVEL_LOG' +'.csv'
       
        # New pipeline
        FATCA_IP_ADD_CA_df1 = (FATCA_IP_ADD_CA_df | 'Select columns ADD_CA' >> beam.ParDo(Get_Col(known_args.input),'CA'))
       
       
		##read previous log from storage
        Read_record_level_log_csv = (pipeline1 | 'reading record level log' >> beam.Create([None])
                                        | 'Initiate reading record level log' >> beam.ParDo(read_record_level_log_csv(known_args.landing),'RECORD_LEVEL_LOG.csv'))
        
        #FATCA_Append logs -------------------------------------------------------#
        FATCA_append_log = (FATCA_Account_log | 'Writing FATCA Log' >> beam.ParDo(FATCA_Append_logs_to_csv(known_args.landing),beam.pvalue.AsSingleton(FATCA_AccountBalance_log),beam.pvalue.AsSingleton(FATCA_InvolvedParty_log),beam.pvalue.AsSingleton(FATCA_IP_IP_log),beam.pvalue.AsSingleton(FATCA_IP_Account_log),beam.pvalue.AsSingleton(FATCA_Payments_log),beam.pvalue.AsSingleton(FATCA_payment_date_validation),beam.pvalue.AsSingleton(FATCA_IP_ADD_CA_log)))
		
        FATCAWriteRecordLog = (FATCA_append_log | 'FTR Record Log File' >> beam.ParDo(Write_Record_log(known_args.landing)))
       
        
        #Formatting Account Balance Amount and Payment Amount
        Fatca_AccountBalanceFormat_df = (FATCA_AccountBalance_df | 'Formatting Fatca Account Balance Currency' >>  beam.ParDo(Fatca_AccountBalanceAmount_Format()))
        

        FATCA_PaymentAmount_df = (FATCA_Account_df | 'FATCA Data Formating' >> beam.ParDo(Data_Format(),beam.pvalue.AsSingleton(FATCA_Payments_df),['Institution_Id','Account_Id']))
    
        FATCA_PaymentFormat_df = (FATCA_PaymentAmount_df | 'Formatting Payment Amount' >> beam.ParDo(Fatca_PaymentAmount_Format()))
        
        
        FATCA_Curency_Conversion = (Fatca_AccountBalanceFormat_df | 'Converting FATCA Account Balance Currency' >>  beam.ParDo(FATCA_Currency_Conversion(known_args.landing),beam.pvalue.AsSingleton(FI)))             
        
        
        FATCA_Curency_Conversion1 = (FATCA_Curency_Conversion | 'Converting FATCA Payment Currency' >> beam.ParDo(FATCA_Currency_Conversion1(known_args.landing),beam.pvalue.AsSingleton(FATCA_PaymentFormat_df),beam.pvalue.AsSingleton(FI)))                    
        
        FATCA_VALID_FLAG = (FATCA_Account_df | 'Check if FATCA Files are Validated at record level' >> beam.ParDo(FATCA_find_csv(),beam.pvalue.AsSingleton(FATCA_Account_df),beam.pvalue.AsSingleton(FATCA_AccountBalance_df),beam.pvalue.AsSingleton(FATCA_Payments_df),beam.pvalue.AsSingleton(FATCA_IP_Account_df),beam.pvalue.AsSingleton(FATCA_IP_IP_df),beam.pvalue.AsSingleton(FATCA_InvolvedParty_df),beam.pvalue.AsSingleton(FATCA_IP_ADD_CA_df1)))    
        
        #------------------------------------------------------FATCA DATA TRANSFOMRATIONS---------------------------------------------------#

        #---------------------------------- -------------------Merging of IP and IPAddCA ---------------------------------------------
        # New Pipeline
        FATCA_InvolvedParty_df1 = (FATCA_InvolvedParty_df | 'FATCA IP and IPADDCA' >> beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(FATCA_IP_ADD_CA_df1),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        
        # New Pipeline
        FATCA_InvolvedParty_df1_new = (FATCA_InvolvedParty_df1 | 'Rename CTCR Process ID_X to CTCR Process ID' >> beam.ParDo(CTCR_rename()))
        
        #Individual transformation--------------------------------------------------------------#
        FATCA_ProcessedAccount = (FATCA_Account_df | 'Process FATCA Account' >> beam.ParDo(FATCA_Process_Accounts(),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        FATCA_ProcessedAccountBalance = (FATCA_Curency_Conversion | 'Process FATCA Account Balance' >> beam.ParDo(Process_AccountBalance(),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        FATCA_ProcessedPayments = (FATCA_Curency_Conversion1 | 'Process FATCA Payments' >> beam.ParDo(FATCA_Process_Payments(),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
                                                                                                                                                                                         
        FATCA_ProcessedIPAccountRel = (FATCA_IP_Account_df | 'Process FATCA IPAcct' >> beam.ParDo(Process_IPAccountRel(),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))


        # New fix
        FATCA_ProcessedIP = (FATCA_InvolvedParty_df1_new | 'Process FATCA IP' >> beam.ParDo(FATCA_Process_IP(),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))                                                                                                                                                                                                                      
        #FATCA JOINS----------------------------------------------------------------------------------#

        FATCA_AccountwithBalance = (FATCA_ProcessedAccount | 'FATCA Account and Balance Join' >> beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(FATCA_ProcessedAccountBalance),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        ###=============================UK De Minimus==========================================================================
        
        FATCA_AccountwithBalance_withUKDMS = (FATCA_AccountwithBalance | 'UK DeMinimus' >> beam.ParDo(UK_DeMinimus(),beam.pvalue.AsSingleton(FATCA_InvolvedParty_df),beam.pvalue.AsSingleton(FI), BQ_table_id).with_outputs(
                            UK_DeMinimus.OUTPUT_TAG_LOG,
                            main='res_final_reportable'))
                            
        FATCA_AccountwithBalance_withUKDMS_df = FATCA_AccountwithBalance_withUKDMS['res_final_reportable']
        res_depository_non_reportable_df = FATCA_AccountwithBalance_withUKDMS[UK_DeMinimus.OUTPUT_TAG_LOG]
        
        Test0 = (FATCA_AccountwithBalance_withUKDMS_df | ' Test0 csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FATCA_AccountwithBalance_withUKDMS_df.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        
        Test4 = (res_depository_non_reportable_df | ' Test4 csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"res_depository_non_reportable_df.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        
        
        
        FATCA_AccountDetails = (FATCA_AccountwithBalance_withUKDMS_df | 'FATCA AccountDetails' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(FATCA_ProcessedPayments),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        
        Test1 = (FATCA_AccountDetails | ' Test1 csv' >> beam.ParDo(Write_df_to_csv(known_args.output),f"FATCA_AccountDetails.csv",FATCA_csv_input_prefix,beam.pvalue.AsSingleton(Unique_ID)))
        
        # #  ================================================================================
		
		
        #FATCA_AccountDetails = (FATCA_AccountwithBalance | 'FATCA AccountDetails' >>  beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(FATCA_ProcessedPayments),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
		
		
        FATCA_AccountDetails = (FATCA_AccountDetails | 'FATCA Process AccountDetails' >> beam.ParDo(Process_AccountDetails()))
        FATCA_Account_IPRel = (FATCA_AccountDetails | 'FATCA Account_IP_Rel' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(FATCA_ProcessedIPAccountRel),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        
       
        #FATCA_FinalDF = (FATCA_Account_IPRel | 'FATCA Account+IP' >> beam.ParDo(LeftJoin(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))

        
        #FATCA_ReportabilityFinalDF = (FATCA_FinalDF | 'FATCA Check Reportability from FinalDF' >> beam.ParDo(FATCA_ProcessReportability(known_args.landing),beam.pvalue.AsSingleton(FI)))

        
        FATCA_RepLength_Flag = (FATCA_Account_IPRel | 'FATCA Reportable check' >> beam.ParDo(FATCA_ProcessAHLength_Flag()))

        FATCA_FinalDF0 = (FATCA_Account_IPRel | 'FATCA Reportable No Split' >> beam.ParDo(NoSplitandleftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag)))



        FATCA_FinalDF1 = (FATCA_Account_IPRel | 'FATCA Reportable Split1' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),1))
        FATCA_FinalDF2 = (FATCA_Account_IPRel | 'FATCA Reportable Split2' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),2))
        FATCA_FinalDF3 = (FATCA_Account_IPRel | 'FATCA Reportable Split3' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),3))
        FATCA_FinalDF4 = (FATCA_Account_IPRel | 'FATCA Reportable Split4' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),4))
        FATCA_FinalDF5 = (FATCA_Account_IPRel | 'FATCA Reportable Split5' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),5))
        FATCA_FinalDF6 = (FATCA_Account_IPRel | 'FATCA Reportable Split6' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),6))
        FATCA_FinalDF7 = (FATCA_Account_IPRel | 'FATCA Reportable Split7' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),7))
        FATCA_FinalDF8 = (FATCA_Account_IPRel | 'FATCA Reportable Split8' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),8))
        FATCA_FinalDF9 = (FATCA_Account_IPRel | 'FATCA Reportable Split9' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),9))
        FATCA_FinalDF10 = (FATCA_Account_IPRel | 'FATCA Reportable Split10' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),10))
        FATCA_FinalDF11 = (FATCA_Account_IPRel | 'FATCA Reportable Split11' >> beam.ParDo(SplitandLeftjoin_RData(),beam.pvalue.AsSingleton(FATCA_ProcessedIP),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_RepLength_Flag),11))

        
        
        FATCA_FinalDF_SplitMerge =  (FATCA_FinalDF0 | 'FATCA Reportable Split and Merge AH Data' >> beam.ParDo(FATCA_Process_SplitMerge(),beam.pvalue.AsSingleton(FATCA_FinalDF1),beam.pvalue.AsSingleton(FATCA_FinalDF2),beam.pvalue.AsSingleton(FATCA_FinalDF3),beam.pvalue.AsSingleton(FATCA_FinalDF4),beam.pvalue.AsSingleton(FATCA_FinalDF5),beam.pvalue.AsSingleton(FATCA_FinalDF6),beam.pvalue.AsSingleton(FATCA_FinalDF7),beam.pvalue.AsSingleton(FATCA_FinalDF8),beam.pvalue.AsSingleton(FATCA_FinalDF9),beam.pvalue.AsSingleton(FATCA_FinalDF10),beam.pvalue.AsSingleton(FATCA_FinalDF11),beam.pvalue.AsSingleton(FATCA_RepLength_Flag)))
        
         

        FATCA_ReportabilityFinalDF = (FATCA_FinalDF_SplitMerge | 'FATCA Check Reportability from FinalDF' >> beam.ParDo(FATCA_ProcessReportability(known_args.landing),beam.pvalue.AsSingleton(FI)))
   
        
        FATCA_ReportableFinalDF = (FATCA_ReportabilityFinalDF | 'FATCA Process Reprtable Dataset from Reportability' >> beam.ParDo(FATCA_ProcessReportableFinalDF()))
       
        
        #------------------------------------------AH & CP CHECK REPORTABILITY-----------------------------------------------
        FATCA_ReportableCP = (FATCA_ReportableFinalDF | 'FATCA Required CP' >> beam.ParDo(FATCA_Reportable_CP()))
        FATCA_ReportableAH = (FATCA_ReportableFinalDF | 'FATCA Required AH' >> beam.ParDo(FATCA_Reportable_AH()))
        FATCA_ReportablePR = (FATCA_ReportableFinalDF | 'FATCA Required PR' >> beam.ParDo(FATCA_Reportable_PR()))

        FATCA_RFinalDF = (FATCA_ReportableCP | 'FATCA Required columns for CP from reportable Dataset' >> beam.ParDo(FATCA_ProcessRFinalDF()))

        FATCA_RFinalDF_A = (FATCA_RFinalDF | 'FATCA Get reportable accounts for IP Acct' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(FATCA_ProcessedIPAccountRel),['Institution_Id','Account_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        FATCA_INST_IP = (FATCA_ProcessedIP | 'FATCA Get Inst ID for IP' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(FATCA_RFinalDF_A),['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        FATCA_INST_IP = (FATCA_INST_IP | 'FATCA Process INST_IP' >> beam.ParDo(FATCA_Process_Inst_IP()))
        FATCA_ParentIP = FATCA_INST_IP 
        FATCA_ChildIP = FATCA_ProcessedIP
        FATCA_ProcessedIPIP = (FATCA_IP_IP_df | 'FATCA Process IP-IP' >> beam.ParDo(FATCA_Process_IPIP()))
        FATCA_ProcessParentIP = (FATCA_ParentIP | 'FATCA Process Parent IP' >> beam.ParDo(FATCA_Process_Parent_IP()))
        FATCA_Process_ChildIP = (FATCA_ChildIP | 'FATCA Process Child IP' >> beam.ParDo(FATCA_Process_Child_IP()))
        
        
        FATCA_ParentIP_IPRel = (FATCA_ProcessParentIP | 'FATCA Get Relation for Parent IP' >> beam.ParDo(Join(),beam.pvalue.AsSingleton(FATCA_ProcessedIPIP),['Institution_Id','Parent_Party_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
       
        FATCA_Relation_type = (FATCA_ParentIP_IPRel | 'FATCA Required BO' >> beam.ParDo(FATCA_Rel_type()))
        
        FATCA_ParentIP_IPRel_ChildIP = (FATCA_Relation_type | 'FATCA Get Relation for CHILD IP' >> beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(FATCA_Process_ChildIP),['Institution_Id','Child_Party_Id'],['Institution_Id','Involved_Party_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
        
        FATCA_Reportability_CP_DF = (FATCA_ParentIP_IPRel_ChildIP | 'FATCA Check Reportability from FinalDF CP' >> beam.ParDo(FATCA_ProcessReportability_CP(known_args.landing),beam.pvalue.AsSingleton(FI)))
        FATCA_ReportableFinalDF_CP = (FATCA_Reportability_CP_DF | 'FATCA Process Reprtable Dataset from Reportability for CP' >> beam.ParDo(FATCA_ProcessReportableFinalDF_CP()))

        FATCA_NonReportableFinalDF_CP = (FATCA_Reportability_CP_DF | 'FATCA Process Non Reprtable Dataset for CP' >> beam.ParDo(FATCA_ProcessNonReportableFinalDF_CP(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))
        
        #------MERGE FATCA_ReportableCP and FATCA_ReportableFinalDF_CP

        FATCA_Reportability_AHonCP = (FATCA_ReportableCP | 'Merge Reportable CP with AH' >> beam.ParDo(FATCA_ProcessReportability_AHonCP(),beam.pvalue.AsSingleton(FATCA_ReportableFinalDF_CP)))
        #Merged  reportable Dataset
        FATCA_Reportability_AHwithCP = (FATCA_Reportability_AHonCP | 'Merge Reportable AH with CP' >> beam.ParDo(FATCA_ProcessReportability_AHwithCP(),beam.pvalue.AsSingleton(FATCA_ReportableAH)))
        
        
        
        #Merged Non reportable logs
        FATCA_NonReportableFinalDF = (FATCA_ReportabilityFinalDF | 'Process Non Reprtable log from FATCA Reportability' >> beam.ParDo(FATCA_ProcessNonReportableFinalDF(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime), beam.pvalue.AsSingleton(res_depository_non_reportable_df)))
        
        
        #FATCA_NonReportableFinalDF = (FATCA_ReportabilityFinalDF | 'Process Non Reprtable log from FATCA Reportability' >> beam.ParDo(FATCA_ProcessNonReportableFinalDF(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))

        FATCA_NonReportable_AHandCP = (FATCA_Reportability_AHonCP | 'Merge Reportable AH without CP' >> beam.ParDo(FATCA_ProcessReportability_AHwoCP(),beam.pvalue.AsSingleton(FATCA_NonReportableFinalDF_CP),beam.pvalue.AsSingleton(FATCA_NonReportableFinalDF),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime)))
        
        #FInal Account Holder DataSet
        FATCA_AccountHolder = (FATCA_Reportability_AHwithCP | 'FATCA AccountHolder' >> beam.ParDo(FATCA_ProcessFinalAccount()))
                                                                                                                                               
        #FATCA_AccountHolder_Join = (FI | 'FATCA Join Account Holder and FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(FATCA_AccountHolder),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
                                                                                                                                                   
																																				   
        FATCA_AHLength_Flag = (FATCA_AccountHolder | 'FATCA Account count check' >> beam.ParDo(FATCA_ProcessAHLength_Flag()))

        FATCA_AccountHolder0 = (FATCA_AccountHolder | 'FATCA No Split' >> beam.ParDo(NoSplitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag)))



        FATCA_AccountHolder1 = (FATCA_AccountHolder | 'FATCA Split1' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),1))
        FATCA_AccountHolder2 = (FATCA_AccountHolder | 'FATCA Split2' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),2))
        FATCA_AccountHolder3 = (FATCA_AccountHolder | 'FATCA Split3' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),3))
        FATCA_AccountHolder4 = (FATCA_AccountHolder | 'FATCA Split4' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),4))
        FATCA_AccountHolder5 = (FATCA_AccountHolder | 'FATCA Split5' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),5))
        FATCA_AccountHolder6 = (FATCA_AccountHolder | 'FATCA Split6' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),6))
        FATCA_AccountHolder7 = (FATCA_AccountHolder | 'FATCA Split7' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),7))
        FATCA_AccountHolder8 = (FATCA_AccountHolder | 'FATCA Split8' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),8))
        FATCA_AccountHolder9 = (FATCA_AccountHolder | 'FATCA Split9' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),9))
        FATCA_AccountHolder10 = (FATCA_AccountHolder | 'FATCA Split10' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),10))
        FATCA_AccountHolder11 = (FATCA_AccountHolder | 'FATCA Split11' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_AHLength_Flag),11))

        
        
        FATCA_AccountHolder_SplitMerge =  (FATCA_AccountHolder0 | 'FATCA Split and Merge AH Data' >> beam.ParDo(FATCA_Process_SplitMerge(),beam.pvalue.AsSingleton(FATCA_AccountHolder1),beam.pvalue.AsSingleton(FATCA_AccountHolder2),beam.pvalue.AsSingleton(FATCA_AccountHolder3),beam.pvalue.AsSingleton(FATCA_AccountHolder4),beam.pvalue.AsSingleton(FATCA_AccountHolder5),beam.pvalue.AsSingleton(FATCA_AccountHolder6),beam.pvalue.AsSingleton(FATCA_AccountHolder7),beam.pvalue.AsSingleton(FATCA_AccountHolder8),beam.pvalue.AsSingleton(FATCA_AccountHolder9),beam.pvalue.AsSingleton(FATCA_AccountHolder10),beam.pvalue.AsSingleton(FATCA_AccountHolder11),beam.pvalue.AsSingleton(FATCA_AHLength_Flag)))
		
        FATCA_AccountHolder_New = (FATCA_AccountHolder_SplitMerge | 'FATCA Process Account Holder Join' >>  beam.ParDo(Process_AccountHolder_Join(),'FATCA'))

        #Orphan Join
        #1
        ftr_orphan_acct_bal = (FATCA_Account_df|"FATCA ACCT-BAL" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(FATCA_AccountBalance_df),['Institution_Id','Account_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking Account with Balance'))
        #2
        ftr_orphan_acct_pay = (FATCA_Account_df|"FATCA ACCT and Pay" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(FATCA_Payments_df),['Institution_Id','Account_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking Account Payment'))
        #3
        ftr_orphan_acct_ip_acct = (FATCA_Account_df|"FATCA Account and IP Account" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(FATCA_IP_Account_df),['Institution_Id','Account_Id'],['Institution_Id','Account_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking Account AND IP Account','None'))
        #4
        ftr_orphan_ipacct_ip = (FATCA_InvolvedParty_df1_new|"FATCA ip acct  and ip" >> beam.ParDo(Orphan_rows_join(),beam.pvalue.AsSingleton(FATCA_IP_Account_df),['Institution_Id','Involved_Party_Id'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IP Account AND IP'))
        #5
        ftr_orphan_ip_ipip_child = (FATCA_InvolvedParty_df1_new|"FATCA IP AND IPIP on child" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(FATCA_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Child_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IP AND IPIP on child party id','Left')) 
        #6
        ftr_orphan_ipip_ip_parent = (FATCA_InvolvedParty_df1_new|"FATCA IPIP AND IP on parent" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(FATCA_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Parent_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IPIP AND IP on parent party id','Left'))
        #Fix:
        FATCA_FIOrphan = (FATCA_FinalDF_SplitMerge | 'Rename Fields to Log fields' >> beam.ParDo(FTR_FieldRename()))     
        #7
        ftr_fi_acct_orphan = (FI|"FATCA FI AND Accountholder" >> beam.ParDo(Orphan_Acct_holder_Fi(),beam.pvalue.AsSingleton(FATCA_FIOrphan),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],'left',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking FI and AccountHolder')) 
        #8
        ftr_orphan_ipip_ipacct_parent = (FATCA_IP_Account_df|"FATCA IPIP AND IP ACC on parent" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(FATCA_IP_IP_df),['Institution_Id','Involved_Party_Id'],['Institution_Id','Parent_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IPIP and IP Account on parent party id','Left'))
        #9
        ftr_ip_orphan = (FATCA_InvolvedParty_df1_new|"Ftr ip orphan rows" >> beam.ParDo(Ip_Ipip_IpAcct_Orphan(),beam.pvalue.AsSingleton(FATCA_IP_IP_df),beam.pvalue.AsSingleton(FATCA_IP_Account_df),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IP, IPIP and IP Account'))
        
        

        # New Pipeline
        ftr_ip_ipadd_CA_orphan = (FATCA_InvolvedParty_df1_new|"FATCA ip addCA  and ip" >> beam.ParDo(Orphan_rows_inner_join(),beam.pvalue.AsSingleton(FATCA_IP_ADD_CA_df1),['Institution_Id','Involved_Party_Id'],['Institution_Id','Involved_Party_Id'],'inner',beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),'FTR','Linking IP ADDCA AND IP','Left'))

        crs_orphan_log = (crs_orphan_acct_bal|"crs_orphan log" >> beam.ParDo(CRS_Orphan_logs_to_csv(known_args.landing),beam.pvalue.AsSingleton(crs_orphan_acct_pay),beam.pvalue.AsSingleton(crs_orphan_acct_ip_acct),beam.pvalue.AsSingleton(crs_orphan_ip_iptd),beam.pvalue.AsSingleton(crs_orphan_ipacct_ip),beam.pvalue.AsSingleton(crs_orphan_ip_ipip_child),beam.pvalue.AsSingleton(crs_orphan_ipip_ip_parent),beam.pvalue.AsSingleton(crs_fi_acct_orphan),beam.pvalue.AsSingleton(crs_orphan_ipip_ipacct_parent),beam.pvalue.AsSingleton(crs_orphan_ip_ipaddn),beam.pvalue.AsSingleton(crs_ip_orphan)))
        
        ftr_orphan_log = (ftr_orphan_acct_bal|"ftr_orphan log" >> beam.ParDo(FATCA_Orphan_logs_to_csv(known_args.landing),beam.pvalue.AsSingleton(ftr_orphan_acct_pay),beam.pvalue.AsSingleton(ftr_orphan_acct_ip_acct),beam.pvalue.AsSingleton(ftr_orphan_ipacct_ip),beam.pvalue.AsSingleton(ftr_orphan_ip_ipip_child),beam.pvalue.AsSingleton(ftr_orphan_ipip_ip_parent),beam.pvalue.AsSingleton(ftr_fi_acct_orphan),beam.pvalue.AsSingleton(ftr_orphan_ipip_ipacct_parent),beam.pvalue.AsSingleton(ftr_ip_ipadd_CA_orphan),beam.pvalue.AsSingleton(ftr_ip_orphan)))

        
        FATCA_ControlPerson = (FATCA_ReportableFinalDF_CP | 'FATCA Controlling Person' >> beam.ParDo(FATCA_ProcessControlPerson()))
        

        FATCA_CPLength_Flag = (FATCA_ControlPerson | 'FATCA CP Account count check' >> beam.ParDo(FATCA_ProcessAHLength_Flag()))
        
        FATCA_ControlPerson0 = (FATCA_ControlPerson | 'FATCA CP No Split' >> beam.ParDo(NoSplitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag)))

                                                                                                                                                                                                                                                                                                                          
        FATCA_ControlPerson1 = (FATCA_ControlPerson | 'FATCA CP Split1' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),1))
        FATCA_ControlPerson2 = (FATCA_ControlPerson | 'FATCA CP Split2' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),2))
        FATCA_ControlPerson3 = (FATCA_ControlPerson | 'FATCA CP Split3' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),3))
        FATCA_ControlPerson4 = (FATCA_ControlPerson | 'FATCA CP Split4' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),4))
        FATCA_ControlPerson5 = (FATCA_ControlPerson | 'FATCA CP Split5' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),5))
        FATCA_ControlPerson6 = (FATCA_ControlPerson | 'FATCA CP Split6' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),6))
        FATCA_ControlPerson7 = (FATCA_ControlPerson | 'FATCA CP Split7' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),7))
        FATCA_ControlPerson8 = (FATCA_ControlPerson | 'FATCA CP Split8' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),8))
        FATCA_ControlPerson9 = (FATCA_ControlPerson | 'FATCA CP Split9' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),9))
        FATCA_ControlPerson10 = (FATCA_ControlPerson | 'FATCA CP Split10' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),10))
        FATCA_ControlPerson11 = (FATCA_ControlPerson | 'FATCA CP Split11' >> beam.ParDo(Splitandjoin_AHData(),beam.pvalue.AsSingleton(FI_Subset),['FI_ID'],['Abbrev. (Insitution ID)                 (8 char limit)'],beam.pvalue.AsSingleton(FATCA_CPLength_Flag),11))

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
        
        FATCA_CP_SplitMerge =  (FATCA_ControlPerson0 | 'FATCA CP Split and Merge AH Data' >> beam.ParDo(FATCA_Process_SplitMerge(),beam.pvalue.AsSingleton(FATCA_ControlPerson1),beam.pvalue.AsSingleton(FATCA_ControlPerson2),beam.pvalue.AsSingleton(FATCA_ControlPerson3),beam.pvalue.AsSingleton(FATCA_ControlPerson4),beam.pvalue.AsSingleton(CRS_ControlPerson5),beam.pvalue.AsSingleton(FATCA_ControlPerson6),beam.pvalue.AsSingleton(FATCA_ControlPerson7),beam.pvalue.AsSingleton(FATCA_ControlPerson8),beam.pvalue.AsSingleton(FATCA_ControlPerson9),beam.pvalue.AsSingleton(FATCA_ControlPerson10),beam.pvalue.AsSingleton(FATCA_ControlPerson11),beam.pvalue.AsSingleton(FATCA_CPLength_Flag)))
                                                                                                             
                                                                                                                                   
        FATCA_ControlPerson_New = (FATCA_CP_SplitMerge | 'FATCA Process Control Person Join' >>  beam.ParDo(Process_ControlPerson_Join()))

        FATCA_FI_CP_Acct = (FI | 'FATCA IP and Account for FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(FATCA_ReportableFinalDF),['Abbrev. (Insitution ID)                 (8 char limit)'],['Institution_Id'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
       
        FATCA_FI_Final = (FATCA_FI_CP_Acct | 'FATCA FFI and SE' >> beam.ParDo(FATCA_ProcessFI_CP_Acct())) 

        
        FTR_Header_Mapping = (FATCA_FI_Final | 'FATCA Mapping header' >> beam.ParDo(header_mapping(),beam.pvalue.AsSingleton(FI)))

        #Pool Reporting
        FATCA_PoolAH = (FATCA_ReportablePR | 'FATCA Pool AccountHolder' >> beam.ParDo(FATCA_ProcessFinalAccount()))
        FATCA_PoolAH_Join = (FI | 'FATCA Join Pool Account Holder and FI' >>  beam.ParDo(JoinDiffKeys(),beam.pvalue.AsSingleton(FATCA_PoolAH),['Abbrev. (Insitution ID)                 (8 char limit)'],['FI_ID'],beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))
             
                                                                                                                                                                                       


        FATCA_Pool_Reportable = (FATCA_PoolAH_Join | 'FATCA Required pool Reporting Data' >> beam.ParDo(FATCA_ProcessPoolReportable(),beam.pvalue.AsSingleton(Read_usdms),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))

             
                                                                                                                                                                                                   
        
        FATCA_PoolAHforLog_Join = FATCA_PoolAH_Join
        
        FATCA_Pool_ReportLog = (FATCA_PoolAHforLog_Join | 'FATCA pool Reporting Log' >> beam.ParDo(FATCA_ProcessPoolLog(),beam.pvalue.AsSingleton(Read_usdms),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))

        #Exception log --------------------------------------------------------------------------------------------#
        FATCA_Exceptionlog = (FATCA_append_log | 'FATCA Exception Logs' >>  beam.ParDo(ProcessExceptionLog(),beam.pvalue.AsSingleton(FATCA_IP_Account)))
		

  
        #FATCA Write Zip file----------------------------------------------------------------------------------#
        FATCA_Writezip = (FATCA_ControlPerson_New | 'FATCA Append Sheets to zip' >> beam.ParDo(FATCA_Append_df_to_zip(known_args.input,known_args.business_secrets,known_args.landing),beam.pvalue.AsSingleton(FATCA_AccountHolder_New),beam.pvalue.AsSingleton(FATCA_FI_Final),beam.pvalue.AsSingleton(FTR_Header_Mapping),beam.pvalue.AsSingleton(FATCA_Pool_Reportable),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(ETL_start_datetime),beam.pvalue.AsSingleton(Vol_Split_Thresold),beam.pvalue.AsSingleton(FATCA_Exceptionlog)))   
        																									
        #FATCA Write API Log
        FATCAWriteAPILog = (FATCA_Writezip | 'FATCA API LOG File' >> beam.ParDo(Write_API_log(known_args.input,known_args.business_secrets,known_args.landing)))
        
        #FTR Update Zip Flag, Load Status and write back to storage
        FTR_UpdateZip = (CRS_UpdateZip | 'Update Zipflag FTR to ProcessLog' >> beam.ParDo(Update_Zipflag_ProcessLog(),beam.pvalue.AsSingleton(Unique_ID),beam.pvalue.AsSingleton(FATCAWriteAPILog),'FTR', beam.pvalue.AsSingleton(FATCA_VALID_FLAG)))               
        
        WriteFTR_ZipFlag = (FTR_UpdateZip | 'Write Zip Flag to Process Log for FTR' >> beam.ParDo(Write_log_to_csv(known_args.landing),'Process_Log.csv'))
        
        #FATCA Write Non Reportable & pool Report Log file------------------------------------------------------------------------#
        
        FATCA_Write_File = (FATCA_NonReportable_AHandCP| 'Write FATCA Non-Reportable File' >> beam.ParDo(Write_file_to_csv(known_args.landing),'Non-Reportable Log.csv'))
        FATCA_Write_Log = (FATCA_Pool_ReportLog| 'Write FATCA PoolReporting Log File' >> beam.ParDo(Write_PoolReportLog(known_args.landing),'Pool Report Details.csv'))

#---------------------------------Bigquery Pipeline -----------------------------------        
        
        Error_log_bq = (pipeline1| 'Load Error Log to BQ' >> beam.Create([None])
                         | 'Error log csv' >> beam.ParDo(Error_log(known_args.landing),'Error_Master.csv',CI_prefix,BQ_dataset+'ErrorMaster'))
        bq_token = (CRS_FATCA_WriteValidation
                |'setting Bigquery token' >> beam.ParDo(bq_token_value(known_args.landing),beam.pvalue.AsSingleton(FATCA_Reconciliation),beam.pvalue.AsSingleton(crs_orphan_log),beam.pvalue.AsSingleton(ftr_orphan_log),beam.pvalue.AsSingleton(CRS_WriteNRFinalDF),beam.pvalue.AsSingleton(FATCA_Write_File),beam.pvalue.AsSingleton(CRS_append_log),beam.pvalue.AsSingleton(FATCA_append_log),beam.pvalue.AsSingleton(WriteFTR_ZipFlag),beam.pvalue.AsSingleton(CRSWriteAPILog),beam.pvalue.AsSingleton(FATCAWriteAPILog),beam.pvalue.AsSingleton(Write_CRS_FileInventory),beam.pvalue.AsSingleton(Write_FATCA_FileInventory),beam.pvalue.AsSingleton(Error_log_bq)))

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          

        
#       Big Query ETL Logs Pipeline--------------------------------------------------------------------------------------#
        
        #1
        Filelevel_log_bq = (bq_token| 'Read file leve log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'FILE_LEVEL_LOG.csv',log_prefix,BQ_dataset+'ETLFileValidation'))
        
        #2
        Recon_log_bq = (bq_token | 'Read recon log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'Reconciliation_Log.csv',recon_prefix,BQ_dataset+'ETLReconcilationLog'))

        
        #3
        non_repo_bq = (bq_token| 'Read non repo log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'Non-Reportable Log.csv',log_prefix,BQ_dataset+'ETLNonReportableLog'))

        
        #4
        orphan_bq = (bq_token| 'Read orphan log csv' >> (beam.ParDo(Read_from_logs(known_args.landing),'Orphan_LOG.csv',log_prefix,BQ_dataset+'ETLOrphan_Log')))

        #5
        record_level_bq = (bq_token| 'Read record_level csv' >> beam.ParDo(Read_from_logs(known_args.landing),'RECORD_LEVEL_LOG.csv',log_prefix,BQ_dataset+'ETLRecordValidation'))

        
        #6
        process_log_bq = (bq_token | 'Read process_log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'Process_Log.csv',log_prefix,BQ_dataset+'ETLProcessLog'))

        
        #7

        api_log_bq = (bq_token| 'Read api_log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'EYFIRST_API_LOG.csv',log_prefix+Api_folder,BQ_dataset+'ETLAPILog'))
   
        
        #8
        #pool_log_bq = (bq_token | 'pool log csv' >> beam.ParDo(Read_from_logs(known_args.landing),'Pool Report Details.csv',log_prefix,BQ_dataset+'ETLPool_Report'))
        
                                                                                                                                                                    
        #9
        CRS_volumnesplitlog = (bq_token| 'Read CRS volumnesplitlog log csv' >> beam.ParDo(move_etlvolumnesplitlog(known_args.landing),'ETLVolumeSplitLog_CRS.csv',log_prefix,BQ_dataset+'ETLVolumeSplitLog'))
        
        #10
        FTR_volumnesplitlog = (bq_token| 'Read FTR volumnesplitlog log csv' >> beam.ParDo(move_etlvolumnesplitlog(known_args.landing),'ETLVolumeSplitLog_FTR.csv',log_prefix,BQ_dataset+'ETLVolumeSplitLog'))


        file_inventory_bq = (bq_token | 'Read file_inventory xls' >> beam.ParDo(Read_file_inventory_log(known_args.landing),'CRS_FileInventory.xls','FATCA_FileInventory.xls',mdt_prefix,BQ_dataset+'ETLFileInventoryList'))
        
        error_message_update = (Filelevel_log_bq|'update error messages'>>beam.ParDo(bq_error_message(),beam.pvalue.AsSingleton(record_level_bq),BQ_table_id))

        move_File_log_bq = (Filelevel_log_bq| 'move and delete filelevel logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'FILE_LEVEL_LOG.csv',log_prefix))
        
        move_Recon_bq = (Recon_log_bq| 'move and delete recon logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'Reconciliation_Log.csv',recon_prefix))

        move_non_repo = (non_repo_bq| 'move and delete non_reportable logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'Non-Reportable Log.csv',log_prefix))
        
        move_orphan = (orphan_bq| 'move and delete orphan logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'Orphan_LOG.csv',log_prefix))
        
        move_recordlevel = (record_level_bq| 'move and delete record level logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'RECORD_LEVEL_LOG.csv',log_prefix))
        
        move_process = (process_log_bq| 'move and delete process logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'Process_Log.csv',log_prefix))
        
        #move_pool_log = (pool_log_bq| 'move and delete pool logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'Pool Report Details.csv',log_prefix))
        move_api = (api_log_bq| 'move and delete api logs' >> beam.ParDo(move_logs(known_args.landing,known_args.processed),'EYFIRST_API_LOG.csv',log_prefix+Api_folder))
        
        move_CRSETLVolumesplit_log = (CRS_volumnesplitlog| 'move and delete ETLVolume Split log CRS' >> beam.ParDo(move_volsplitlogs(known_args.landing,known_args.processed),'ETLVolumeSplitLog_CRS.csv',log_prefix))

        move_FTRETLVolumesplit_log = (FTR_volumnesplitlog| 'move and delete ETLVolume Split log FTR' >> beam.ParDo(move_volsplitlogs(known_args.landing,known_args.processed),'ETLVolumeSplitLog_FTR.csv',log_prefix))
																																												 
        CRS_Move_stage1_csv = (bq_token| 'move and delete Stage1 CRS CSV to cloud storage' >> beam.ParDo(move_Csv(known_args.output,known_args.processed),CRS_csv_input_prefix))

        FATCA__Move_stage1_csv = (bq_token| 'move and delete Stage1 FATCA CSV to cloud storage' >> beam.ParDo(move_Csv(known_args.output,known_args.processed),FATCA_csv_input_prefix))
        
        
        
                                                                                                                                                                                                 
        
                                                                                                                                                                                                          
#----------------Running Dataflow pipeline------------------------

if __name__ == '__main__':
   #logging.getLogger().setLevel(logging.INFO)
   
   run()
