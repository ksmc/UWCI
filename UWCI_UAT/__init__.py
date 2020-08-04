import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
#from azure.storage.blob import BlockBlobService
import pandas as pd
import numpy as np
from io import StringIO
import pkgutil
import os
import re
import encodings
import chardet
from datetime import datetime
import pyodbc
from pandas import ExcelFile
from pandas import ExcelWriter
from datetime import datetime, timedelta


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    SERVER = 'uwciuatv1sqlserver.database.windows.net'
    DATABASE = 'impact-funds-uat'
    username = 'uwciadmin'
    pwd = 'Wd4cdz/7'
    driver= '{ODBC Driver 17 for SQL Server}'
    logging.info(f"Read all the sql server varoables but not connected yet")
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+username+';PWD='+ pwd)
    cursor = cnxn.cursor()
    sql='''select count(*) from [Stage].[CBO_Detail_Person]'''
    cursor.execute(sql)
    logging.info(f"Read till this point")
    count=cursor.fetchone()
    print("Connected to Azure SQL%s"%count)
    logging.info(f"Connected to Azure SQL. Total Records in CBO_Detail_Person {count}")
    Curr_dt = datetime.now()
    
    #BLOB_STORAGEACCOUNTNAME="uwciuatv1ftp"
    #BLOB_STORAGEACCOUNTKEY="b35w8vKSfdhp0xPT8ab6ZuLU5DxW6Pi6RtG1M8qF9TgArQc0h1LwtckDsUNY+YegjELx4KjRApQgV+hbKhNgOA=="
    BLOBNAME="SFTP_File"
    CONTAINERNAME= "sftpfile"
    #ARCHIVE_CONTAINER="uwci-sftp-archive"
    logging.info(f"Read all the blob variables")
    
    def Metric_Insert(File_Master_ID,Person_CUID,Family_ID,Age,Metric,Metric_value,Metric_desc,Metric_type,Metric_group):
        cnxn1 = pyodbc.connect('DRIVER='+driver+';SERVER='+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+username+';PWD='+ pwd)
        cursor = cnxn1.cursor()
        #print("Connected in Metric_Insert")
        sql9='''Insert into Stage.CBO_Detail_Person(File_Master_ID,Person_CUID,
                Family_ID,Age,Metric,Metric_value,Metric_desc,Metric_type,Metric_group) Values (?,?,?,?,?,?,?,?,?);'''
        Val9=(File_Master_ID[0],Person_CUID,Family_ID,Age,Metric,Metric_value,Metric_desc,Metric_type,Metric_group)
        cursor.execute(sql9,Val9)
        cnxn1.commit()
        cnxn1.close()
        #print("Connected in Metric_Insert")
        return

    def Insert_Error(File_Proc_ID,Error_source,Error_date,Error_desc,Create_Date,File_ID):
        cnxn2 = pyodbc.connect('DRIVER='+driver+';SERVER='+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+username+';PWD='+ pwd)
        cursor = cnxn2.cursor()
        print("Running Insert Error")
        sqlcommand4 = ("Insert into Audit.Error_Log(File_Proc_ID,Error_source,Error_date,Error_desc,Create_date) Values(?,?,?,?,?)")
        Values4 = [File_proc_ID[0],Error_source,Curr_dt,Error_desc,Curr_dt]
        print("Not executed on insert yet")
        cursor.execute(sqlcommand4,Values4)
        print("Not comitted on insert yet")
        cnxn2.commit()
        #Updating error on File_processing_log
        End_dt=datetime.now()
        cnxn2.commit()
        print("FileID :%s"%File_ID[0])
        sql7='''Select Error_ID from Audit.Error_Log order by Error_date desc'''
        cursor.execute(sql7)
        Error_ID=cursor.fetchone()
        cnxn2.commit()
        print("Error ID:%s"%Error_ID[0])
        sql8 = '''UPDATE Audit.File_Processing_Log SET Proc_End_Date=? , Proc_status_code=?, 
                Proc_error_ind=?, Error_ID=?
                WHERE File_ID=?'''
        Val8 = [End_dt,'Error',1,Error_ID[0],File_ID[0]]
        cursor.execute(sql8,Val8)
        cnxn2.commit()
        cnxn2.close()
        return


    def rename_blob(blob_name1):
        connection_string="DefaultEndpointsProtocol=https;AccountName=uwciuatv1ftp;AccountKey=cXOsBDrRKg3rM2FoZeQmitus5vCy88zN13umkKi4jacBmiavvsE9XkI7+jJtIzKeaZou0izHWaBqn5vWY5brhA==;EndpointSuffix=core.windows.net"
        account_name="uwciuatv1ftp"
        # Source
        source_container_name = "sftpfile"
        source_file_path = "SFTP_File"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        source_blob = (f"https://{account_name}.blob.core.windows.net/{source_container_name}/{source_file_path}")

        # Target
        target_container_name = "uwci-sftp-archive"
        target_file_path = blob_name1
        copied_blob = blob_service_client.get_blob_client(target_container_name, target_file_path)
        copied_blob.start_copy_from_url(source_blob)

        # If you would like to delete the source file
        remove_blob = blob_service_client.get_blob_client(source_container_name, source_file_path)
        remove_blob.delete_blob()

        return
        

    Person_CUID=[]
    Family_ID=[]
    Age=[]
     

    try:
        print("Connected to blob")
        logging.info(f"Yet to get the blob path")
        '''blob_service=BlockBlobService(account_name=BLOB_STORAGEACCOUNTNAME,account_key=BLOB_STORAGEACCOUNTKEY)
        blob_service.get_blob_to_path(CONTAINERNAME,BLOBNAME,LOCALFILENAME)'''
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=uwciuatv1ftp;AccountKey=cXOsBDrRKg3rM2FoZeQmitus5vCy88zN13umkKi4jacBmiavvsE9XkI7+jJtIzKeaZou0izHWaBqn5vWY5brhA==;EndpointSuffix=core.windows.net")
        blob_client=blob_service_client.get_blob_client(CONTAINERNAME,BLOBNAME)
        Localfile=blob_client.download_blob().readall()
        # Connecting to the Azure SQL to load the file
        #Loading data to File_log
        logging.info(f"Got the blob path")
        sql = ("INSERT INTO Audit.File_Log(File_Name,File_Date,File_Source,Create_Date) Values(?,?,?,?);")
        Val = ['SFTP_CSV',Curr_dt,'SFTP',Curr_dt]
        cursor.execute(sql,Val)
        cnxn.commit()
        sql=("Select File_ID from Audit.File_Log ORDER BY File_ID desc")
        cursor.execute(sql)
        File_ID=cursor.fetchone()
        cnxn.commit()
        #cnxn.close()
        #Loading data to File_Processing_Log
        sql2 = ("INSERT INTO Audit.File_Processing_Log(File_ID,Proc_Start_Date,Proc_End_Date,Proc_status_code,Proc_error_ind,Create_Date) Values(?,?,?,?,?,?);")
        Val2 = [File_ID[0],Curr_dt,'','Running',0,Curr_dt]
        cursor.execute(sql2,Val2)
        cnxn.commit()
        logging.info(f"Data inserted {File_ID[0]}")
        try:
            #This block will load Stage table and error log
            df1=pd.read_excel(Localfile,sheet_name=['CBO Information','Adult','Child','Family',
                'System Level','Demographics'],header=1,index=False,orient='index')
            
            df_CBO_Info=df1['CBO Information'].dropna()
            logging.info(f"Read into CBO Dataframe")

            for k,v in df_CBO_Info['Organization Information'].items():
                for k1,v1 in df_CBO_Info['Unnamed: 1'].items():
                    if k==k1 and v.find('Grantee Name')>=0:
                        CBO_Name=v1
                    elif k==k1 and v.find('Grantee ID')>=0:
                        CBO_Grantee_ID=v1
                    elif k==k1 and v.find('Grantee Type')>=0:
                        Fund=v1
                    elif k==k1 and v.find('Report Period')>=0:
                        CBO_Rpt_Period=v1
                    else:
                        pass
            #Insert into Metrics.Dim_Fund
            #Insert into Metrics.Dim_Rpt_Period
            #Insert into Stage.File_Master
            blob_name1=CBO_Name+str(Curr_dt)
            sql3= ("INSERT INTO Stage.File_Master(File_ID,File_Type,File_Date,Create_date,File_Status,Update_date,Fund,CBO,CBO_Name,Report_Period) Values (?,?,?,?,?,?,?,?,?,?);")
            Val3 = (File_ID[0],'D',Curr_dt,Curr_dt,'Received',Curr_dt,Fund,CBO_Grantee_ID,CBO_Name,CBO_Rpt_Period)
            cursor.execute(sql3,Val3)
            cnxn.commit()
            logging.info(f"CBO Data has been entered for File ID:{File_ID[0]}")

            #Loading data into Stage.CBO_Detail_Person
            df_Adult = df1['Adult'].replace(np.nan,'', regex=True)
            #print(df_Adult.head)
            df_All = pd.read_excel(Localfile,sheet_name=['Adult','Child','Family',
                'System Level','Demographics'],header=0,index=True) 
            #print("Adult Dataframe keys:%s "%df_Adult.keys())
            
            #This is for the Metric group name from the merged cells
            df_Adult_All = df_All['Adult']
            df_Adult_All.columns=df_Adult_All.columns.str.replace('Unnamed.*', '')
            Adult_Metric_group=[]
            for key in df_Adult_All.keys():
                #print("Key Value:%s"%key)
                Adult_Metric_group.append(key)
            for i in range(len(Adult_Metric_group)):
                if Adult_Metric_group[i]=='' and i>0:
                    Adult_Metric_group[i]=Adult_Metric_group[i-1]
                else:
                    pass
            #print("Adult Metric group:%s"%Adult_Metric_group)
            

            df_Child = df1['Child'].replace(np.nan,'', regex=True)

            #This is for the Metric group name from the merged cells
            df_Child_All = df_All['Child']
            df_Child_All.columns=df_Child_All.columns.str.replace('Unnamed.*', '')
            Child_Metric_group=[]
            for key in df_Child_All.keys():
                #print("Key Value:%s"%key)
                Child_Metric_group.append(key)
            for i in range(len(Child_Metric_group)):
                if Child_Metric_group[i]=='' and i>0:
                    Child_Metric_group[i]=Child_Metric_group[i-1]
                else:
                    pass
            #print("Number of elements:%s"%len(Child_Metric_group))
            #print("Child Metric group:%s"%Child_Metric_group)
            

            df_Family = df1['Family'].replace(np.nan,'', regex=True)
            #This is for the Metric group name from the merged cells
            df_Family_All = df_All['Family']
            df_Family_All.columns=df_Family_All.columns.str.replace('Unnamed.*', '')
            Family_Metric_group=[]
            for key in df_Family_All.keys():
                #print("Key Value:%s"%key)
                Family_Metric_group.append(key)
            for i in range(len(Family_Metric_group)):
                if Family_Metric_group[i]=='' and i>0:
                    Family_Metric_group[i]=Family_Metric_group[i-1]
                else:
                    Family_Metric_group[i]=Family_Metric_group[i]
                #print("Value of i:%s"%i)
                #print("Family Metric group:%s"%Family_Metric_group[i])

            df_demographics = df1['Demographics'].replace(np.nan,'', regex=True)
            #This is for the Metric group name from the merged cells
            df_Demographics_All = df_All['Demographics']
            df_Demographics_All.columns=df_Demographics_All.columns.str.replace('Unnamed.*', '')
            Demographics_Metric_group=[]
            for key in df_Family_All.keys():
                #print("Key Value:%s"%key)
                Demographics_Metric_group.append(key)
            for i in range(len(Demographics_Metric_group)):
                if Demographics_Metric_group[i]=='' and i>0:
                    Demographics_Metric_group[i]=Demographics_Metric_group[i-1]
                else:
                    pass

            df_System_Level = df1['System Level'].replace(np.nan,'', regex=True)

            sql4 = ("Select File_Master_ID from Stage.File_Master order by Create_Date desc")
            cursor.execute(sql4)
            File_Master_ID=cursor.fetchone()
            cnxn.commit()

            logging.info(f"Initial Data Loaded into data frames. One of the File Master ID added is {File_Master_ID[0]}")

            try:
                Person_CUID=[]
                Family_ID=[]
                Age=[]
                i=5
                for key in df_Adult.keys():
                    for k,v in df_Adult[key].items():
                        if key.find('Client Unique ID (CUID)')>=0 and k>0:
                            Person_CUID.append(v)
                        elif key.find('Family_ID')>=0 and k>0:
                            Family_ID.append(v)
                        elif key.find('Age')>=0 and k>0:
                            Age.append(v)
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Family_ID')<0 and key.find('Age')<0 and k==0:
                            Metric1=v
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Family_ID')<0 and key.find('Age')<0 and k>=1:
                            Metric1_value=v
                            #print("Value of i:%s"%i)
                            #print("Adult_Metric_group:%s"%Adult_Metric_group[i])
                            #print("Metric:%s"%Metric1)
                            Metric_Insert(File_Master_ID,Person_CUID[k-1],Family_ID[k-1],Age[k-1],Metric1,Metric1_value,key,'Adult',Adult_Metric_group[i])
                            i=i+1
            
            except Exception as ex:
                print("Issue in Adult tab")
                print(ex)
                sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
                cursor.execute(sqlcommand3)
                File_proc_ID=cursor.fetchone()
                cnxn.commit()
                #cnxn.close()
                Curr_dt = datetime.now()
                print("Fetch One:%s"%File_proc_ID[0])
                Insert_Error(File_proc_ID,'Issue in Adult tab',Curr_dt,str(ex),Curr_dt,File_ID)
            
            try:
                Person_CUID=[]
                Family_ID=[]
                Age=[]
                i=4
                for key in df_Child.keys():
                    for k,v in df_Child[key].items():
                        #print("Actual Key:%s"%key)
                        #print("Keys:%s"%k)
                        #print("Value:%s"%v)
                        if key.find('Client Unique ID (CUID)')>=0 and k>0:
                            Person_CUID.append(v)
                        elif key.find('Family_ID')>=0 and k>0:
                            Family_ID.append(v)
                        elif key.find('Age')>=0 and k>0:
                            Age.append(v)
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Family_ID')<0 and key.find('Age')<0 and k==0:
                            Metric1=v
                            i=i+1
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Family_ID')<0 and key.find('Age')<0 and k>=1:
                            Metric1_value=v
                            #print("Value of i:%s"%i)
                            #print("Child_Metric_group:%s"%Child_Metric_group[i])
                            #print("Metric:%s"%Metric1)
                            #print("Metric Value:%s"%Metric1_value)
                            Metric_Insert(File_Master_ID,Person_CUID[k-1],Family_ID[k-1],Age[k-1],Metric1,Metric1_value,key,'Child',Child_Metric_group[i])
                        else:
                            pass

            except Exception as ex:
                print("Issue in Child tab")
                print(ex)
                sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
                cursor.execute(sqlcommand3)
                File_proc_ID=cursor.fetchone()
                cnxn.commit()
                #cnxn.close()
                Curr_dt = datetime.now()
                print("Fetch One:%s"%File_proc_ID[0])
                Insert_Error(File_proc_ID,'Issue in Child tab',Curr_dt,str(ex),Curr_dt,File_ID)   
            
            
            try:      
                FamilyID=[]
                i=2
                #print("Family ID:%s"%Family_ID)
                for key in df_Family.keys():
                    for k,v in df_Family[key].items():
                        #print("Actual Key:%s"%key)
                        #print("Next lvl key:%s"%k)
                        #print("Value:%s"%v)
                        if key.find('Family_ID')>=0 and k>0:
                            #print("Value:%s"%v)
                            FamilyID.append(v)
                            #print("Family ID:%s"%FamilyID)
                        else:
                            pass

                        #print("Family ID:%s"%Family_ID[0])
                for key in df_Family.keys():
                    for k,v in df_Family[key].items():
                        if key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metrics Name')<0 and key.find('CBO_ID')<0 and key.find('Family_ID')<0 and k==0:
                            Metric1=v
                            if i<len(Family_Metric_group)-1:
                                i=i+1
                            else:
                                pass
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metrics Name')<0  and key.find('CBO_ID')<0 and key.find('Family_ID')<0 and k>=1:
                            Metric1_value=v
                            Metric_Insert(File_Master_ID,'',FamilyID[k-1],'',Metric1,Metric1_value,key,'Family',Family_Metric_group[i])
                        else:
                            pass

            except Exception as ex:
                print("Issue in Family tab")
                print(ex)
                sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
                cursor.execute(sqlcommand3)
                File_proc_ID=cursor.fetchone()
                cnxn.commit()
                #cnxn.close()
                Curr_dt = datetime.now()
                print("Fetch One:%s"%File_proc_ID[0])
                Insert_Error(File_proc_ID,'Issue in Family tab',Curr_dt,str(ex),Curr_dt,File_ID) 
            
            
            try:
                Person_CUID=[]
                Family_ID=[]
                Age=[]
                i=4
                for key in df_demographics.keys():
                    for k,v in df_demographics[key].items():
                        if key.find('Family_ID')>=0 and k>0:
                            Family_ID.append(v)
                        elif key.find('Client Unique ID (CUID)')>=0 and k>0:
                            Person_CUID.append(v)
                        elif key.find('Age')>=0 and k>0:
                            Age.append(v)
                        else:
                            pass

                for key in df_demographics.keys():
                    for k,v in df_demographics[key].items():

                        if key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metrics Name')<0 and key.find('CBO_ID')<0 and key.find('Family_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Age')<0 and k==0:
                            Metric1=v
                            i=i+1
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metrics Name')<0 and key.find('CBO_ID')<0 and key.find('Family_ID')<0 and key.find('Client Unique ID (CUID)')<0 and key.find('Age')<0 and k>=1:
                            Metric1_value=v
                            Metric_Insert(File_Master_ID,Person_CUID[k-1],Family_ID[k-1],Age[k-1],Metric1,Metric1_value,key,'Demographics',Demographics_Metric_group[i])
                        else:
                            pass

            except Exception as ex:
                print("Issue in Demographics tab")
                print(ex)
                sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
                cursor.execute(sqlcommand3)
                File_proc_ID=cursor.fetchone()
                cnxn.commit()
                #cnxn.close()
                Curr_dt = datetime.now()
                print("Fetch One:%s"%File_proc_ID[0])
                Insert_Error(File_proc_ID,'Issue in Demographics tab',Curr_dt,str(ex),Curr_dt,File_ID)
            
            #print("All Keys:%s" %df_Adult.keys())

            #For System Level data load
            
            try:
                for key in df_System_Level.keys():
                    for k,v in df_System_Level[key].items():
                        
                        if key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and k==0:
                            Metric1=v
                        elif key.find(key)>=0 and key.find('Metric ID')<0 and key.find('Metric Name')<0 and key.find('CBO_ID')<0 and k>=1:
                            Metric1_value=v
                            Metric_Insert(File_Master_ID,'','','',Metric1,Metric1_value,key,'System Level','Systems Level Improvement Metrics')
                        else:
                            pass

            except Exception as ex:
                print("Issue in System Level tab")
                print(ex)
                sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
                cursor.execute(sqlcommand3)
                File_proc_ID=cursor.fetchone()
                cnxn.commit()
                #cnxn.close()
                Curr_dt = datetime.now()
                print("Fetch One:%s"%File_proc_ID[0])
                Insert_Error(File_proc_ID,'Issue in System Level tab',Curr_dt,str(ex),Curr_dt,File_ID)


        except Exception as ex:
            logging.info(f"Issue with reading the blob: {str(ex)}")
            print(ex)
            Excel_Exception=str(ex)
            sqlcommand3=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
            cursor.execute(sqlcommand3)
            File_proc_ID=cursor.fetchone()
            cnxn.commit()
            #cnxn.close()
            Curr_dt = datetime.now()
            print("Fetch One:%s"%File_proc_ID[0])
            Insert_Error(File_proc_ID,'Excel load issue',Curr_dt,Excel_Exception,Curr_dt,File_ID)

    except Exception as ex:
        print('Azure Blob Exception:')
        print(ex)
        logging.info(f"In Azure Blob exception block{str(ex)}")
        cnxn=pyodbc.connect('DRIVER='+driver+';SERVER='+SERVER+';PORT=1433;DATABASE='+DATABASE+';UID='+username+';PWD='+ pwd)
        cursor = cnxn.cursor()
        sql1=("Select File_proc_ID from Audit.File_Processing_Log ORDER BY File_ID desc")
        cursor.execute(sql1)
        File_proc_ID=cursor.fetchone()
        cnxn.commit()
        sqlcommand = ("Insert into Audit.Error_Log(File_Proc_ID,Error_source,Error_date,Error_desc,Create_date) Values(?,?,?,?,?)")
        Values = [File_proc_ID[0],'Azure Blob',Curr_dt,str(ex),Curr_dt,File_ID]
        cursor.execute(sqlcommand,Values)
        cnxn.commit()
    
    rename_blob(blob_name1)
    End_dt=datetime.now()
    
    sql5 = ("Select File_Master_ID from Stage.File_Master order by Create_Date desc")
    cursor.execute(sql5)
    File_Master_ID=cursor.fetchone()
    cnxn.commit()

    sql6 = '''Select count(*) from [Stage].[CBO_Detail_Person] where File_Master_ID=? '''
    val6 = File_Master_ID[0]
    cursor.execute(sql6,val6)
    Records_processed = cursor.fetchone()
    cnxn.commit()

    sql7 = '''UPDATE Audit.File_Processing_Log SET Proc_End_Date=? , Proc_status_code=?, 
            Proc_error_ind=?, Proc_rec_count=?
                WHERE File_ID=? and Proc_status_code<>'Error' '''
    Val7 = [End_dt,'Completed',0,Records_processed[0],File_ID[0]]
    cursor.execute(sql7,Val7)
    cnxn.commit()
    logging.info(f"File Processing logs should have got updated")
