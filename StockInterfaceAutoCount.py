import sqlalchemy as sa
import pandas as pd
import numpy as np
from datetime import date
import datetime
import calendar
import sys
import os
from openpyxl import load_workbook
from openpyxl.styles import Font
from tkinter import messagebox

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


class StockAutoCountInterface:
    def __init__(self, connection_string, companyName):
        self.connection_string=connection_string
        self.companyName=companyName


    def connectionString(self):
        connection_url=sa.engine.URL.create("access+pyodbc", query={"odbc_connect": self.connection_string})

        self.engine=sa.create_engine(connection_url, echo=False)

    def sqlQryDB(self):

        try:
            conn=self.engine.connect()

            sql="SELECT * FROM [StockListT]"
            self.result=pd.read_sql(sql, conn)
            
            sql1="SELECT IIf([CompanyShort]='SE','Seroi',IIf([CompanyShort]='GM','Golden Majuharta',IIf([CompanyShort]='ES','Evergreen Status',IIf([CompanyShort]='PE','Pyon Estate',IIf([CompanyShort]='FS','FSM'))))) \
                    AS Company, StockSQ.ID, StockSQ.Stock, StockSQ.Quantity, StockSQ.Unit, StockSQ.UID, StockSQ.Price1 AS Price, StockSQ.Quantity1 AS Quantity1, Price*Quantity1 AS TotalPrice, Mid([UID],1,2) AS CompanyShort, \
                    CDate(Mid([UID],5,2) & ',' & Mid([UID],7,2) & ',' & Mid([UID],9,2)) AS [Date], StockSQ.[Sub-Company] FROM StockSQ WHERE Company LIKE '"+companyName+"'"+" ORDER BY UID"
            self.result1=pd.read_sql(sql1, conn)
            
            
            sql2="SELECT IIf([CompanyShort]='SE','Seroi',IIf([CompanyShort]='GM','Golden Majuharta',IIf([CompanyShort]='ES','Evergreen Status',IIf([CompanyShort]='PE','Pyon Estate',IIf([CompanyShort]='FS','FSM'))))) AS Company, \
                  StockSQ.ID, StockSQ.Stock, StockSQ.Quantity, StockSQ.Unit, StockSQ.UID, StockSQ.Price2 AS Price, StockSQ.Quantity2 AS Quantity1, Price*Quantity1 AS TotalPrice, Mid([UID],1,2) AS CompanyShort, CDate(Mid([UID],5,2) \
                  & ',' & Mid([UID],7,2) & ',' & Mid([UID],9,2)) AS [Date], StockST.[Sub-Company] FROM StockSQ WHERE StockSQ.Price2<>0 AND StockSQ.Quantity2<>0 AND Company LIKE '"+companyName+"'"+" ORDER BY UID"
            self.result2=pd.read_sql(sql2, conn)

            
            sql3="SELECT IIf([CompanyShort]='SE','Seroi',IIf([CompanyShort]='GM','Golden Majuharta',IIf([CompanyShort]='ES','Evergreen Status',IIf([CompanyShort]='PE','Pyon Estate',IIf([CompanyShort]='FS','FSM'))))) AS Company, \
                  StockSQ.ID, StockSQ.Stock, StockSQ.Quantity, StockSQ.Unit, StockSQ.UID, StockSQ.Price3 AS Price, StockSQ.Quantity3 AS Quantity1, Price*Quantity1 AS TotalPrice, Mid([UID],1,2) AS CompanyShort, CDate(Mid([UID],5,2) \
                  & ',' & Mid([UID],7,2) & ',' & Mid([UID],9,2)) AS [Date], StockST.[Sub-Company] FROM StockSQ WHERE StockSQ.Price3<>0 AND StockSQ.Quantity3<>0 AND Company LIKE '"+companyName+"'"+" ORDER BY UID"
            self.result3=pd.read_sql(sql3, conn)


            frames=[self.result1, self.result2, self.result3]

            self.df_concat=pd.concat(frames, axis=0, ignore_index=True)
            
                       
            #sql4="SELECT * FROM [TaskListT]"
            #self.result4=pd.read_sql(sql4, conn)

            sql5="SELECT * FROM [BlockListT] WHERE Company LIKE '"+companyName+"'"
            self.result5=pd.read_sql(sql5, conn)

            sql6=r"SELECT MasterQ.[Combined_ID], MasterQ.[Company], MasterQ.Day, MasterQ.Block, MasterQ.Task, MasterQ.Job, TaskListT.[Account_Code]FROM ([MasterQ] INNER JOIN StockSQ ON MasterQ.[Combined_ID]=StockSQ.UID) INNER JOIN TaskListT ON MasterQ.[Task]=TaskListT.[Task] WHERE Company LIKE '"+companyName+"'"+" ORDER BY Combined_ID"
            self.result6=pd.read_sql(sql6, conn)

        except Exception as e:
            messagebox.showinfo(e.__class__, e.orig)

        finally:
            conn.close()


    
    def generateAccountCode(self):
        status={"Mature":"600", "Replant": "220", "Others": "710"}
        # match Combined_ID from result6 to UID from df_concat
        for index, row in self.df_concat.iterrows():
            # generate AccountName & Block Column in df_concat dataset
            if self.result6["Combined_ID"].str.contains(row["UID"]).any():
                self.df_concat.loc[index, "AccountName"]=self.result6[self.result6["Combined_ID"].str.strip()==row["UID"]]["Task"].values[0]
                self.df_concat.loc[index, "Block"]=self.result6[self.result6["Combined_ID"].str.strip()==row["UID"]]["Block"].values[0]
                self.df_concat.loc[index, "AccountCode"]=self.result6[self.result6["Combined_ID"].str.strip()==row['UID']]["Account_Code"].values[0]

        # generate the status code e.g Mature/Replant/Others
        for index, row in self.df_concat.iterrows():
            if self.result5["Block"].eq(row["Block"]).any():
                self.df_concat.loc[index, "Status"]=self.result5[self.result5["Block"]==row["Block"]]["Status"].values[0]
            elif row["Block"].strip()=="OTHERS":
                self.df_concat.loc[index, "Status"]="Others"
                                       
        # generate Account Code
        for index, row in self.df_concat.iterrows():
            if row["Status"]=="Mature":
                self.df_concat.loc[index, "Account_Code"]=status["Mature"]+"-"+row["AccountCode"]
            elif row["Status"]=="Replant":
                self.df_concat.loc[index, "Account_Code"]=status["Replant"]+"-"+row["AccountCode"]
            elif row["Status"]=="Others":
                self.df_concat.loc[index, "Account_Code"]=status["Others"]+"-"+row["AccountCode"]

        for index, row in self.df_concat.iterrows():
            if row["Block"]=="OTHERS" and row["AccountCode"]=="U001":
                self.df_concat.loc[index, "Account_Code"]="700-U001"
            elif row["Block"]=="OTHERS":
                self.df_concat.loc[index, "Account_Code"]="700-U001"

        # make uppercase AccountName column description
        self.df_concat["AccountName"]=self.df_concat["AccountName"].str.upper()
        
                
                     
            
                
            
            

    def generateJVInterface(self):
        
        print(self.df_concat)
    

companyName="Pyon Estate"
#companyName=sys.argv[1].strip('\"')
#fullPath=sys.argv[2].strip('\"')


connection_string=(
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=C:\Users\User\Desktop\WorkingFolder\Project_ReadMasterSheet\Master Sheet - Estate 0523.accdb;"
    r"ExtendedAnsiSQL=1;")



obj=StockAutoCountInterface(connection_string, companyName)

obj.connectionString()

obj.sqlQryDB()

obj.generateAccountCode()

obj.generateJVInterface()





            



            
            
        
        
