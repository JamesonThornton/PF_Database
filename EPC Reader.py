import pandas as pd
import re
import numpy as np
import matplotlib.pyplot as plt
import time
import datetime
from datetime import date

filepath = r"C:\Users\tiger\Google Drive\PF_DataBase\IEEE 300-Bus System"
file1 = filepath + "\\" "IEEE300Bus.epc"

stop = False

#tables
global data_tables0
data_tables0 = pd.DataFrame(
["branch data",
"bus data",
"transformer data",
"generator data",
"load data",
"shunt data",
"svd data",
"area data",
"zone data",
"interface data",
"interface branch data",
"dc bus data",
"dc line data",
"dc converter data",
"z table data",
"gcd data",
"transaction data",
"owner data",
"qtable data",
"end"])

def df_headers(file1):
    header_text = pd.DataFrame(index=data_tables0, columns=[0])
    #read all header rows based on assumed leading string (data tables)
    #this will also provide the correct order of the data tables found in the file
    stop = False
    header_count=0
    file = open(file1, "r")
    for step in range(0,1000000):
        line = file.readline()
        for item in data_tables0.to_dict(orient='list')[0]:
            check = re.search(item, line)
            try:
                if check.span()[0]==0:
                    header_text.loc[item]=line
                    header_count +=1
                    temp =line
                    #stop = True
                    break
            except:
                continue
        if stop:
            break
        if(header_count>=len(data_tables0)):
            break
    
    header_text.dropna(inplace=True)

    table_headers = header_text[0].str.split(']', expand=True)[1].str.split(expand=True)
    return table_headers

def nextline(file, stop_word):
    stop = False
    templine = file.readline()
    check = re.search(stop_word, templine)
    try:
        stop = (check.span()[0]==0)
    except:
        pass
    #repeat for second line, 3rd line, etc. 
    #'/' denotes continued information on next line
    #repeat for second line
    if(re.search('/', templine)!=None):
        line2 = file.readline()
        line = templine + line2
        #repeat for possible 3rd line
        if(re.search('/', line2)!=None):
            line3 = file.readline()
            line = templine + line2 + line3
            #4th line
            if(re.search('/', line3)!=None):
                line4 = file.readline()
                line = templine + line2 + line3 + line4
    
    line = templine.replace('/','').replace('\n','')
    ######
    temp = re.split('("[^"]*")|([[][^]]*])|\s+|:', line)
    temp1 = pd.DataFrame(temp)
    temp2 = temp1.dropna().reset_index(drop=True)
    temp3 = temp2.replace('',np.nan).dropna().reset_index(drop=True)
    line = temp3
    ######

    return line, stop

def headerfix(table):
    if (table=='bus data'):
        return ['BUS', 'BUSNAME', 'BUSKV', 'TYPE', 'vsched', 'volt', 'angle', 'AREA',
       'ZONE', 'vmax', 'vmin', 'date_in', 'date_out', 'pid', 'L', 'own', 'st',
       'latitude', 'longitude', 'island', 'sdmon', 'vmax1', 'vmin1']
    elif (table == 'branch data'):
        return ['FBUS', 'FBUSNAME', 'FBUSKV','TBUS', 'TBUSNAME', 'TBUSKV']
    elif (table == 'transformer data'):
        return ['FBUS', 'FBUSNAME', 'FBUSKV','TBUS', 'TBUSNAME', 'TBUSKV']
    elif (table == 'generator data' ):
        return ['BUS', 'BUSNAME', 'BUSKV', 'ID', 'LONG_ID', 'ST', 'NO', 'REG_NAME', 'REG_KV', 'PRF', 'QRF', 'AR', 'ZONE',
                   'PGEN', 'PMAX', 'PMIN', 'QGEN', 'QMAX', 'QMIN', 'MBASE', 'CMP_R', 'CMP_X', 'GEN_R', 'GEN_X',
                   'HBUS', 'HBUS2', 'HBUS3', 'TBUS', 'TBUS2', 'TBUS3', 'DATE_IN', 'DATE_OUT', 'PID', 'N']
    elif (table == 'load data' ):
        return ['BUS', 'BUSNAME', 'BUSKV']
    elif(table=='shunt data'):
        return ['BUS', 'BUSNAME', 'BUSKV', 'ID', 'TBUS', 'TBUSNAME', 'TBUSKV', 'CKT', 'SEC', 'LONGID', 'ST', 'AR', 'ZONE', 
                'MW_PU', 'MVAR_PU', 'DATEIN', 'DATEOUT', 'PID', 'N', 
                'OWN', 'PART1', 'OWN2', 'PART2', 'OWN3', 'PART3', 'OWN4', 'PART4', 
               'REG_NUM', 'REG_NAME', 'REG_KV', 'M', 'NUM', 'NAME', 'KV', 'ID', 'ST']
    else:
        return ['']

def readtable(filename, table):
    header = pd.DataFrame()
    table_out = pd.DataFrame()
    file = open(filename, "r")
    stop = False
    #find data table of interest
    while not stop:
        line, stop = nextline(file, table)# 'bus data'
    stop = False
    check= None
    # fill header data
    header = line[3:][0]
    #determine next key word based on header table order
    next_indx = data_tables[0][data_tables[0]==table].index[0]+1
    #read and fill table until next key word
    while not stop:
        line, stop = nextline(file,data_tables[0][next_indx])
        if stop:
            break
        #append model item to aggregate table
        table_out = table_out.append(line.transpose(), ignore_index=True)
    new_header = pd.Series(headerfix(table) + list(header))
    table_out.rename(columns=new_header.to_dict(), inplace=True)
    return table_out


start = time.time()
global data_tables
temp_tables = df_headers(file1)
data_tables = pd.DataFrame(temp_tables.index.values)
BUSD = readtable(file1, 'bus data')
SECDD = readtable(file1, 'branch data')
XFMR = readtable(file1, 'transformer data')
GENS = readtable(file1, 'generator data')
LOAD = readtable(file1, 'load data')
SHUNT = readtable(file1, 'shunt data')
SVD = readtable(file1, "svd data")
AREA = readtable(file1, "area data")
ZONE = readtable(file1, "zone data")
IFACE = readtable(file1, "interface data")
BFACE = readtable(file1, "interface branch data")
DCBUS = readtable(file1, "dc bus data")
DCLINE = readtable(file1, "dc line data")
DCC = readtable(file1, "dc converter data")
ZTABLE= readtable(file1, "z table data")
GCD = readtable(file1, "gcd data")
TRANS = readtable(file1, "transaction data")
OWN = readtable(file1, "owner data")
QTAB = readtable(file1, "qtable data")
endtime = time.time()-start
print(endtime)

print("Buses: " + str(BUSD.shape[0]))
print("Line Sections: " +str(SECDD.shape[0]))
print("Transformers: " +str(XFMR.shape[0]))
print("Generators: "+ str(GENS.shape[0]))
print("Loads: " + str(LOAD.shape[0]))
print("Shunts: " + str(SHUNT.shape[0]))

# # Write to Excel File

path = r'C:\Users\tiger\Google Drive\PF_DataBase'
file1 = 'IEEE_300_Bus.xlsx'
outfile = path + '\\' + file1

writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
BUSD.to_excel(writer, sheet_name='BUSD')
SECDD.to_excel(writer, sheet_name='SECDD')
XFMR.to_excel(writer, sheet_name='XFMR')
GENS.to_excel(writer, sheet_name='GENS')
LOAD.to_excel(writer, sheet_name='LOAD')
SHUNT.to_excel(writer, sheet_name='SHUNT')
writer.save()
