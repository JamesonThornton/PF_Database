#!/usr/bin/env python
# coding: utf-8

# In[1587]:


import pandas as pd
import re
import numpy as np


# In[1588]:


#tables
data_tables = pd.DataFrame(["bus data",
"branch data",
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
"qtable data"])


# In[1589]:


filepath = r"C:\Users\tiger\Google Drive\PF_DataBase\IEEE 300-Bus System"
file1 = filepath + "\\" "IEEE300Bus.epc"
file2 = open(file1, "r")

global linecount
linecount = 0


# In[ ]:





# In[1590]:



header_text = pd.DataFrame(index=data_tables, columns=[0])


# In[1591]:


data_tables.to_dict(orient='list')[0]


# In[1592]:


#read all header rows based on assumed leading string (data tables)
stop = False
header_count=0
for step in range(0,100000):
    linecount +=1
    line = file2.readline()
    for item in data_tables.to_dict(orient='list')[0]:
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
    if(header_count>=len(data_tables)):
        break


# In[1593]:


header_text.dropna(inplace=True)


# In[1594]:


table_headers = header_text[0].str.split(']', expand=True)[1].str.split(expand=True)
table_headers


# In[1595]:


def nextline(file, next_table):
    global stop
    global linecount
    line1 = file.readline()
    linecount +=1
    check = re.search(next_table, line1)
    try:
        if (check.span()[0]==0):
            stop = True
            #print('breaking loop')
    except:
        pass
    #repeat for second line
    if(re.search('/', line1)!=None):
        line2 = file2.readline()
        linecount +=1
        line = line1 + line2
        #repeat for possible 3rd line
        if(re.search('/', line2)!=None):
            line3 = file2.readline()
            linecount +=1
            line = line1 + line2 + line3
            #4th line
            if(re.search('/', line3)!=None):
                line4 = file2.readline()
                linecount +=1
                line = line1 + line2 + line3 + line4
    else:
        line = line1
    line = line.replace('/','').replace('\n','')
    ######
    temp = re.split('("[^"]*")|\s+|:', line)
    temp1 = pd.DataFrame(temp)
    temp2 = temp1.dropna().reset_index(drop=True)
    temp3 = temp2.replace('',np.nan).dropna().reset_index(drop=True)
    line = temp3
    ######

    return line, stop, linecount


# # Read Bus Table

# In[1596]:


filepath = r"C:\Users\tiger\Google Drive\PF_DataBase\IEEE 300-Bus System"
file1 = filepath + "\\" "IEEE300Bus.epc"
file2 = open(file1, "r")
linecount = 0


# In[1597]:


#skip headers and get to bus data table
stop = False
while not stop:
    line, stop, linecount = nextline(file2, 'bus data')

BUSD = pd.DataFrame()

#loop to fill bus table
stop = False
check= None
next_table = table_headers.index[1]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    BUSD = BUSD.append(line.transpose(), ignore_index=True)

new_header = pd.Series(['BUSID', 'BUSNAME', 'BUSKV'] + list(table_headers.loc['bus data']))
BUSD.rename(columns=new_header.to_dict(), inplace=True)


# In[1598]:


BUSD


# # Read Branch Table

# In[1599]:


SECDD =  pd.DataFrame()

#loop to fill table
stop = False
check= None
next_table = table_headers.index[2]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    SECDD = SECDD.append(line.transpose())

new_header = pd.Series(['FBUS', 'FBUSNAME', 'FBUSKV','TBUS', 'TBUSNAME', 'TBUSKV'] + list(table_headers.loc['branch data']))
SECDD.rename(columns=new_header.to_dict(), inplace=True)


# In[1600]:


SECDD


# # Read XFMR Table

# In[1601]:


line, linecount


# In[1602]:


XFMR = pd.DataFrame()
#loop to fill table
stop = False
check= None
next_table = table_headers.index[3]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    XFMR = XFMR.append(line.transpose())

new_header = pd.Series(['FBUS', 'FBUSNAME', 'FBUSKV','TBUS', 'TBUSNAME', 'TBUSKV'] + list(table_headers.loc['transformer data']))
XFMR.rename(columns=new_header.to_dict(), inplace=True)   
XFMR.reset_index(drop=True, inplace=True)


# In[1603]:


XFMR


# # Gen Data

# In[1604]:


line


# In[1605]:


GENS = pd.DataFrame()
#loop to fill table
stop = False
check= None
next_table = table_headers.index[4]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    GENS = GENS.append(line.transpose())

new_header = pd.Series(['BUS', 'BUSNAME', 'BUSKV'] + list(table_headers.loc['generator data']))
GENS.rename(columns=new_header.to_dict(), inplace=True)   
GENS.reset_index(drop=True, inplace=True)


# In[1606]:


GENS


# # Load Data

# In[1607]:


LOAD = pd.DataFrame()
#loop to fill table
stop = False
check= None
next_table = table_headers.index[5]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    LOAD = LOAD.append(line.transpose())

new_header = pd.Series(['BUS', 'BUSNAME', 'BUSKV'] + list(table_headers.loc['load data']))
LOAD.rename(columns=new_header.to_dict(), inplace=True)   
LOAD.reset_index(drop=True, inplace=True)


# In[1608]:


LOAD


# In[1609]:


table_headers.index[5]


# # Shunt Data

# In[1610]:


SHUNT = pd.DataFrame()
#loop to fill table
stop = False
check= None
next_table = table_headers.index[6]
while not stop:
    line, stop, linecount = nextline(file2,next_table)
    if stop:
        break
    #append model item to aggregate table
    SHUNT = SHUNT.append(line.transpose())

####################
###need to figure out Shunt headers####
##############################
new_header = pd.Series(['BUS', 'BUSNAME', 'BUSKV'] + list(table_headers.loc['shunt data']))
SHUNT.rename(columns=new_header.to_dict(), inplace=True)   
SHUNT.reset_index(drop=True, inplace=True)


# In[1611]:


SHUNT


# # Write to Excel File

# In[1612]:


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


# # Archive

# In[ ]:


"bus data"
"branch data"
"transformer data"
"generator data"
"load data"
"shunt data"
"svd data"
"area data"
"zone data"
"interface data"
"interface branch data"
"dc bus data"
"dc line data"
"dc converter data"
"z table data"
"gcd data"
"transaction data"
"owner data"
"qtable data"

