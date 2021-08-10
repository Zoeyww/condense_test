#!/usr/bin/env python
# coding: utf-8

# In[67]:


import os
import pandas as pd
import numpy as np
import pyarrow.feather as feather
from datetime import date
import sys 


# In[65]:


def date_converter(dateline):
    words = dateline.split()[2:5]
    day = words[0]
    mon = words[1]
    year = words[2]
    if year.startswith('0'):
        year = year.replace('000','200')
    return date(int(year),month_converter(mon),int(day))

def month_converter(m):
    MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month= MON.index(m)+1
    return month


# In[66]:


def user(addr):
    if '@enron.com' not in addr:
        return '-1'
    addr = addr[0:addr.index('@')]
    if '<' in addr or '#' in addr or "/o" in addr:
        return '-1'
    if "'" in addr:
        addr = addr.replace("'", "")
    if len(addr)>0 and addr[0] == '.':
        addr = addr[1:]
    if len(addr)==0:
        return '-1'
    return addr


# In[10]:


def get_end(onepath):
    with open(onepath, 'r',encoding='latin1') as f:
        for num, line in enumerate(f):  
            if line.startswith('Subject: '):
                break
        return num


# In[69]:


DIR = sys.argv[1]
walked = sorted(os.walk(DIR))
index1,index2=0,0
dict_to,dict_first6= {},{}
for x, dir, files in walked:
    files[:]=[d for d in files if '.DS_Store' not in d]
    for filename in files:
        fullpath = os.path.join(x, filename)  
        with open(fullpath, 'r',encoding='latin1') as f:
            filepath=fullpath.replace(DIR+'/','')
         
            
            first = f.readline()
            
            
            dateline = f.readline()
            Date = date_converter(dateline)
          
            
            fromline = f.readline()
            nameline = fromline.split()[1]
            person = user(nameline)
          
            
            list=[]
            e = get_end(fullpath)
            tolist = f.readlines()[:e-3]
            if len(tolist) !=0:
                for sulist in tolist:
                    xx = sulist.strip('To:')
                    yy = xx.strip('\n')
                    zz = yy.strip()
                    ll = zz.split(',')
                    ll = [l for l in ll if l]
                    for ii in ll:
                        qq=ii.strip()
                        to = user(qq)
                        list+=[to]
                        
            else: 
                list =[]
                to = '-1'
                list=[to]
            
            
            dict_to[index1]=list  
            count_to1 = len(list)
            if '-1' in list:
                if list.count('-1')!=len(list):
                    count_to2 = len(list)-list.count('-1')
                else:
                    count_to2 = len(list)
            else:
                count_to2 = len(list)
                
            index1+=1
           
            
            with open(fullpath, 'r',encoding='latin1') as f:
                subjectline = f.readlines()[e]
                subject = subjectline[8:-1]
               
            dict_first6[index2]=(Date,person,subject,filepath,count_to1,count_to2)
            index2+=1
                


# In[73]:


index_w=0
dict_word={}
for key, values in dict_to.items():
    if len(values)<2:
        toword = dict_to[key]
        dict_word[str(index_w)]=''.join(toword)
        index_w+=1
        pass
    
    if len(values)>=2:
        for i in range(len(values)):
            toword = dict_to[key][i]
            dict_word[str(index_w)]=toword
            index_w+=1


# In[91]:


df_1 = pd.DataFrame(dict_first6.values(),columns=['Date','From','Subject','filename','RE1','RE2'])


# In[90]:


df_repeat = pd.DataFrame(df_1.values.repeat(df_1['RE1'], axis=0), columns=df_1.columns)
df_repeat['To']=dict_word.values()
df_2 = df_repeat[(df_repeat['From']!='-1')& (df_repeat['To']!='-1')]
df_3 = df_2.drop_duplicates('filename')
df_re2 = pd.DataFrame(df_3['RE2'])
dd = df_re2.reset_index()
df = dd.drop('index', 1)


# In[80]:


df_4 = pd.DataFrame()
df_4['file'] = df_3['filename']


# In[81]:


df_4['MailID'] = np.arange(1,len(df_4)+1)
df_5 = df_4.reset_index()
df_6 = df_5.drop('index',1)


# In[82]:


df_6['dup']=df['RE2']


# In[83]:


df_7 = pd.DataFrame(df_6.values.repeat(df_6['dup'], axis=0), columns=df_6.columns)


# In[84]:


df_0 = df_2.reset_index().drop(['index','RE1'],1)


# In[85]:


df_0['MailID']=df_7['MailID']


# In[86]:


df_0.columns = ['Date', 'From','Subject','filename','Recipients','To','MailID']


# In[87]:


column_names = ['MailID','Date','From','To','Recipients','Subject','filename']

df_00= df_0.reindex(columns=column_names)


# In[88]:


df_00.to_feather("./enron.feather")

