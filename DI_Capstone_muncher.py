#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

DataIncubator Fellowship 2019
R. Azzollini

Capstone Project
Functions to load and parse input data.


Created on Sat Nov  2 12:30:05 2019

@author: raf
"""

# IMPORT STUFF
import numpy as np
import pandas as pd
import pandas_datareader as pdr
import datetime as dt
from pdb import set_trace as stop
import os
from matplotlib import pyplot as plt
import pickle
# END IMPORT

capstonepath = 'DATA_CAPSTONE' 

def clean_companyname(cname):
    cname = cname.lower() 
    for item in [' ','inc.','ltd.',',']:
        cname = cname.replace(item,'')
    return cname

def clean_name_inrow(row,namecol):
    return clean_companyname(row[namecol])

def cleanLinkedIn():
        
    linkedinf = os.path.join(capstonepath,'temp_datalab_records_linkedin_company.csv')
        
    linkedin = pd.read_csv(linkedinf)
    
    clean_names = linkedin.apply(lambda row: clean_name_inrow(row, 'company_name'), axis=1)

    linkedin = linkedin.assign(company=clean_names.values)
    
    linkedin.to_csv('temp_datalab_records_linkedin_company_clean.csv')
    

def thindown_joblistings():
    """ """
    
    darkplace='/media/raf/darkplace_ext4/CAPSTONE'   
    chunksize = 5E5
    
    for partnum in range(1,8):
        
        print('Processing part %i...\n' % partnum)
        
        joblistings = os.path.join(darkplace, 'temp_datalab_records_job_listings_part%i' % partnum,
                           'temp_datalab_records_job_listings.csv')
    
        chunkcounter=0
        for chunk in pd.read_csv(joblistings, chunksize=chunksize):
            subchunk = chunk[chunk.brand.notnull()]
            subchunk = subchunk.drop(columns=['dataset_id','listing_id','domain','url','description',
                                   'entity_id','city_lat','city_lng','cusip','isin'])
    
            if chunkcounter ==0:
                narrow=subchunk.copy()
            else:
                narrow = pd.concat([narrow,subchunk],ignore_index=True)
            
            print('finished chunk %i' % chunkcounter)
            chunkcounter +=1
        
        outchunkcsv = os.path.join(darkplace,'temp_datalab_records_job_listings_part%i_narrow.csv' % partnum)
        
        narrow.to_csv(outchunkcsv)
        
        print('Finished thinning %i chunks in part %i' % (chunkcounter,partnum))

def extract_joblisting_uniquebrands():
    """ """
    
    chunksize=1E6
    
    for partnum in range(1,8):
        
        print('Processing part %i...\n' % partnum)
        joblistings = os.path.join(capstonepath, 'JobListings',
                                   'temp_datalab_records_job_listings_part%i_narrow.csv' % partnum)
        
        chunkcounter=0
        
        for chunk in pd.read_csv(joblistings, chunksize=chunksize):
            print('processing chunk %i' % chunkcounter)    
            
                        
            _ubrands = np.unique(chunk.brand)
            _ubrands_clean = [item for item in map(clean_companyname, 
                                                 _ubrands.tolist())]
            
            if chunkcounter==0 and partnum==1:                
                ubrands = np.unique(_ubrands_clean).tolist()
            else:
                ubrands = np.unique(ubrands+_ubrands_clean).tolist()
            
            chunkcounter +=1
    
    pickle.dump(ubrands,open('ubrands_joblistings.pick','wb'))

def extract_jobListings(company,symbol,companypath):
    """ """

    chunksize=1E6
    
    parsed = None
    
    for partnum in range(1,8):
        
        print('Processing part %i...\n' % partnum)
        joblistings = os.path.join(capstonepath, 'JobListings',
                                   'temp_datalab_records_job_listings_part%i_narrow.csv' % partnum)
        
        chunkcounter=0
        
        for chunk in pd.read_csv(joblistings, chunksize=chunksize):
            print('processing chunk %i' % chunkcounter)    
                        
            _ubrands = np.unique(chunk.brand)
            _ubrands_clean = np.array([item for item in map(clean_companyname, 
                                                 _ubrands.tolist())])
            
            ix = np.where(_ubrands_clean == company)
            
            if len(ix[0])==0:                
                chunkcounter +=1
                continue
            else:
                subchunk = chunk[chunk.brand==_ubrands[ix][0]]    
                
                if parsed is None:
                    parsed = subchunk.copy()
                else:
                    parsed = pd.concat([parsed,subchunk],ignore_index=True)
            
            chunkcounter +=1
      
    if parsed is not None:
        
        print('Done Parsing Joblistings for %s\n' % company)
        
        parsedcsv = os.path.join(companypath,
                               'temp_datalab_records_job_listings_%s.csv' % symbol)
            
        parsed.to_csv(parsedcsv)
    
def extract_LinkedIn(company,symbol,companypath):
    """ """
    
    linkedincsv = os.path.join(capstonepath,'temp_datalab_records_linkedin_company_clean.csv')    
    linkedin = pd.read_csv(linkedincsv)
    sublinkedin = linkedin[linkedin.company==company]
    
    outcsv = os.path.join(companypath,
                          'temp_datalab_records_linkedin_%s.csv' % symbol)
    sublinkedin.to_csv(outcsv)
    


def _parse_stock_codes(csv, companycol):
    """ """
    
    data = pd.read_csv(csv)
    company = data[companycol]
    
    company_clean = [item for item in map(clean_companyname,company.tolist())]
    
    pdata = pd.DataFrame.from_dict(dict(Symbol=data.Symbol.values,
                                       Company=company_clean))
    return pdata

def _get_stock_codes(conames,cocodes,indexdf): 
    
    for iconame, coname in enumerate(conames):        
        ix = np.where(indexdf.Company == coname)
        if len(ix[0])>0:
            cocodes[iconame] = str(indexdf.Symbol[ix[0]].values[0])
    
    return conames, cocodes

def find_stock_codes():
    """ """
    
    matchco = pickle.load(open('matches_linkedin_joblistings.pick','rb'))
    
    nasdaqcsv = os.path.join(capstonepath,'datahub','nasdaq-listings_zip',
                              'data','nasdaq-listed_csv.csv')
    sp500csv = os.path.join(capstonepath,'datahub','s-and-p-500-companies-financials_zip',
                              'data','constituents_csv.csv')
    
    nasdaq = _parse_stock_codes(nasdaqcsv,companycol='Company Name')    
    sp500 = _parse_stock_codes(sp500csv,companycol='Name')
    
    allcodes = pd.concat([nasdaq,sp500]).drop_duplicates().reset_index(drop=True)
    
    conames = list(matchco)
    cocodes = ['']*len(conames)
    
    conames, cocodes = _get_stock_codes(conames, cocodes, allcodes)
    
    cocodes = np.array(cocodes)
    
    ixnoempty = np.where(cocodes != '')
    
    conames = np.array(conames)[ixnoempty].tolist()
    cocodes = np.array(cocodes[ixnoempty]).tolist()
    
    resdf = pd.DataFrame.from_dict(dict(Company=conames,
                                  Symbol=cocodes))
    
    resdf.to_csv('matches_linkedin_joblistings_symbol.csv')

def extractAll():
    """Extracts info from target companies:
        matchted across: {LinkedIn, JobListings, Stock-Symbols}
    """
    
    parseddatapath = 'parseddata'
    mastercsv = 'matches_linkedin_joblistings_symbol.csv'
    doListings = True
    doLinkedIn = True
    doYahoo = True
    
    master = pd.read_csv(mastercsv)
    
    if not os.path.exists(parseddatapath):
        os.system('mkdir %s' % parseddatapath)
    
    alreadydone = ['discoverycommunications','apple','viacom','visa','google','redhat']
    
    for ix in range(len(master)):
    #for ix in range(1):
        
        company = master.Company[ix]
        symbol = master.Symbol[ix]
        
        if company in alreadydone:
            continue
        
        print('extracting %s,%s' % (company,symbol))
        
        companypath = os.path.join(parseddatapath,symbol)
        
        if not os.path.exists(companypath):
            os.system('mkdir %s' % companypath)
        
        if doListings:
            extract_jobListings(company,symbol,companypath)
        
        if doLinkedIn:
            extract_LinkedIn(company,symbol,companypath)
            
        if doYahoo:
            
            try:
                symboldata = pdr.get_data_yahoo(symbols=symbol, start=dt.datetime(2014, 1, 1), 
                                                end=dt.datetime(2019, 11, 3))
                symbolcsv = os.path.join(companypath,'yahoo_finance_%s.csv' % symbol)
            
                symboldata.to_csv(symbolcsv)
            except:
                pass
    
    
    
if __name__ == '__main__':
    #thindown_joblistings()
    #cleanLinkedIn()
    #extract_joblisting_uniquebrands()
    #find_stock_codes()
    #extractAll()
    
