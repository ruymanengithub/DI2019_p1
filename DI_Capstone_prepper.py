#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

DataIncubator Fellowship 2019
R. Azzollini

Capstone Project - Data Parser for Plotting

Created on Sun Nov  3 18:59:54 2019

@author: raf
"""

# IMPORT STUFF
import numpy as np
import pandas as pd
from pdb import set_trace as stop
import os
from matplotlib import pyplot as plt
import pickle
# END IMPORT

parsedpath = 'parseddata'

def remove_jumps(F,threshold):
    """ """
    Fp1 = F[1:]
    Fm1 = F[0:-1]
    
    diffs = (Fp1-Fm1)
    
    jump = np.where(np.abs(diffs) > threshold)
    
    for ix in jump[0]:
        F[ix+1:] -= (F[ix+1]-F[ix])

    return F

def load_cornerstone():
    """ """
    cornerstonefile = 'matches_linkedin_joblistings_symbol.csv'
    cornerstonedata = pd.read_csv(cornerstonefile)
    
    cornerstone = dict()
    
    for i in range(len(cornerstonedata)):
        company = cornerstonedata.Company.iloc[i]
        symbol = cornerstonedata.Symbol.iloc[i]
        path = os.path.join(parsedpath,symbol)
        cornerstone[company] = dict(symbol=symbol,path=path)
    
    cornerstone['allcompanies'] = cornerstonedata.Company.values.copy()
    
    return cornerstone

def parse_lvl1_listings(listings):
    """ """
    
    clistings = listings.assign(date=pd.to_datetime(listings.date_added.values))
    
    clistings.index=clistings['date']
    Hlistings=clistings.groupby(pd.Grouper(freq='W'))
    
    H = Hlistings.count()['date'].values
    Hdatestr = Hlistings.count()['date'].index.values
    Hdate = pd.to_datetime(Hdatestr)
    #print('Listings: %i = %i' % (len(listings),H.sum()))
    outdict = dict(
            H = H,
            date = Hdate
    )
    return outdict
    
def parse_lvl1_linkedin(linkedin):
    """ """
    emp = linkedin.employees_on_platform.values.copy()
    
    emp = remove_jumps(emp,threshold=50)
    
    outdict = dict(
            date = pd.to_datetime(linkedin.as_of_date.values.copy()),
            emp= emp,
            industry = linkedin.industry.copy(),
            )
    return outdict

def parse_lvl1_stock(stock):
    """ """
    outdict = dict(
            date= pd.to_datetime(stock.Date.values.copy()),
            adclose=stock['Adj Close'].values.copy(),
            volume = stock.Volume.values.copy()
            )
    return outdict
   



def get_company_parsed(company,cornerstone):
    """ """
    
    #trange = [dt.datetime(2017,10,1),dt.datetime(2018,12,31)]
    
    path = cornerstone[company]['path']
    symbol = cornerstone[company]['symbol']
    
    listingsf = os.path.join(path,'temp_datalab_records_job_listings_%s.csv' % symbol)
    linkedinf = os.path.join(path,'temp_datalab_records_linkedin_%s.csv' % symbol)
    stockf = os.path.join(path,'yahoo_finance_%s.csv' % symbol)
    
    listings = parse_lvl1_listings(pd.read_csv(listingsf))
    linkedin = parse_lvl1_linkedin(pd.read_csv(linkedinf))
    stock = parse_lvl1_stock(pd.read_csv(stockf))
    
    #trange = [linkedin['date'].min(),linkedin['date'].max()]
    
    #kwargs1=dict(company=company,trange=trange)
    #dofig_ListLink(listings,linkedin,**kwargs1)
    
    #kwargs2=dict(company=company,trange=trange)
    #dofig_LinkSto(linkedin,stock,**kwargs2)
    
    companydata = dict(listings=listings,
                       linkedin=linkedin,
                       stock=stock)
    
    return companydata
    
    
def parseItAll():
    
    
    cornerstone = load_cornerstone()
    
    alldata = dict()
    
    for company in cornerstone['allcompanies']:
        print('company: %s' % company)
        
        path = cornerstone[company]['path']
        symbol = cornerstone[company]['symbol']
        
        stockf = os.path.join(path,'yahoo_finance_%s.csv' % symbol)
        hasstock = os.path.exists(stockf)
        if not hasstock:
            print('%s: %s missing' % (company,symbol))
            cornerstone.pop(company)
            allcompaniesarr= cornerstone['allcompanies']
            cornerstone['allcompanies'] = np.delete(allcompaniesarr,np.where(allcompaniesarr == company),axis=0)
            
        if os.path.exists(path) and hasstock:
            companydata = get_company_parsed(company,cornerstone=cornerstone)            
            alldata[company] = companydata
            
    alldata['cornerstone'] = cornerstone
    
    pickle.dump(alldata,open('Capstone_parsed.pick','wb'))
    
if __name__ == '__main__':
    
    parseItAll()