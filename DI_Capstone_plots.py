#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""

DataIncubator Fellowship 2019
R. Azzollini

Capstone Project - Data Parser Plotter

Created on Mon Nov 4 15:52:13 2019

@author: raf
"""
# IMPORT STUFF
import numpy as np
import os
from matplotlib import pyplot as plt
import pickle
# END IMPORT

def dofig_LinkSto(linkedin,stock,**kwargs):
    """ """
    
    trange = kwargs['trange']
    
    x1 = linkedin['date']
    sel1 = np.where((x1>=trange[0]) & (x1<=trange[1]))
    x1 = x1[sel1]
    feat1 = linkedin['emp'].astype('float32')[sel1]
    feat1 /= np.median(feat1)
    
    fig = plt.figure(figsize=(10,6))
    ax1 = fig.add_subplot(121)
    ax1.plot(x1,feat1,'k-')
    ax1.set_ylabel('Increase relative to median]')
    ax1.set_title('Employees @ LinkedIn')
    ax1.set_xlim(trange)
    #ax1.set_ylim([0.5,1.5])
    
    x2 = stock['date']
    sel2 = np.where((x2>=trange[0]) & (x2<=trange[1]))
    x2=x2[sel2]
    feat2 = stock['adclose'].astype('float32')[sel2]
    feat2 /= np.median(feat2)
    
    ax2 = fig.add_subplot(122)
    ax2.set_title('Stock')
    ax2.set_ylabel('Increase relative to median]')
    ax2.plot(x2,feat2,'k-')
    ax2.set_xlim(trange)
    #ax2.set_ylim([0.5,1.5])
    
    #plt.gca().xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(format_date))
    fig.autofmt_xdate()
    
    if 'company' in kwargs:
        plt.suptitle(kwargs['company'])

    plt.show()
    

def dofig_LinkList(linkedin,listings,**kwargs):
    """ """
    
    trange = kwargs['trange']
    
    x1 = linkedin['date']
    sel1 = np.where((x1>=trange[0]) & (x1<=trange[1]))
    x1 = x1[sel1]
    feat1 = linkedin['emp'].astype('float32')[sel1]
    #feat1 /= np.median(feat1[np.where((x1>=trange[0]) & (x1<=trange[1]))])
    
    fig = plt.figure(figsize=(10,6))
    ax1 = fig.add_subplot(121)
    ax1.plot(x1,feat1,'k-')
    ax1.set_title('Employees @ LinkedIn')
    ax1.set_xlim(trange)
    ax1.set_ylabel('N')
    #ax1.set_ylim([0.5,1.5])
    
    x2 = listings['date']
    sel2 = np.where((x2>=trange[0]) & (x2<=trange[1]))
    x2 = x2[sel2]
    feat2 = listings['H'].astype('float32')[sel2]
    #feat2 /= np.median(feat2[np.where((x2>=trange[0]) & (x2<=trange[1]))])
    
    ax2 = fig.add_subplot(122)
    ax2.set_title('Job Posts per Month')
    ax2.plot(x2,feat2,'k-')
    ax2.set_xlim(trange)
    ax2.set_ylabel('N')
    #ax2.set_ylim([0.5,1.5])
    
    #plt.gca().xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(format_date))
    fig.autofmt_xdate()
    
    if 'company' in kwargs:
        plt.suptitle(kwargs['company'])

    plt.show()


class Plotter(object):
    
    def __init__(self,data):
        
        self.data = data
        
    def launch_LinkSto(self,company):
        """ """
        
        linkedin = self.data[company]['linkedin']
        stock = self.data[company]['stock']
    
        trange = [linkedin['date'].min(),linkedin['date'].max()]
        
        kwargs = dict(trange=trange,
                  company=company)
    
        dofig_LinkSto(linkedin,stock,**kwargs)


    def launch_LinkList(self,company):
        """ """
        
        linkedin = self.data[company]['linkedin']
        listings = self.data[company]['listings']
        
        trange = [listings['date'].min(),listings['date'].max()]
        
        kwargs = dict(trange=trange,
                  company=company)
    
        dofig_LinkList(linkedin,listings,**kwargs)


def test():
    """ """
    from pdb import set_trace as stop
    data = pickle.load(open('Capstone_parsed.pick','rb'))
    
    plotter = Plotter(data)
    plotter.launch_LinkSto('google')

if __name__ == '__main__':
    test()