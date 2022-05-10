#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  9 17:42:08 2022

@author: sammy
"""
import h5py
import numpy as np
import os
import glob
from multiprocessing import Pool
from cycler import cycler
#from tqdm.auto import  tqdm
from p_tqdm import p_map
import sys
#from multiprocessing.pool import ThreadPool as tp
import time

def filtrack(f):
    #print("Running cuts on: "+str(f)+"\n")
    sub_pr_start=time.time()
    with h5py.File(f, "r") as h5r:
        key_jets = list(h5r.keys())[0]
        key_tracks = list(h5r.keys())[1]
        #print (key_jets, h5r[key_jets].shape, h5r[key_jets].dtype)
        #print (key_tracks, h5r[key_tracks].shape, h5r[key_tracks].dtype)
        
        jets = np.copy(np.array(h5r['jets']))
        #print("The shape of the jets dataset in the file is: ",jets.shape)
        tracks = np.copy(np.array(h5r['tracks_from_jet']))
        #print("The shape of the tracks dataset in the file is: ",tracks.shape)
        
        
        tracks_d0 = np.array(h5r['tracks_from_jet']['d0'])
        tracks_z0 = np.array(h5r['tracks_from_jet']['z0SinTheta'])
        
        #print("The shape of the d0 array is: ",tracks_d0.shape)
        #print("The shape of the z0 array is: ",tracks_z0.shape)
        
        #print("The d0 removal arguments are: ",np.where(tracks_d0 > 3.5))
        #print("The z0 removal arguments are: ",np.where(tracks_z0 > 5.0))
        
        rem_list=np.where((tracks_d0 > 3.5) | (tracks_z0 > 5.0))
        
        if not rem_list:
            sys.exit("The rem list is empty, nothing to do..")
            
        #print("The removal list is: ", rem_list)
        
        newfile = 'refined_std_tight_' + os.path.basename(f)
        completeName = os.path.join(path_std, newfile)
         
        #dt stores the track key fields
        dt=[]
        for key,typ in h5r[key_tracks].dtype.fields.items():
            dt.append(key)
        
        with h5py.File(completeName, 'w') as fwrite:
            fwrite.create_dataset('jets', data=jets,compression='gzip',compression_opts=7)
            #print("The native track data types are: ",np.dtype(h5r[key_tracks]))
            
            #print("The track data shape for: "+str(key)+" is ",tracks[key].shape)
            
            for i,j in zip(rem_list[0],rem_list[1]):
                    #a=str(i)
                    #b=str(j)
                    
                    #print(f"Track index to be overwritten in {a} th jet is {b}.....")
                    tracks[i][j]=tracks[i][39]
            fwrite.create_dataset('tracks_from_jet',data=tracks,compression='gzip',compression_opts=7)
        #print(f"End of file ops for file {completeName} in loop num  ==================\n")
        print("Sub process took %s seconds" %(time.time()-sub_pr_start))
###################################################################################################################    
start_time=time.time()
inpflist = glob.glob('std/*output.h5',recursive=True)
#print(inpflist)

path_std = '../ttbar_exp/std'

exists_std = os.path.exists(path_std)

if not exists_std:
    os.makedirs(path_std)
    print("The new std-exp-directory has been created!")

#This shii is an alternatve non fancy method ;)

with Pool(processes=8) as pool:
    #pool.map(filtrack, inpflist)
    pool.map_async(filtrack, inpflist)
    pool.close()
    pool.join()
'''
#But you want fancy, huh?
num_cpus = 8
miniters=1
mininterval=0
p_map(filtrack, inpflist, **{"num_cpus": num_cpus,"miniters":miniters,"mininterval":mininterval})
'''
end_time=time.time()
time_taken=(end_time-start_time)/3600.0
print(f"Total time taken in hours= {time_taken}h")
