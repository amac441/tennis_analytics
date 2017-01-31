import pandas as pd
import numpy as np
import multiprocessing
import os

#def mp_import(basefiles, nprocs):
def read_csv(fileslist):
    """ Reads the base csv
    """
    #print ("Reading",basefiles)
    #file=r"C:\Users\amac\Documents\GoogleDrive\Washu\01-17-Dai\comprehensive\rank%s.csv" % basefiles
    return pd.read_csv(fileslist,encoding = "ISO-8859-1",header=None,names=['rank_date','rank','player_id','rank_points'])#,parse_dates=['Rank Date']


def callfunction(folder):
    fileslist = os.listdir(folder)
    fileslist = [os.path.join(folder,x) for x in fileslist]
    pool = multiprocessing.Pool(processes=4) # or whatever your hardware can support
    # have your pool map the file names to dataframes
    df_list = pool.map(read_csv, fileslist)
    # reduce the list of dataframes to a single dataframe
    files = pd.concat(df_list, ignore_index=True)
    return files

if __name__ == '__main__':  

    #list(ktfinal)
    #===
    #Import rankings
    #===
    folder=r'E:\Development\0117-DaiAnalysis\gitHub_atp_project\tennis_atp\rankings'
    print ("Calling MultiProcess")
    callfunction(folder)

    #where duplicates
    #dupes = players[players.duplicated(['combinedname', 'date'], keep=False)]
    plist.to_csv('full_rankings.csv')#,convert_dates={'date':'tc','Rank Date':'tc'})
    #dupes.to_csv('dupes.csv')
    a=1