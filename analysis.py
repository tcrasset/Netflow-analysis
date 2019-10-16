import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import multiprocessing as mp
import pandas as pd

CHUNKSIZE_TOM = 1000000  # processing 1 000 000 rows at a time
CHUNKSIZE_THIB = 1000000  # processing 1 000 000 rows at a time

def process_frame(df):
    columns_to_drop = [ 'dir',
        'nh',
        'nhb',
        'svln',
        'dvln',
        'ismc',
        'odmc',
        'idmc',
        'osmc',
        'mpls1',
        'mpls2',
        'mpls3',
        'mpls4',
        'mpls5',
        'mpls6',
        'mpls7',
        'mpls8',
        'mpls9',
        'mpls10',
        'cl',
        'sl',
        'al',
        'ra',
        'engine_type',
        'exid']
    df.drop(columns=columns_to_drop,inplace=True)
    print(df.src_mask.describe())

    df['packet_size'] = df['in_bytes']/df['in_packets']

    return df['packet_size']


def get_flow_dur(df):
    return df['time_duration']


def question1(df_reader):
    pool = mp.Pool(4) # use 4 processes

    # QUESTION 1
    joblist = []
    for df in df_reader:
        joblist.append(pool.apply_async(process_frame,[df]))
        break

    df_list = []
    for f in joblist:
        df_list.append(f.get(timeout=10))

    complete_df = pd.concat(df_list)
    print(complete_df[:10])

    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(complete_df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(complete_df, density=True, cumulative=True, histtype='step',
                                label='CDF', bins=n_bins, color='tab:orange')

    plt.show()

def question2(df_reader):

    pool = mp.Pool(4) # use 4 processes

    # QUESTION 2
    joblist = []
    for df in df_reader:
        joblist.append(pool.apply_async(get_flow_dur,[df]))
        break

    df_list = []
    for f in joblist:
        df_list.append(f.get(timeout=10))

    complete_df = pd.concat(df_list)
    print(complete_df[:10])
    sorted_df = complete_df.sort_values()

    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(sorted_df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(sorted_df, density=True, cumulative=-1, histtype='step',
                                label='CCDF', bins=n_bins, color='tab:orange')

    plt.show()


def question4(df):
    # # QUESTION 4
    gb = df.groupby(['src_addr'])[['in_bytes']].agg('sum').reset_index()
    print(gb.sort_values(by=['in_bytes']))

    

if __name__ == '__main__':
    filename = "/mnt/hdd/netflow_split89"
    # filename = "data.csv"

    new_names = [
        'time_start',
        'time_end',
        'time_duration',
        'src_addr',
        'dest_addr',
        'src_port',
        'dest_port',
        'ip_protocol',
        'tcp_flags',
        'fwd',
        'source_tos',
        'in_packets',
        'in_bytes',
        'out_packets',
        'out_bytes',
        'in_snmp',
        'out_snmp',
        'source_as',
        'dest_as',
        'src_mask',
        'dest_mask',
        'dest_tos',
        'dir',
        'nh',
        'nhb',
        'svln',
        'dvln',
        'ismc',
        'odmc',
        'idmc',
        'osmc',
        'mpls1',
        'mpls2',
        'mpls3',
        'mpls4',
        'mpls5',
        'mpls6',
        'mpls7',
        'mpls8',
        'mpls9',
        'mpls10',
        'cl',
        'sl',
        'al',
        'ra',
        'engine_type',
        'exid']

    df_reader = pd.read_csv(filename, chunksize=CHUNKSIZE_TOM, header=0, delimiter=',', names=new_names)
    df = pd.read_csv(filename, header=None, delimiter=',', names=new_names)
    # question1(df_reader)
    # question2(df_reader)
    
    question4(df)


