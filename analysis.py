import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import multiprocessing as mp
import pandas as pd

CHUNKSIZE_TOM = 5000000  # processing 1 000 000 rows at a time
CHUNKSIZE_THIB = 1000000  # processing 1 000 000 rows at a time

def get_packet_size(df):
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


def get_packets_flow(df):
    return df['in_packets']


def get_bytes_flow(df):
    return df['in_bytes']


def create_graph(df_reader, pool, function_par, cum):
    joblist = []
    for df in df_reader:
        joblist.append(pool.apply_async(function_par,[df]))
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
    n, bins, patches = ax2.hist(complete_df, density=True, cumulative=cum, histtype='step',
                                label='CDF', bins=n_bins, color='tab:orange')

    plt.show()


def question1(df_reader, pool):
    create_graph(df_reader, pool, get_packet_size, 1)


def question2(df_reader, pool):
    # QUESTION 2: CCDF of flow duration
    create_graph(df_reader, pool, get_flow_dur, -1)

    # QUESTION 2: CCDF of number of bytes in a flow
    create_graph(df_reader, pool, get_bytes_flow, -1)

    # QUESTION 2: CCDF of number of packets in a flow
    create_graph(df_reader, pool, get_packets_flow, -1)


def question4(df_reader, pool):
    joblist = []
    for df in df_reader:
        joblist.append(pool.apply_async(get_flow_dur,[df]))
        break

    df_list = []
    for f in joblist:
        df_list.append(f.get(timeout=10))

    
if __name__ == '__main__':
    # filename = "/mnt/hdd/netflow.csv"
    filename = "data.csv"

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

    df_reader = pd.read_csv(filename, chunksize=CHUNKSIZE_THIB, header=0, names=new_names)
    pool = mp.Pool(4) # use 4 processes

    # question1(df_reader, pool)
    question2(df_reader, pool)
    
    #question4(df_reader, pool)


