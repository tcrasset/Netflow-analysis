import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import multiprocessing as mp
import pandas as pd

CHUNKSIZE_TOM = 1000000  # processing 1 000 000 rows at a time
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


def get_volume_src_port(df):
    return df.groupby(['src_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10]


def get_volume_dest_port(df):
    return df.groupby(['dest_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10]


def exec_multi_proc_f(df_reader, pool, function_par):
    joblist = []
    for df in df_reader:
        joblist.append(pool.apply_async(function_par,[df]))
        break

    df_list = []
    for f in joblist:
        df_list.append(f.get(timeout=10))
    return df_list


def create_graph(df_reader, pool, function_par, cum, is_log):
    df_list = exec_multi_proc_f(df_reader, pool, function_par)

    complete_df = pd.concat(df_list)
    print(complete_df[:10])

    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(complete_df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(complete_df, density=True, cumulative=cum, histtype='step',
                                label='CDF', bins=n_bins, log=is_log, color='tab:orange')

    plt.show()


def question1(df_reader, pool):
    # QUESTION 1: CDF of packet size
    create_graph(df_reader, pool, get_packet_size, 1, False)


def question2(df_reader, pool):
    # QUESTION 2: CCDF of flow duration, linear axis
    create_graph(df_reader, pool, get_flow_dur, -1, False)

    # QUESTION 2: CCDF of number of bytes in a flow, linear axis
    create_graph(df_reader, pool, get_bytes_flow, -1, False)

    # QUESTION 2: CCDF of number of packets in a flow, linear axis
    create_graph(df_reader, pool, get_packets_flow, -1, False)

    # QUESTION 2: CCDF of flow duration, log axis
    create_graph(df_reader, pool, get_flow_dur, -1, True)

    # QUESTION 2: CCDF of number of bytes in a flow, log axis
    create_graph(df_reader, pool, get_bytes_flow, -1, True)

    # QUESTION 2: CCDF of number of packets in a flow, log axis
    create_graph(df_reader, pool, get_packets_flow, -1, True)

def question4(df):
    # # QUESTION 4
    gb = df.groupby(['src_addr'])[['in_bytes']].agg('sum').reset_index()
    print(gb.sort_values(by=['in_bytes'], ascending=False))

def question3(df_reader, pool):
    df_list = exec_multi_proc_f(df_reader, pool, get_volume_src_port)
    print(df_list)

    df_list = exec_multi_proc_f(df_reader, pool, get_volume_dest_port)
    print(df_list)


    
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

    pool = mp.Pool(4) # use 4 processes
    # question1(df_reader, pool)
    # question2(df_reader, pool)
    # question3(df_reader, pool)
    question4(df)


