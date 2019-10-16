import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import multiprocessing as mp


import pandas as pd

CHUNKSIZE_TOM = 5000000  # processing 1 000 000 rows at a time
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

    # print(df.packet_size[0:2].transpose())
    # print(df.columns)
    # print(df.describe().transpose())

    # process data frame
    return df['packet_size']

if __name__ == '__main__':
    filename = "/mnt/hdd/netflow.csv"

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

    reader = pd.read_csv(filename, chunksize=CHUNKSIZE_TOM, header=0, names=new_names)

    pool = mp.Pool(4) # use 4 processes
    joblist = []
    for df in reader:
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