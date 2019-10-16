import pandas as pd
from matplotlib import pyplot as plt


def create_graph(df, cum, is_log):
    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(df, density=True, cumulative=cum, histtype='step',
                                label='CDF', bins=n_bins, log=is_log, color='tab:orange')

    plt.show()


def question1(filename, new_names):
    # QUESTION 1: CDF of packet size
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes', 'in_packets'])
    create_graph(df['in_bytes']/df['in_packets'], 1, False)


def question2(filename, new_names):
    # QUESTION 2: CCDF of flow duration, linear axis
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['time_duration', 'in_bytes', 'in_packets'])
    create_graph(df['time_duration'], -1, False)
    create_graph(df['time_duration'], -1, True)

    # QUESTION 2: CCDF of number of bytes in a flow, linear axis
    create_graph(df['in_bytes'], -1, False)
    create_graph(df['in_bytes'], -1, True)

    # QUESTION 2: CCDF of number of packets in a flow, linear axis
    create_graph(df['in_packets'], -1, False)
    create_graph(df['in_packets'], -1, True)


def question3(filename, new_names):
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['src_port', 'in_bytes'])
    print(df.groupby(['src_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10])

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['dest_port', 'in_bytes'])
    print(df.groupby(['dest_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10])


def question4(filename, new_names):
    return

    
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

    #question1(filename, new_names)
    question2(filename, new_names)
    question3(filename, new_names)
    # question4(filename, new_names)