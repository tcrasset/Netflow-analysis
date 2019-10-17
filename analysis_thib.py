import pandas as pd
from matplotlib import pyplot as plt
import subprocess


def create_graph(df, cum, is_log, name):
    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(df, density=True, cumulative=cum, histtype='step',
                                label='CDF', bins=n_bins, log=is_log, color='tab:orange')

    plt.savefig(name+".pdf")


def question1(filename, new_names):
    # QUESTION 1: CDF of packet size
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes', 'in_packets'])
    create_graph(df['in_bytes']/df['in_packets'], 1, False, "CDF_pkts_size")


def question2(filename, new_names):
    # QUESTION 2: CCDF of flow duration, linear axis
    # df = pd.read_csv(filename, header=0, names=new_names, dtype={'time_duration': 'O'}, usecols=['time_duration'])
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['time_duration'], nrows=92507632) # 92 507 632 ok, 92507633 not ok, real = 92507636
    create_graph(df['time_duration'], -1, False, "CCDF_flow_dur_lin_92")
    create_graph(df['time_duration'], -1, True, "CCDF_flow_dur_log_92")
    print("CCDF plots (linear and log axis) of flow duration have been saved")

    return

    # QUESTION 2: CCDF of number of bytes in a flow, linear axis
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes'])
    create_graph(df['in_bytes'], -1, False, "CCDF_nb_bytes_in_flow_lin")
    create_graph(df['in_bytes'], -1, True, "CCDF_nb_bytes_in_flow_log")
    print("CCDF plots (linear and log axis) of number of bytes in a flow have been saved")

    # QUESTION 2: CCDF of number of packets in a flow, linear axis
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_packets'])
    create_graph(df['in_packets'], -1, False, "CCDF_nb_pkts_in_flow_lin")
    create_graph(df['in_packets'], -1, True, "CCDF_nb_pkts_in_flow_log")
    print("CCDF plots (linear and log axis) of number of packets in a flow have been saved")


def question3(filename, new_names):
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['src_port', 'in_bytes'])
    print(df.groupby(['src_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10])

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['dest_port', 'in_bytes'])
    print(df.groupby(['dest_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10])


def question4(df):
    # # QUESTION 4
    gb = df.groupby(['src_addr'])[['in_bytes']].agg('sum').reset_index()
    print(gb.sort_values(by=['in_bytes'], ascending=False))

    
if __name__ == '__main__':
    # filename = "/mnt/hdd/netflow_split89"
    filename = "data.csv"

    cmd_get_size_file = "sed -n '$=' "+filename
    data_size = int(subprocess.check_output(cmd_get_size_file, shell=True))
    print(data_size)

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
    # question3(filename, new_names)
    # question4(filename, new_names)