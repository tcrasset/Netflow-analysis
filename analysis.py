import pandas as pd
from matplotlib import pyplot as plt
import ipaddress as ip
import sys


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
    print(df.groupby(['src_port'])[['in_bytes']].sum().reset_index()
            .sort_values(by=['in_bytes'], ascending=False)[:10])

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['dest_port', 'in_bytes'])
    print(df.groupby(['dest_port'])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10])


def question4(filename, new_names, prefix_length, rows):
    df = pd.read_csv(filename, header=0, delimiter=',', names=new_names, 
                        usecols=['src_addr', 'in_bytes'], nrows=rows)

    # Take a certain number of prefix bits
    df['src_addr_prefix'] = df['src_addr'].apply(lambda x : extractPrefix(x, prefix_length))
    df.drop(columns=['src_addr']) # To conserve memory

    total_traffic = df['in_bytes'].sum()
    # Group by the prefix and sum the total volume of traffic
    gb = df.groupby(['src_addr_prefix'], sort=False)[['in_bytes']].agg('sum').reset_index()
    gb.sort_values(by=['in_bytes'], ascending=False, inplace=True)
    
    gb['percentage'] = (gb['in_bytes']/total_traffic*100)
    gb['percentage_cum'] = gb['percentage'].cumsum()
    print(gb)


    # # Take only a certain percentage of prefixes
    # top10_prefixes = int(10/100 * gb.shape[0])
    # top1_prefixes = int(1/100 * gb.shape[0])
    # top01_prefixes = int(0.1/100 * gb.shape[0])

    # total_sum = gb.in_bytes.sum()

    # partial_sum = gb.in_bytes[:top10_prefixes].sum()
    # traffic_fraction = partial_sum / total_sum * 100
    # print("Fraction of traffic of 10% most popular IP: {:.1f}% ({:.1f} GB /{:.1f} GB)"
    #         .format(traffic_fraction, partial_sum/10**9, total_sum/10**9))
    
    # partial_sum = gb.in_bytes[:top1_prefixes].sum()
    # traffic_fraction = partial_sum / total_sum * 100
    # print("Fraction of traffic of 1% most popular IP: {:.1f}% ({:.1f} GB /{:.1f} GB)"
    #         .format(traffic_fraction, partial_sum/10**9, total_sum/10**9))
    
    # partial_sum = gb.in_bytes[:top01_prefixes].sum()
    # traffic_fraction = partial_sum / total_sum * 100
    # print("Fraction of traffic of 0.1% most popular IP: {:.1f}% ({:.1f} GB /{:.1f} GB)"
    #         .format(traffic_fraction, partial_sum/10**9, total_sum/10**9))

    # simpleBarPlot(gb, top10_prefixes,'10')
    # simpleBarPlot(gb, top1_prefixes,'1')
    # simpleBarPlot(gb, top01_prefixes,'01')

def simpleBarPlot(df, nb_prefix, percentage):
    df = df[:nb_prefix]
    df.plot(kind='bar')
    plt.xticks(range(0,nb_prefix),df['src_addr_prefix'].values)
    plt.xlabel("IP address prefix")
    plt.ylabel("Traffic volume (in bytes)")
    fig = plt.gcf()
    fig.set_size_inches(10, 6)
    fig.subplots_adjust(bottom=0.2)
    # plt.show()
    plt.savefig("Top{}_Barplot.svg".format(percentage))


def extractPrefix(x, prefix_length):
    return ip.ip_network('{}/{}'.format(x, prefix_length), strict=False)

def searchIp(x, network):
    binary_address = int(ip.ip_address(x))
    binary_mask = int(network.netmask)
    binary_network_addr = int(network.network_address)
    # print(binary_address & binary_mask == binary_network_addr)
    if(binary_address & binary_mask == binary_network_addr):
        print("True")
    return (binary_address & binary_mask == binary_network_addr)

def question5(filename, new_names, rows):
    df = pd.read_csv(filename, header=0, delimiter=',', names=new_names, usecols=['src_addr', 'dest_addr', 'in_bytes'], nrows=rows)

    uliege_network = ip.IPv4Network('139.165.0.0/16') # Real one
    montefiore_network = ip.IPv4Network('139.165.223.0/24')
    run_network = ip.IPv4Network('139.165.222.0/24')

    uliege_index_sent = [searchIp(i, uliege_network) for i in df['src_addr'].values]
    uliege_index_rcv = [searchIp(i, uliege_network) for i in df['dest_addr'].values]
    uliege_df_sent = df[uliege_index_sent]
    uliege_df_rcv = df[uliege_index_rcv]

    total_sent_from_uliege = uliege_df_sent['in_bytes'].sum()
    total_rcv_at_uliege = uliege_df_rcv['in_bytes'].sum()

    montefiore_sent_index = [searchIp(i, montefiore_network) for i in uliege_df_sent['src_addr'].values]
    montefiore_rcv_index = [searchIp(i, montefiore_network) for i in uliege_df_rcv['dest_addr'].values]
    montef_df_sent = uliege_df_sent[montefiore_sent_index]
    montef_df_rcv = uliege_df_rcv[montefiore_rcv_index]

    run_sent_index = [searchIp(i, run_network) for i in uliege_df_sent['src_addr'].values]
    run_rcv_index = [searchIp(i, run_network) for i in uliege_df_rcv['dest_addr'].values]
    run_df_sent = uliege_df_sent[run_sent_index]
    run_df_rcv = uliege_df_rcv[run_rcv_index]


    total_sent_from_montef = montef_df_sent['in_bytes'].sum()
    total_rcv_at_montef = montef_df_rcv['in_bytes'].sum()

    total_sent_from_run = run_df_sent['in_bytes'].sum()
    total_rcv_at_run = run_df_rcv['in_bytes'].sum()

    montef_sent_fraction = total_sent_from_montef/total_sent_from_uliege * 100
    montef_rcv_fraction = total_rcv_at_montef/total_rcv_at_uliege * 100
    run_sent_fraction = total_sent_from_run/total_sent_from_uliege * 100
    run_rcv_fraction = total_rcv_at_run/total_rcv_at_uliege * 100

    print("Fraction of traffic sent to RUN: {:.1f}% ({:.1f} GB /{:.1f} GB)"
            .format(run_sent_fraction, total_sent_from_run/10**9, total_sent_from_uliege/10**9))

    print("Fraction of traffic received at RUN: {:.1f}% ({:.1f} GB /{:.1f} GB)"
                .format(run_rcv_fraction, total_rcv_at_run/10**9, total_rcv_at_uliege/10**9))

    print("Fraction of traffic sent to Montefiore: {:.1f}% ({:.1f} GB /{:.1f} GB)"
                .format(montef_sent_fraction, total_sent_from_montef/10**9, total_sent_from_uliege/10**9))

    print("Fraction of traffic received at Montefiore: {:.1f}% ({:.1f} GB /{:.1f} GB)"
            .format(montef_rcv_fraction, total_rcv_at_montef/10**9, total_rcv_at_uliege/10**9))


    
if __name__ == '__main__':
    # filename = "/mnt/hdd/netflow_split89"
    filename = "/mnt/hdd/netflow.csv"
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

    #question1(filename, new_names)
    # question2(filename, new_names)
    # question3(filename, new_names)
    question4(filename, new_names, prefix_length=24, rows=1000000)
    # question5(filename, new_names, rows=100000)


    # prefix_length = sys.argv[1]
    # question4(filename, new_names, prefix_length=prefix_length, rows=92507632)

