import pandas as pd
from matplotlib import pyplot as plt
import ipaddress as ip


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


def question4(filename, new_names, percentage):
    prefix_length = 12

    df = pd.read_csv(filename, header=None, delimiter=',', names=new_names, usecols=['src_addr', 'in_bytes'], nrows=1000)

    # Take a certain number of prefix bits
    df['src_addr_prefix'] = df['src_addr'].apply(lambda x : extractPrefix(x, prefix_length))
    df.drop(columns=['src_addr']) # To conserve memory
    print(df['src_addr_prefix'][:10])

    # Group by the prefix and sum the total volume of traffic
    gb = df.groupby(['src_addr_prefix'])[['in_bytes']].agg('sum').reset_index()
    gb.sort_values(by=['in_bytes'], ascending=False, inplace=True)

    # Take only a certain percentage of prefixes
    top_prefixes = int(percentage/100 * gb.shape[0])
    print(top_prefixes)
    gb = gb[:top_prefixes]

    # gb.plot(kind='pie', y='src_addr_prefix')

    gb.plot(kind='bar')
    plt.xticks(range(0,top_prefixes),gb['src_addr_prefix'].values)
    plt.xlabel("IP address prefix")
    plt.ylabel("Traffic volume (in bytes)")
    fig = plt.gcf()
    fig.set_size_inches(10, 6)
    fig.subplots_adjust(bottom=0.4)
    plt.show()
    # plt.savefig("test.png")
    print(gb)


def extractPrefix(x, prefix_length):
    return ip.ip_network('{}/{}'.format(x, prefix_length), strict=False)



def searchIp(x, network):
    # TODO: Check for IPv6 addresses
    binary_address = int(ip.ip_address(x))
    binary_mask = int(network.netmask)
    binary_network_addr = int(network.network_address)
    # print(binary_address & binary_mask == binary_network_addr)
    if(binary_address & binary_mask == binary_network_addr):
        print("True")
    return (binary_address & binary_mask == binary_network_addr)

def question5(filename, new_names):
    df = pd.read_csv(filename, header=None, delimiter=',', names=new_names, usecols=['src_addr', 'dest_addr', 'in_bytes'])

    uliege_network = ip.IPv4Network('139.165.0.0/16')
    montefiore_network = ip.IPv4Network('139.165.223.0/24')
    run_network = ip.IPv4Network('139.165.222.0/24')

    uliege_index = [searchIp(i, uliege_network) for i in df['src_addr'].values]
    uliege_index = [False for i in range(10)]
    uliege_index[0] = True
    print(df[uliege_index])
    total_from_uliege = df[uliege_index]['in_bytes'].sum()
    print(total_from_uliege)
   
     # total_from_montefiore = df[df['src_addr'] == montefiore_network]['in_bytes'].sum()
    # total_from_run = df[df['src_addr'] == run_network]['in_bytes'].sum()

    # frac_from_montefiore = total_from_montefiore / total_from_uliege
    # frac_from_run = total_from_run / total_from_uliege

    # print("Fraction from montefiore : {}".format(frac_from_montefiore))
    # print("Fraction from RUN : {}".format(frac_from_run))

    
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

    #question1(filename, new_names)
    # question2(filename, new_names)
    # question3(filename, new_names)
    question4(filename, new_names, 10)
    # question5(filename, new_names)
