import pandas as pd
from matplotlib import pyplot as plt
import ipaddress as ip
import sys
from anytree import AnyNode, RenderTree, AsciiStyle, find
from anytree.exporter import DotExporter

def nodenamefunc(node):
    return "%s (%s B)" % (node.ip, node.traffic)
def edgeattrfunc(node, child):
    return 'label="%s (%s bytes)"' % (node.ip, child.name)
def edgetypefunc(node, child):
    return '--'

def createTree(root):
    DotExporter(root, 
                graph="graph",
                nodenamefunc=nodenamefunc,
                # nodeattrfunc= lambda node: "shape=box",
                # edgeattrfunc=edgeattrfunc,
                edgetypefunc=edgetypefunc).to_picture("tree.pdf")

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

    total_traffic = df['in_bytes'].sum()

    # Count the leave nodes and sum their traffic
    df = df.groupby('src_addr', sort=False).agg({'src_addr':'count', 'in_bytes':'sum'})
    df = df.rename_axis(None).reset_index()
    df.columns = ['src_addr','src_addr_frequency','sum_in_bytes']
    curr_root = AnyNode(ip="root",frequency=0,traffic=0) # Node pointing at upper_level subnets

    for cnt, prefix_length in enumerate(range(8,4,-2)): 
        if(cnt == 0):
            for _, row in df.iterrows():
                subnet_of_ip = extractPrefix(row['src_addr'], prefix_length, True)
                print(subnet_of_ip)
                # Find in the tree if the subnet exists alerady
                existing_subnet = find(node=curr_root,
                                        filter_=lambda node: node.ip == subnet_of_ip,
                                        maxlevel = 2)
                
                subnet = None
                # If not, create a new one
                if(existing_subnet is None):
                    subnet = AnyNode(parent = curr_root,
                                    ip=subnet_of_ip,
                                    frequency=0,
                                    traffic=0)
                else:
                    subnet = existing_subnet
                    

                # Add the child (the ip address) and update the parent (subnet)
                AnyNode(parent = subnet,
                        ip = row['src_addr'], 
                        frequency = row['src_addr_frequency'], 
                        traffic = row['sum_in_bytes'])
                subnet.traffic += row['sum_in_bytes']
                subnet.frequency += row['src_addr_frequency']

        else: 
            new_root = AnyNode(ip="new_root",frequency=0,traffic=0) # Node pointing at upper_level subnets

            # The current root contains the subnets
            for subnet in curr_root.children:
                subnet_ip = str(subnet.ip).split('/')[0]
                subnet_of_subnet = extractPrefix(subnet_ip, prefix_length, True)

                # Find in the tree if the subnet exists alerady
                existing_subnet = find(node=new_root,
                                        filter_= lambda node: node.ip == subnet_of_subnet,
                                        maxlevel = 2)
                
                # If not, create a new one and add the current subnet as his child
                if(existing_subnet is None):
                    AnyNode(parent = new_root,
                                children= [subnet],
                                ip=subnet_of_subnet,
                                frequency=subnet.frequency,
                                traffic=subnet.traffic)
                else:
                    subnet.parent = existing_subnet
                    existing_subnet.traffic += subnet.traffic
                    existing_subnet.frequency += subnet.frequency
            # Update the current root
            curr_root = new_root
            curr_root.ip = "root"

    # Update root node with it's childs attributes
    curr_root.traffic = sum(x.traffic for x in curr_root.children)
    curr_root.frequency = sum(x.frequency for x in curr_root.children)

    # Save the tree as a graph
    createTree(curr_root)
    # print(RenderTree(curr_root))
    # print(RenderTree(curr_root).by_attr('ip'))

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


def extractPrefix(x, prefix_length, as_string):
    if(as_string):
        return str(ip.ip_network('{}/{}'.format(x, prefix_length), strict=False))
    else:
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
    question4(filename, new_names, prefix_length=8, rows=10)
    # question5(filename, new_names, rows=100000)

    # prefix_length = sys.argv[1]
    # question4(filename, new_names, prefix_length=prefix_length, rows=92507632)

