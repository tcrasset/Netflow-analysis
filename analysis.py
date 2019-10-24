import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import ipaddress as ip
import sys
from anytree import AnyNode, RenderTree, AsciiStyle, find, findall
from anytree.exporter import DotExporter
import subprocess
import pickle


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

def create_graph(df, cum, abs_leg, ord_leg, is_log, name, col_num=1):
    """Create a (complementary) cumulative distribution function plot
    
    Parameters:
    df (pd.DataFrame): "col_num"-column data frame
    cum (int): 1 to plot the CDF, -1 to plot the CCDF
    abs_leg (String): the abscissa legend
    ord_leg (String): ordinate legend
    is_log (bool): True to have log axis, False otherwise
    name (String): The name of the file to save the plot
    col_num (int): The number of columns of the data frame

    Return:
    /

    """
    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100

    # 1st column = x, 2nd column = y
    if(col_num == 2):
        dic = df.set_index('packet_size').T.to_dict('in_packets')
        plt.hist(list(dic['in_packets'].keys()), weights=list(dic['in_packets'].values()), histtype='step', cumulative=cum, density=False, bins=n_bins, log=is_log)
    else:
        n, bins, patches = ax.hist(df, bins=n_bins, density=False)
        n, bins, patches = ax2.hist(df, density=True, cumulative=cum, histtype='step',
                                    label='CDF', bins=n_bins, log=is_log, color='tab:orange')
    ax.set_xlabel(abs_leg)
    ax.set_ylabel(ord_leg)
    plt.savefig(name+".pdf")


def create_pie(df, arg_pie, arg_leg, title, legend, name, interior=False):
    """Create a (complementary) cumulative distribution function plot
    
    Parameters:
    df (pd.DataFrame): Three-column data frame (1st: arg_leg, 2nd: arg_pie, 3rd: "volume_perc" -> percentage of volume per instance of arg_leg)
    arg_pie (String): The name of the column used to compute the percentage volume
    arg_leg (String): The name of the column used as a legend for the percentage
    title (String): Title of the graph
    legend (String): Title of the legend of the graph
    name (String): The name of the file to save the plot
    interior (bool): True to have the percentages displayed inside the pie plot, False otherwise.

    Return:
    /

    """
    df = df.sort_values(by=[arg_pie], ascending=False).reset_index()
    fig, ax = plt.subplots(figsize=(10, 6), subplot_kw=dict(aspect="equal"))

    if interior:
        wedges, texts, autotexts = ax.pie(df[arg_pie], autopct='%1.1f%%', textprops=dict(color="w"))
        plt.setp(autotexts, size=8, weight="bold")
        
    else:
        col = ["b", "darkorange", "g", "lime", "r", "grey", "yellow", "magenta", "royalblue", "burlywood", "black"]
        wedges, texts = ax.pie(df[arg_pie], startangle=180, counterclock=False, colors=col)
        add_args = dict(arrowprops=dict(arrowstyle="-"), zorder=0, va="center")

        # Handling the arrows display.
        for i, p in enumerate(wedges):
            ang = (p.theta2 - p.theta1)/2. + p.theta1
            y = np.sin(np.deg2rad(ang))
            x = np.cos(np.deg2rad(ang))
            horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
            connectionstyle = "angle,angleA=0,angleB={}".format(ang)
            add_args["arrowprops"].update({"connectionstyle": connectionstyle})
            ax.annotate(df["volume_perc"].loc[i].round(6), xy=(x, y), xytext=(1.35*np.sign(x), 1.4*y), horizontalalignment=horizontalalignment, **add_args)

    ax.legend(wedges, df[arg_leg], title=legend, loc="upper right", bbox_to_anchor=(1, 0, 0.5, 1))
    ax.set_title(title)
    plt.savefig(name+".pdf")


def question1(filename, new_names, use_saved_model):
    """Plot the CDF of the packet size

    Parameters:
    filename (String): The name of the file containing the data
    new_names (String): The new names of the columns of the data

    Return:
    /

    """
    if(use_saved_model):
        df = pd.read_pickle("df_size_nb_pkt.pkl")
    else:
        df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes', 'in_packets'], nrows=92507632)
        df['packet_size'] = df['in_bytes']/df['in_packets']
        df = df.drop(columns=['in_bytes'])

        # Determining the number of packets of a certain size
        df = df.groupby(['packet_size'])[['in_packets']].sum().sort_values(by=['packet_size'], ascending=True).reset_index()

        # Saving the dataframe
        df.to_pickle("df_size_nb_pkt.pkl")

    # Plotting and saving the CDF of packet size using linear axis.
    create_graph(df, 1, "pkt size (bytes)", "pkt density", False, "CDF_pkts_size", col_num=2)
    print("CDF plots of packet size have been saved")

    # Printing additional information to ease the analysis of the data.
    info = df.loc[:,"packet_size"].describe()
    print(info)


def question2(filename, new_names):
    """Plot the CCDF of the packet size

    Parameters:
    filename (String): The name of the file containing the data
    new_names (String): The new names of the columns of the data

    Return:
    /

    """
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['time_duration'], nrows=92507632) # 92 507 632 ok, 92507633 not ok, real = 92507636

    # Plotting and saving the CCDF of flow duration using linear axis.
    create_graph(df['time_duration'], -1, "flow duration (sec)", "nb flows", False, "CCDF_flow_dur_lin")

    # Plotting and saving the CCDF of flow duration using logarithmic axis.
    create_graph(df['time_duration'], -1, "flow duration (sec)", "nb flows", True, "CCDF_flow_dur_log")
    print("CCDF plots (linear and log axis) of flow duration have been saved")

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes'], nrows=92507632)

    # Plotting and saving the CCDF of number of bytes in a flow using linear axis.
    create_graph(df['in_bytes'], -1, "nb bytes", "nb flows", False, "CCDF_nb_bytes_in_flow_lin")

    # Plotting and saving the CCDF of number of bytes in a flow using logarithmic axis.
    create_graph(df['in_bytes'], -1, "nb bytes", "nb flows", True, "CCDF_nb_bytes_in_flow_log")
    print("CCDF plots (linear and log axis) of number of bytes in a flow have been saved")

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_packets'], nrows=92507632)

    # Plotting and saving the CCDF of number of packets in a flow using linear axis.
    create_graph(df['in_packets'], -1, "nb pkts", "nb flows", False, "CCDF_nb_pkts_in_flow_lin")

    # Plotting and saving the CCDF of number of packets in a flow using logarithmic axis.
    create_graph(df['in_packets'], -1, "nb pkts", "nb flows", True, "CCDF_nb_pkts_in_flow_log")
    print("CCDF plots (linear and log axis) of number of packets in a flow have been saved")


def question3(filename, new_names, use_saved_model=False, sender=True):
    """Create 2 tables and 2 pie charts listing the top-ten port number by sender and receiver traffic volume 

    Parameters:
    filename (String): The name of the file containing the data
    new_names (String): The new names of the columns of the data
    sender (bool): True to consider the sender traffic volume, Flase to consider the receiver traffic volume

    Return:
    /

    """
    if(sender):
        at =  'src_port'
    else:
        at =  'dest_port'

    if(use_saved_model):
        if(sender):
            df_traf_vol = pd.read_pickle("df_traf_vol_send.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Sender", "src ports", "pie_traf_vol_sender")
            print("Top-ten sender port numbers ordered by decreasing amount of traffic volume")
        else:
            df_traf_vol = pd.read_pickle("df_traf_vol_rcv.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Receiver", "rcv ports", "pie_traf_vol_rcver")
            print("Top-ten receiver port numbers ordered by decreasing amount of traffic volume")

        print(df_traf_vol)

    else:
        df = pd.read_csv(filename, header=0, names=new_names, usecols=[at, 'in_bytes'], nrows=92507632)
        
        # Summing the traffic volume (in bytes) of the different sender (or receiver) port number, taking the top-ten by traffic volume.
        df_traf_vol = df.groupby([at])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10]

        tot_volume = df['in_bytes'].sum()

        # Adding the remaining traffic volume under the name "other" (useful when plotting the pie charts).
        df_traf_vol = df_traf_vol.append({at : 'other' , 'in_bytes' : tot_volume - df_traf_vol['in_bytes'].sum()} , ignore_index=True)

        # Computing the percentage of traffic volume for the top-ten sender (or receiver) port number and for the "other"
        df_traf_vol['volume_perc'] = df_traf_vol['in_bytes']/tot_volume

        if(sender):
            # Saving the the top-ten sender port number informations in a pkl file
            df_traf_vol.to_pickle("df_traf_vol_send.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Sender", "src ports", "pie_traf_vol_sender")
            print("Top-ten sender port numbers ordered by decreasing amount of traffic volume")
        else:
            # Saving the the top-ten receiver port number informations in a pkl file
            df_traf_vol.to_pickle("df_traf_vol_rcv.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Receiver", "rcv ports", "pie_traf_vol_rcver")
            print("Top-ten receiver port numbers ordered by decreasing amount of traffic volume")

        print(df_traf_vol)


def question4(filename, new_names, prefix_length_max, prefix_length_min, prefix_length_step,  rows, use_saved_model=False):
    if(not use_saved_model):
        df = pd.read_csv(filename, header=0, delimiter=',', names=new_names, 
                            usecols=['src_addr', 'in_bytes'], nrows=rows)

        total_traffic = df['in_bytes'].sum()
        np.save('total_traf.npy', total_traffic)

        # Count the IP addresses and sum their traffic
        df = df.groupby('src_addr', sort=False).agg({'src_addr':'count', 'in_bytes':'sum'})
        df = df.rename_axis(None).reset_index()
        df.columns = ['src_addr','src_addr_frequency','sum_in_bytes']

        df.to_pickle("df_groupby_q4.pkl")
    else:
        df = pd.read_pickle("df_groupby_q4.pkl")
        total_traffic = np.load('total_traf.npy')
    # Node pointing at upper_level subnets
    curr_root = AnyNode(ip="root",frequency=0,traffic=0) 

    # Create a tree to aggregate subnets
    for cnt, prefix_length in enumerate(range(prefix_length_max,prefix_length_min,-prefix_length_step)): 
        if(cnt == 0):
            # Process every IP address
            for _, row in df.iterrows():
                subnet_of_ip = extractPrefix(row['src_addr'], prefix_length, True)
               
                # Find in the tree if the subnet exists alerady
                existing_subnet = find(node=curr_root,
                                        filter_=lambda node: node.ip == subnet_of_ip,
                                        maxlevel = 2)
                
                # If not, create a new one
                subnet = None
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

    # # Save the tree as a graph
    # createTree(curr_root)

    # Count the number of prefix above a certain traffic threshold
    max_level = int((prefix_length_max - prefix_length_min)/prefix_length_step) + 1

    top10_prefix_nodes = findall(node=curr_root, 
                            filter_ = lambda node: node.traffic/total_traffic >=0.10,
                            maxlevel = max_level)
    top1_prefix_nodes = findall(node=curr_root, 
                            filter_ = lambda node: node.traffic/total_traffic >=0.01,
                            maxlevel = max_level)
    top01_prefix_nodes = findall(node=curr_root, 
                            filter_ = lambda node: node.traffic/total_traffic >=0.001,
                            maxlevel = max_level)
    top10_prefix = [x.ip for x in top10_prefix_nodes]
    top1_prefix = [x.ip for x in top1_prefix_nodes]
    top01_prefix = [x.ip for x in top01_prefix_nodes]

    print(top10_prefix)

    with open('top10_prefix.txt', 'w') as f:
        for item in top10_prefix:
            f.write("{}\n".format(item))

    with open('top1_prefix.txt', 'w') as f:
        for item in top1_prefix:
            f.write("{}\n".format(item))

    with open('top01_prefix.txt', 'w') as f:
        for item in top01_prefix:
            f.write("{}\n".format(item))


def countPrefixOccurence(df, network):
    result = df['src_addr'].apply(lambda x : searchIp(x, ip.ip_network(network))).values
    result = np.append(result,True)
    return np.count_nonzero(result == True)


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

    use_saved = False

    # question1(filename, new_names, use_saved)
    # question2(filename, new_names)
    # question3(filename, new_names, use_saved_model=use_saved, sender=True)
    # question3(filename, new_names, use_saved_model=use_saved, sender=False)
    question4(filename, new_names, 24, 7, 8,  100, True)
    # question5(filename, new_names, rows=100000)


    # df = pd.read_csv(filename, header=0, names=new_names, usecols=['src_addr'], nrows=10000)
    
    # ip = {'src_addr': ['192.168.56.1','192.168.56.5','155.168.56.1']}
    # df = pd.DataFrame(ip,columns= ['src_addr'])

    # cnt_sub_Uliege = countPrefixOccurence(df, '139.165.0.0/16')
    # print(cnt_sub_Uliege)
