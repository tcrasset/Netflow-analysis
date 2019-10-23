import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import subprocess


def create_graph(df, cum, is_log, name):
    """Create a (complementary) cumulative distribution function plot
    
    Parameters:
    df (pd.DataFrame): One-column data frame
    cum (int): 1 to plot the CDF, -1 to plot the CCDF
    is_log (bool): True to have log axis, False otherwise
    name (String): The name of the file to save the plot

    Return:
    /

    """
    fig, ax = plt.subplots()
    ax2 = ax.twinx()

    n_bins = 100
    n, bins, patches = ax.hist(df, bins=n_bins, density=False)
    n, bins, patches = ax2.hist(df, density=True, cumulative=cum, histtype='step',
                                label='CDF', bins=n_bins, log=is_log, color='tab:orange')
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


def question1(filename, new_names):
    """Plot the CDF of the packet size

    Parameters:
    filename (String): The name of the file containing the data
    new_names (String): The new names of the columns of the data

    Return:
    /

    """
    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes', 'in_packets'], nrows=92507632)
    df['packet_size'] = df['in_bytes']/df['in_packets']

    # Plotting and saving the CDF of packet size using linear axis.
    create_graph(df['packet_size'], 1, False, "CDF_pkts_size")
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
    create_graph(df['time_duration'], -1, False, "CCDF_flow_dur_lin")

    # Plotting and saving the CCDF of flow duration using logarithmic axis.
    create_graph(df['time_duration'], -1, True, "CCDF_flow_dur_log")
    print("CCDF plots (linear and log axis) of flow duration have been saved")

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_bytes'], nrows=92507632)

    # Plotting and saving the CCDF of number of bytes in a flow using linear axis.
    create_graph(df['in_bytes'], -1, False, "CCDF_nb_bytes_in_flow_lin")

    # Plotting and saving the CCDF of number of bytes in a flow using logarithmic axis.
    create_graph(df['in_bytes'], -1, True, "CCDF_nb_bytes_in_flow_log")
    print("CCDF plots (linear and log axis) of number of bytes in a flow have been saved")

    df = pd.read_csv(filename, header=0, names=new_names, usecols=['in_packets'], nrows=92507632)

    # Plotting and saving the CCDF of number of packets in a flow using linear axis.
    create_graph(df['in_packets'], -1, False, "CCDF_nb_pkts_in_flow_lin")

    # Plotting and saving the CCDF of number of packets in a flow using logarithmic axis.
    create_graph(df['in_packets'], -1, True, "CCDF_nb_pkts_in_flow_log")
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
        else:
            df_traf_vol = pd.read_pickle("df_traf_vol_rcv.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Receiver", "rcv ports", "pie_traf_vol_rcver")

    else:
        df = pd.read_csv(filename, header=0, names=new_names, usecols=[at, 'in_bytes'], nrows=92507632)
        
        # Summing the traffic volume (in bytes) of the different sender (or receiver) port number, taking the top-ten by traffic volume.
        df_traf_vol = df.groupby([at])[['in_bytes']].sum().reset_index().sort_values(by=['in_bytes'], ascending=False)[:10]

        tot_volume = df['in_bytes'].sum()

        # Adding the remaining traffic volume under the name "other" (useful when plotting the pie charts).
        df_traf_vol = df_traf_vol.append({at : 'other' , 'in_bytes' : tot_volume - df_traf_vol['in_bytes'].sum()} , ignore_index=True)

        # Computing the percentage of traffic volume for the top-ten sender (or receiver) port number and for the "other"
        df_traf_vol['volume_perc'] = df_traf_vol['in_bytes']/tot_volume
        print("Top-ten sender port numbers ordered by decreasing amount of traffic volume")
        print(df_traf_vol)

        if(sender):
            # Saving the the top-ten sender port number informations in a pkl file
            df_traf_vol.to_pickle("df_traf_vol_send.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Sender", "src ports", "pie_traf_vol_sender")
        else:
            # Saving the the top-ten receiver port number informations in a pkl file
            df_traf_vol.to_pickle("df_traf_vol_rcv.pkl")
            create_pie(df_traf_vol, "in_bytes", at, "Traffic Volume: Receiver", "rcv ports", "pie_traf_vol_rcver")


def question4(df):
    # # QUESTION 4
    gb = df.groupby(['src_addr'])[['in_bytes']].agg('sum').reset_index()
    print(gb.sort_values(by=['in_bytes'], ascending=False))

    
if __name__ == '__main__':
    # filename = "/mnt/hdd/netflow_split89"
    filename = "data.csv"

    # cmd_get_size_file = "sed -n '$=' "+filename
    # data_size = int(subprocess.check_output(cmd_get_size_file, shell=True))
    # print(data_size)

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

    question1(filename, new_names)
    question2(filename, new_names)
    question3(filename, new_names, use_saved_model=use_saved, sender=True)
    question3(filename, new_names, use_saved_model=use_saved, sender=False)
    # question4(filename, new_names)