
import glob
import json
import math  # NOQA
import os

import argh
# import matplotlib
# from pylab import *
import matplotlib  # NOQA
import numpy
from pylab import *  # NOQA

import helpers


def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def get_server_index_by_key(key, number_of_servers):
    return java_string_hashcode(key) % number_of_servers


def plot_graph(title, data):
    fig, ax = plt.subplots()
    plt.title(title)

    # ax.plot(xy1[0], xy1[1], label='1 thread', linewidth=1)
    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('Percentage')

    colors = ['r', 'g', 'b', 'm', 'c', 'k', 'y']
    color_index = 0
    for threads, _, xy in data:
        if color_index == len(colors):
            color_index = 0
        ax.plot(xy[0], xy[1], colors[color_index], label='{0} threads'.format(threads))

        color_index += 1

    # #
    # ax2 = ax.twinx()
    # ax2.plot(xy2[0], xy2[1], 'r', label='50 threads')
    # ax2.set_ylabel('Packet loss count')
    # #
    # ax3 = ax.twinx()
    # ax3.plot(xy3[0], xy3[1], 'g', label='150 threads')
    # ax3.plot(0, 0, 'g', label='150 threads')
    # ax3.set_ylabel('Packet loss count')
    # #
    # ax4 = ax.twinx()
    # ax4.plot(xy4[0], xy4[1], 'm', label='250 threads')
    # ax4.plot(0, 0, 'm', label='250 threads')
    # ax4.set_ylabel('Packet loss count')
    #

    ax.set_xlim(left=0, right=1000)
    ax.set_ylim(0, 100)
    #
    plt.grid(True)
    plt.legend(loc=1)
    #
    plt.xticks(numpy.arange(0, 1000, 25))
    show()


def plot_multi_server_graph(title, data, interpolate_graph=False, packet_loss_cdf=False, colors=None):
    fig, ax = plt.subplots()
    plt.title(title)

    # ax.plot(xy1[0], xy1[1], label='1 thread', linewidth=1)
    if packet_loss_cdf:
        ax.set_xlabel('Number of packets lost')
        ax.set_ylabel('Cumulative Packet Loss Percentage')
    else:
        ax.set_xlabel('Latency (ms)')
        ax.set_ylabel('Cumulative Requests Percentage')

    # colors = ['r', 'g', 'b', 'm', 'c', 'k', 'y']
    if colors is not None:
        colors = [c for c in colors]
    else:
        colors = ['m', 'r', 'b', 'm', 'c', 'k', 'y']
    color_index = 0

    server_index = 0

    from scipy import interpolate

    for xy in data:
        if color_index == len(colors):
            color_index = 0

        if interpolate_graph:
            print('Interpolate!')
            g = interpolate.interp1d(xy[0], xy[1], 'linear')
            x = xy[0]
            x = numpy.linspace(min(x), max(x), num=50)
            y = g(x)
        else:
            x = xy[0]
            y = xy[1]

        ax.plot(x, y, colors[color_index], linewidth=2, label='server {0}'.format(server_index))
        server_index += 1
        color_index += 1

    # #
    # ax2 = ax.twinx()
    # ax2.plot(xy2[0], xy2[1], 'r', label='50 threads')
    # ax2.set_ylabel('Packet loss count')
    # #
    # ax3 = ax.twinx()
    # ax3.plot(xy3[0], xy3[1], 'g', label='150 threads')
    # ax3.plot(0, 0, 'g', label='150 threads')
    # ax3.set_ylabel('Packet loss count')
    # #
    # ax4 = ax.twinx()
    # ax4.plot(xy4[0], xy4[1], 'm', label='250 threads')
    # ax4.plot(0, 0, 'm', label='250 threads')
    # ax4.set_ylabel('Packet loss count')
    #

    # xlimit = 200
    xlimit = max([max(xy[0]) for xy in data])

    ax.set_xlim(left=0, right=xlimit)
    ax.set_ylim(0, 100)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%d%%"))
    #
    plt.grid(True)
    plt.legend(loc=1)
    #
    plt.xticks(numpy.arange(0, xlimit, 25))
    show()


def plot_requests_per_server(per_server_data, bar_colors=None):

    servers = ['Server {0}'.format(i+1) for i, _ in enumerate(per_server_data)]
    y_pos = numpy.arange(len(servers))
    requests = [len(x) for x in per_server_data]

    total_requests = sum(requests)

    requests = [(x / float(total_requests)) * 100 for x in requests]

    print(requests)

    bar_colors = bar_colors or 'gr'

    plt.bar(y_pos, requests, align='center', color=bar_colors, alpha=0.5)
    plt.xticks(y_pos, servers)
    plt.yticks()
    # plt.xlabel('Servers')
    plt.ylabel('Requests Percentage')
    # plt.title('Servers load distribution (Zipf) [total-requests={0}]'.format(total_requests))

    ax = plt.subplot(111)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FormatStrFormatter("%d%%"))

    plt.ylim([0, 75])

    plt.show()


def plot_zipf_distribution(data_files, total_keys):
    occurences = helpers.load_keys_occurences_from_file(data_files)
    raw_data = helpers.load_data_from_file(data_files)

    # transofrm dict to list of tuples
    sorted_data = [(k, v) for k, v in occurences.items()]

    print(sorted_data)

    sorted_data = sorted(sorted_data, reverse=True, key=lambda (_, v): v)
    keys = [k for (k, _) in sorted_data]
    sorted_data = [v for _, v in sorted_data]
    # sorted_data = sorted(occurences.values(), reverse=True)
    sorted_data = [x / len(data_files) for x in sorted_data]
    x = range(len(sorted_data))

    _, ax = plt.subplots()

    rects = ax.bar(x, sorted_data, color='bgrcy')
    # for i, rect in enumerate(rects):
    #     height = rect.get_height()
    #     server_index = helpers.get_server_index_by_key(keys[i], raw_data, get_actual_server_index=True)
    #     ax.text(rect.get_x() + rect.get_width() / 2., 1.05 * height,
    #             str(server_index), ha='center', va='bottom')

    plt.xlabel('Key index')
    plt.ylabel('Number of requests')
    plt.title('Zipf key requests distribution (average,{0}) total-keys={1}'.format(len(data_files), total_keys))
    plt.xlim([0, total_keys-1])
    plt.ylim([0, sorted_data[0] + 100])

    plt.show()


def main(num_of_servers=-1,
         obj_size='500k',
         num_of_requests=10000,
         total_keys=100,
         data_file=None,
         show_requests_distribution=False,
         show_zipf_distribution=False,
         ignore_packet_loss=False,
         use_actual_server_indices=False,
         interpolate=False,
         notitle=False,
         packet_loss_cdf=False,
         colors=None):

    if show_zipf_distribution:
        plot_zipf_distribution(data_file, total_keys)
    elif num_of_servers > 0:
        title = 'object size = {0}, {1} requests, num_of_servers = {2}, file = {3}'.format(obj_size, num_of_requests, num_of_servers, data_file)
        if show_requests_distribution:
            data = helpers.load_data_from_file(data_file)
            data = helpers.split_data_per_server(data, num_of_servers, use_actual_server_index=use_actual_server_indices)
            # data = helpers.load_multi_server_data(data_file, num_of_servers, ignore_packet_loss, according_to_response=use_actual_server_indices)
            plot_requests_per_server(data, colors)
        else:
            if packet_loss_cdf:
                data = helpers.load_data_from_file(data_file)
                data = helpers.split_data_per_server(data, num_of_servers, use_actual_server_index=True)
                data = helpers.calculate_packet_loss_per_server_per_request(data)
                data = helpers.calculate_int_based_cdf_for_per_server_data(data)
            else:
                data = helpers.load_and_process_mutli_server_data(
                    data_file,
                    num_of_servers,
                    ignore_packet_loss=ignore_packet_loss,
                    use_actual_server_indices=use_actual_server_indices)
            title = '' if notitle else title
            plot_multi_server_graph(title, data, interpolate, packet_loss_cdf=packet_loss_cdf, colors=colors)
    else:
        title = 'object size = {0}, {1} requests'.format(obj_size, num_of_requests)
        pattern = 'latency-{0}*.txt'.format(obj_size)
        files_to_load = glob.glob(pattern)

        # files_to_load = [files_to_load[0], files_to_load[1]]
        # files_to_load = [files_to_load[0]]

        data = [
            (int(f.split('-')[-1].replace('.txt', '')), f, helpers.load_and_process_data(f))
            for f in files_to_load
        ]

        data.sort(key=lambda x: x[0])

        print('data files: {0}'.format(data))

        # x1, y1 = load_and_process_data('latency-1-thread.txt')
        # x2, y2 = load_and_process_data('latency-50.txt')
        # x3, y3 = load_and_process_data('latency-150.txt')
        # x4, y4 = load_and_process_data('latency-250.txt')

        plot_graph(title, data)


if __name__ == '__main__':
    argh.dispatch_command(main)
