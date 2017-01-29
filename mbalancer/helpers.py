
import argparse
import glob
import json
import math
import os
import time
from datetime import datetime

import requests


KEY_INDEX = 1
REQUEST_SERVER_INDEX = 3
RESPONSE_SERVER_INDEX = 4
PACKETS_COUNT_INDEX = 5

TIMESTAMP_FORMAT = '%H:%M:%S:%f'

try:
    import memcache
except ImportError:
    memcache = None


class RequestData(list):

    def __init__(self, data):
        self.extend(data)

    @property
    def request_server_index(self):
        return int(self[REQUEST_SERVER_INDEX])

    @property
    def response_server_index(self):
        return int(self[RESPONSE_SERVER_INDEX])

    @property
    def time(self):
        return datetime.fromtimestamp(time.mktime(time.strptime(self[0], TIMESTAMP_FORMAT)))

    @property
    def key(self):
        return self[KEY_INDEX]

    @property
    def total_packets_count(self):
        return int(self[PACKETS_COUNT_INDEX].split('/')[1])

    @property
    def received_packets_count(self):
        return int(self[PACKETS_COUNT_INDEX].split('/')[0])

    @property
    def packets_lost_count(self):
        total = self.total_packets_count
        if total == -1:
            return -1
        else:
            return total - self.received_packets_count

    @property
    def has_packet_loss(self):
        return not self.has_packets or self[2] == '-1.0'

    @property
    def has_packets(self):
        return self.received_packets_count != -1


def load_and_process_data(filename, start=0, interval=0.1):
    print('loading file: {0}'.format(filename))

    with open(filename, 'r') as f:
        data = [x for x in f.read().split(os.linesep) if x and x[0] != ';']

    data = sorted(data, key=lambda item: float(item[0]))
    return process_data(data, start, interval)


def process_data(data, start, interval):

    data = [float(x[2]) for x in data]

    organized = {}

    end = start + interval

    index = 0

    while index < len(data):
        # print('index = {0}, d = {1}, start = {2}, end = {3}'.format(index, d, start, end))
        d = data[index]

        if d < 0:
            index += 1
            continue

        key = (start, end)
        if d >= start and d < end:
            if key not in organized:
                organized[key] = 1
            else:
                organized[key] += 1
            index += 1
        else:
            start += interval
            end += interval

    # ddd = {'{0} -> {1}'.format(k[0], k[1]): v for k, v in organized.items()}
    # print(json.dumps(ddd, indent=2))

    organized = {k: (v / float(len(data))) * 100 for k, v in organized.items()}
    # ddd = {'{0} -> {1}'.format(k[0], k[1]): v for k, v in organized.items()}
    # print(json.dumps(ddd, indent=2))

    organized = [(k[1], v) for k, v in organized.items()]

    organized.sort(key=lambda x: x[0])

    c_organized = []
    c_sum = 0

    for i in range(len(organized)):
        c_sum += organized[i][1]
        c_organized.append((organized[i][0], c_sum))

    organized = c_organized

    x = [aaa[0] for aaa in organized]
    y = [aaa[1] for aaa in organized]

    return x, y


def load_data_from_file(data_file):
    def _read_file(_data_file):
        print('Loading file: {0}'.format(_data_file))
        with open(_data_file, 'r') as f:
            return f.read().split(os.linesep)

    if '*' in data_file:
        files = glob.glob(data_file)
        print('found the following files for pattern: {0}:'.format(data_file))
        print(json.dumps(files, indent=2))
        if len(files) == 0:
            raise IOError('No files found for pattern: {0}'.format(data_file))
        all_data = [_read_file(x) for x in files]
        first_file = all_data[0]
        for i in range(1, len(all_data)):
            current_data = all_data[i]
            only_data = [x for x in current_data if not x.startswith(';')]
            first_file.extend(only_data)
        all_data = first_file
    else:
        all_data = _read_file(data_file)
    return all_data


def split_data_per_server(all_data, number_of_servers, ignore_packet_loss=False, use_actual_server_index=False):

    def _extract_server_data(_server_index):
        items = []
        for item in [x for x in all_data if x and not x.startswith(';')]:
            request = RequestData(item.split(','))
            if ignore_packet_loss and request.has_packet_loss:
                continue
            request_server_index = request.request_server_index
            response_server_index = request.response_server_index
            server_index = response_server_index if use_actual_server_index and response_server_index != -1 else request_server_index
            if server_index == _server_index:
                items.append(request)
        return items

    return [_extract_server_data(i) for i in range(number_of_servers)]


def load_multi_server_data(filename, number_of_servers, ignore_packet_loss, according_to_response=False):
    """
    ignore_packet_loss - On packet loss, actual server index is -1 and needs to be converted
    to a real index according to previous requests, otherwise -1 -> last item in the array.
    """
    print('Loading file: {0} [according_to_response={1}]'.format(filename, according_to_response))

    data = [RequestData(x.split(',')) for x in load_data_from_file(filename) if x and x[0] != ';']

    per_server_data = [[] for _ in range(number_of_servers)]

    for item in data:
        if ignore_packet_loss and item[2] == '-1.0':
            continue
        if according_to_response:
            if len(item) < 5:
                raise RuntimeError('Response server index not available in data.')
            index = int(item[4])
            if index == -1:
                index = get_server_index_by_key(item[1], data, get_actual_server_index=True)
        elif len(item) >= 4:
            index = int(item[3])
        else:
            raise RuntimeError('enable fallback to hashcode function in code')
            # index = get_server_index_by_key(item[1], number_of_servers)
        per_server_data[index].append(item)

    return per_server_data


def load_and_process_mutli_server_data(
        filename, number_of_servers, start=0, interval=0.1, ignore_packet_loss=False, use_actual_server_indices=False):

    all_data = load_data_from_file(filename)
    per_server_data = split_data_per_server(all_data, number_of_servers, ignore_packet_loss=ignore_packet_loss, use_actual_server_index=use_actual_server_indices)
    # per_server_data = load_multi_server_data(filename, number_of_servers, ignore_packet_loss, according_to_response=use_actual_server_indices)

    processed_data = []

    for i, server_data in enumerate(per_server_data):
        print('server {0}: {1} keys'.format(i, len(server_data)))
        sorted_data = sorted(server_data, key=lambda x: float(x[2]))
        processed_data.append(process_data(sorted_data, start, interval))

    return processed_data


def load_keys_occurences_from_file(data_file, occurences=None):
    if occurences is None:
        occurences = {}
    print('Loading keys occurences from file: {0}'.format(data_file))
    data = [x for x in load_data_from_file(data_file) if len(x) > 0 and x[0] != ';']
    for d in data:
        request = RequestData(d.split(','))
        if request.key not in occurences:
            occurences[request.key] = 1
        else:
            occurences[request.key] += 1
    return occurences


def get_server_index_by_key(key, requests, get_actual_server_index=False):
    counter = 0
    fallback_index = None
    for r in requests:
        if not isinstance(r, list) and (r.startswith(';') or len(r) == 0):
            continue
        values = r.split(',') if not isinstance(r, list) else r
        r_key = values[1]
        if r_key == key:
            counter += 1
            fallback_index = int(values[3])
            r_server_index = int(values[4]) if get_actual_server_index else int(values[3])
            if r_server_index == -1:  # can happen on packet loss
                continue
            return r_server_index
    if counter <= 5:
        return fallback_index
    raise RuntimeError('key: {0} not found in requests.'.format(key))


def match_hot_keys_to_servers(hot_keys, all_data, according_to_response):
    requests = [x for x in all_data if len(x) and x[0] != ';']
    return [(x[0], x[1], get_server_index_by_key(x[0], requests, according_to_response)) for x in hot_keys]


def get_hot_keys_from_servers(hot_keys, servers):
    def get_key(_key_server_tuple):
        client = memcache.Client([servers[_key_server_tuple[2]]])
        value = client.get(_key_server_tuple[0])
        if value is None:
            raise RuntimeError('key: {0} not found in server: {1}'.format(_key_server_tuple[0], servers[_key_server_tuple[2]]))
        return value
    return {x[0]: get_key(x) for x in hot_keys}


def create_add_key_controller_request(key, target_mac, target_ip, target_port, priority=2, group_id=None):
    print('Creating request for key: {0} [target_mac={1}, target_ip={2}, target_port={3}, priority={4}]'.format(key, target_mac, target_ip, target_port, priority))
    if group_id:
        actions = [
            {
                "OFPActionGroup": {
                    "group_id": group_id
                }
            }
        ]
    else:
        actions = [
            {
                "OFPActionSetField": {
                    "eth_dst": target_mac
                }
            },
            {
                "OFPActionSetField": {
                    "ipv4_dst": target_ip
                }
            },
            {
                "OFPActionOutput": {
                    "port": target_port
                }
            }
        ]
    body = {
        "KeyReq": {
            "priority": priority,
            "key": key,
            "command": "ADD",
            "match": {
            },
            "instructions": {
                "OFPInstructionActions": {
                    "type": "OFPIT_APPLY_ACTIONS",
                    "actions": actions
                }
            }
        }
    }
    return body


def create_optimal_key_to_server_mapping(hot_keys, requests_per_server, servers):
    rps = requests_per_server[:]
    print('Requests per server: {0}'.format(rps))

    for _, number_of_requests, server_index in hot_keys:
        rps[server_index] -= number_of_requests

    print('Requests per server (hot keys removed): {0}'.format(rps))

    hot_keys_optimal = hot_keys[:]

    def _get_server_index_with_minimum_requests():
        index_and_requests = [(i, c) for i, c in enumerate(rps)]
        return sorted(index_and_requests, key=lambda x: x[1])[0][0]

    for i in range(len(hot_keys_optimal)):
        number_of_requests = hot_keys_optimal[i][1]
        greedy_server_index = _get_server_index_with_minimum_requests()
        rps[greedy_server_index] += number_of_requests
        hot_keys_optimal[i] += (greedy_server_index,)
        print('i = {0}'.format(i))
        print('hot_key = {0}'.format(hot_keys_optimal[i]))
        print('servers = {0}'.format(rps))

    return hot_keys_optimal


def spread_hot_keys_and_add_openflow_rules(hot_keys, requests_per_server, data_file, controller_host, according_to_response, use_group=False):
    all_data = load_data_from_file(data_file)
    data = [x for x in all_data if x.startswith(';')]
    if len(data) < 3:
        raise RuntimeError('Servers line could not be found in data file.')
    servers = data[2].replace(';', '').split(',')
    print('Found servers: {0}'.format(servers))
    hot_keys = match_hot_keys_to_servers(hot_keys, all_data, according_to_response)
    print('Hot keys to servers mapping: {0}'.format(hot_keys))

    optimal_mapping = create_optimal_key_to_server_mapping(hot_keys, requests_per_server, servers)
    print('Optimal hot keys mapping to servers: {0}'.format(optimal_mapping))

    if controller_host is not None:
        if memcache is None:
            raise RuntimeError('python-memcached not installed in environment.')

        hot_keys_and_values = get_hot_keys_from_servers(hot_keys, servers)
        print('Hot keys values length: {0}'.format({k: len(v) for k, v in hot_keys_and_values.items()}))
        for key, _, _ in hot_keys:
            for server in servers:
                print('Writing key: {0} to server: {1}'.format(key, server))
                client = memcache.Client([server])
                client.set(key, hot_keys_and_values[key])

        switch_ports = requests.get('http://{0}:8080/controller/ports'.format(controller_host)).json()
        print('Switch ports: {0}'.format(json.dumps(switch_ports, indent=2)))

        controller_requests = []
        group_id = None
        if use_group:
            print('Adding a select group to the switch...')
            group_id = 5

            servers_ips = [x.split(':')[0] for x in servers]
            relevant_ports = [x for x in switch_ports if x['ip_address'] in servers_ips]
            print('Servers IPs: {}'.format(servers_ips))
            print('Relevant switch ports: {}'.format(relevant_ports))

            buckets = [{
                "actions": [
                    {
                        "OFPActionSetField": {
                            "eth_dst": x['mac']
                        }
                    },
                    {
                        "OFPActionSetField": {
                            "ipv4_dst": x['ip_address']
                        }
                    },
                    {
                        "OFPActionOutput": {
                            "port": x['port']
                        }
                    }
                ]
            } for x in relevant_ports]

            body = {
                "OFPGroupMod": {
                    "command": "OFPGC_ADD",
                    "type": "OFPGT_SELECT",
                    "group_id": group_id,
                    "buckets": buckets
                }
            }
            r = requests.post('http://{0}:8080/controller/groups'.format(controller_host), json=body)
            if r.status_code != requests.codes.created:
                raise RuntimeError(
                    'Add group failed [status_code={0}]'.format(r.status_code))

            for hot_key in hot_keys:
                key = hot_key[0]
                request = create_add_key_controller_request(key, None, None, None, group_id=group_id)
                controller_requests.append(request)

        else:
            # create simple flow table rules
            for key, _, current_server, target_server in optimal_mapping:
                if current_server != target_server:
                    target_server_ip = servers[target_server].split(':')[0]
                    port_info = [x for x in switch_ports if x['ip_address'] == target_server_ip]
                    if len(port_info) != 1:
                        raise RuntimeError('ip address: {0} not found in switch.'.format(target_server_ip))
                    port_info = port_info[0]
                    mac = port_info['mac']
                    port = port_info['port']
                    print('OpenFlow rule: key={0}, mac={1}, target_server_ip={2}, port={3}'.format(key, mac, target_server_ip, port))
                    controller_requests.append(create_add_key_controller_request(key, mac, target_server_ip, port))

        print('Adding rules to OpenFlow switch...')
        for x in controller_requests:
            r = requests.post('http://{0}:8080/controller/keys'.format(controller_host), json=x)
            if r.status_code != 201:
                print('Error adding rule to switch:')
                print(json.dumps(x))
                raise RuntimeError(r.json())
            time.sleep(1)
        print('Done.')


def calculate_requests_per_server_percentage(data):
    total_requests = sum([len(x) for x in data])
    return [len(x) / float(total_requests) * 100 for x in data]


def calculate_requests_per_server(data, only_successful=False):
    return [len([y for y in x if not only_successful or y.has_packets == only_successful]) for x in data]


def calculate_requests_per_second(server_data):
    data = [x for x in all_data if x and not x.startswith(';')]
    start = data[0].split(',')[0]
    end = data[-1].split(',')[0]

    # 3:51:11:801

    print(start)
    print(end)

    start = time.strptime(start, '%H:%M:%S:%f')
    end = time.strptime(end, '%H:%M:%S:%f')

    start = datetime.fromtimestamp(time.mktime(start))
    end = datetime.fromtimestamp(time.mktime(end))

    print(start)
    print(end)
    a = end-start

    print(a.__class__)
    print(a.seconds)

    print(end-start)

    return len(server_data) / float(a.seconds)


def calculate_packet_loss_per_server_per_request(per_server_data):
    """Extracts the packet loss (how many packets were lost) for each request
    and returns a list object where each items is a list containing a specific
    server's requests packet loss."""

    def _extract_packet_loss(_request):
        values = _request[PACKETS_COUNT_INDEX].split('/')
        total_packets = int(values[1])
        received_packets = int(values[0])
        if total_packets > 0:
            return total_packets - received_packets
        else:
            return -1

    return [[
            _extract_packet_loss(x) for x in server_data]
            for server_data in per_server_data
            ]


def calculate_int_based_cdf_for_per_server_data(per_server_data):

    def _calculate_server_cdf(_server_data):
        sums = {}
        packet_loss_requests_count = 0
        for x in _server_data:
            if x <= 0:
                continue
            packet_loss_requests_count += 1
            if x not in sums:
                sums[x] = 1
            else:
                sums[x] += 1

        as_list = sorted([(k, v) for k, v in sums.items()], key=lambda (k, v): k)

        c_organized = []
        c_sum = 0

        for i in range(len(as_list)):
            c_sum += as_list[i][1]
            c_organized.append((as_list[i][0], c_sum))

        as_list = c_organized

        as_list = [(k, v / float(packet_loss_requests_count) * float(100))
                   for k, v in as_list]

        print(as_list)

        return [x[0] for x in as_list], [x[1] for x in as_list]

    return [_calculate_server_cdf(x) for x in per_server_data]


def calculate_mbit_per_second(num_of_requests, duration_in_seconds, object_size_in_bytes):
    return num_of_requests * object_size_in_bytes * 8 / float(1000000) / float(duration_in_seconds)


def calculate_duration(data):
    return (data[-1].time - data[0].time).seconds

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    sdp = subparsers.add_parser('servers-distribution')
    sdp.set_defaults(which='sdp')
    sdp.add_argument('-d', '--data-file', type=str, required=True)
    sdp.add_argument('-atr', '--according-to-response', action='store_true')
    sdp.add_argument('-t', '--experiment-duration', type=int, default=None, required=False, help='Expriment duration in seconds (used for calculating MBit/s).')
    sdp.add_argument('-s', '--object-size', type=int, default=None, required=False, help='Object size in bytes (used for calculating MBit/s).')
    sdp.add_argument('-n', '--number-of-requests', type=int, required=False, default=None, help='Number of requests to calculate MBit/s for.')

    kdp = subparsers.add_parser('keys-distribution')
    kdp.set_defaults(which='kdp')
    kdp.add_argument('-d', '--data-file', type=str, required=True, help='Experiment data file name.')
    kdp.add_argument('-n', '--number-of-keys', type=int, required=False, help='Number of hot keys to extract.')
    kdp.add_argument('-c', '--controller', type=str, required=False, help='OpenFlow controller hostname.')
    kdp.add_argument('-g', '--group', action='store_true', default=False, help='Use an OpenFlow group for load-balancing.')
    kdp.add_argument('-atr', '--according-to-response', action='store_true')

    latency_parser = subparsers.add_parser('latency')
    latency_parser.set_defaults(which='latency')
    latency_parser.add_argument('-d', '--data-file', type=str, required=True, help='Experiment data file name.')
    latency_parser.add_argument('-a', '--average', action='store_true')
    latency_parser.add_argument('-atr', '--according-to-response', action='store_true')

    rps_parser = subparsers.add_parser('requests-per-second')
    rps_parser.set_defaults(which='rps')
    rps_parser.add_argument('-d', '--data-file', type=str, required=True, help='Experiment data file name.')
    rps_parser.add_argument('-atr', '--according-to-response', action='store_true')

    pl_parser = subparsers.add_parser('packet-loss')
    pl_parser.set_defaults(which='pl')
    pl_parser.add_argument('-d', '--data-file', type=str, required=True, help='Experiment data file name.')
    pl_parser.add_argument('-atr', '--according-to-response', action='store_true')

    args = parser.parse_args()

    all_data = load_data_from_file(args.data_file)
    raw_data = [x for x in all_data if len(x) and not x.startswith(';')]
    flat_requests = [RequestData(x.split(',')) for x in raw_data]

    meta_lines = [x for x in all_data if x.startswith(';')]
    if len(meta_lines) == 0:
        raise RuntimeError('Server line does not exist in data')

    number_of_servers = len(meta_lines[-1].replace(';', '').split(','))

    use_actual_server_indices = args.according_to_response if hasattr(args, 'according_to_response') else False

    print('Loading {0} [number-of-servers={1}]'.format(args.data_file, number_of_servers))
    data = load_multi_server_data(args.data_file, number_of_servers, False, use_actual_server_indices)
    percentage_per_server = calculate_requests_per_server_percentage(data)
    requests_per_server = calculate_requests_per_server(data)

    if args.which == 'sdp':
        successful_requests_per_server = calculate_requests_per_server(data, only_successful=True)
        print('# Percentage per server: {0}'.format(percentage_per_server))
        print('# Requests per server: {0}'.format(requests_per_server))
        print('# Successful requests per server: {}'.format(successful_requests_per_server))
        if args.object_size is not None:
            duration = args.experiment_duration or calculate_duration(flat_requests)
            print('# Experiment duration: {}'.format(duration))
            number_of_requests = args.number_of_requests
            if number_of_requests is None:
                number_of_requests = len(flat_requests)
            print('# MBps for {0} -> {1}'.format(number_of_requests, calculate_mbit_per_second(number_of_requests, duration, args.object_size)))
            print('# MBps per server: {0}'.format([math.trunc(calculate_mbit_per_second(x, duration, args.object_size)) for x in requests_per_server]))
            print('# Total requests per second: {}'.format(sum(requests_per_server) / float(duration)))
            print('# Requests per second per server: {}'.format([x / float(duration) for x in requests_per_server]))
    elif args.which == 'kdp':
        occurences = load_keys_occurences_from_file(args.data_file)
        keys = sorted([(k, v) for k, v in occurences.items()], key=lambda (k, v): v, reverse=True)
        if args.number_of_keys is None:
            keys_count = len(occurences)
        else:
            keys_count = args.number_of_keys
        hot_keys = keys[:keys_count]

        to_display = [(x[0], x[1], get_server_index_by_key(x[0], raw_data, use_actual_server_indices)) for x in hot_keys]
        print(json.dumps(to_display, indent=2))
        spread_hot_keys_and_add_openflow_rules(hot_keys, requests_per_server, args.data_file, args.controller, use_actual_server_indices, use_group=args.group)
    elif args.which == 'latency':
        all_data = load_data_from_file(args.data_file)
        data_per_server = split_data_per_server(all_data, number_of_servers, args.according_to_response)

        def _calculate_average_latency(_server_data):
            data = [float(x[2]) for x in _server_data if float(x[2]) >= 0]
            if len(data) == 0:
                return -1
            return sum(data) / float(len(data))

        average_latency_per_server = [_calculate_average_latency(x) for x in data_per_server]

        print('Average latency per server: {0}'.format(average_latency_per_server))
    elif args.which == 'rps':
        all_data = load_data_from_file(args.data_file)
        data_per_server = split_data_per_server(all_data, number_of_servers, args.according_to_response)
        requests_per_second = [calculate_requests_per_second(x) for x in data_per_server]

        print('Requests per second: {0}'.format(requests_per_second))
    elif args.which == 'pl':
        all_data = load_data_from_file(args.data_file)
        per_server_data = split_data_per_server(all_data, number_of_servers, args.according_to_response)

        all_data = [z for z in reduce(lambda x, y: x+y, per_server_data) if z.has_packets]
        total_packets = sum([x.total_packets_count for x in all_data])
        total_packets_lost = sum([x.packets_lost_count for x in all_data if x.packets_lost_count != -1])

        packet_loss_data = calculate_packet_loss_per_server_per_request(per_server_data)
        pl_per_server = [sum([a for a in x if a != -1]) for x in packet_loss_data]
        total_packets_per_server = [sum([y.total_packets_count for y in x]) for x in per_server_data]
        pl_percentage_per_server = []
        for i in range(len(pl_per_server)):
            pl_percentage_per_server.append(pl_per_server[i] / float(total_packets_per_server[i]) * float(100))

        print('Packet loss per server: {0}'.format(json.dumps(pl_per_server)))
        print('Total packets per server: {0}'.format(total_packets_per_server))
        print('Packet loss per server %: {0}'.format(json.dumps(pl_percentage_per_server)))
        print('Total packet loss: {0}/{1}'.format(total_packets_lost, total_packets))
        print('Total packet loss %: {0}'.format(total_packets_lost / float(total_packets) * float(100)))
