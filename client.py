import socket
import struct
import queue
import sys
import threading
import random
import time
import math
import os
import stat


_MAX_DATA_LENGTH = 400
_TIMEOUT_INTERVAL = 1

DEBUG_MODE_1 = False
# mode 1 is used to drop package randomly, we can use it to check reliable transmission, congestion control
DEBUG_MODE_2 = False
# mode 2 is used to check flow control, if the raw data received is more than receive window, we print an error
SERVER_IP_TYPE = socket.AF_INET
SERVER_NAME = ''
SERVER_NAME_V6 = '2001:250:3002:4410:7854:6f74:a77c:b4f1'
SERVER_PORT = 10000
RECV_WINDOW = 2000
# we can change the window size to optimize the transmission, in our testing, we change it to 1000 with bad performance
# but when we set it to 200, it will be good (we also test for window =2, 10, 100 and so on, but find 200 best)
# we should set it to 1, for checking the flow control


def check_ip(ip):
    try:
        socket.inet_pton(socket.AF_INET, ip)
    except socket.error:
        return False
    return True


def check_ip_v6(ip):
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:
        return False
    return True


class Status(object):
    slow_start = 0
    congestion_avoid = 1
    fast_recovery = 2

    def __init__(self):
        pass

    @staticmethod
    def get_name(_status):
        if _status == 0:
            return 'slow_start'
        elif _status == 1:
            return 'congestion_avoid'
        else:
            return 'fast_recovery'


class Fragmentation(object):
    def __init__(self, seq, data):
        self.seq = seq
        self.data = data

    def __lt__(self, other):
        return self.seq < other.seq


class SendUnit(object):
    def __init__(self, port):
        self.port = port
        self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])
        self.sock = socket.socket(SERVER_IP_TYPE, socket.SOCK_DGRAM)
        self.sock.bind(('', 22222))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
        self.dst_addr = (SERVER_NAME, self.port)
        self.piece_num = 0
        self.send_base = 0
        self.send_window = queue.PriorityQueue()
        self.rwnd = RECV_WINDOW
        self.cwnd = 1
        self.ssthresh = self.rwnd // 2
        self.ack_count = 0
        self.curr_stat = Status.slow_start

    def send_file(self, filepath):
        file_size = os.stat(filepath)[stat.ST_SIZE]
        fp = open(filepath, 'rb')
        self.piece_num = int(math.ceil(file_size / _MAX_DATA_LENGTH))
        print(self.piece_num)

        self.sock.setblocking(False)
        next_seq_num = 0

        while True:
            if self.cwnd >= self.ssthresh:
                self.curr_stat = Status.congestion_avoid

            try:
                if self.send_base >= self.piece_num:
                    # transfer completed
                    self.timer.cancel()
                    break

                data, addr = self.sock.recvfrom(1024)
                if not data:
                    print('connection closed')
                    self.sock.close()
                    break
                else:
                    self.handle_ack(data, next_seq_num)

            except socket.error:
                # send data
                if next_seq_num - self.send_base <= min(self.rwnd, self.cwnd):
                    piece = fp.read(_MAX_DATA_LENGTH)
                    # if file reading is over
                    if not piece:
                        break
                    content = struct.pack('2L%ds' % (len(piece),), next_seq_num, self.piece_num, piece)
                    self.send_window.put(Fragmentation(next_seq_num, content))
                    next_seq_num += 1

                    if not self.timer.is_alive():
                        self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])
                        self.timer.start()

                    self.sock.sendto(content, self.dst_addr)

                if self.rwnd == 0:
                    # wait for rwnd
                    time.sleep(0.5)
                    self.sock.sendto(struct.pack('c', 'w'.encode('utf-8')), self.dst_addr)
                    print('wait for rwnd')

        print('transfer completed!')
        self.sock.close()

    def handle_ack(self, ack_data, next_seq_num):
        ack_num, self.rwnd = struct.unpack('LI', ack_data)
        print('curr_stat =', Status.get_name(self.curr_stat), 'thread:', threading.current_thread().ident)
        print(ack_num, self.send_base, next_seq_num)
        # print(self.cwnd, self.rwnd)

        if ack_num > self.send_base:
            acked = ack_num - self.send_base
            self.send_base = ack_num

            for i in range(0, acked):
                # move the window
                self.send_window.get()

            if self.timer.is_alive():
                self.timer.cancel()

            if self.send_base < next_seq_num:
                self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])
                self.timer.start()

            self.ack_count = 0

            if self.curr_stat == Status.slow_start:
                self.cwnd += acked
            elif self.curr_stat == Status.congestion_avoid:
                self.cwnd += float(acked) * (acked / self.cwnd)
            else:
                # curr_stat == Status.fast_recovery
                self.cwnd = self.ssthresh

        else:
            if self.curr_stat == Status.fast_recovery:
                self.cwnd += 1
            else:
                self.ack_count += 1
                if self.ack_count == 3:
                    self.ssthresh = self.cwnd // 2
                    self.cwnd = self.ssthresh + 3

                    # fast retransmit
                    print('fast retransmit')
                    self.ack_count = 0
                    self.retransmit(1)

    def retransmit(self, flag):
        if flag == 0:
            self.curr_stat = Status.slow_start
            self.begin_slow_start()
        else:
            # flag == 1
            self.curr_stat = Status.fast_recovery

        if self.send_window.qsize() > 0 and self.rwnd > 0:
            print('retransmit')
            content = self.send_window.queue[0].data
            print('retransmit number:', self.send_window.queue[0].seq)
            try:
                self.sock.sendto(content, self.dst_addr)
            except socket.error:
                print('more transfer useless')
                return

            if self.timer.is_alive():
                self.timer.cancel()
            self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])
            self.timer.start()
        else:
            print('empty send window or rwnd is 0')

    def begin_slow_start(self):
        print('enter slow start')
        self.ssthresh = self.cwnd // 2
        self.cwnd = 1.0
        self.ack_count = 0


class RecvUnit(object):

    def __init__(self, transport):
        self.ack_send_interval = 0.3   # when self.dis_order == False, send ack per 500 ms
        self.client_socket = socket.socket(SERVER_IP_TYPE, socket.SOCK_DGRAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)
        self.transport = transport     # the port to send ack
        self.top = 0                   # the first package not recv, top - 1 also means last byte read by the app layer
        self.last_byte_received = 0    # last_byte_received - 1 means last byte received by the transfer layer
        self.clock_using = False       # whether the clock is using
        self.send_ack_operation = threading.Timer(self.ack_send_interval, self.send_ack_per_interval)
        self.package_num = -1          # the whole file was divided into package_num packages
        self.have_received = []        # if the package i have received, we set have_received[i] = 1

    def send_ack(self):
        receive_window_size = RECV_WINDOW - (self.last_byte_received - self.top)
        if receive_window_size < 0:
            print('error: receive window size out of range: ', receive_window_size)
            receive_window_size = 0
        first_package_not_recv = struct.pack('LI', self.top, receive_window_size)
        self.client_socket.sendto(first_package_not_recv, (SERVER_NAME, self.transport))
        print(' ')
        print('send ack', self.top, '.    lastByteReceived ', self.last_byte_received, '.    receive window size ',
              receive_window_size)

    def send_ack_per_interval(self):
        self.send_ack()
        self.clock_using = False

    def start_clock(self):
        self.send_ack_operation = threading.Timer(self.ack_send_interval, self.send_ack_per_interval)
        self.clock_using = True
        self.send_ack_operation.start()

    def stop_clock(self):
        self.send_ack_operation.cancel()

    def close_socket(self):
        self.client_socket.close()

    def recv_file(self, filename):
        file_head = struct.pack('I%ds' % (len(filename),), len(filename), filename.encode('utf-8'))
        self.client_socket.sendto(file_head, (SERVER_NAME, self.transport))
        self.client_socket.setblocking(False)

        q = queue.PriorityQueue()
        raw_data_queue = queue.Queue()  # this is the buffer of receiver

        fp = open(filename, 'wb')
        print('downloading ......')

        while True:
            try:
                raw_data, addr = self.client_socket.recvfrom(1024)

                if DEBUG_MODE_1 and (random.random() > 0.8):
                    continue

                # receive one byte package, it is because receive_window_size == 0
                if len(raw_data) == struct.calcsize('c') and raw_data.decode() == 'w':
                    self.stop_clock()
                    self.send_ack()
                    continue

                temp_size = struct.calcsize('L')
                if len(raw_data) < 8:
                    print(raw_data.decode())
                (seq, ), data = struct.unpack('L', raw_data[:temp_size]), raw_data[temp_size*2:]
                if len(self.have_received) == 0:
                    self.package_num, = struct.unpack('L', raw_data[temp_size:temp_size*2])
                    self.have_received = [0]*self.package_num

                if self.package_num > seq >= self.top and self.have_received[seq] == 0:
                    self.have_received[seq] = 1
                    raw_data_queue.put(raw_data)
                    # print 'receive package: ', seq
                    self.last_byte_received += 1

            except socket.error:
                if not raw_data_queue.empty():
                    raw_data = raw_data_queue.get()
                    temp_size = struct.calcsize('L')
                    (seq, ), data = struct.unpack('L', raw_data[:temp_size]), raw_data[temp_size*2:]

                    if seq == self.top:
                        if not self.clock_using:
                            self.start_clock()

                        fp.write(data)
                        self.top += 1
                        while not q.empty() and q.queue[0].seq == self.top:
                            fp.write(q.get().data)
                            self.top += 1

                    elif seq > self.top:
                        self.stop_clock()
                        self.send_ack()
                        self.start_clock()

                        q.put(Fragmentation(seq, data))

                    else:
                        print('error: package received added to raw data queue')

                if self.package_num != -1 and self.top == self.package_num:
                    break

        print('transfer finished')
        close_socket_operation = threading.Timer(self.ack_send_interval*2, self.close_socket)
        close_socket_operation.start()


def main():
    if len(sys.argv) != 4:
        print('Correct Usage: client.py lget/lsend host filename')
        exit(1)
    command = sys.argv[1]
    hostname = sys.argv[2]
    filename = sys.argv[3]

    if command != 'lget' and command != 'lsend':
        print('Command error: command should be lget or lsend')
        return

    if not check_ip_v6(hostname):
        print('Ip error: you should input correct ipv6 addr, and ipv4 address is not allowed')
        print('Because we find that ipv4 packet is less than 65536, then the buffer will be constraint')
        return

    global SERVER_NAME
    SERVER_NAME = hostname
    global SERVER_IP_TYPE
    if check_ip(hostname):
        SERVER_IP_TYPE = socket.AF_INET
    else:
        SERVER_IP_TYPE = socket.AF_INET6

    try:
        client_socket = socket.socket(SERVER_IP_TYPE, socket.SOCK_DGRAM)
        _command = struct.pack('%ds' % (len(command),), command.encode('utf-8'))
        client_socket.sendto(_command, (SERVER_NAME, SERVER_PORT))
        client_socket.settimeout(3)
        data, addr = client_socket.recvfrom(1024)
    except socket.timeout as e:
        print('Connect error: ', e)
        return e

    transport, = struct.unpack('L', data)

    if transport <= SERVER_PORT:
        print('Port used out')
    else:
        if command == 'lget':
            print('connection set up, ', filename, ' will be received on ', transport)
            client_socket.close()
            rec = RecvUnit(transport)
            rec.recv_file(filename)
        else:
            print('connection set up, ', filename, ' will be send on ', transport)
            _filename = struct.pack('%ds' % (len(filename),), filename.encode('utf-8'))
            client_socket.sendto(_filename, (SERVER_NAME, SERVER_PORT))
            client_socket.close()
            snd = SendUnit(transport)
            snd.send_file(filename)


if __name__ == '__main__':
    main()
