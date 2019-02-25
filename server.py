import math
import os
import queue
import socket
import stat
import struct
import threading
import time
import random
from enum import Enum, unique

from client import Fragmentation, DEBUG_MODE_1, DEBUG_MODE_2

_MAX_DATA_LENGTH = 400
_INITIAL_PORT = 10000
_AVAILABLE_PORT_NUM = 10
_TIMEOUT_INTERVAL = 1
_RECV_WINDOW = 2000
port_stat = [0] * _AVAILABLE_PORT_NUM


@unique
class Status(Enum):
    slow_start = 0
    congestion_avoid = 1
    fast_recovery = 2


class SendUnit(object):
    def __init__(self, port):
        self.port = port                            # send and receive data through this port
        self.sock = socket.socket                   # using this socket to send and receive data
        self.dst_addr = 0                           # the destination ip address
        self.piece_num = 0                          # the total number of data pieces
        self.send_base = 0                          # send base
        self.send_window = queue.PriorityQueue()    # send window ( buffer the data sent )
        self.rwnd = _RECV_WINDOW                    # rwnd
        self.cwnd = 1                               # cwnd
        self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])           # the timer
        self.ssthresh = self.rwnd // 2              # ssthresh
        self.ack_count = 0                          # duplicated ack counter
        self.curr_stat = Status.slow_start          # current status ( congestion control )

    def send(self):
        """send file to remote host."""
        self.sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        send_buf = 10 * 1024 * 1024
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf)
        self.sock.bind(('', self.port))
        self.sock.settimeout(3)

        try:
            data, addr = self.sock.recvfrom(1024)

            int_size = struct.calcsize('I')
            (length,), filepath = struct.unpack('I', data[:int_size]), data[int_size:]
            filepath, = struct.unpack('%ds' % (length,), filepath)
        except socket.timeout as exc:
            self.sock.close()
            port_stat[self.port - _INITIAL_PORT] = 0
            print(exc)
            return

        file_size = os.stat(filepath)[stat.ST_SIZE]
        fp = open(filepath, 'rb')
        self.piece_num = int(math.ceil(file_size / _MAX_DATA_LENGTH))
        print(self.piece_num)

        self.dst_addr = addr

        self.sock.setblocking(False)
        next_seq_num = 0

        while True:
            if self.curr_stat == Status.slow_start and self.cwnd >= self.ssthresh:
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
        port_stat[self.port - _INITIAL_PORT] = 0

    def handle_ack(self, ack_data, next_seq_num):
        """handle the ack received

        :param ack_data: the ack data received
        :param next_seq_num: the number of the last sent data packet
        """
        ack_num, self.rwnd = struct.unpack('LI', ack_data)
        print('curr_stat =', self.curr_stat.name, 'thread:', threading.current_thread().ident)
        print('ACK:', ack_num, 'send base:', self.send_base, 'next seq_num:', next_seq_num)
        print('current cwnd:', self.cwnd, 'current rwnd:', self.rwnd)
        print('')

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
        """retransmit a data packet (SR).

        :param flag: 0 -> triggered by timeout; 1 -> fast retransmit
        """
        if flag == 0:
            self.curr_stat = Status.slow_start
            self.enter_slow_start()
        else:
            # flag == 1
            self.curr_stat = Status.fast_recovery

        if self.send_window.qsize() > 0 and self.rwnd > 0:
            try:
                print('retransmit')
                content = self.send_window.queue[0].data
                print('retransmit number:', self.send_window.queue[0].seq)
                self.sock.sendto(content, self.dst_addr)

                if self.timer.is_alive():
                    self.timer.cancel()
                self.timer = threading.Timer(_TIMEOUT_INTERVAL, self.retransmit, [0])
                self.timer.start()
            except socket.error as exc:
                print(exc)

        else:
            print('empty send window or rwnd is 0')

    def enter_slow_start(self):
        """initialize slow start status."""
        print('enter slow start')
        self.ssthresh = self.cwnd // 2
        self.cwnd = 1.0
        self.ack_count = 0


class RecvUnit(object):
    def __init__(self, port):
        self.port = port
        self.ack_send_interval = 0.3    # when self.dis_order == False, send ack per 500 ms
        self.client_socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self.client_socket.bind(('', self.port))
        self.dst_addr = ()
        self.top = 0                    # the first package not recv, top - 1 also means last byte read by the app layer
        self.last_byte_received = 0     # last_byte_received - 1 means last byte received by the transfer layer
        self.clock_using = False        # whether the clock is using
        self.send_ack_operation = threading.Timer(self.ack_send_interval, self.send_ack_per_interval)
        self.package_num = -1           # the whole file was divided into package_num packages
        self.have_received = []         # if the package i have received, we set have_received[i] = 1

    def send_ack(self):
        receive_window_size = _RECV_WINDOW - (self.last_byte_received - self.top)
        if DEBUG_MODE_2 and receive_window_size < 0:
            print('error: receive window size out of range: ', receive_window_size)
        first_package_not_recv = struct.pack('LI', self.top, receive_window_size)
        self.client_socket.sendto(first_package_not_recv, self.dst_addr)
        print('send ack', self.top, '.    lastByteReceived ', self.last_byte_received,
              '.    receive window size ', receive_window_size)

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
        port_stat[self.port - _INITIAL_PORT] = 0

    def recv_file(self, filename):
        self.client_socket.setblocking(False)

        q = queue.PriorityQueue()
        raw_data_queue = queue.Queue()  # this is the buffer of receiver

        fp = open(filename, 'wb')
        print('downloading ......')

        while True:
            try:
                raw_data, addr = self.client_socket.recvfrom(1024)

                if len(self.have_received) == 0:
                    self.dst_addr = addr

                if DEBUG_MODE_1 and (random.random() > 0.98):
                    continue

                # receive one byte package, it is because receive_window_size == 0
                if len(raw_data) == struct.calcsize('c') and struct.unpack('c', raw_data)[0] == 'w':
                    self.send_ack()
                    continue

                temp_size = struct.calcsize('L')
                (seq, ), data = struct.unpack('L', raw_data[:temp_size]), raw_data[temp_size*2:]
                print('seq', seq)
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


def transfer_file(this_port):
    print('transfer in thread', threading.current_thread().ident)
    unit = SendUnit(this_port)
    unit.send()


def receive_file(this_port, filename):
    print('receive in thread', threading.current_thread().ident)
    unit = RecvUnit(this_port)
    unit.recv_file(filename)


def main():
    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    sock.bind(('', _INITIAL_PORT))
    port_stat[0] = 1

    while True:
        data, addr = sock.recvfrom(1024)
        command = data.decode()

        next_port = -1
        for i in range(1, _AVAILABLE_PORT_NUM):
            if port_stat[i] != 1:
                next_port = i + _INITIAL_PORT
                break

        if next_port != -1:
            sock.sendto(struct.pack('L', next_port), addr)
            port_stat[next_port - _INITIAL_PORT] = 1

            if command == 'lget':
                # create a new thread to transfer file
                thread = threading.Thread(target=transfer_file, args=(next_port,))
                thread.start()
            elif command == 'lsend':
                print('lsend start')
                data, addr = sock.recvfrom(1024)
                thread = threading.Thread(target=receive_file, args=(next_port, data.decode()))
                thread.start()

        else:
            sock.sendto(struct.pack('L', 0), addr)


if __name__ == '__main__':
    main()
