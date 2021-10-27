import socket
import struct
from datetime import datetime, timedelta

from BellmanFord import BellmanFord

BUF_SZ = 4096  # tcp receive buffer size
PUBLISHER_ADDRESS = ('127.0.0.1', 21212)
MICROS_PER_SECOND = 1_000_000
NUM_CURRENCY = 7
DEFAULT_RATE = 0
RATE_LIFETIME = 1.5


class Subscriber:
    def __init__(self):
        self.publisher_address = PUBLISHER_ADDRESS
        self.publisher_ip = PUBLISHER_ADDRESS[0]
        self.publisher_port = int(PUBLISHER_ADDRESS[1])

        self.sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.listener, self.listener_address = self.start_a_listener()
        self.listener_ip, self.listener_port = self.listener_address

        self.rates_matrix = [[]]
        self.rates_matrix = [[(DEFAULT_RATE, datetime(2020, 1, 1))
                              for c in range(NUM_CURRENCY)] for r in range(NUM_CURRENCY)]

        self.currencies = ('USD', 'GBP', 'EUR', 'AUD', 'JPY', 'CHF', 'CAD')

        self.currencies_to_index = {}
        i = 0
        for currency in self.currencies:
            self.currencies_to_index[currency] = i
            i += 1

    def subscribe(self):
        print("Sending Subscription Message to the Publisher")
        # serialize the listener address
        data = self.serialize_address(self.listener_ip, self.listener_port)
        print("sending bytes: ", data)
        send = self.sender.sendto(data, self.publisher_address)

    def read(self):
        print('---------------------------------------')
        print('Listening to Publisher:  {}'.format(self.publisher_address))
        while True:
            data = self.receive(self.listener)
            byte_len = len(data)
            start = 0
            end = 32
            while end <= byte_len:
                self.unmarshal_message(data, start)
                start = end
                end = end + 32
            opportunity = BellmanFord(self.currencies)
            # for row in self.rates_matrix:
            #     print(row)
            opportunity.arbitrage(self.rates_matrix)

    def unmarshal_message(self, data: bytes, start):
        time = self.deserialize_utcdatetime(data[start:start + 8])
        c1 = self.bytes_to_string(data[start + 8: start + 11])
        c2 = self.bytes_to_string(data[start + 11:start + 14])
        price = self.deserialize_price(data[start + 14:start + 22])
        print("Datetime: ", time, "Currency: ", c1, "/", c2,
              " Price: ", price, "\n")

        # price = c2/c1 , cost of the vertex:  -log(price)
        # direction of the vertex:  c1 -> c2
        self.add_rate(c1, c2, price, time)  # c1 -> c1

    def add_rate(self, c1, c2, rate, publish_time: datetime):
        # index of c1& c2: c2 c1
        # this is to adapt  to bellman ford algorithm
        i1 = self.c_to_i(c1)
        i2 = self.c_to_i(c2)
        self.rates_matrix[i2][i1] = (rate, publish_time)
        self.rates_matrix[i1][i2] = (1/rate, publish_time)

    def c_to_i(self, currency: str) -> int:
        return self.currencies_to_index[currency]

    @staticmethod
    def bytes_to_string(data: bytes) -> str:
        return data.decode('UTF-8')

    @staticmethod
    def start_a_listener():
        """
            Start a socket bound to 'localhost' at a random port.

            :return: listening socket and its address
        """
        print("initializing a listener")
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print(type(listener))
        listener.bind(('localhost', 0))  # use any free socket
        return listener, listener.getsockname()

    @staticmethod
    def receive(listener, buffer_size = BUF_SZ):
        """
        Receives an incoming message from the given socket.

        :param listener: socket to recv
        :param buffer_size: buffer size of socket.recv
        :return: the de-serialized data received from publisher
        :raises: whatever socket.recv or pickle.loads could raise
        """
        print('\nblocking, waiting to receive message')
        data = listener.recv(BUF_SZ)
        if not data:
            raise ValueError('socket closed')
        print('received {} bytes'.format(len(data)))
        return data

    @staticmethod
    def deserialize_price(data: bytes, little_endian=True) -> float:
        if little_endian:
            return struct.unpack('<d', data)[0]
        return struct.unpack('>d', data)[0]

    @staticmethod
    def deserialize_utcdatetime(data: bytes) -> datetime:
        micros = int.from_bytes(data, 'big')
        return datetime(1970, 1, 1) + timedelta(microseconds=micros)

    @staticmethod
    def serialize_address(ip, port) -> bytes:
        ip_bytes = socket.inet_aton(ip)
        port_bytes = port.to_bytes(2, 'big')
        return ip_bytes + port_bytes


# if __name__ == '__main__':
#     print("testing lab3 subscriber")
#     lab3 = Lab3()
#     lab3.subscribe()
#     lab3.read()
#     exit(1)
