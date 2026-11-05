import sys
import socket
import time
import argparse
from utils import PacketHeader, compute_checksum

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3

MAX_MTU = 1472
HEADER_SIZE = 16
MAX_PAYLOAD_SIZE = MAX_MTU - HEADER_SIZE  
TIMEOUT = 0.5  


def make_packet(pkt_type, seq_num, data=b""):
    pkt_header = PacketHeader(
        type=pkt_type,
        seq_num=seq_num,
        length=len(data),
        checksum=0
    )
    pkt_header.checksum = compute_checksum(pkt_header / data)
    return bytes(pkt_header / data)


def parse_and_verify(raw_bytes):
    if len(raw_bytes) < HEADER_SIZE:
        return None, None
    pkt_header = PacketHeader(raw_bytes[:HEADER_SIZE])
    data = raw_bytes[HEADER_SIZE:HEADER_SIZE + pkt_header.length]
    pkt_checksum = pkt_header.checksum
    pkt_header.checksum = 0
    if compute_checksum(pkt_header / data) != pkt_checksum:
        return None, None
    return pkt_header, data


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(False)
    dest = (receiver_ip, receiver_port)

    message = sys.stdin.buffer.read()
    chunks = [
        message[i:i + MAX_PAYLOAD_SIZE]
        for i in range(0, len(message), MAX_PAYLOAD_SIZE)
    ]
    num_packets = len(chunks)

    data_packets = {}
    for idx, chunk in enumerate(chunks, start=1):
        data_packets[idx] = make_packet(TYPE_DATA, idx, chunk)

    
    start_pkt = make_packet(TYPE_START, 0)
    s.sendto(start_pkt, dest)
    timer_start = time.time()

    while True:
        try:
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_and_verify(raw)
            if hdr and hdr.type == TYPE_ACK and hdr.seq_num in (0, 1):
                break
        except BlockingIOError:
            pass

        if time.time() - timer_start >= TIMEOUT:
            s.sendto(start_pkt, dest)
            timer_start = time.time()

    
    base = 1
    next_to_send = 1
    acked = set()

    window_end = min(base + window_size, num_packets + 1)
    while next_to_send < window_end:
        s.sendto(data_packets[next_to_send], dest)
        next_to_send += 1
    timer_start = time.time()

    while base <= num_packets:
        try:
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_and_verify(raw)
            if hdr and hdr.type == TYPE_ACK:
                if base <= hdr.seq_num < base + window_size:
                    acked.add(hdr.seq_num)

                    
                    if hdr.seq_num == base:
                        while base in acked:
                            base += 1
                        window_end = min(base + window_size, num_packets + 1)
                        while next_to_send < window_end:
                            s.sendto(data_packets[next_to_send], dest)
                            next_to_send += 1
                        
                        timer_start = time.time()
        except BlockingIOError:
            pass

        
        if base <= num_packets and (time.time() - timer_start >= TIMEOUT):
            for seq in range(base, min(base + window_size, num_packets + 1)):
                if seq not in acked:
                    s.sendto(data_packets[seq], dest)
            timer_start = time.time()

    
    end_seq = num_packets + 1
    end_pkt = make_packet(TYPE_END, end_seq)
    s.sendto(end_pkt, dest)
    end_timer = time.time()

    while time.time() - end_timer < TIMEOUT:
        try:
            raw, _ = s.recvfrom(2048)
            hdr, _ = parse_and_verify(raw)
            if hdr and hdr.type == TYPE_ACK and hdr.seq_num in (end_seq, end_seq + 1):
                break
        except BlockingIOError:
            pass

    s.close()


def main():
    parser = argparse.ArgumentParser(
        description="RTP-opt Sender (Selective Repeat / Individual ACK)"
    )
    parser.add_argument("receiver_ip", help="IP address of the receiver")
    parser.add_argument("receiver_port", type=int, help="Port of the receiver")
    parser.add_argument("window_size", type=int, help="Sliding window size")
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()