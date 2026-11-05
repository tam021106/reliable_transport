import sys
import socket
import argparse
from utils import PacketHeader, compute_checksum

TYPE_START = 0
TYPE_END = 1
TYPE_DATA = 2
TYPE_ACK = 3

HEADER_SIZE = 16


def make_ack(seq_num):
    pkt_header = PacketHeader(
        type=TYPE_ACK,
        seq_num=seq_num,
        length=0,
        checksum=0
    )
    pkt_header.checksum = compute_checksum(pkt_header / b"")
    return bytes(pkt_header / b"")


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


def receiver(ip, port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((ip, port))

    in_connection = False
    sender_addr = None
    expected_seq = 1
    buffer = {}
    received_chunks = []
    end_seq = None

    while True:
        raw, addr = s.recvfrom(2048)
        hdr, data = parse_and_verify(raw)
        if hdr is None:
            continue

        
        if hdr.type == TYPE_START:
            if not in_connection:
                in_connection = True
                sender_addr = addr
                expected_seq = 1
                s.sendto(make_ack(1), sender_addr)
            elif addr == sender_addr and expected_seq == 1 and not buffer:
                s.sendto(make_ack(1), sender_addr)
            continue

        if not in_connection or addr != sender_addr:
            continue

        
        if hdr.type == TYPE_DATA:
            if hdr.seq_num >= expected_seq + window_size:
                continue

            if hdr.seq_num >= expected_seq:
                buffer[hdr.seq_num] = data
                while expected_seq in buffer:
                    received_chunks.append(buffer.pop(expected_seq))
                    expected_seq += 1

            
            s.sendto(make_ack(hdr.seq_num), sender_addr)

            if end_seq is not None and expected_seq == end_seq:
                s.sendto(make_ack(end_seq + 1), sender_addr)
                break

        
        elif hdr.type == TYPE_END:
            end_seq = hdr.seq_num
            if expected_seq == end_seq:
                s.sendto(make_ack(end_seq + 1), sender_addr)
                break

    sys.stdout.buffer.write(b"".join(received_chunks))
    sys.stdout.buffer.flush()
    s.close()


def main():
    parser = argparse.ArgumentParser(
        description="RTP-opt Receiver (Individual ACK)"
    )
    parser.add_argument("ip", help="IP address to bind")
    parser.add_argument("port", type=int, help="Port to bind")
    parser.add_argument("window_size", type=int, help="Sliding window size")
    args = parser.parse_args()

    receiver(args.ip, args.port, args.window_size)


if __name__ == "__main__":
    main()