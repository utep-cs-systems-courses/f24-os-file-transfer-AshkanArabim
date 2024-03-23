#! /usr/bin/env python3


import os, sys, argparse, socket, struct


class OutOfBandFramer:
    def __init__(self, filename: str, sock: socket.socket):
        # cmt: replace all instances of archive_fd
        
        # save archive fd
        self.sock = sock

        # fd = open file
        self.file_fd = os.open(filename, os.O_RDONLY)

        # save file name
        self.filename = filename

    def start_frame(self):
        # get the length of filename, format in 64 bits
        b = binary_format_64(len(self.filename))

        # write the filename size and filename
        self.sock.send(b)
        self.sock.send(self.filename.encode())

        # get the content size, format it
        b = binary_format_64(os.fstat(self.file_fd).st_size)

        # write file size
        self.sock.send(b)

    def write_frame(self):
        # write the whole file
        while True:
            buffer = os.read(self.file_fd, 100)
            l = len(buffer)
            if l == 0:
                break

            self.sock.send(buffer)

    def end_frame(self):
        pass

    def close(self):
        os.close(self.file_fd)


def binary_format_64(n: int):
    b = struct.pack("Q", n)
    return b


if __name__ == "__main__":
    # parser arguments
    parser = argparse.ArgumentParser(
        description="Receives out-of-band framed data, extracts it to disk"
    )
    parser.add_argument("-a", default="127.0.0.1", help="address to send to", type=str)
    parser.add_argument("-p", default=50001, help="port to send to", type=int)
    parser.add_argument("files", nargs='*')
    args = parser.parse_args()

    sock = None

    for af, socktype, proto, canonname, sa in socket.getaddrinfo(
        args.a, args.p, socket.AF_UNSPEC, socket.SOCK_STREAM
    ):
        try:
            print("creating sock: af=%d, type=%d, proto=%d" % (af, socktype, proto))
            sock = socket.socket(af, socktype, proto)
        except socket.error as msg:
            print(" error: %s" % msg)
            sock = None
            continue
        try:
            print(" attempting to connect to %s" % repr(sa))
            sock.connect(sa)
        except socket.error as msg:
            print(" error: %s" % msg)
            sock.close()
            sock = None
            continue
        break
    
    if sock is None:
        print('could not open socket')
        sys.exit(1)
    
    for filename in args.files:
        framer = OutOfBandFramer(filename, sock)

        framer.start_frame()
        framer.write_frame()
        framer.end_frame()
        framer.close()
