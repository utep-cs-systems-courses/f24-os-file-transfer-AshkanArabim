#! /usr/bin/env python3


import os, sys, argparse, socket, struct, time


# copied straight out of my archiver and modified, because I'm lazy
class OutOfBandExtractor:
    def __init__(self, listenSock: socket.socket):
        self.sock = listenSock

    def read_header(self) -> tuple[str, int]:
        # get filename
        filename_size = self.sock.recv(64 // 8)
        if len(filename_size) == 0:
            return None, 0  # if no more headers left to read
        filename_size = struct.unpack("Q", filename_size)[0]
        filename = self.sock.recv(filename_size).decode("ascii")

        # get file size
        file_size = struct.unpack("Q", self.sock.recv(64 // 8))[0]

        return filename, file_size
    
    def extract(self):
        while True:
            filename, file_size = self.read_header()

            # if no headers left, break
            if filename == None:
                break

            # make directories if they don't exist
            dirname = "/".join(filename.split("/")[:-1])
            if len(dirname) > 0:
                os.makedirs(dirname, exist_ok=True)

            # create files with name
            file_fd = os.open(filename, os.O_RDWR | os.O_CREAT | os.O_TRUNC)

            # extract file contents
            read_idx = 0
            while read_idx < file_size:
                buffer_size = min(100, file_size - read_idx)
                read_idx += buffer_size
                buffer = self.sock.recv(buffer_size)

                os.write(file_fd, buffer)


def receiveFiles(connAddr):
    # note: once thread comes here, it can't go back; it will die
    
    # adding sleep timer to demonstrate ability to talk to multiple clients
    time.sleep(2)
    
    sock, addr = connAddr
    print(f"Child: pid={os.getpid()} connected to client at {addr}")

    # receive stream and save files
    extractor = OutOfBandExtractor(sock)
    extractor.extract()
    
    print(f"saved files for process {os.getpid()}")
    
    sock.shutdown(socket.SHUT_WR)
    sys.exit(0)
    

if __name__ == "__main__":
    # parser arguments
    parser = argparse.ArgumentParser(
        description="Receives out-of-band framed data, extracts it to disk"
    )
    parser.add_argument("-p", default=50001, help="port to listen on")
    parser.add_argument("-a", default="127.0.0.1", help="address to listen on")
    args = parser.parse_args()

    pidAddr = {}

    # make socket, listen on it
    listenSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listenSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listenSock.settimeout(5)
    listenSock.bind((args.a, args.p))
    listenSock.listen(1)

    while True:
        while pidAddr.keys():
            # Check for exited children (zombies).  If none, don't block (hang)
            if waitResult := os.waitid(os.P_ALL, 0, os.WNOHANG | os.WEXITED):
                zPid, zStatus = waitResult.si_pid, waitResult.si_status
                print(
                    f"""zombie reaped:
                \tpid={zPid}, status={zStatus}
                \twas connected to {pidAddr[zPid]}"""
                )
                del pidAddr[zPid]
            else:
                break  # no zombies; break from loop
        print(f"Currently {len(pidAddr.keys())} clients")

        try:
            connSockAddr = listenSock.accept()  # accept connection from a new client
        except TimeoutError:
            connSockAddr = None

        if connSockAddr is None:
            continue

        forkResult = os.fork()  # fork child for this client
        if forkResult == 0:  # child
            listenSock.close()  # child doesn't need listenSock
            receiveFiles(connSockAddr)
        # parent
        sock, addr = connSockAddr
        sock.close()  # parent closes its connection to client
        pidAddr[forkResult] = addr
        print(f"spawned off child with pid = {forkResult} at addr {addr}")
