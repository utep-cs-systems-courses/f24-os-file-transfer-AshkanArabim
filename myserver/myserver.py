import os, sys, argparse, socket, struct


# copied straight out of my archiver and modified, because I'm lazy
class Extractor:
    def __init__(self, framer_type: str, archive_fd: int):
        self.archive_fd = archive_fd

        # define functions based on framer type
        self.framer_type = framer_type
        self.extract = self._define_extrat()

        if framer_type == "out":  # attributes specific to out-of-bound
            self.read_header = self._define_read_header()
        else:  # attributes specific to inbound
            self.remainder = b""
            self.r_idx = 0  # keeps track of how much of the remainder has been read
            self.read = self._define_read()
            self.read_till_terminator = self._define_read_till_terminator()

    def _define_read(self):
        # this allows me to do `read(1)` without worrying that reading only one byte is expensive
        # the only thing it does it to keep an internal buffer, and refill it when it's exhausted
        def read(n: int) -> bytes:  # returns buffer_of_length_n
            b = b""
            i = 0  # counts how many bytes we've read
            while i < n:
                # load 100 more bytes if buffer finished
                if self.r_idx + 1 >= len(self.remainder):
                    self.remainder = self.remainder[self.r_idx :] + bytes(
                        os.read(self.archive_fd, 100)
                    )
                    self.r_idx = 0

                # if nothing left to read...
                if len(self.remainder) <= self.r_idx:
                    return b

                b += bytes(self.remainder[self.r_idx : self.r_idx + 1])
                self.r_idx += 1
                i += 1

            # print("requested sequence:", len(b), b)
            # print("i:", i)
            return b

        return read

    def _define_read_till_terminator(self):
        def read_till_terminator(dest_fd: int = -1) -> bytes:
            b = b""
            c = b""
            escaped_zero = False
            c_next = self.read(1)
            while True:
                c = c_next
                c_next = self.read(1)
                if c == b"\x00" and c_next == b"\x00" and escaped_zero == False:
                    # 0x00 0x00 is an escaped sequence for 0x00
                    escaped_zero = True
                elif (c == b"\x00" and c_next == b"\x01" and escaped_zero == False) or len(c) == 0:
                    # 0x00 0x01 is our terminator
                    # if len(c) is 0, the source archive has ended
                    if dest_fd == -1:
                        return b
                    else:
                        os.write(dest_fd, b)
                        return b""
                else:
                    escaped_zero = False
                    b += c
                    if dest_fd != -1 and len(b) >= 100:
                        os.write(dest_fd, b)
                        b = b""  # reset buffer after writing

        return read_till_terminator

    def _define_read_header(self):
        def read_header() -> tuple[str, int]:
            # get filename
            filename_size = os.read(self.archive_fd, 64 // 8)
            if len(filename_size) == 0:
                return None, 0  # if no more headers left to read
            # filename_size = int(filename_size, 2)
            filename_size = struct.unpack("Q", filename_size)[0]
            filename = os.read(self.archive_fd, filename_size).decode("ascii")

            # get file size
            file_size = struct.unpack("Q", os.read(self.archive_fd, 64 // 8))[0]

            return filename, file_size

        return read_header

    def _define_extrat(self):
        if self.framer_type == "out":

            def extract():
                while True:
                    filename, file_size = self.read_header()

                    # if no headers left, break
                    if filename == None:
                        break

                    # make directories if they don't exist
                    os.makedirs("/".join(filename.split("/")[:-1]), exist_ok=True)

                    # create files with name
                    file_fd = os.open(filename, os.O_RDWR | os.O_CREAT | os.O_TRUNC)

                    # extract file contents
                    read_idx = 0
                    while read_idx < file_size:
                        buffer_size = min(100, file_size - read_idx)
                        read_idx += buffer_size
                        buffer = os.read(self.archive_fd, buffer_size)

                        os.write(file_fd, buffer)

            return extract
        else:

            def extract() -> None:
                while True:
                    filename = self.read_till_terminator()
                    filename = filename.decode("ascii")
                    
                    print(filename) # debug

                    # if filename is empty, there are no more files to extract
                    if len(filename) == 0:
                        return

                    # print(f"{filename} is not empty!") # debug
                    file_fd = os.open(filename, os.O_RDWR | os.O_CREAT | os.O_TRUNC)

                    self.read_till_terminator(file_fd)

            return extract


def receiveFiles(connAddr):
    # note: once thread comes here, it can't go back; it will die
    # TODO:
    sock, addr = connAddr
    print(f"Child: pid={os.getpid()} connected to client at {addr}")

    # receive stream and save files
    # note: received files will be located in the "received" folder
    extractor = Extractor("out", sock)
    extractor.extract()
    
    sock.shutdown(socket.SHUT_WR)
    sys.exit(0)
    

if __name__ == "__main__":
    # parser arguments
    parser = argparse.ArgumentParser(
        description="Receives out-of-band framed data, extracts it to disk"
    )
    parser.add_argument("-p", default=50001, help="port to listen on")
    parser.add_argument("-a", default="127.0.0.1", help="port to listen on")
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

        # while data still coming in, extract content until nothing left
        # ideally, pass file descriptor directly to framer extractor; it's already a package

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
