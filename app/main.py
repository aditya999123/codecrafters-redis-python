# Uncomment this to pass the first stage
import socket
import threading

PONG = "+PONG\r\n"



def handle_conn(conn):
    with conn:
        while True:
            message = conn.recv(1024)
            if not message:
                break
            print("this is the message %s" %(message.decode()))
            conn.send(PONG.encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    threads = []

    while True:
        conn, _ = server_socket.accept()  # wait for client
        t = threading.Thread(target=handle_conn, args=(conn,))
        threads.append(t)

        t.start()


if __name__ == "__main__":
    main()
