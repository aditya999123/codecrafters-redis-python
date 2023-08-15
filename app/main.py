# Uncomment this to pass the first stage
import socket
import threading

db = dict()

ERROR = 'args err'

def handle_ping(args):
    return "PONG"


def handle_echo(args):
    return args[0]


def handle_set(args):
    if len(args) != 2:
        return ERROR

    db[args[0]] = args[1]
    return "OK"

def handle_get(args):
    return db[args[0]]

commands = {
    'command': handle_ping,
    'ping': handle_ping,
    'echo': handle_echo,
    'set': handle_set,
    'get': handle_get
}


def handle_command(command, args):
    command_func = commands[command.lower()]
    output = command_func(args)

    formatted_output = "+%s\r\n" % (output)
    return formatted_output


def handle_conn(conn):
    with conn:
        while True:
            message = conn.recv(1024)
            if not message:
                break

            command_args = message.decode().rstrip('\r\n').split('\r\n')
            command = command_args[2]

            args = []
            for i in range(4, len(command_args), 2):
                args.append(command_args[i])

            r = handle_command(command, args)

            conn.send(r.encode())


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
