import sys
import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass


ERROR = 'args err'

@dataclass
class Value:
    content: str
    expiry: datetime | None = None    

class RedisServer:
    db = dict()
    config = dict()

    def handle_config(self, args):
        if args[0].lower() == 'get':
            return self.config.get(args[1])        

    def handle_ping(self, args):
        return "PONG"


    def handle_echo(self, args):
        return args[0]


    def handle_set(self, args):
        if len(args) == 2:
            self.db[args[0]] = Value(content=args[1])
        elif len(args) == 4 and args[2].lower() == 'px':
            self.db[args[0]] = Value(content=args[1], expiry=datetime.now()+timedelta(milliseconds=int(args[3])))
        else:
            return ERROR

        return "OK"


    def handle_get(self, args):
        value: Value = self.db.get(args[0])
        
        if value:
            if value.expiry:
                if datetime.now() < value.expiry:
                    return value.content
            else:
                return value.content

        return None

    def handle_command(self, command, args):
        commands = {
            'command': self.handle_ping,
            'ping': self.handle_ping,
            'echo': self.handle_echo,
            'set': self.handle_set,
            'get': self.handle_get,
            'config': self.handle_config
        }


        command_func = commands[command.lower()]
        output = command_func(args)

        if output is None:
            formatted_output = '$-1\r\n'
        else:
            formatted_output = "+%s\r\n" % (output)
        
        return formatted_output


    async def handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while True:
            message = await reader.read(1024)
            if not message:
                break

            command_args = message.decode().rstrip('\r\n').split('\r\n')
            command = command_args[2]

            args = []
            for i in range(4, len(command_args), 2):
                args.append(command_args[i])

            r = self.handle_command(command, args)
            if r and writer:
                writer.write(r.encode())
                await writer.drain()

        print("Closing connection...")
        writer.close()
        await writer.wait_closed()

    async def main(self):
        server = await asyncio.start_server(self.handle_conn, "localhost", 6379)
        async with server:
            await server.serve_forever()
            print(f"Shutting down")


    def __init__(self, sys_args) -> None:
        for i, arg in enumerate(sys_args):
            if arg == '--dir':
                self.config['dir'] = sys_args[i+1]
            elif arg == '--dbfilename':
                self.config['dbfilename'] = sys_args[i+1]
            else:
                pass


if __name__ == "__main__":
    redis_server = RedisServer(sys.argv)
    asyncio.run(redis_server.main())
