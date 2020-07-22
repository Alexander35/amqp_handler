import asyncio
from aio_pika import connect_robust, Message
import time
from aio_pika.exceptions import AMQPConnectionError, ConnectionClosed, ChannelClosed

class AMQPHandler():
    def __init__(self, asyncio_loop):
        self.loop = asyncio_loop

    async def connect(self, amqp_connect_string="amqp://guest:guest@127.0.0.1:5672/"):
        try:
            self.amqp_connect_string = amqp_connect_string
            self.connection = await connect_robust(self.amqp_connect_string)
            self.channel = await self.connection.channel()
        except Exception as exc:
            time.sleep(5)
            await self.connect(self.amqp_connect_string)

    async def close(self):
        await self.connection.close()

    async def send(self, amqp_exchange, amqp_queue, msg):
        routing_key = amqp_queue
        exchange = await self.channel.declare_exchange(amqp_exchange, auto_delete=False)
        queue = await self.channel.declare_queue(amqp_queue, auto_delete=False)
        await queue.bind(exchange, routing_key)
        await exchange.publish(
                Message(
                        bytes(msg, 'utf-8')
                    ),
                    routing_key               
            )

    async def receive(self, amqp_exchange, amqp_queue, msg_proc_func=None, redirect_to_exchange=None, redirect_to_queue=None):
        routing_key = amqp_queue
        exchange = await self.channel.declare_exchange(amqp_exchange, auto_delete=False)
        queue = await self.channel.declare_queue(amqp_queue, auto_delete=False)        
        await queue.bind(exchange, routing_key)

        async for message in queue:
            proc_status, proc_result = msg_proc_func(message.body)

            if((redirect_to_exchange != None) and (redirect_to_queue != None)):
                await self.send(redirect_to_exchange, redirect_to_queue, proc_result)

            if proc_status == True:
                message.ack()

def test_msg_processor(msg):
    print('{}!!!!'.format(msg) ) 
    return True, msg.decode('utf-8')

def main():
    loop = asyncio.get_event_loop()
    
    AMQPH = AMQPHandler(loop)

    loop.run_until_complete(AMQPH.connect())
    # loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message!'))
    loop.close()

if __name__ == "__main__":
    main()