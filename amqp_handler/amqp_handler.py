import asyncio
from aio_pika import connect_robust, Message

class AMQPHandler():
    def __init__(self, asyncio_loop):
        self.loop = asyncio_loop

    async def connect(self, amqp_connect_string="amqp://localhost:5672"):
        self.connection = await connect_robust(amqp_connect_string)
        self.channel = await self.connection.channel()

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

    async def receive(self, amqp_exchange, amqp_queue, msg_proc_func=None):
        routing_key = amqp_queue
        exchange = await self.channel.declare_exchange(amqp_exchange, auto_delete=False)
        queue = await self.channel.declare_queue(amqp_queue, auto_delete=False)        
        await queue.bind(exchange, routing_key)

        async for message in queue:
            proc_status = msg_proc_func(message.body, self.loop)
            
            if proc_status == True:
                message.ack()

def test_msg_processor(msg, loop):
    print('{}!!!!'.format(msg) ) 
    return True

def main():
    loop = asyncio.get_event_loop()
    
    AMQPH = AMQPHandler(loop)

    loop.run_until_complete(AMQPH.connect())
    # loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message!'))

    loop.run_until_complete(AMQPH.receive('test_ex', 'test_queue', test_msg_processor))
    loop.close()

if __name__ == "__main__":
    main()