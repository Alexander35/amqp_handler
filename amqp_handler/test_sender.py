from amqp_handler import AMQPHandler
import asyncio

def main():
    loop = asyncio.get_event_loop()
    
    AMQPH = AMQPHandler(loop)

    loop.run_until_complete(AMQPH.connect())
    print('connected')
    loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message!'))
    print('sended')
    loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message1'))
    loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message2'))
    asyncio.sleep(15)
    loop.run_until_complete(AMQPH.send('test_ex', 'test_queue', 'Test Message2'))

    # loop.run_until_complete(AMQPH.receive('test_ex', 'test_queue', test_msg_processor))
    loop.close()


if __name__ == '__main__':
	main()    