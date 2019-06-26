import pika
import functools

class ConnectionModule:
    connection_parameters = pika.ConnectionParameters('localhost')
    @classmethod
    def get_rabbitmq_connection(cls):
        pika_connection = pika.BlockingConnection(cls.connection_parameters)
        if pika_connection.is_closed:
            pika_connection = pika.BlockingConnection(cls.connection_parameters)
        return pika_connection
    @classmethod
    def get_rabbitmq_channel(cls):
        channel = cls.get_rabbitmq_connection().channel()
        if channel.is_closed:
            channel.open()
        return channel

collector_exchange_name = ''
calculator_queue_name = 'retreived_test'
calculator_exchange_name = 'calculated_ex_test'

class MQSettings(ConnectionModule):
    def __init__(self):
        self.channel = self.get_rabbitmq_channel()

        ## 3 modules: Collector, Calculator, Visualizer
        
        ## -- [Settings for Collector]
        # Collector - Producer: exchange
        self.__collector_exchange_para = {
            'exchange': collector_exchange_name,
            'exchange_type': 'direct',
        }
        # Collector - Producer: basic.publish parameters
        self.collector_publish_para = {
            'exchange': collector_exchange_name,    # Default exchange is direct
            'routing_key': calculator_queue_name,   # CalculatorQueuePara Queue name
            'properties':                           # Make message persistent (along with Calculator Queue declared durable, save task/message to cache)
                pika.BasicProperties(delivery_mode=2),
        }
        
        ## -- [Settings for Calculator]
        # Consumer: queue_declare parameters
        self.__calculator_queue_para = {
            'queue':calculator_queue_name,      # Queue name
            'durable': True,                    # Quene not delete evenif RabbitMQ server quits/crashes/restarts (along with Calculator Queue declared durable, save task/message to cache)
        }
        # Consumer: basic_consume parameters
        self.calculator_consume_para = {
            'queue': calculator_queue_name,
        }
        # Producer: exchange parameters
        self.__calculator_exchange_para = {
            'exchange': calculator_exchange_name,
            'exchange_type': 'fanout',
        }
        # Producer: basic.publish parameters
        self.calculator_publish_para = {
            'exchange': calculator_exchange_name,
            'routing_key': '',
        }

        ## -- Visualizer
        # Visualizer - Consumer: queue_declare parameters
        self.__visualizer_queue_para = {
            'queue': '', # To be set by program later (auto-generated)
            'exclusive': True,  # for each consumer, once the connection is closed, the queue should be deleted, using exclusive=True to implement this
        }

        # Visualizer - Consumer: basic_consume parameters
        self.visualizer_consume_para = {
            'queue': '', # To be set by program later (auto-generated)
        }

    # Exchange, queue declarations and bindings(if any)
    def collector_init(self):
        # self.channel.exchange_declare(**(self.__collector_exchange_para))
        self.channel.queue_declare(**(self.__calculator_queue_para))
    
    def calculator_init(self):
        self.channel.queue_declare(**(self.__calculator_queue_para)) # Queue belongs to CALC MODULE, message from FETCH MODULE
        self.channel.exchange_declare(**(self.__calculator_exchange_para))
    
    def visualizer_init(self):
        result = self.channel.queue_declare(**(self.__visualizer_queue_para))  # visualizer_queue_name set by rabbitmq automaticlly
        queue_name = result.method.queue
        self.__visualizer_queue_para['queue'] = queue_name  # set back visualizer_queue_name
        self.visualizer_consume_para['queue'] = queue_name  # set back visualizer_queue_name for consumer
        self.channel.queue_bind(exchange=self.__calculator_exchange_para['exchange'], queue=self.__visualizer_queue_para['queue'])

    # Keep channel open Decorator for channel operation callback functions
    def keep_channel_open(self, channel_operation_callback_func):
        @functools.wraps(channel_operation_callback_func)
        def wrap(self, *args, **kwargs):
            if self.channel.is_closed:
                self.channel.open()
            # print('reopening the channel %s' % name)
            return channel_operation_callback_func(self, *args, **kwargs)
        return wrap

    def close(self):
        self.channel.close()

class Collector(MQSettings):
    def __init__(self):
        super().__init__()      # Initiate MQSettings, get publish_para/consume_para and channel(alike cursor)
        self.collector_init()   # Initiate exchange/queue

    # keep_channel_open = super().keep_channel_open
    # @keep_channel_open
    def send_message(self, message):
        self.channel.basic_publish(body=message, **self.collector_publish_para)

class Calculator(MQSettings):
    def __init__(self):
        super().__init__()      # Initiate MQSettings, get publish_para/consume_para and channel(alike cursor)
        self.calculator_init()  # Initiate exchange/queue
    
    # keep_channel_open = super().keep_channel_open
    # @keep_channel_open
    def __send_message(self, message):
        self.channel.basic_publish(body=message, **self.calculator_publish_para)
    
    # @keep_channel_open
    def __block_receiving_callback_wrapper(self, channel, method, properties, body):
        # Do some callback(calculation)
        out_message = self.callback(in_data = body, **self.callback_args)
        if out_message is not None:
            self.__send_message(out_message)
        else:
            pass    # In pure calculation mode, calculator send nothing to visualizer
        channel.basic_ack(delivery_tag = method.delivery_tag) # Acknoledge the MQ that the message(data) was well recieved and callback(calculation) is done
    
    # @keep_channel_open
    def block_receiving(self, callback, callback_args={}):
        self.callback=callback
        self.callback_args = callback_args
        self.channel.basic_qos(prefetch_count=1) # set prefetch_count to 1, make sure each Calculator is busy doing something
        self.channel.basic_consume(on_message_callback=self.__block_receiving_callback_wrapper, **self.calculator_consume_para)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print('Calculator Aborted by User')

class Visualizer(MQSettings):
    def __init__(self):
        super().__init__()      # Initiate MQSettings, get publish_para/consume_para and channel(alike cursor)
        self.visualizer_init()  # Initiate exchange/queue
    
    # keep_channel_open = super().keep_channel_open
    # @keep_channel_open
    def block_receiving_one(self):
        try:
            for (_,_, message) in self.channel.consume(**self.visualizer_consume_para):
                self.channel.cancel()   # Redundant ?
                return message
        except KeyboardInterrupt:
            print('Visualizer Aborted by User')

def calculate(in_message):
    print(in_message)
    out_message = in_message + 'calculated'
    return out_message

# 
# class Example:
    
#     def __init__(self):
#         self.channelname = '12345'

#     def __keep_channel_open(func):
#         # @functools.wraps(func)
#         def wrap(self, *args, **kwargs):
#             name = self.channelname
#             print('reopening the channel %s' % name)
#             return func(self, *args, **kwargs)
#         return wrap

#     @__keep_channel_open
#     def channel_operation(self):
#         print('doing channel operations')

# class X(Example):

if __name__ == '__main__':
    col = Collector()
