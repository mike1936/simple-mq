from mq_setup import Collector

if __name__ == '__main__':
    col = Collector()
    
    data = 'Collected data'

    col.send_message(data)