from mq_setup import Calculator

def callback(in_data):

    # with collected data, do something
    print(in_data)

    result = 'Calculated data'

    return result

if __name__ == '__main__':
    cal = Calculator()
    cal.block_receiving(callback)