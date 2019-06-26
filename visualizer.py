from mq_setup import Visualizer

if __name__ == '__main__':
    vis = Visualizer()
    message = vis.block_receiving_one()
    while message:
        print(message)
        message = vis.block_receiving_one()