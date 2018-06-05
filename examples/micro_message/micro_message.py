"""
Microbenchmark for message passing: how long does it to send a message over a queue to another task.

The `handler` task sends messages to a queue while the child task "busy-waits" for the message (i.e., doesn't
terminate) and sends an ack back.

Parameters:
    - message_size: size of message to send (MB).
    - num_messages: number of messages to send.
"""
import time

import rt


def child(ping, pong):
    while True:
        msg_id, data = ping.dequeue()
        if msg_id is None:
            break
        print("recv: {}, {}, {}".format(msg_id, time.time(), len(data)))
        pong.enqueue(None)


def handler(event, _):
    message_size = int(event["message_size"] * 2**20)
    num_messages = event["num_messages"]

    print("Message size = {}".format(message_size))

    ping = rt.create_queue(max_size=1)
    pong = rt.create_queue(max_size=1)

    fut = rt.spawn(child, (ping, pong))

    for i in range(num_messages):
        print("send: {}, {}".format(i, time.time()))
        ping.enqueue([i, "x" * message_size])
        pong.dequeue()
        time.sleep(1)

    ping.enqueue((None, None))
    fut.wait()
