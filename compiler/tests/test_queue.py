import rt


def child(q_recv, q_send):
    s = q_recv.dequeue()
    q_send.enqueue(s + "pong")

def handler(event, context):
    qsize = event["qsize"]
    q1 = rt.create_queue(qsize)
    q2 = rt.create_queue(qsize)
    rt.spawn(child, (q1, q2))
    q1.enqueue("ping")
    return q2.dequeue()
