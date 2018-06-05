from rt.storage import s3


def handler(event, _):
    s3.put(event["bucket"], event["key"], event["value"].encode("utf-8"), is_async=False)
    return s3.get(event["bucket"], event["key"]).decode("utf-8")
