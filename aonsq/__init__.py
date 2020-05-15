
from .nsq import NSQ
from .nsq_basic import NSQBasic
from .nsq_message import NSQMessage

# if __name__ == "__main__":

#     async def msg_handler(msg: NSQMessage) -> bool:
#         # d(f"msg: {msg.id}")
#         return True

#     async def test():
#         mq = NSQ(host="127.0.0.1", port=4070)
#         await mq.connect()
#         await mq.sub("demo", "test", msg_handler)

#         while True:
#             for j in range(1000):
#                 await mq.pub("demo", orjson.dumps({"id": j, "ts_created": datetime.now(timezone.utc)}))

#             await asyncio.sleep(1)

#     try:
#         asyncio.get_event_loop().run_until_complete(test())
#     except KeyboardInterrupt:
#         pass
