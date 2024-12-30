from fault_tolerant_redis import FaultTolerantRedisClone
from rpc import RPCServer

server = RPCServer('127.0.0.1', 8080)
redis_instance = FaultTolerantRedisClone()
server.registerInstance(redis_instance)
server.run()
