from rpc import RPCClient

def main():
    print("Redis Clone Client")
    print("Type your commands (e.g., `SET key value`) or type `EXIT` to quit.")

    client = RPCClient('127.0.0.1', 8080)
    client.connect()

    try:
        while True:
            command = input("> ").strip()
            if command.lower() == "exit":
                print("Goodbye!")
                break

            parts = command.split()
            if not parts:
                continue

            cmd, *args = parts
            cmd = cmd.lower()

            if cmd == "del":
                cmd = "delete"

            try:
                if cmd in ["set", "get", "delete", "append", "keys", "flushall", 
                          "expire", "ttl", "persist", "exists", 
                          "hset", "hget", "hdel", "hgetall", "hdelall",
                           "zset",  "zrange", "zrevrange", "zdelvalue", "zdelkey", "zrank", "zgetall"
                          
                          ]:
                    if cmd == "set" and len(args) >= 4 and args[-2].lower() == "ex":
                        key, value = args[0], args[1]
                        ex = int(args[-1])
                        result = client.set(key, value, ex=ex)
                    else:
                        method = getattr(client, cmd)
                        result = method(*args)
                        
                    print(result)
                else:
                    print("Unknown command.")
            except Exception as e:
                print(f"Error: {str(e)}")
    finally:
        client.disconnect()

if __name__ == "__main__":
    main()



# set key hihi
# get key 
# del key


# > SET mykey value EX 60   Set key với TTL 60 giây
# > TTL mykey              Kiểm tra thời gian còn lại
# > EXPIRE mykey 30        Set/update TTL
# > PERSIST mykey          Xóa TTL

# SORTED SET
# zset scores 100 alice
# zset scores 20 bobe
# zset scores 30 hihi
# zrank scores bobe
# zrange scores 0 3  
# zrevrange scores 0 3  
# zdelvalue scores hihi
# zgetall scores
# zdelkey scores

# For Hash
# hset student name kious  
# hset student age 19  
# hget student name
# hgetall student 
# hdel student name 
# hdelall student 
