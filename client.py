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
                           "zset",  "zrange", "zrevrange", "zdelvalue", "zdelkey", "zrank", "zgetall",
                            "lpush", "rpush", "lpop", "rpop", "lrange", "llen", "delpush",
                          ]:
                    if cmd == "set" and len(args) >= 4 and args[-2].lower() == "ex":
                        key, value = args[0], args[1]
                        ex = int(args[-1])
                        result = client.set(key, value, ex=ex)
                    else:
                        method = getattr(client, cmd)
                        result = method(*args)

                    # Handle special TTL cases
                    if cmd == "ttl":
                        if result == -2:
                            result= "Key does not exist."
                        elif result == -1:
                            result="Key exists but has no expiration."
                        
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
# zrank scores bobe     -> rank position
# zrange scores 0 3        -> sort from low to high
# zrevrange scores 0 3   ->high to low
# zdelvalue scores hihi   ->delete 
# zgetall scores               ->get all
# zdelkey scores               ->delete all

# For Hash
# hset student name kious  
# hset student age 19  
# hget student name
# hgetall student                
# hdel student name 
# hdelall student 

# For List
# lpush alphabet a b c   ->return 3
# lpush alphabet d e f g h  -> return 8 (3+5)
# llen alphabet      -> return 8 
# lrange alphabet 1 4    ->  ['e', 'f', 'g', 'h']
# rpop alphabet           -> c   
# lpop alphabet           -> d
# lpop alphabet            -> e
# delpush alphabet      ->Success