from rpc import RPCServer
import threading
import time
import json
import os
from typing import Any, Dict, List, Optional
import logging
from collections import defaultdict

class FaultTolerantRedisClone:
    def __init__(self, snapshot_interval: int = 30, snapshot_file: str = "redis_snapshot.json"):
        self.data_store: Dict[str, Any] = {}
        self.sorted_sets: Dict[str, List[tuple]] = {} 
        self.expiry_times: Dict[str, float] = {} 
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        self.lock_list = threading.Lock()
        self.snapshot_interval = snapshot_interval
        self.snapshot_file = snapshot_file
        self.last_snapshot_time = time.time()
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='redis_clone.log'
        )
        
        # Load existing data if available
        self._load_snapshot()
        
        # Xử lí key hết hạn 
        self.cleanup_thread = threading.Thread(target=self._cleanup_expired_keys, daemon=True)
        self.cleanup_thread.start()

        # Start background snapshot thread
        self.snapshot_thread = threading.Thread(target=self._periodic_snapshot, daemon=True)
        self.snapshot_thread.start()

    def _cleanup_expired_keys(self):
        """Remove keys that have expired."""
        while True:
            time.sleep(1)  # Check every second
            with self.lock:
                current_time = time.time()
                expired_keys = [
                    key for key, expire_time in self.expiry_times.items()
                    if expire_time <= current_time
                ]
                for key in expired_keys:
                    self.data_store.pop(key, None)
                    self.expiry_times.pop(key, None)
                    logging.info(f"Key expired and removed: {key}")

    def _load_snapshot(self) -> None:
        """Load data from snapshot file if it exists."""
        try:
            if os.path.exists(self.snapshot_file):
                with open(self.snapshot_file, 'r') as f:
                    snapshot_data = json.load(f)
                    self.data_store = snapshot_data.get('data', {})
                    self.expiry_times = {
                        k: float(v) for k, v in snapshot_data.get('expiry', {}).items()
                    }
                    self.sorted_sets = snapshot_data.get('sorted_sets', {})
                logging.info(f"Loaded snapshot from {self.snapshot_file}")
        except Exception as e:
            logging.error(f"Error loading snapshot: {str(e)}")

    def _save_snapshot(self) -> None:
        """Save current data store to snapshot file."""
        try:
            with self.lock:
                snapshot_data = {
                    'data': self.data_store,
                    'expiry': self.expiry_times,
                    'sorted_sets': self.sorted_sets 
                }
                with open(self.snapshot_file, 'w') as f:
                    json.dump(snapshot_data, f)
            self.last_snapshot_time = time.time()
            logging.info("Snapshot saved successfully")
        except Exception as e:
            logging.error(f"Error saving snapshot: {str(e)}")

    def _periodic_snapshot(self) -> None:
        """Periodically save snapshots in the background."""
        while True:
            time.sleep(self.snapshot_interval)
            if time.time() - self.last_snapshot_time >= self.snapshot_interval:
                self._save_snapshot()

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> str:
        """Set key-value pair with optional expiry time."""
        try:
            with self.lock:
                self.data_store[key] = value
                if ex is not None:
                    self.expiry_times[key] = time.time() + int(ex)
                    logging.info(f"Set key {key} with {ex} seconds TTL")
                else:
                    # Remove any existing TTL
                    self.expiry_times.pop(key, None)
                    logging.info(f"Set key {key} without TTL")
                return "OK"
        except Exception as e:
            logging.error(f"Error setting key {key}: {str(e)}")
            raise

    def get(self, key: str) -> Optional[Any]:
        """Get value for key with error handling."""
        try:
            with self.lock:
                if key in self.expiry_times:
                    if time.time() >= self.expiry_times[key]:
                        self.data_store.pop(key, None)
                        self.expiry_times.pop(key)
                        return None
                value = self.data_store.get(key)
                if value is None:
                    logging.info(f"Key not found: {key}")
                return value
        except Exception as e:
            logging.error(f"Error getting key {key}: {str(e)}")
            raise

    def delete(self, key: str) -> Optional[Any]:
        """Delete key with error handling."""
        try:
            with self.lock:
                value = self.data_store.pop(key, None)
                self.expiry_times.pop(key, None)
                if value is not None:
                    logging.info(f"Deleted key: {key}")
                return value
        except Exception as e:
            logging.error(f"Error deleting key {key}: {str(e)}")
            raise

    def keys(self) -> list:
        """Get all keys with error handling."""
        try:
            with self.lock:
                # Only return non-expired keys
                current_time = time.time()
                return [
                    key for key in self.data_store.keys()
                    if key not in self.expiry_times or self.expiry_times[key] > current_time
                ]
        except Exception as e:
            logging.error(f"Error getting keys: {str(e)}")
            raise

    def flushall(self) -> str:
        """Clear all data with error handling."""
        try:
            with self.lock:
                self.data_store.clear()
                self.expiry_times.clear()
                self._save_snapshot()  # Save empty state
                logging.info("Executed FLUSHALL command")
                return "OK"
        except Exception as e:
            logging.error(f"Error in FLUSHALL: {str(e)}")
            raise

    def append(self, key: str, value: str) -> Any:
        """Append to string value with type checking and error handling."""
        try:
            with self.lock:
                if key in self.data_store:
                    if isinstance(self.data_store[key], str):
                        self.data_store[key] += value
                        logging.info(f"Appended to key: {key}")
                        return len(self.data_store[key])
                    else:
                        msg = f"Value for key {key} is not a string"
                        logging.warning(msg)
                        return msg
                else:
                    msg = f"Key {key} does not exist"
                    logging.warning(msg)
                    return msg
        except Exception as e:
            logging.error(f"Error appending to key {key}: {str(e)}")
            raise

    def expire(self, key: str, seconds: int) -> bool:
        """Set TTL (time to live) for a key."""
        try:
            with self.lock:
                if key in self.data_store:
                    self.expiry_times[key] = time.time() + int(seconds)
                    logging.info(f"Set TTL for key {key}: {seconds} seconds")
                    return True
                logging.info(f"Key {key} not found for expire")
                return False
        except Exception as e:
            logging.error(f"Error setting expire for key {key}: {str(e)}")
            raise

    def ttl(self, key: str) -> int:
        """Get remaining TTL (time to live) for a key."""
        try:
            with self.lock:
                if key not in self.data_store:
                    logging.info(f"Key {key} not found for TTL check")
                    return -2  # Key không tồn tại
                
                if key not in self.expiry_times:
                    logging.info(f"Key {key} has no TTL set")
                    return -1  # Key không có TTL
                
                remaining = int(self.expiry_times[key] - time.time())
                if remaining <= 0:
                    # Key đã hết hạn
                    self.data_store.pop(key, None)
                    self.expiry_times.pop(key, None)
                    logging.info(f"Key {key} expired during TTL check")
                    return -2
                    
                return remaining
        except Exception as e:
            logging.error(f"Error checking TTL for key {key}: {str(e)}")
            raise

    def persist(self, key: str) -> bool:
        """Remove TTL from a key."""
        try:
            with self.lock:
                if key not in self.data_store:
                    logging.info(f"Key {key} not found for persist")
                    return False
                    
                if key not in self.expiry_times:
                    logging.info(f"Key {key} already has no TTL")
                    return False
                    
                self.expiry_times.pop(key)
                logging.info(f"Removed TTL for key {key}")
                return True
        except Exception as e:
            logging.error(f"Error persisting key {key}: {str(e)}")
            raise

        #  Hash 
    def hset(self, hash_key: str, field: str, value: Any) -> str:   
        """Set a field in a hash stored at hash_key"""
        try:
            with self.lock:
                if hash_key not in self.data_store:
                    self.data_store[hash_key] = {}
                self.data_store[hash_key][field] = value
                logging.info(f"Set {field} in hash {hash_key}: {value}")
                return "OK"
        except Exception as e:
            logging.error(f"Error in HSET {hash_key}: {str(e)}")
            raise

    def hget(self, hash_key: str, field: str) -> Optional[Any]:
        """Get value of a field from hash stored at hash_key"""
        try:
            with self.lock:
                hash_data = self.data_store.get(hash_key, {})
                value = hash_data.get(field)
                if value is None:
                    logging.info(f"Field {field} not found in hash {hash_key}")
                return value
        except Exception as e:
            logging.error(f"Error in HGET {hash_key}: {str(e)}")
            raise

    def hdel(self, hash_key: str, field: str) -> bool:
        """Delete a field from hash stored at hash_key"""
        try:
            with self.lock:
                hash_data = self.data_store.get(hash_key, {})
                if field in hash_data:
                    del hash_data[field]
                    logging.info(f"Deleted field {field} from hash {hash_key}")
                    return True
                logging.info(f"Field {field} not found in hash {hash_key}")
                return False
        except Exception as e:
            logging.error(f"Error in HDEL {hash_key}: {str(e)}")
            raise

    def hgetall(self, hash_key: str) -> dict:
        """Get all fields and values of hash stored at hash_key"""
        try:
            with self.lock:
                hash_data = self.data_store.get(hash_key, {})
                return hash_data
        except Exception as e:
            logging.error(f"Error in HGETALL {hash_key}: {str(e)}")
            raise

    def hdelall(self, hash_key: str) -> bool:
        """Delete all field in hash"""
        try:
            with self.lock:
                if hash_key in self.data_store:
                    self.data_store.pop(hash_key)
                    logging.info(f"Deleted all fields from hash {hash_key}")
                    return True
                logging.info(f"Hash {hash_key} does not exist")
                return False
        except Exception as e:
            logging.error(f"Error in HDELALL {hash_key}: {str(e)}")
            raise
    # End Hash

    # Sorted sets
    def zset(self, zset_key: str, score: float, value: Any) -> int:
        """Add elements to Sorted Set with scores"""
        try:
            with self.lock:
                if zset_key not in self.sorted_sets:
                    self.sorted_sets[zset_key] = []
                # Add elements to the Sorted Set, (score, value) is a tuple pair
                self.sorted_sets[zset_key].append((score, value))
                # sort
                self.sorted_sets[zset_key].sort()
                logging.info(f"Added value {value} with score {score} to ZSET {zset_key}")
                return 1  
        except Exception as e:
            logging.error(f"Error in ZADD {zset_key}: {str(e)}")
            raise

    def zrange(self, zset_key: str, start: int, end: int) -> List[Any]:
        """Gets the elements in the Sorted Set from zset_key, according to the specified range"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    start = int(start)
                    end = int(end)
                    # Get the range of elements in the sorted set
                    zset = self.sorted_sets[zset_key]
                    return [value for _ , value in zset[start:end+1]]
                logging.warning(f"ZSET {zset_key} không tồn tại.")
                return []
        except Exception as e:
            logging.error(f"Error in ZRANGE {zset_key}: {str(e)}")
            raise

    def zrevrange(self, zset_key: str, start: int, end: int) -> List[Any]:
        """Get elements from the Sorted Set, sort them in descending order by score"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    start = int(start)
                    stop = int(stop)
                    # Gets the range of elements in the sorted set in descending order
                    zset = self.sorted_sets[zset_key]
                    return [value for score, value in reversed(zset[start:end+1])]
                logging.warning(f"ZSET {zset_key} không tồn tại.")
                return []
        except Exception as e:
            logging.error(f"Error in ZREVRANGE {zset_key}: {str(e)}")
            raise

    def zdelvalue(self, zset_key: str, value: Any) -> int:
        """Delete elements in the Sorted Set"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    # Find and remove elements from the Sorted Set
                    zset = self.sorted_sets[zset_key]
                    self.sorted_sets[zset_key] = [item for item in zset if item[1] != value]
                    logging.info(f"Removed value {value} from ZSET {zset_key}")
                    return 1  
                logging.warning(f"ZSET {zset_key} không tồn tại.")
                return 0
        except Exception as e:
            logging.error(f"Error in ZREM {zset_key}: {str(e)}")
            raise

    def zdelkey(self, zset_key: str) -> int:
        """Delete the entire Sorted Set identified by zset_key"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    del self.sorted_sets[zset_key]  # Delete the entire ZSET
                    logging.info(f"Deleted entire ZSET {zset_key}")
                    return 1  # Return 1 to indicate successful deletion
                logging.warning(f"ZSET {zset_key} không tồn tại.")
                return 0  # Return 0 if ZSET doesn't exist
        except Exception as e:
            logging.error(f"Error in ZDEL {zset_key}: {str(e)}")
            raise


    def zrank(self, zset_key: str, value: Any) -> Optional[int]:
        """Check the position of an element in the Sorted Set"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    zset = self.sorted_sets[zset_key]
                    for idx, (score, item) in enumerate(zset):
                        if item == value:
                            logging.info(f"Rank of {value} in ZSET {zset_key}: {idx}")
                            return idx
                logging.warning(f"Value {value} not found in ZSET {zset_key}")
                return None
        except Exception as e:
            logging.error(f"Error in ZRANK {zset_key}: {str(e)}")
            raise

    def zgetall(self, zset_key: str) -> List[tuple]:
        """Get all the elements in the Sorted Set"""
        try:
            with self.lock:
                if zset_key in self.sorted_sets:
                    return self.sorted_sets[zset_key]
                logging.warning(f"ZSET {zset_key} không tồn tại.")
                return []
        except Exception as e:
            logging.error(f"Error in ZGETALL {zset_key}: {str(e)}")
            raise
    # End Sorted sets

    # Start List
    def _get_list(self, key):   
        """Helper method to get a list from the data store."""
        if key not in self.data_store:
            self.data_store[key] = []
        elif not isinstance(self.data_store[key], list):
            raise TypeError(f"Key '{key}' does not hold a list.")
        return self.data_store[key]
    
    def lpush(self, key, *values):
        """Push values to the head of the list."""
        with self.lock_list:
            lst = self._get_list(key)
            for value in reversed(values):  # Maintain LPUSH semantics
                lst.insert(0, value)
        return len(lst)

    def rpush(self, key, *values):
        """Push values to the tail of the list."""
        with self.lock_list:
            lst = self._get_list(key)
            lst.extend(values)
        return len(lst)

    def lpop(self, key):
        """Pop a value from the head of the list."""
        with self.lock_list:
            lst = self._get_list(key)
            if not lst:
                return None
            return lst.pop(0)

    def rpop(self, key):
        """Pop a value from the tail of the list."""
        with self.lock_list:
            lst = self._get_list(key)
            if not lst:
                return None
            return lst.pop()

    def lrange(self, key, start, stop):
        """Get a subrange from the list."""
        with self.lock_list:
            lst = self._get_list(key)
            start = int(start)
            stop = int(stop)
            return lst[start:stop + 1]


    def llen(self, key):
        """Get the length of the list."""
        with self.lock_list:
            lst = self._get_list(key)
            return len(lst)
        
    def delpush(self, key):
        """
        Delete the entire key and push new values to a list (head by default).
        """
        with self.lock_list:
            # Remove the existing key if it exists
            self.data_store.pop(key, None)

        return "Success"
        
    # End list


    def exists(self, key: str) -> bool:
        try:
            with self.lock:
                logging.info(f"Checking existence of key: {key}") 
                if key in self.data_store:
                    # Check if the key has expired
                    if key in self.expiry_times and self.expiry_times[key] <= time.time():
                        # If it has expired, delete the key from data_store and Expiration_times
                        self.data_store.pop(key, None)
                        self.expiry_times.pop(key, None)
                        logging.info(f"Key {key} is expired and removed.")
                        return False
                    logging.info(f"Key {key} exists and is valid.")
                    return True
                logging.info(f"Key {key} does not exist.")
                return False
        except Exception as e:
            logging.error(f"Error checking existence of key {key}: {str(e)}")
            raise
