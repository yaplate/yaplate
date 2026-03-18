# 1. decode:
decode() method is used bytes object to convert it into a human-readable string object using a specified character encoding, such as utf-8
```python
bytes_object.decode(encoding="utf-8", errors="strict")
```
### Example
```python
# A bytes object (note the 'b' prefix)
encoded_bytes = b'Hello, world!'

# Decode using UTF-8 encoding
decoded_string = encoded_bytes.decode('utf-8')

print(decoded_string)
# Output: Hello, world!

```

# 2. bytes:
It is an immutable sequence of single bytes, used to store raw binary data

# 3. isinstance: 
isinstance helps to verify whether any value or variable whether is a part of class/subclass or not
isinstance(object, classinfo)
### Example:
```python
a = 5
print(isinstance(a, int)) # returns True because a is int
```
OR
```python
value = "hello"
### Check if value is an instance of str or tuple
if isinstance(value, (str, tuple)):
    print("Is a string or a tuple")
else:
    print("Is neither a string nor a tuple")
### Output: Is a string or a tuple
```

set
delete
exists
get
scan_iter
hset
hgetall
zadd
zrem
zrange
zrangebyscore
zscore
rename