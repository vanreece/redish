# redish
A lightweight redis-ish key value store.

## Getting started
Tested and run on Python 2.7.13.

To run, use `python redish.py <maxkeys>`.
Input commands on stdin, and results will come on stdout. To run test suite, run `python testRedish.py`

Also included is a simple performance testing program `performanceTest.py`, which can do some really simple thrashing tests against an actual local redis server using redis-cli and compare them against redish.

## API Documentation/Notes

###Data model
The key value store has a maximum number of keys that it can hold on to, specified by `maxkeys` on the command line. If you write a new value beyond the maximum count, the least recently read/written key/value(s) will be evicted and returned with the result of the command which did so.

###Communication protocol
Communication happens via JSON for each command and each reply.
Each command consists of a single JSON object on a single line of stdin.
Each reply is likewise a single JSON object returned on a single line of stdout.
When you CONNECT it assigns and returns a new connection id.
All non-CONNECT calls should include an "id" field on the json object with the id from above.
At the end, you DISCONNECT with the same id.
This is a simple way of tracking simultaneous persistent server connections via the stdin/stdout model.

Keys are all strings.
Though actually you can likely use any other hashable json encodable type, but it's largely untested and unsupported, so stick with strings.
Values supported are strings and integers.

All commands are JSON objects which must include `command` and `id` fields (except CONNECT, which does not require an id).
Some commands take the `args` field, which is a JSON array with arguments.
Return values are JSON objects with information. Every one has a `status`, which is either OK, ERROR, or QUEUED.
If the status is ERROR, `detail` will provide more information about the error.
If the status is QUEUED, the command came after a MULTI and has been enqueued for later processing when an EXEC is called.
Other return values are as listed for each command.

##Supported commands

- CONNECT
 - arguments: none
 - returns: `id`
 - functionality: Establish a new connection to the server, generating a unique id which is used for the rest of the interactions with this connection
- DISCONNECT
 - arguments: none
 - returns: none
 - functionality: Free this connection id. That id is longer valid to use.
- SET
 - arguments: key value
 - returns: none
 - functionality: Set a new value at a given key
- GET
 - arguments: key
 - returns: `result`
 - functionality: Returns the value for key
- MSET
 - arguments: key value [key value ...]
 - returns: none
 - functionality: Arguments must be specified in key value pairs. Atomically sets all specified key value pairs.
- MGET
 - arguments: key [key ...]
 - returns: `result`
 - functionality: Returns as a json array all of the values in the order specified
- INCR
 - arguments: key
 - returns: `result`
 - functionality: Must operate on a new key, or an existing key which stores a 64 bit signed integer. It will atomically increment the integer by 1. If the result doesn't fit in a 64 bit signed integer or the value is not an integer it will return an error.
- DECR
 - arguments: key
 - returns: `result`
 - functionality: Must operate on a new key, or an existing key which stores a 64 bit signed integer. It will atomically decrement the integer by 1. If the result doesn't fit in a 64 bit signed integer or the value is not an integer it will return an error.
- MULTI
 - arguments: none
 - returns: none
 - functionality: Begins a transaction block which will execute atomically when EXEC is called. Barring some conditions.
- EXEC
 - arguments: none
 - returns: "results"
 - functionality: Executes the block of enqueued commands. Puts the results of each of the commands executed in a json array and provides that in the "results" field of the return object. If a WATCHed value changes after calling WATCH (from this or another connection), then none of the commands between MULTI and EXEC will be executed, and there will be no "results" field returned.
- DISCARD
 - arguments: none
 - returns: none
 - functionality: Undoes a MULTI call. The following commands are no longer being enqueued and are back to executing when called.
- WATCH
 - argument: key
 - returns: none
 - Sets up a watch on one key. If, after calling WATCH, the value changes, the next EXEC will not execute and return no results.
- UNWATCH
 - argument: none
 - returns: none
 - Removes all watched keys from being watched for the given connection.

## Simple example
Here is a simple example of inputs on stdin to redish:

	{"command": "CONNECT"}
	{"args": ["key", "value"], "command": "SET", "id": 1}
	{"args": ["key"], "command": "GET", "id": 1}
	{"args": ["key", 1], "command": "SET", "id": 1}
	{"args": ["key"], "command": "GET", "id": 1}
	{"command": "DISCONNECT", "id": 1}

And the corresponding outputs:

	{"status": "OK", "id": 1}
	{"status": "OK"}
	{"status": "OK", "result": "value"}
	{"status": "OK"}
	{"status": "OK", "result": 1}
	{"status": "OK"}

For more complex examples, take a look at `testRedish.py`
