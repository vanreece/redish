import redish
import unittest
import json

class TestRedish(unittest.TestCase):
    def init(self, instance):
        resp = instance.processRequest({"command": "CONNECT"})
        self.assertEqual(resp["status"], "OK")
        self.assertTrue("id" in resp)
        connectionID = resp["id"]
        def processNative(cmd, args, expectedResponse):
            request = {}
            request["id"] = connectionID
            request["command"] = cmd
            if args:
                request["args"] = args
            self.assertEqual(
                    instance.processRequest(request),
                    expectedResponse)
        def processJSON(cmd, args, expectedResponse):
            request = {}
            request["id"] = connectionID
            request["command"] = cmd
            if args:
                request["args"] = args
            requestJSON = json.dumps(request)
            responseJSON = instance.processRequestJSON(requestJSON)
            response = json.loads(responseJSON)
            self.assertEqual(expectedResponse, response)
        # This can be switched to processNative for ease of debugging if needed
        return processJSON

    def testDISCONNECT(self):
        process = self.init(redish.Redish(10))
        process("DISCONNECT", ["bad arg"],
                {"status": "ERROR", "detail": "DISCONNECT has no arguments"})
        process("DISCONNECT", None,
                {"status": "OK"})
        instance = redish.Redish(1)
        self.assertEqual(
                instance.processRequest({"command": "DISCONNECT", "id": 500}),
                {"status": "ERROR", "detail": "id 500 not known"})

    def testGETAndSET(self):
        process = self.init(redish.Redish(10))
        process("GET", ["notthere"],
                {"status": "OK", "result": ""})
        process("SET", ["key", "value"],
                {"status": "OK"})
        process("GET", ["key"],
                {"status": "OK", "result": "value"})
        process("SET", ["key", 1],
                {"status": "OK"})
        process("GET", ["key"],
                {"status": "OK", "result": 1})
        process("SET", ["key", "1"],
                {"status": "OK"})
        process("GET", ["key"],
                {"status": "OK", "result": "1"})
        process("GET", ["key", "key2"],
                {"status": "ERROR", "detail": "GET requires one argument: key"})
        process("SET", ["key"],
                {"status": "ERROR", "detail": "SET requires two arguments: key and value"})
        process("SET", ["key", "value", "extra"],
                {"status": "ERROR", "detail": "SET requires two arguments: key and value"})

    def testNonStringKeys(self):
        process = self.init(redish.Redish(10))
        process("GET", [1],
                {"status": "OK", "result": ""})
        process("SET", [1, 2],
                {"status": "OK"})
        process("GET", [1],
                {"status": "OK", "result": 2})

    def testMGET(self):
        process = self.init(redish.Redish(10))
        process("SET", ["key1", "Hello"],
                {"status": "OK"})
        process("SET", ["key2", "World"],
                {"status": "OK"})
        process("MGET", ["key1", "key2", "nonexisting"],
                {"status": "OK", "result": ["Hello", "World", ""]})
        process("MGET", ["key1"],
                {"status": "OK", "result": ["Hello"]})
        process("MGET", [],
                {"status": "ERROR",
                 "detail": "MGET requires at least one argument: key [key ...]"})

    def testMSET(self):
        process = self.init(redish.Redish(10))
        process("MSET", ["key1", "Hello", "key2", "World"],
                {"status": "OK"})
        process("GET", ["key1"],
                {"status": "OK", "result": "Hello"})
        process("GET", ["key2"],
                {"status": "OK", "result": "World"})
        process("MSET", ["key1", "Hola"],
                {"status": "OK"})
        process("GET", ["key1"],
                {"status": "OK", "result": "Hola"})

        err = {"status": "ERROR",
               "detail": "MSET requires at least one pair of arguments: key value [key value ...]"}
        process("MSET", [], err)
        process("MSET", ["key1"], err)
        process("MSET", [], err)
        process("MSET", ["key1", "value1", "key2"], err)

    def testCacheEviction(self):
        process = self.init(redish.Redish(2))
        process("MSET", ["key1", "one", "key2", "two", "key3", "three"],
                {"status": "OK", "evicted": ["key1", "one"]})
        process("GET", ["key1"],
                {"status": "OK", "result": ""})
        process("SET", ["key2", "newTwo"],
                {"status": "OK"})
        process("SET", ["key4", "four"],
                {"status": "OK", "evicted": ["key3", "three"]})
        process("GET", ["key3"],
                {"status": "OK", "result": ""})
        process("MSET", ["big", 9223372036854775807, "reg", 3],
                {"status": "OK", "evicted": ["key2", "newTwo", "key4", "four"]})
        process("INCR", ["big"],
                {"status": "ERROR",
                 "detail": "INCR would overflow"})

        #Should not update the LRU, so big is still oldest...
        process("SET", ["new", "two"],
                {"status": "OK",
                 "evicted": ["big", 9223372036854775807]})
        process("INCR", ["reg"],
                {"status": "OK",
                 "result": 4})
        #Should update the LRU, so "new" is now oldest
        process("SET", ["newnew", "whatever"],
                {"status": "OK",
                 "evicted": ["new", "two"]})

        #Test to make sure new incr evicts old values
        process("INCR", ["allnewkey"],
                {"status": "OK", "result": 1, "evicted": ["reg", 4]})
        process("DECR", ["evennewerkey"],
                {"status": "OK", "result": -1, "evicted": ["newnew", "whatever"]})
    def testINCR(self):
        process = self.init(redish.Redish(10))
        process("SET", ["key1", 1],
                {"status": "OK"})
        process("GET", ["key1"],
                {"status": "OK", "result": 1})
        process("INCR", ["key1"],
                {"status": "OK", "result": 2})
        process("GET", ["key1"],
                {"status": "OK", "result": 2})
        process("SET", ["notint", "1"],
                {"status": "OK"})
        process("INCR", ["notint"],
                {"status": "ERROR",
                 "detail": "INCR works only on 64 bit signed integers"})
        process("SET", ["notint", 1.1],
                {"status": "OK"})
        process("INCR", ["notint"],
                {"status": "ERROR",
                 "detail": "INCR works only on 64 bit signed integers"})
        process("INCR", None,
                {"status": "ERROR",
                 "detail": "INCR requires one argument: key"})
        process("INCR", [],
                {"status": "ERROR",
                 "detail": "INCR requires one argument: key"})
        process("INCR", ["newKey"],
                {"status": "OK", "result": 1})
        process("INCR", ["newKey"],
                {"status": "OK", "result": 2})

        # Test overflow case
        process("SET", ["big", 9223372036854775806],
                {"status": "OK"})
        process("INCR", ["big"],
                {"status": "OK", "result": 9223372036854775807})
        process("INCR", ["big"],
                {"status": "ERROR",
                 "detail": "INCR would overflow"})

    def testDECR(self):
        process = self.init(redish.Redish(10))
        process("SET", ["key1", 1],
                {"status": "OK"})
        process("GET", ["key1"],
                {"status": "OK", "result": 1})
        process("DECR", ["key1"],
                {"status": "OK", "result": 0})
        process("GET", ["key1"],
                {"status": "OK", "result": 0})
        process("SET", ["notint", "1"],
                {"status": "OK"})
        process("DECR", ["notint"],
                {"status": "ERROR",
                 "detail": "DECR works only on 64 bit signed integers"})
        process("SET", ["notint", 1.1],
                {"status": "OK"})
        process("DECR", ["notint"],
                {"status": "ERROR",
                 "detail": "DECR works only on 64 bit signed integers"})
        process("DECR", None,
                {"status": "ERROR",
                 "detail": "DECR requires one argument: key"})
        process("DECR", [],
                {"status": "ERROR",
                 "detail": "DECR requires one argument: key"})
        process("DECR", ["newKey"],
                {"status": "OK", "result": -1})
        process("DECR", ["newKey"],
                {"status": "OK", "result": -2})

        # Test overflow case
        process("SET", ["negbig", -9223372036854775807],
                {"status": "OK"})
        process("DECR", ["negbig"],
                {"status": "OK", "result": -9223372036854775808})
        process("DECR", ["negbig"],
                {"status": "ERROR",
                 "detail": "DECR would overflow"})

    def testTransactionBadArgs(self):
        process = self.init(redish.Redish(10))
        process("MULTI", ["hi"],
                {"status": "ERROR",
                 "detail": "MULTI should have no arguments"})
        process("EXEC", ["hi"],
                {"status": "ERROR",
                 "detail": "EXEC should have no arguments"})
        process("DISCARD", ["hi"],
                {"status": "ERROR",
                 "detail": "DISCARD should have no arguments"})
        process("UNWATCH", ["hi"],
                {"status": "ERROR",
                 "detail": "UNWATCH should have no arguments"})
        process("WATCH", None,
                {"status": "ERROR",
                 "detail": "WATCH requires one argument: key"})
        process("WATCH", [],
                {"status": "ERROR",
                 "detail": "WATCH requires one argument: key"})

    def testSimpleTransaction(self):
        process = self.init(redish.Redish(10))
        process("MULTI", None, {"status": "OK"})
        process("INCR", ["foo"], {"status": "QUEUED"})
        process("INCR", ["bar"], {"status": "QUEUED"})
        process("SET", ["baz", "what"], {"status": "QUEUED"})
        process("GET", ["baz"], {"status": "QUEUED"})
        process("MGET", ["foo", "bar", "baz"], {"status": "QUEUED"})
        process("MSET", ["foo", "all", "bar", "new", "baz", "values"], {"status": "QUEUED"})
        process("MGET", ["foo", "bar", "baz"], {"status": "QUEUED"})
        process("EXEC", None,
               {"status": "OK",
                "results": [{"status": "OK", "result": 1},
                            {"status": "OK", "result": 1},
                            {"status": "OK"},
                            {"status": "OK", "result": "what"},
                            {"status": "OK", "result": [1, 1, "what"]},
                            {"status": "OK"},
                            {"status": "OK", "result": ["all", "new", "values"]}]})

    def testTransactionWithRuntimeError(self):
        process = self.init(redish.Redish(10))
        process("MULTI", None, {"status": "OK"})
        process("SET", ["foo", "bar"], {"status": "QUEUED"})
        process("INCR", ["foo"], {"status": "QUEUED"})
        process("EXEC", None,
               {"status": "OK",
                "results": [{"status": "OK"},
                            {"status": "ERROR",
                             "detail": "INCR works only on 64 bit signed integers"}]})

    def testTransactionWithDiscard(self):
        process = self.init(redish.Redish(10))
        process("EXEC", None,
               {"status": "ERROR",
                "detail": "EXEC called without MULTI"})
        process("DISCARD", None,
               {"status": "ERROR",
                "detail": "DISCARD called without MULTI"})
        process("SET", ["foo", 1], {"status": "OK"})
        process("MULTI", None, {"status": "OK"})
        process("INCR", ["foo"], {"status": "QUEUED"})
        process("DISCARD", None, {"status": "OK"})
        process("EXEC", None,
               {"status": "ERROR",
                "detail": "EXEC called without MULTI"})
        process("GET", ["foo"], {"status": "OK", "result": 1})

    def testTransactionWithBadSyntax(self):
        process = self.init(redish.Redish(10))
        process("MULTI", None, {"status": "OK"})
        process("INCR", None,
                {"status": "ERROR",
                 "detail": "INCR requires one argument: key"})
        process("INCR", ["bar"], {"status": "QUEUED"})
        process("EXEC", None,
                {"status": "ERROR",
                 "detail": "Transaction discarded because of previous errors"})

    def testNestedTransaction(self):
        process = self.init(redish.Redish(10))
        process("MULTI", None, {"status": "OK"})
        process("MULTI", None, {"status": "ERROR", "detail": "MULTI calls can not be nested"})
        process("INCR", ["bar"], {"status": "QUEUED"})
        process("EXEC", None,
                {"status": "OK",
                 "results": [{"status": "OK", "result": 1}]})

    def testSimultaneousTransactions(self):
        instance = redish.Redish(10)
        process1 = self.init(instance)
        process2 = self.init(instance)
        process1("SET", ["foo", 1], {"status": "OK"})
        process2("GET", ["foo"], {"status": "OK", "result": 1})

        # Baseline, uninterrupted
        process1("WATCH", ["foo"], {"status": "OK"})
        process1("GET", ["foo"], {"status": "OK", "result": 1})
        process1("MULTI", None, {"status": "OK"})
        process1("SET", ["foo", 2], {"status": "QUEUED"})
        process1("EXEC", None,
                 {"status": "OK",
                  "results": [{"status": "OK"}]})
        process1("GET", ["foo"], {"status": "OK", "result": 2})

        # Interrupted
        process1("WATCH", ["foo"], {"status": "OK"})
        process2("SET", ["foo", 2], {"status": "OK"})
        process1("GET", ["foo"], {"status": "OK", "result": 2})
        process1("MULTI", None, {"status": "OK"})
        process1("SET", ["foo", 3], {"status": "QUEUED"})
        process1("EXEC", None, {"status": "OK"})
        process1("GET", ["foo"], {"status": "OK", "result": 2})

        # Interrupted with aborted watch
        process1("WATCH", ["foo"], {"status": "OK"})
        process1("UNWATCH", None, {"status": "OK"})
        process2("SET", ["foo", 2], {"status": "OK"})
        process1("GET", ["foo"], {"status": "OK", "result": 2})
        process1("MULTI", None, {"status": "OK"})
        process1("SET", ["foo", 3], {"status": "QUEUED"})
        process1("EXEC", None, {"status": "OK", "results": [{"status": "OK"}]})
        process1("GET", ["foo"], {"status": "OK", "result": 3})

    def testBadInput(self):
        instance = redish.Redish(1)
        self.assertEqual(
                instance.processRequest(
                    {"command": "CONNECT", "args": ["bad arg"]}),
                {"status": "ERROR", "detail": "CONNECT has no arguments"})
        self.assertEqual(
                instance.processRequest({"command": "INCR", "args": ["key"]}),
                {"status": "ERROR",
                 "detail": "id not supplied"})
        self.assertEqual(
                json.loads(instance.processRequestJSON("not json")),
                {"status": "ERROR", "detail": "could not parse json"})
        self.assertEqual(
                instance.processRequest({}),
                {"status": "ERROR",
                 "detail": "'command' not present in request"})
        process = self.init(instance)
        process("NOTACOMMAND", None,
                {"status": "ERROR",
                 "detail": "command 'NOTACOMMAND' not found"})

if __name__ == '__main__':
    unittest.main()
