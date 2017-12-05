import sys
import json
import collections
import argparse

class Redish():
    def __init__(self, maxKeys):
        self.database = collections.OrderedDict()
        self.conectionIDs = set()
        self.nextConnectionID = 1
        self.maxKeys = maxKeys
        self.transactionQueues = {}
        self.connectionsWithTransactionInputErrors = set()
        self.watchedKeysForConnectionID = collections.defaultdict(set)
        self.connectionIDsWithWatchViolations = set()

    def _set(self, key, value, request):
        # Need to identify if this database write is being watched
        connectionID = request["id"]
        if key in self.watchedKeysForConnectionID[connectionID]:
            self.connectionIDsWithWatchViolations.add(connectionID)

        # Then move on with the writing
        if key in self.database:
            # We need to delete to maintain the LRU order
            del self.database[key]
        self.database[key] = value
        if len(self.database) > self.maxKeys:
            # Get the oldest item
            key, value = self.database.popitem(False)
            return [key, value]
        return []

    def _get(self, key):
        value = ""
        if key in self.database:
            value = self.database[key]
            # Need to evict key and re add to update the LRU
            del self.database[key]
            self.database[key] = value
        return value

    def _enqueueRequest(self, request):
        # Detect if we're in a MULTI block and enqueue instead of executing
        connectionID = request["id"]
        if connectionID in self.transactionQueues:
            self.transactionQueues[connectionID].append(request)
            return {"status": "QUEUED"}
        return

    def _reportErrorForTransaction(self, request):
        connectionID = request["id"]
        if connectionID in self.transactionQueues:
            self.connectionsWithTransactionInputErrors.add(connectionID)

    def handleCONNECT(self, request):
        if "args" in request and len(request["args"]) != 0:
            return {"status": "ERROR",
                    "detail": "CONNECT has no arguments"}
        newID = self.nextConnectionID
        self.nextConnectionID += 1
        self.conectionIDs.add(newID)
        return {"status": "OK", "id": newID}

    def handleDISCONNECT(self, request):
        if "args" in request and len(request["args"]) != 0:
            return {"status": "ERROR",
                    "detail": "DISCONNECT has no arguments"}
        self.conectionIDs.remove(request["id"])
        return {"status": "OK"}

    def handleSET(self, request):
        if "args" not in request or len(request["args"]) != 2:
            self._reportErrorForTransaction(request)
            return {"status": "ERROR",
                    "detail": "SET requires two arguments: key and value"}

        enqueue = self._enqueueRequest(request)
        if enqueue:
            return enqueue

        key = request["args"][0]
        value = request["args"][1]
        evicted = self._set(key, value, request)
        response = {"status": "OK"}
        if evicted:
            response["evicted"] = evicted
        return response

    def handleGET(self, request):
        if "args" not in request or len(request["args"]) != 1:
            self._reportErrorForTransaction(request)
            return {"status": "ERROR",
                    "detail": "GET requires one argument: key"}

        enqueue = self._enqueueRequest(request)
        if enqueue:
            return enqueue

        key = request["args"][0]
        value = self._get(key)
        return {"status": "OK", "result": value}

    def handleMGET(self, request):
        if "args" not in request or len(request["args"]) < 1:
            self._reportErrorForTransaction(request)
            return {"status": "ERROR",
                    "detail": "MGET requires at least one argument: key [key ...]"}

        enqueue = self._enqueueRequest(request)
        if enqueue:
            return enqueue

        results = []
        for key in request["args"]:
            results.append(self._get(key))
        return {"status": "OK", "result": results}

    def handleMSET(self, request):
        err = {"status": "ERROR",
               "detail": "MSET requires at least one pair of arguments: key value [key value ...]"}

        if "args" not in request:
            # If arguments are missing, report error
            self._reportErrorForTransaction(request)
            return err

        argLen = len(request["args"])
        if argLen == 0 or argLen % 2 == 1:
            # If arguments don't exist in pairs, report error
            self._reportErrorForTransaction(request)
            return err

        enqueue = self._enqueueRequest(request)
        if enqueue:
            return enqueue

        evicted = []
        for i in range(0, argLen, 2):
            # Iterate through argument pairs
            key = request["args"][i]
            value = request["args"][i+1]
            evicted.extend(self._set(key, value, request))
        response = {"status": "OK"}
        if evicted:
            response["evicted"] = evicted
        return response

    def handleINCRDECR(self, request):
        incrementAmount = 1
        cmd = request["command"]
        if cmd == "DECR":
            incrementAmount = -1
        if "args" not in request or len(request["args"]) != 1:
            self._reportErrorForTransaction(request)
            return {"status": "ERROR",
                    "detail": "%s requires one argument: key" % cmd}

        enqueue = self._enqueueRequest(request)
        if enqueue:
            return enqueue

        key = request["args"][0]
        if key not in self.database:
            # Key not present, so incr an implied 0. Same as setting incrementAmount directly
            evicted = self._set(key, incrementAmount, request)
            response = {"status": "OK", "result": incrementAmount}
            # New entry could evict an old one
            if evicted:
                response["evicted"] = evicted
            return response

        # Existing value that needs to be altered in place
        value = self.database[key]
        if type(value) is not int:
            return {"status": "ERROR",
                    "detail": "%s works only on 64 bit signed integers" % cmd}
        newValue = value + incrementAmount
        if type(newValue) is not int:
            return {"status": "ERROR",
                    "detail": "%s would overflow" % cmd}
        # Don't need to check for eviction, because this was already in the db
        self._set(key, newValue, request)
        response = {"status": "OK", "result": newValue}
        return response

    def handleMULTI(self, request):
        if "args" in request and len(request["args"]) > 0:
            return {"status": "ERROR",
                    "detail": "MULTI should have no arguments"}
        connectionID = request["id"]
        if connectionID in self.transactionQueues:
            return {"status": "ERROR",
                    "detail": "MULTI calls can not be nested"}
        self.transactionQueues[connectionID] = []
        return {"status": "OK"}

    def handleEXEC(self, request):
        if "args" in request and len(request["args"]) > 0:
            return {"status": "ERROR",
                    "detail": "EXEC should have no arguments"}
        connectionID = request["id"]
        if connectionID not in self.transactionQueues:
            return {"status": "ERROR",
                    "detail": "EXEC called without MULTI"}
        transactionQueue = self.transactionQueues.pop(connectionID)
        if connectionID in self.connectionsWithTransactionInputErrors:
            return {"status": "ERROR",
                    "detail": "Transaction discarded because of previous errors"}
        results = []
        if connectionID in self.connectionIDsWithWatchViolations:
            # If there was a watch violation, don't execute, return no results
            self.connectionIDsWithWatchViolations.remove(connectionID)
            del self.watchedKeysForConnectionID[connectionID]
            return {"status": "OK"}

        for command in transactionQueue:
            results.append(self.processRequest(command))
        del self.watchedKeysForConnectionID[connectionID]
        return {"status": "OK", "results": results}

    def handleDISCARD(self, request):
        if "args" in request and len(request["args"]) > 0:
            return {"status": "ERROR",
                    "detail": "DISCARD should have no arguments"}
        connectionID = request["id"]
        if connectionID not in self.transactionQueues:
            return {"status": "ERROR",
                    "detail": "DISCARD called without MULTI"}
        del self.transactionQueues[connectionID]
        return {"status": "OK"}

    def handleWATCH(self, request):
        if "args" not in request or len(request["args"]) != 1:
            return {"status": "ERROR",
                    "detail": "WATCH requires one argument: key"}
        connectionID = request["id"]
        key = request["args"][0]
        self.watchedKeysForConnectionID[connectionID].add(key)
        return {"status": "OK"}

    def handleUNWATCH(self, request):
        if "args" in request and len(request["args"]) != 0:
            return {"status": "ERROR",
                    "detail": "UNWATCH should have no arguments"}
        connectionID = request["id"]
        del self.watchedKeysForConnectionID[connectionID]
        return {"status": "OK"}

    def processRequestJSON(self, jsonRequest):
        try:
            request = json.loads(jsonRequest)
        except ValueError:
            return json.dumps(
                    {"status": "ERROR", "detail": "could not parse json"})
        return json.dumps(self.processRequest(request))

    def processRequest(self, request):
        if "command" not in request:
            return {"status": "ERROR",
                    "detail": "'command' not present in request"}

        command = request["command"]

        # Connect command doesn't supply an ID. All others must.
        if command == "CONNECT":
            return self.handleCONNECT(request)

        # Check for id presence first
        if "id" not in request:
            return {"status": "ERROR", "detail": "id not supplied"}
        thisId = request['id']
        if thisId not in self.conectionIDs:
            return {"status": "ERROR", "detail": "id %u not known" % thisId}

        if command == "DISCONNECT":
            return self.handleDISCONNECT(request)
        if command == "SET":
            return self.handleSET(request)
        if command == "GET":
            return self.handleGET(request)
        if command == "MGET":
            return self.handleMGET(request)
        if command == "MSET":
            return self.handleMSET(request)
        if command == "INCR" or command == "DECR":
            return self.handleINCRDECR(request)
        if command == "MULTI":
            return self.handleMULTI(request)
        if command == "EXEC":
            return self.handleEXEC(request)
        if command == "DISCARD":
            return self.handleDISCARD(request)
        if command == "WATCH":
            return self.handleWATCH(request)
        if command == "UNWATCH":
            return self.handleUNWATCH(request)

        # Unhandled command
        return {"status": "ERROR",
                "detail": "command '%s' not found" % command}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("maxKeys")
    args = parser.parse_args()
    instance = Redish(args.maxKeys)
    line = sys.stdin.readline()
    while line != '':
        print instance.processRequestJSON(line)
        line = sys.stdin.readline()
