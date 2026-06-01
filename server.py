from concurrent import futures

import grpc
from cacheout import Cache

import kvstore_pb2
import kvstore_pb2_grpc


class KeyStoreServicer(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self.store = Cache(maxsize=10)

    def Put(self, request, context):
        key = request.key
        value = request.value
        ttl = request.ttl_seconds
        self.store.set(key, value, ttl=ttl)
        print(f"Put: {key} = {value} (TTL={ttl})")
        return kvstore_pb2.PutResponse()

    def Get(self, request, context):
        key = request.key

        value = self.store.get(key)
        if value is not None:
            return kvstore_pb2.GetResponse(value=value)
        context.set_code(grpc.StatusCode.NOT_FOUND)
        return kvstore_pb2.GetResponse()

    def Delete(self, request, context):
        key = request.key
        if key in self.store:
            self.store.delete(key)
        return kvstore_pb2.DeleteResponse()

    def List(self, request, context):
        prefix = request.prefix
        items = []

        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                value = self.store.get(key)
                if value is not None:
                    items.append(kvstore_pb2.KeyValue(key=key, value=value))
        return kvstore_pb2.ListResponse(items=items)


def start_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyStoreServicer(), server
    )
    server.add_insecure_port("[::]:8000")
    print("start server on port 8000")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    start_server()
