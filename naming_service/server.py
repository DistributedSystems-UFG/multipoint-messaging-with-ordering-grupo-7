"""
Naming Service — flat, no hierarchy, no replication.

Operations
----------
bind(name, address)     — create name→address record; ok/error
lookup(name)            — return address for name; error if not found
unbind(name)            — remove name and its record
register(name, type)    — attach a type to a bound name; error if name unknown
discover(type)          — return all (name, address) pairs with the given type

Also acts as a Directory Service: discover("peer") replaces the Group Manager.
"""

import os
import sys
import threading
import logging
from concurrent import futures

import grpc

sys.path.insert(0, "/app")
import naming_pb2
import naming_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [NAMING-SVC] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


class NamingServicer(naming_pb2_grpc.NamingServiceServicer):
    def __init__(self):
        self._mu = threading.Lock()
        # registry: name -> {"address": str, "type": str | None}
        self._registry: dict = {}

    def Bind(self, request, context):
        name, addr = request.name.strip(), request.address.strip()
        if not name or not addr:
            return naming_pb2.BindResponse(ok=False, message="name and address required")
        with self._mu:
            if name in self._registry:
                self._registry[name]["address"] = addr
                log.info("REBIND  %-20s  %s", name, addr)
                return naming_pb2.BindResponse(ok=True, message="rebound")
            self._registry[name] = {"address": addr, "type": None}
        log.info("BIND    %-20s  %s", name, addr)
        return naming_pb2.BindResponse(ok=True, message="ok")

    def Lookup(self, request, context):
        name = request.name.strip()
        with self._mu:
            entry = self._registry.get(name)
        if entry is None:
            log.info("LOOKUP  %-20s  NOT FOUND", name)
            return naming_pb2.LookupResponse(ok=False, message="name not found")
        log.info("LOOKUP  %-20s  %s", name, entry["address"])
        return naming_pb2.LookupResponse(ok=True, address=entry["address"])

    def Unbind(self, request, context):
        name = request.name.strip()
        with self._mu:
            if name not in self._registry:
                return naming_pb2.UnbindResponse(ok=False, message="name not found")
            del self._registry[name]
        log.info("UNBIND  %-20s", name)
        return naming_pb2.UnbindResponse(ok=True, message="ok")

    def Register(self, request, context):
        name, ptype = request.name.strip(), request.type.strip()
        if not ptype:
            return naming_pb2.RegisterResponse(ok=False, message="type required")
        with self._mu:
            if name not in self._registry:
                log.warning("REGISTER %-20s  FAIL (not bound)", name)
                return naming_pb2.RegisterResponse(ok=False, message="name not found; call bind first")
            self._registry[name]["type"] = ptype
        log.info("REGISTER %-20s  type=%s", name, ptype)
        return naming_pb2.RegisterResponse(ok=True, message="ok")

    def Discover(self, request, context):
        ptype = request.type.strip()
        with self._mu:
            results = [
                naming_pb2.ProcessInfo(name=n, address=e["address"])
                for n, e in self._registry.items()
                if e["type"] == ptype
            ]
        log.info("DISCOVER type=%-10s  %d result(s)", ptype, len(results))
        return naming_pb2.DiscoverResponse(processes=results)


def serve():
    port = os.environ.get("PORT", "50050")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    naming_pb2_grpc.add_NamingServiceServicer_to_server(NamingServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    log.info("Naming service listening on port %s", port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=5)


if __name__ == "__main__":
    serve()
