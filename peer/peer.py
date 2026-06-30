"""
Peer node — multipoint messaging with naming-service discovery.
Implements Causal-Order (CO) consistency via vector clocks.

Startup sequence
----------------
1. Read NAME_SERVICE_ADDRESS (the only static address in the config).
2. Wait for naming service to be reachable.
3. bind(PEER_NAME, PEER_HOST:PEER_PORT)  — register address.
4. register(PEER_NAME, "peer")            — mark process type.
5. Start gRPC server to receive messages.
6. Periodically discover("peer") and BROADCAST messages to ALL peers.
7. On SIGTERM: unbind(PEER_NAME) then exit cleanly.

Causal-Order (CO) algorithm
-----------------------------
Each peer P keeps a vector clock VC_P (dict peer→int, default 0).

SEND:
  - Increment VC_P[P]
  - Piggyback a copy of VC_P on the message
  - Multicast the message to all currently known peers

RECEIVE (message from Q with piggybacked VC_msg):
  - Deliverable iff:
      VC_msg[Q] == VC_P[Q] + 1         (next message from Q)
      VC_msg[X] <= VC_P[X]  ∀ X ≠ Q   (we've seen everything Q had seen)
  - If deliverable:  deliver immediately, update VC_P = max(VC_P, VC_msg)
  - If not yet:      add to hold-back queue (HBQ); retry after every delivery
"""

import os
import sys
import time
import random
import signal
import threading
import logging
from concurrent import futures

import grpc

sys.path.insert(0, "/app")
import naming_pb2
import naming_pb2_grpc
import peer_pb2
import peer_pb2_grpc

# ── config ────────────────────────────────────────────────────────────────────

PEER_NAME       = os.environ["PEER_NAME"]
PEER_PORT       = int(os.environ.get("PEER_PORT", "50070"))
PEER_HOST       = os.environ.get("PEER_HOST", PEER_NAME)
NAME_SERVICE    = os.environ["NAME_SERVICE_ADDRESS"]

MSG_INTERVAL_LO = float(os.environ.get("MSG_INTERVAL_LO", "3.0"))
MSG_INTERVAL_HI = float(os.environ.get("MSG_INTERVAL_HI", "7.0"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{PEER_NAME}] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_my_address = f"{PEER_HOST}:{PEER_PORT}"

# ── naming-service helpers ────────────────────────────────────────────────────

def _ns_stub() -> naming_pb2_grpc.NamingServiceStub:
    return naming_pb2_grpc.NamingServiceStub(grpc.insecure_channel(NAME_SERVICE))


def _wait_for_naming_service(retries: int = 30, delay: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            ch = grpc.insecure_channel(NAME_SERVICE)
            grpc.channel_ready_future(ch).result(timeout=3)
            ch.close()
            log.info("Naming service ready at %s", NAME_SERVICE)
            return
        except Exception:
            log.warning("Naming service not ready yet (attempt %d/%d)…", attempt, retries)
            time.sleep(delay)
    raise RuntimeError(f"Naming service at {NAME_SERVICE} never became ready")


def bind_and_register():
    ns = _ns_stub()
    r = ns.Bind(naming_pb2.BindRequest(name=PEER_NAME, address=_my_address))
    if not r.ok:
        raise RuntimeError(f"bind failed: {r.message}")
    log.info("Bound   %s  →  %s", PEER_NAME, _my_address)

    r = ns.Register(naming_pb2.RegisterRequest(name=PEER_NAME, type="peer"))
    if not r.ok:
        raise RuntimeError(f"register failed: {r.message}")
    log.info("Registered %s as type=peer", PEER_NAME)


def unbind():
    try:
        r = _ns_stub().Unbind(naming_pb2.UnbindRequest(name=PEER_NAME))
        if r.ok:
            log.info("Unbound %s from naming service", PEER_NAME)
    except Exception as e:
        log.warning("Unbind failed: %s", e)


def discover_peers() -> list[tuple[str, str]]:
    try:
        r = _ns_stub().Discover(naming_pb2.DiscoverRequest(type="peer"))
        return [(p.name, p.address) for p in r.processes if p.name != PEER_NAME]
    except Exception as e:
        log.warning("Discover failed: %s", e)
        return []

# ── causal-order consistency (vector clocks) ─────────────────────────────────

# Single lock protecting all CO state
_co_lock = threading.Lock()

_vc: dict[str, int] = {}           # local vector clock; missing entry == 0
_hbq: list[tuple] = []             # hold-back queue: (sender, content, vc_dict)
_msg_log: list[tuple] = []         # delivered messages: (sender, content, vc_dict)


def _vc_str(vc: dict) -> str:
    return "{" + ", ".join(f"{k}:{v}" for k, v in sorted(vc.items())) + "}"


def _is_deliverable(sender: str, msg_vc: dict) -> bool:
    """CO delivery condition (caller must hold _co_lock)."""
    # 1. This must be the immediate next message from 'sender'
    if msg_vc.get(sender, 0) != _vc.get(sender, 0) + 1:
        return False
    # 2. We must have seen every message that 'sender' had already seen
    for peer, count in msg_vc.items():
        if peer != sender and count > _vc.get(peer, 0):
            return False
    return True


def _deliver(sender: str, content: str, msg_vc: dict):
    """Deliver message: update VC and log (caller must hold _co_lock)."""
    for peer, count in msg_vc.items():
        _vc[peer] = max(_vc.get(peer, 0), count)
    _msg_log.append((sender, content, msg_vc.copy()))
    log.info("DELIVER  ← %-20s  \"%s\"  vc=%s", sender, content, _vc_str(msg_vc))


def _drain_hbq():
    """Try to deliver queued messages now that VC advanced (caller must hold _co_lock)."""
    delivered_any = True
    while delivered_any:
        delivered_any = False
        for i, (s, c, vc) in enumerate(_hbq):
            if _is_deliverable(s, vc):
                _deliver(s, c, vc)
                _hbq.pop(i)
                delivered_any = True
                break   # list changed — restart scan


def co_receive(sender: str, content: str, msg_vc: dict):
    """Entry point for every incoming message (called from gRPC thread)."""
    with _co_lock:
        if _is_deliverable(sender, msg_vc):
            _deliver(sender, content, msg_vc)
            _drain_hbq()
        else:
            log.info(
                "HBQ      ← %-20s  \"%s\"  vc=%s  [buffered, waiting]",
                sender, content, _vc_str(msg_vc),
            )
            _hbq.append((sender, content, msg_vc.copy()))


def co_prepare_send() -> dict:
    """Increment own clock entry and return a snapshot to piggyback on send."""
    with _co_lock:
        _vc[PEER_NAME] = _vc.get(PEER_NAME, 0) + 1
        return dict(_vc)

# ── peer-to-peer messaging ────────────────────────────────────────────────────

_peer_channels: dict[str, grpc.Channel] = {}
_peer_channels_mu = threading.Lock()


def _peer_stub(address: str) -> peer_pb2_grpc.PeerServiceStub:
    with _peer_channels_mu:
        if address not in _peer_channels:
            _peer_channels[address] = grpc.insecure_channel(address)
        return peer_pb2_grpc.PeerServiceStub(_peer_channels[address])


def broadcast(peers: list[tuple[str, str]], content: str):
    """Increment VC once, then multicast the message to every known peer."""
    vc = co_prepare_send()
    log.info("SEND     → %-3d peers  \"%s\"  vc=%s", len(peers), content, _vc_str(vc))
    for name, addr in peers:
        try:
            req = peer_pb2.MessageRequest(sender=PEER_NAME, content=content)
            req.vector_clock.update(vc)
            _peer_stub(addr).SendMessage(req, timeout=3.0)
        except grpc.RpcError as e:
            log.warning("Could not reach %s @ %s: %s", name, addr, e.details())


def _message_loop():
    while True:
        time.sleep(random.uniform(MSG_INTERVAL_LO, MSG_INTERVAL_HI))
        peers = discover_peers()
        if not peers:
            log.debug("No peers found in naming service")
            continue

        # Occasionally reply to the last delivered message (causal chain demo)
        with _co_lock:
            last = _msg_log[-1] if _msg_log else None

        if last and random.random() < 0.4:
            last_sender, _, _ = last
            content = f"[{PEER_NAME}→reply:{last_sender}] #{random.randint(10, 99)}"
        else:
            content = f"[{PEER_NAME}] token={random.randint(1000, 9999)}"

        broadcast(peers, content)

# ── gRPC server ───────────────────────────────────────────────────────────────

class PeerServicer(peer_pb2_grpc.PeerServiceServicer):
    def SendMessage(self, request, context):
        msg_vc = dict(request.vector_clock)
        co_receive(request.sender, request.content, msg_vc)
        return peer_pb2.MessageResponse(ok=True)


def _start_grpc_server() -> grpc.Server:
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    peer_pb2_grpc.add_PeerServiceServicer_to_server(PeerServicer(), srv)
    srv.add_insecure_port(f"[::]:{PEER_PORT}")
    srv.start()
    log.info("gRPC server listening on port %d", PEER_PORT)
    return srv

# ── main ──────────────────────────────────────────────────────────────────────

def main():
    _wait_for_naming_service()
    bind_and_register()

    srv = _start_grpc_server()

    def _shutdown(signum, frame):
        log.info("Shutting down…")
        unbind()
        srv.stop(grace=3)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    threading.Thread(target=_message_loop, daemon=True).start()

    log.info("Peer %s active  [ns=%s  addr=%s]", PEER_NAME, NAME_SERVICE, _my_address)
    log.info("CO model: causal ordering via vector clocks")
    srv.wait_for_termination()


if __name__ == "__main__":
    main()
