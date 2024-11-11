# stats.py
#
# Runtime Statistics


from dataclasses import dataclass, field


@dataclass
class ConnStats:
    sent: int = 0
    recv: int = 0
    to_send_bytes: int = 0
    to_recv_bytes: int = 0
    avg_sent: int = 0.0
    avg_recv: int = 0.0
    sent_bytes: int = 0
    recv_bytes: int = 0
    poll_times: int = 0
    OOO_times: int = 0
    send_bloat_times: int = 0
    tr_orders: list[int] = field(default_factory=list)  # default value for tr_orders
