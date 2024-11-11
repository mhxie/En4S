LWIP_SRC_NAMES = [
    '../api/err.c',
    'def.c',
    # 'dns.c',
    'inet_chksum.c',
    # 'mem.c',
    # 'raw.c',
    'stats.c',
    'sys.c',
    # 'altcp.c',
    # 'altcp_alloc.c',
    # 'altcp_tcp.c',
    # 'udp.c',
]
lwip_sources = []

foreach NAME : LWIP_SRC_NAMES
    lwip_sources += join_paths('..', LWIP_ROOT, 'core', NAME)
endforeach

foreach source : lwip_sources
    core_sources += files(source)
endforeach

lwip_if_sources = [
# IX infusion 
    # 'net.h', 'net.c',
    'tcp_api.c', 
    # 'misc.c', 'misc.h',
    'net.c', 'net.h',  
# Modified LWIP sources
    'memp.c', 'memp.h', 'pbuf.c',
# Not modified LWIP sources, only replace memp.h with our own
# If you want to use the latest LWIP, copy the following files
# and replace the memp.h with lwip_if/memp.h
    'init.c', 'netif.c',
    'etharp.c', 'ethernet.c',
    'icmp.c', 'ip.c', 'ip4.c',
    'ip4_addr.c', 'ip4_frag.c',
    'tcp.c', 'tcp_in.c', 'tcp_out.c',
    'timeouts.c',
# also replace the memp.h at lwip/stats.h
]


foreach source : lwip_if_sources
    core_sources += files(source)
endforeach
