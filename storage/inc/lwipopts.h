/*
 * Copyright (C) 2023 Joan Lled� <jlledom@member.fsf.org>
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 */

#ifndef LWIP_LWIPOPTS_H
#define LWIP_LWIPOPTS_H

/* Prevent having to link sys_arch.c */
#define NO_SYS                          1
#define LWIP_NETCONN                    0
#define LWIP_SOCKET                     0
#define SYS_LIGHTWEIGHT_PROT            0

/* Sockets API config */
#define LWIP_COMPAT_SOCKETS       0
#define LWIP_SOCKET_OFFSET        1
#define LWIP_POLL                 1

/* Use Glibc malloc()/free() */
#define MEM_LIBC_MALLOC   0
#define MEMP_MEM_MALLOC   0
#define MEM_USE_IX_POOLS     1
#define MEMP_USE_CUSTOM_POOLS 1
#define LWIP_SUPPORT_CUSTOM_PBUF 1

/* Only send complete packets to the device */
#define LWIP_NETIF_TX_SINGLE_PBUF 1

// /* Randomize local ports */
// #define LWIP_RANDOMIZE_INITIAL_LOCAL_PORTS  1

// /* Glibc sends more than one packet in a row during an ARP resolution */
// #define ARP_QUEUEING    1
// #define ARP_QUEUE_LEN   10
#define ETHARP_SUPPORT_STATIC_ENTRIES   1

/*
 * Activate loopback, but don't use lwip's default loopback interface,
 * we provide our own.
 */
#define LWIP_NETIF_LOOPBACK   1
#define LWIP_HAVE_LOOPIF      0

/* IPv4 stuff */
#define IP_FORWARD  0

/* TCP tuning */
// #define TCP_MSS         1460
// #define TCP_WND         0xFFFF
// #define LWIP_WND_SCALE  1
// #define TCP_RCV_SCALE   0x1
// #define TCP_SND_BUF     TCP_WND

// This will cause data corruption for SET workload
#define TCP_QUEUE_OOSEQ   0
#define TCP_LR_CACHE
#define TCP_SEQUENT
#define TCP_PCB_CHAIN_SIZE 1024
#define TCP_MTF

// #define TCP_MSS           4120
#define TCP_MSS           1460
#define TCP_WND           1 << 15    // client: 1 << 16
#define LWIP_WND_SCALE    1
#define TCP_RCV_SCALE     0x7
#define TCP_SND_BUF       TCP_MSS * 16 // client: TCP_MSS * 2
#define TCP_SND_QUEUELEN  (2 * TCP_SND_BUF/TCP_MSS)
#define TCP_SNDLOWAT      TCP_SND_BUF / 8
#define TCP_SNDQUEUELOWAT TCP_SND_QUEUELEN / 4
#define MEMP_NUM_TCP_SEG  TCP_SND_QUEUELE

// assert LWIP_PBUF_MEMPOOL is not defined
#define MEMP_NUM_PBUF           1 * 1024  // ROM? PBUF REFERENCE?
#define PBUF_POOL_SIZE          4 * 1024  // PBUF with payload, TX only
                                          // RX is using zero-copy mbuf pool
#define MEMP_NUM_TCP_PCB        1024
#define MEMP_NUM_TCP_PCB_LISTEN 1024
#define MEMP_NUM_TCP_SEG        1024
#define MEMP_NUM_SYS_TIMEOUT    1024
// #define MEMP_NUM_REASSDATA      1024
// #define IP_REASS_MAX_PBUFS      1024

/* Throughput settings */
// #define LWIP_CHECKSUM_ON_COPY   1
// IX Settings
#define CHECKSUM_CHECK_IP 0     //offloaded
#define CHECKSUM_CHECK_TCP 0    //offloaded
#define LWIP_INLINE_IP_CHKSUM 1
#define CHECKSUM_GEN_IP 0       //offloaded
#define CHECKSUM_GEN_TCP 0      //offloaded
// #define TCP_ACK_DELAY (1 * ONE_MS)
// #define LWIP_TCP_RTO_TIME (500 * ONE_MS)
#define LWIP_NOASSERT
#define LWIP_EVENT_API 1
#define LWIP_NETIF_HWADDRHINT 1

/* Stats Settings */
// #define LWIP_STATS          1
// #define LWIP_STATS_LARGE    1
// #define LWIP_STATS_DISPLAY  1

#if LWIP_STATS
#define LINK_STATS              0
#define IP_STATS                0
#define ICMP_STATS              0
#define IGMP_STATS              0
#define IPFRAG_STATS            0
#define ETHARP_STATS            0
#define UDP_STATS               0
#define TCP_STATS               1
#define MEM_STATS               0
#define MEMP_STATS              0
#define PBUF_STATS              0
#define SYS_STATS               0
#endif /* LWIP_STATS */

/* Enable modules */
#define LWIP_ARP              1
#define LWIP_ETHERNET         1
#define LWIP_IPV4             1
#define LWIP_ICMP             1
#define LWIP_IGMP             0
#define LWIP_RAW              0
#define LWIP_UDP              0
#define LWIP_UDPLITE          0
#define LWIP_TCP              1
#define LWIP_IPV6             0
#define LWIP_ICMP6            0
#define LWIP_IPV6_MLD         0
#define IP_FRAG               0
#define IP_REASSEMBLY         0

// /* Don't abort the whole stack when an error is detected */
// #define LWIP_NOASSERT_ON_ERROR   1

// /* Threading options */
// #define LWIP_TCPIP_CORE_LOCKING   1

#if !NO_SYS
void sys_check_core_locking(void);
#define LWIP_ASSERT_CORE_LOCKED()  sys_check_core_locking()
#if 0
void sys_mark_tcpip_thread(void);
#define LWIP_MARK_TCPIP_THREAD()   sys_mark_tcpip_thread()

#if LWIP_TCPIP_CORE_LOCKING
void sys_lock_tcpip_core(void);
#define LOCK_TCPIP_CORE()          sys_lock_tcpip_core()
void sys_unlock_tcpip_core(void);
#define UNLOCK_TCPIP_CORE()        sys_unlock_tcpip_core()
#endif
#endif
#endif

// #define LWIP_DEBUG 1
// #define ENABLE_DEBUG 1
#ifdef ENABLE_DEBUG
#define LWIP_DEBUG_LEVEL LWIP_DBG_LEVEL_SERIOUS
#else
#define LWIP_DEBUG_LEVEL LWIP_DBG_OFF
#endif

#define ETHARP_DEBUG      LWIP_DEBUG_LEVEL
#define NETIF_DEBUG       LWIP_DEBUG_LEVEL
#define PBUF_DEBUG        LWIP_DEBUG_LEVEL
#define API_LIB_DEBUG     LWIP_DEBUG_LEVEL
#define API_MSG_DEBUG     LWIP_DEBUG_LEVEL
#define SOCKETS_DEBUG     LWIP_DEBUG_LEVEL
#define ICMP_DEBUG        LWIP_DEBUG_LEVEL
#define IGMP_DEBUG        LWIP_DEBUG_LEVEL
#define INET_DEBUG        LWIP_DEBUG_LEVEL
#define IP_DEBUG          LWIP_DEBUG_LEVEL
#define IP_REASS_DEBUG    LWIP_DEBUG_LEVEL
#define RAW_DEBUG         LWIP_DEBUG_LEVEL
#define MEM_DEBUG         LWIP_DEBUG_LEVEL
#define MEMP_DEBUG        LWIP_DEBUG_LEVEL
#define SYS_DEBUG         LWIP_DEBUG_LEVEL
#define TIMERS_DEBUG      LWIP_DEBUG_LEVEL
#define TCP_DEBUG         LWIP_DEBUG_LEVEL
#define TCP_INPUT_DEBUG   LWIP_DEBUG_LEVEL
#define TCP_FR_DEBUG      LWIP_DEBUG_LEVEL
#define TCP_RTO_DEBUG     LWIP_DEBUG_LEVEL
#define TCP_CWND_DEBUG    LWIP_DEBUG_LEVEL
#define TCP_WND_DEBUG     LWIP_DEBUG_LEVEL
#define TCP_OUTPUT_DEBUG  LWIP_DEBUG_LEVEL
#define TCP_RST_DEBUG     LWIP_DEBUG_LEVEL
#define TCP_QLEN_DEBUG    LWIP_DEBUG_LEVEL
#define UDP_DEBUG         LWIP_DEBUG_LEVEL
#define TCPIP_DEBUG       LWIP_DEBUG_LEVEL
#define SLIP_DEBUG        LWIP_DEBUG_LEVEL
#define DHCP_DEBUG        LWIP_DEBUG_LEVEL
#define AUTOIP_DEBUG      LWIP_DEBUG_LEVEL
#define DNS_DEBUG         LWIP_DEBUG_LEVEL
#define IP6_DEBUG         LWIP_DEBUG_LEVEL


#endif /* UNIX_LWIP_LWIPOPTS_H */
