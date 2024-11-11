# __init__.py
#
# Top-level functions for client

from .connection import ReFlexConnection, ControllerConnection
from .context import ReFlexContext
import asyncio
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional

try:
    import boto3
    NOBOTO3 = False
except ImportError:
    NOBOTO3 = True
    print("boto3 not imported, stateful invoke API will not work")


@dataclass
class JobCtx:
    # required
    app_name: str = "default"
    capacity: int = 1000
    # job-level SLO
    IOPS_SLO: int = 0
    latency_us_SLO: int = 0
    rw_ratio: int = 0
    # hints
    req_size: int = 8
    concurrency: int = 1
    # used to derive regions and flows
    num_regions: int = 1
    encoded_DAG_len: int = 0


@dataclass
class RegionCtx:
    # capacity
    capacity: int = 200
    # hints
    num_flows: int = 1
    learned_flow_pattern: Optional[Dict[str, Any]] = None


@dataclass
class FlowCtx:
    # flow-level SLO
    IOPS_SLO: int = 0
    latency_us_SLO: int = 0
    # hints
    req_size: int = 8
    rw_ratio: int = 0
    sequential: bool = True
    persistent: bool = False

    def add_hints(self, sequential, persistent):
        self.sequential = sequential
        self.persistent = persistent


INT64_DIV = 2 << 64


def hash_name_long(name: str) -> int:
    """Simple hash function that supports unique id of fewer than 8 characters"""
    value = 0
    for ch in name:
        value = value << 8
        value += ord(ch)
    return value % INT64_DIV

def hash_name(s: str) -> int:
    """Convert a string to a 64-bit integer efficiently."""
    h = 0
    for char in s:
        h = (h * 31 + ord(char)) & 0xFFFFFFFFFFFFFFFF
    return h


class ReFlexHandler(object):
    """Class to wrap storage/controller connection and function invocation"""

    def __init__(
        self,
        controller_addr,
        app_name,
        loop=None,
        ctx=None,
        impl="OX",
        user_opts=[],
    ):
        self.control_conn = ControllerConnection(controller_addr, self)
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_running_loop()
        # convert a name to a unique 64 bit id
        self.app_name = hash_name(app_name)
        self.allocated = False
        self.use_socket = False
        self.user_opts = []
        self.send_max = 256
        self.pending_conn_tasks = []
        if not NOBOTO3:
            self.session = boto3.session.Session()
            self.boto_client = self.session.client("lambda")
        assert impl in ["O0", "O1", "O2", "O3", "O4", "OX"]
        try:
            opt_level = int(impl[1])
        except ValueError:
            opt_level = -1
        if opt_level < 0:
            self.user_opts = user_opts
            if "use_socket" in user_opts:
                self.use_socket = True
        if opt_level > 0:
            self.use_socket = True
            self.user_opts = [
                "NODELAY",
                "QUICKACK",
                "NONBLOCKING",
            ]  # three TCP configs
        if opt_level > 1:
            self.user_opts += ["enable_qc"]
        if opt_level > 2:
            self.user_opts += ["enable_fc"]
        if opt_level > 3:
            self.user_opts += ["enable_sca"]
        if user_opts:
            if "smax_4" in user_opts:
                self.send_max = 4
            if "smax_16" in user_opts:
                self.send_max = 16
            if "smax_32" in user_opts:
                self.send_max = 32
            if "smax_64" in user_opts:
                self.send_max = 64
        if ctx:
            # context reconstructed, not need to allocate
            self.jid = ctx["jid"]
            # FIXME: ugly code here
            self.control_conn.jid = self.jid
            self.allocated = True
            self.state_ctx = ReFlexContext(invoker_state=ctx, name=self.app_name)
            self.storage_conn = ReFlexConnection(
                handler=self,
                loop=self.loop,
                use_sock=self.use_socket,
                user_opts=self.user_opts,
                boto_cli=self.boto_client if not NOBOTO3 else None,
                send_max=self.send_max,
            )
            

    async def allocate(self, name="", job: JobCtx = None):
        """request controller to allocate new capacity for ephemral state"""
        if job.app_name != "default" and hash_name(job.app_name) != self.app_name:
            raise Exception("App name set for a new job, but does not match the"
                            f"handler {hash_name(job.app_name)} != {self.app_name}")
        async with self.control_conn as conn:
            ctx = await conn.allocate_capacity(self.app_name, job)
            if ctx:
                # context does not have handler
                self.jid = ctx.jid
                self.control_conn.jid = self.jid
                self.state_ctx = ctx
                self.storage_conn = ReFlexConnection(
                    handler=self,
                    loop=self.loop,
                    use_sock=self.use_socket,
                    user_opts=self.user_opts,
                    boto_cli=self.boto_client if not NOBOTO3 else None,
                    send_max=self.send_max,
                )
            else:
                raise Exception("No context returned, controller reached its capacity!")
        self.allocated = True

    async def free(self):
        """request controller to free the allocated capacity for ephemral state"""
        async with self.control_conn as conn:
            await conn.free_capacity()

    def register(self, flow: FlowCtx = None):
        """register new flow with the controller and return a
        storage connection to the serverless function"""
        if not self.allocated:
            raise Exception("Please allocate necessary capacity first")
        self.pending_conn_tasks = [
            self.loop.create_task(self.storage_conn.setup_new_flow(flow))
        ]
        self.pending_conn_tasks.append(
            self.loop.create_task(self.control_conn.register_flow(flow))
        )

        self.storage_conn.add_exit_callback(self.control_conn.deregister_job)
        self.storage_conn.add_exit_callback(self.control_conn.flush_context)
        # storage conn does not need controller info after registration
        return self.storage_conn

    async def is_connected(self):
        await asyncio.gather(*self.pending_conn_tasks)
        if self.storage_conn.conn:
            return True
        return False

    def get_precond_metadata(self, flow_size):
        self.state_ctx.generate_dummy_metadata(flow_size)

    def __blocking_invoke(self, name, args):
        return self.boto_client.invoke(
            FunctionName=name,
            InvocationType="RequestResponse",
            Payload=json.dumps(args),
        )
    
    def get_invokee_state(self):
        return self.state_ctx.get_invokee_state()

    async def invoke(
        self,
        name,
        args,
        reserve_capacity=0, # in LBAs
        update_ctx=False,
        invoke_type="RR",
    ):
        """wrap the context and send it to the next function"""
        if NOBOTO3:
            print("boto3 not imported, only the controller part will work")
            return
        invokee_state = self.state_ctx.get_invokee_state(re_cap = reserve_capacity)
        if update_ctx:
            await self.control_conn.flush_context()
        # FIXME: we may need to encrypt the context if needed
        args["invoker_ctx"] = invokee_state
        if invoke_type == "SQS":
            self.sqs_client = boto3.client("sqs")
            self.sqs_client.send_message(
                QueueUrl=name,
                MessageBody=json.dumps(args),
            )
        elif invoke_type == "RR":
            # blocking wait
            ret = await self.loop.run_in_executor(
                None,
                self.__blocking_invoke,
                name,
                args,
            )
            reader_ret = ret["Payload"].read()
            # we assume the new context is returned
            ret = json.loads(reader_ret)
            if "invokee_ctx" in ret:
                self.state_ctx.merge_state_with_curr(ret["invokee_ctx"])
            return ret
        else:
            assert False, "select# from invocation types {SQS, RR}"
