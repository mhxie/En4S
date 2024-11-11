import asyncio
from math import ceil

try:
    import asyncreflex
except ImportError:
    print("asyncreflex not installed")

try:
    import pocket as pkt
except ImportError:
    print("pocket not installed")

try:
    import thrift
except ImportError:
    print("jiffy's dependency thrift not installed")

try:
    import jiffy
except ImportError:
    print("jiffy not installed")

try:
    import boto3
except ImportError:
    print("boto3 not installed")

import json
import time
from dataclasses import dataclass


# FIXME: use FlowPolicy from asyncreflex directly
@dataclass
class JobCtx:
    # required
    app_name: str = "default"
    capacity: int = 1000      # capacity in KB
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


class AsyncESHandlerInterface:
    def __init__(self):
        self.lambda_client = None

    def register(self):
        pass

    async def is_connected():
        return True

    async def allocate(self, name, job):
        pass

    async def free(self):
        pass

    def __blocking_invoke(self, name, args):
        args["ctx"] = {}
        # print("blocking invoke")
        return self.lambda_client.invoke(
            FunctionName=name,
            InvocationType="RequestResponse",
            Payload=json.dumps(args),
        )

    async def invoke(self, name, args, reserve_capacity = None):
        if not self.lambda_client:
            self.lambda_client = boto3.client("lambda")
        loop = asyncio.get_running_loop()
        # print(f"Invocating {name} with {args}")
        ret = await loop.run_in_executor(
            None,
            self.__blocking_invoke,
            name,
            args,
        )
        reader_ret = ret["Payload"].read()
        return json.loads(reader_ret)

    def get_precond_metadata(self, flow_size: int):
        pass


class AsyncESConnectionInterface:
    def __init__(self):
        self.rate = 0  # closed-loop
        self.batch = 1
        self.outstanding = 0
        self.suggested = 0
        self.buffer_size = 65536 * 64
        self.buffer = bytearray(self.buffer_size)
        self.pos = 0
        self.start = time.time()

    def add_req_sent_cb(self, callback):
        """Accurate sent timestamp not implemented"""
        pass

    def set_req_done_cb(self, callback):
        self.callback = callback

    def get_suggested_num(self):
        if self.rate:
            ret = ceil(self.rate * (time.time() - self.start) - self.suggested)
        else:
            ret = self.batch - self.outstanding
        self.suggested += ret
        return ret

    def get_object(self, obj_name: str, delegate: bool):
        pass

    def put_object(self, obj: bytes, obj_name: str, delegate: bool):
        pass


class AsyncPocketHandler(AsyncESHandlerInterface):
    def __init__(self, addr: str, name: str):
        super(AsyncPocketHandler, self).__init__()
        self.addr = addr
        self.conn = None
        self.jobname = name
        self.default_dir = ""
        self.allocated_dirs = {
            "": 0,
        }
        self.capacity = 0
        self.controller_enabled = False
        self.registered = False
        self.deferred_allocations = []

    def __blocking_register(self):
        self.jobid = pkt.register_job(
            jobname=self.jobname,
            num_lambdas=self.flow_ctx.concurrency,
            capacityGB=self.capacity / 1024 / 1024,
            peakMbps=self.peakMbps,
            latency_sensitive=self.sensitive,
        )

    def __blocking_create(self, name: str):
        # FIXME: false arg placement
        print(f"Creating a new dir {self.jobid}")
        pkt.create_dir(self.conn.handle, self.jobid, "")

    async def register(self, flow_ctx):
        self.loop = asyncio.get_running_loop()
        self.flow_ctx = flow_ctx
        self.peakMbps = flow_ctx.IOPS_SLO * flow_ctx.req_size * 8 / 1000 / 1000
        self.sensitive = False
        if flow_ctx.latency_us_SLO:
            self.sensitive = True
        for _, capacity in self.deferred_allocations:
            self.capacity += capacity

        if self.controller_enabled and flow_ctx.rw_ratio == 0:
            # preconditioning
            await self.loop.run_in_executor(None, self.__blocking_register)
            print(f"New job registered, got jobid {self.jobid}")
        else:
            # already preconditioned, use the old jobid
            self.jobid = self.jobname + "-0"
        self.registered = True
        self.conn = AsyncPocketConnection(self, self.addr)
        # In pocket, regisration should happen before creating a dir
        for name, capacity in self.deferred_allocations:
            await self.allocate(name, capacity)
        self.deferred_allocations = []
        return self.conn

    async def allocate(self, name: str = "", job=None):
        if name:
            self.allocated_dirs[name] = job.capacity
        else:
            self.allocated_dirs[self.default_dir] = job.capacity
        if self.conn and self.registered:
            await self.loop.run_in_executor(
                None,
                self.__blocking_create,
                name,
            )
        else:
            self.deferred_allocations.append((name, job.capacity))

    async def free(self):
        """Pocket not supporting delete folder"""
        pass

    def get_jobid(self, dir_name: str):
        if dir_name:
            if dir_name in self.allocated_dirs.keys():
                return self.jobid + "/" + dir_name
        return self.jobid


class AsyncPocketConnection(AsyncESConnectionInterface):
    def __init__(self, handler, addr: str):
        super(AsyncPocketConnection, self).__init__()
        self._handler = handler
        self.handle = pkt.connect(addr, 9070)

    async def __aenter__(self):
        self.loop = asyncio.get_running_loop()
        return self

    async def __aexit__(self, *args, **kwargs):
        pass
        # pkt.close(self.handle) # a todo in pocket

    async def __blocking_read(
        self,
        obj_name: str,
        ret: bytes,
        size: int,
        jobid: int,
    ):
        self.outstanding += 1
        pkt.get_buffer(self.handle, obj_name, ret, size, jobid)
        self.callback(ret)
        self.outstanding -= 1

    async def __blocking_write(
        self,
        src: str,
        obj_name: str,
        size: int,
        jobid: int,
    ):
        self.outstanding += 1
        pkt.put_buffer(self.handle, src, size, obj_name, jobid)
        self.callback()
        self.outstanding -= 1

    async def __blocking_lookup(self, obj_name: str, jobid: int):
        self.outstanding += 1
        pkt.lookup(self.handle, obj_name, jobid)
        self.outstanding -= 1

    def get_object(
        self,
        obj_name: str,
        dir_name: str = "",
    ):
        jobid = self._handler.get_jobid(dir_name)
        size = self._handler.flow_ctx.req_size
        ret = " " * size
        self.loop.create_task(
            self.__blocking_read(
                obj_name,
                ret,
                size,
                jobid,
            )
        )
        return ret

    def put_object(
        self,
        obj: bytes,
        obj_name: str,
        dir_name: str = "",
    ):
        jobid = self._handler.get_jobid(dir_name)
        self.loop.create_task(
            self.__blocking_write(
                str(obj),
                obj_name,
                len(obj),
                jobid,
            )
        )

    async def lookup_object(self, obj_name: str, dir_name: str = ""):
        jobid = self._handler.get_jobid(dir_name)
        self.loop.create_task(self.__blocking_lookup(obj_name, jobid))


class AsyncJiffyHandler(AsyncESHandlerInterface):
    def __init__(self, addr: str, root_dir: str = "tmp", table_name: str = "default", impl: str = "O0"):
        super(AsyncJiffyHandler, self).__init__()
        print(f"Connecting to {addr}")
        self.client = jiffy.JiffyClient(addr, 9090, 9091)
        self.root_dir = "local://" + root_dir
        if impl == "O0":
            self.flags = jiffy.Flags.pinned
        else:
            self.flags = None
        self.table_name = "/" + table_name

    async def is_connected(self):
        if self.client:
            return True
        return False

    def trace_metadata_overhead(self):
        self.do_tracing = True
        self.ts = []

    def __blocking_create(self, table_name):
        self.client.open_or_create_hash_table(
            table_name,
            self.root_dir,
            flags=self.flags,
        )

    async def allocate(self, name: str = "", job=None):
        print(f"creating a jiffy hash table with name {name}")
        loop = asyncio.get_running_loop()
        self.table_name = "/" + name
        await loop.run_in_executor(
            None,
            self.__blocking_create,
            self.table_name,
        )

    async def free(self):
        # not a actual free now
        # self.client.remove(self.table_name)
        self.client.disconnect()

    def register(self, flow_ctx=None):
        conn = AsyncJiffyConnection(self)
        if flow_ctx:
            conn.rate = flow_ctx.IOPS_SLO
        else:
            conn.rate = 0
        return conn


class AsyncJiffyConnection(AsyncESConnectionInterface):
    """Jiffy client Wrapper"""

    def __init__(self, handler):
        super(AsyncJiffyConnection, self).__init__()
        self.client = handler.client
        self.root_dir = handler.root_dir
        self.table_name = handler.table_name
        self.table = self.client.open_hash_table(self.table_name)
        self.loop = asyncio.get_running_loop()
        self.retry_times = 0

    async def __blocking_read(self, fut, id: str):
        self.outstanding += 1
        id_bytes = id.encode()
        buf = None
        while self.retry_times < 4:
            if self.table.exists(id_bytes):
                buf = self.table.get(id_bytes)
                break
            else:
                self.retry_times += 1
        if self.retry_times == 4:
            print(f"{id_bytes} does not exist")
        self.retry_times = 0
        fut.set_result(buf)
        self.callback(buf)
        self.outstanding -= 1

    async def __blocking_write(self, fut, obj: bytes, id: str):
        self.outstanding += 1
        id_bytes = id.encode()
        # print(f"putting {id_bytes}")
        try:
            self.table.put(id_bytes, obj)
        except KeyError:
            self.table.update(id_bytes, obj)
        fut.set_result(True)
        self.callback()
        self.outstanding -= 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        self.client.close(self.table_name)

    async def poll(self):
        await asyncio.sleep(0)

    def get_object(self, id: str, delegate: bool = False):
        ret = self.loop.create_future()
        self.loop.create_task(self.__blocking_read(ret, id))
        return ret

    def put_object(self, obj: bytes, id: str, delegate: bool = False):
        ret = self.loop.create_future()
        self.loop.create_task(self.__blocking_write(ret, obj, id))
        return ret

    def delete_object(self, id: str):
        self.outstanding += 1
        self.table.remove(id.encode())
        self.callback()
        self.outstanding -= 1


class AsyncS3Handler(AsyncESHandlerInterface):
    def __init__(self, bucket_name, subfolder=None):
        super(AsyncS3Handler, self).__init__()
        self.session = boto3.session.Session()
        self.client = self.session.client("s3")
        self.bucket_name = bucket_name
        self.subfolder = subfolder

    async def is_connected(self):
        if self.client:
            return True
        return False

    def __blocking_create(self):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except:
            print("Maybe the bucket was never created")
            self.client.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "us-west-1"},
            )

    async def allocate(self, name="", job=None):
        print(f"creating bucket with name {self.bucket_name}")
        loop = asyncio.get_running_loop()
        self.subfolder = name
        await loop.run_in_executor(None, self.__blocking_create)

    async def free(self):
        # do not free to save time in preconditioning
        return
        loop = asyncio.get_running_loop()
        await self.loop.run_in_executor(
            None, self.client.delete_bucket(Bucket=self.bucket_name)
        )

    def register(self, flow_ctx=None):
        conn = AsyncS3Bucket(self.client, self.bucket_name, self.subfolder)
        if flow_ctx:
            conn.rate = flow_ctx.IOPS_SLO
        else:
            conn.rate = 0
        return conn


class AsyncS3Bucket(AsyncESConnectionInterface):
    """Boto3 S3 client Wrapper"""

    def __init__(self, client, bucket_name, subfolder=None):
        super(AsyncS3Bucket, self).__init__()
        self.semaphore = asyncio.Semaphore(128)  # Limit to 128 concurrent operations
        self.client = client
        self.bucket_name = bucket_name
        self.subfolder = subfolder if subfolder else ""  # Handle empty subfolder if None
        self.loop = asyncio.get_running_loop()
        self.retry_times = 0
        # FIXME: hardcoded as of now
        self.retry_timeout = [0.1, 0.2, 0.4, 0.8]

    async def __blocking_read(self, fut, id):
        self.outstanding += 1
        response = None
        async with self.semaphore:
            while self.retry_times < 4:
                try:
                    response = self.client.get_object(
                        Bucket=self.bucket_name,
                        Key=self.subfolder + "/" + id,
                    )
                    break
                except self.client.exceptions.NoSuchKey:
                    time.sleep(self.retry_timeout[self.retry_times])
                    self.retry_times += 1
            self.retry_times = 0
            if response:
                buf = response["Body"].read()
            else:
                buf = None
            # print(f"successfully got {id}, size = {len(buf)}")
            fut.set_result(buf)
            self.callback(buf)
            self.outstanding -= 1

    async def __blocking_write(self, fut, obj, id):
        self.outstanding += 1
        id = self.subfolder + "/" + id
        async with self.semaphore:
            if isinstance(obj, str):
                # upload as a file
                _ = self.client.upload_file(
                    Filename=obj,
                    Bucket=self.bucket_name,
                    Key=id,
                )
                fut.set_result(True)
                self.callback()
                self.outstanding -= 1
                return
            if len(obj) < 16 * 1024 * 1024:
                _ = self.client.put_object(
                    Body=obj,
                    Bucket=self.bucket_name,
                    Key=id,
                )
            else:
                response = self.client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=id,
                )
                upload_id = response["UploadId"]
                obj_len = len(obj)
                PART_SIZE = 16 * 1024 * 1024
                PART_NUM = obj_len // PART_SIZE
                if obj_len % (16 * 1024 * 1024):
                    PART_NUM += 1
                mpu = []
                for i in range(PART_NUM):
                    start = i * PART_SIZE
                    end = min((i + 1) * PART_SIZE, obj_len)
                    # print(f"uploading {start}:{end}, len={end-start}")
                    response = self.client.upload_part(
                        Bucket=self.bucket_name,
                        Key=id,
                        PartNumber=i + 1,
                        UploadId=upload_id,
                        Body=obj[start:end],
                    )
                    mpu.append(
                        {
                            "ETag": response["ETag"],
                            "PartNumber": i + 1,
                        }
                    )

                self.client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=id,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": mpu},
                )
            fut.set_result(True)
            self.callback()
            self.outstanding -= 1

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args, **kwargs):
        # self.client.close()
        pass

    async def poll(self):
        await asyncio.sleep(0)

    def get_object(self, id, delegate=False):
        ret = self.loop.create_future()
        self.loop.create_task(self.__blocking_read(ret, id))
        # self.loop.run_in_executor(None, self.__blocking_read(id))
        return ret

    def put_object(self, obj, id, delegate=False):
        ret = self.loop.create_future()
        self.loop.create_task(self.__blocking_write(ret, obj, id))
        # self.loop.run_in_executor(None, self.__blocking_write(obj, id))
        return ret

    def delete_object(self, id):
        self.outstanding += 1
        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=id,
        )
        self.callback()
        self.outstanding -= 1


class AsyncRedisHandler(AsyncESHandlerInterface):
    pass


class AsyncEFSHandler(AsyncESHandlerInterface):
    pass


def create_handler(
    es_type: str,
    addr: str,
    name: str = None,
    ctx=None,
    impl: str = None,
    opt=[],
):
    if es_type == "en4s":
        addr = addr + ":9999"
        # print(f"Connecting to {addr} with the default port")
        return asyncreflex.ReFlexHandler(
            addr,
            name,
            ctx=ctx,
            impl=impl,
            user_opts=opt,
        )
    elif es_type == "pocket":
        return AsyncPocketHandler(addr, name)
    elif es_type == "jiffy":
        return AsyncJiffyHandler(addr=addr, table_name=name, impl=impl)
    elif es_type == "s3":
        return AsyncS3Handler(bucket_name=addr, subfolder=name)
    #    elif es_type == "redis":
    #        return AsyncRedisHandler(addr, name)
    #    elif es_type == "efs":
    #        return AsyncEFSHandler(name)
    else:
        raise Exception(f"{es_type} not implemented")
