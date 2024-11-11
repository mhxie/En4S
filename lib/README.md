# asyncreflex: A Fast ReFlex Client Library for Python3/asyncio

## Environment

    # on Ubuntu
    sudo apt-get update
    sudo apt-get install build-essential python3-dev python3-pip
    # on Amazon Linux 2023
    sudo yum update -y
    sudo yum groupinstall "Development Tools" -y
    sudo yum install python3-devel

## Get started

### Using Poetry

```
# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Create and activate the virtual environment
poetry install
poetry shell

# Build the package
poetry build

# Install the package
pip install dist/asyncreflex-*.whl

# Run your application
poetry run python your_app.py
```

### Building from Source

```
# Install build dependencies
pip install --upgrade pip
pip install build

# Build the package
python -m build

# Install the built package
pip install dist/*.whl
```

## Usage

```python
import asyncio
import uvloop
from asyncreflex import ReFlexHandler, JobCtx, FlowCtx

# Example usage for the first time
async def io_main(args):
    # Initialize the ReFlexHandler with the controller's address and application name
    handler = ReFlexHandler(controller_addr='127.0.0.1:9999', app_name="test-app")
    
    # Define job context with SLOs for IOPS, latency, and read/write ratio
    job_ctx = JobCtx(IOPS_SLO=1000, latency_us_SLO=1000, rw_ratio=50)
    
    # Allocate storage (in kilobytes) for the job
    await handler.allocate(capacity=1024, job=job_ctx)  # 1024 KB allocation
    
    # Set flow context parameters for a new I/O flow (e.g., specific IOPS and latency)
    flow_ctx = FlowCtx(IOPS_SLO=500, latency_us_SLO=2000, rw_ratio=0)
    
    # Register a new flow for the allocated storage node
    async with handler.register(flow=flow_ctx) as conn:
        
        # Callback function to be called when an I/O request finishes
        def req_done_cb(buffer=None):
            if buffer:
                # Handle read completion
                pass
            else:
                # Handle write completion
                pass
        
        # Register the callback to be triggered after each request completes
        conn.add_req_done_cb(req_done_cb)
        
        # Example write operation
        conn.put_object(buf2write, '1')  
        
        # Example read operation, returns an empty allocated buffer
        buf2read = conn.get_object('1')
    
    # After exiting the context, the flow is automatically deregistered
    
    # Invoke the next processing stage or pipeline with custom arguments
    func_args = {'frame_range': [5, 10]}  # Example custom arguments
    handler.invoke(name='pipeline-1', args=func_args)

# Example for subsequent calls (with a different flow configuration)
async def io_main(args):
    # Reuse the existing ReFlex context
    handler = ReFlexHandler(ctx=event['ReFlexContext'])
    
    # Set a new flow context
    flow_ctx = FlowCtx(IOPS_SLO=1000, latency_us_SLO=500, rw_ratio=100)
    
    # Register and perform I/O operations within the flow
    async with handler.register(flow=flow_ctx) as conn:
        # I/O operations go here
        pass
    
    # Invoke a different pipeline with custom arguments
    func_args = {}  # Custom arguments
    handler.invoke(name='pipeline-2', args=func_args)

# Releasing resources when done
async def io_main(args):
    # Free up resources associated with the ReFlex context
    handler = ReFlexHandler(ctx=event['ReFlexContext'])
    await handler.free()

# Optionally install uvloop for better performance with asyncio
uvloop.install()

# Run the main I/O function
asyncio.run(io_main(args))
```

## Run the dummy system

    python mock/dummy_storage.py # run dummy api storage server to test
    python mock/dummy_controller.py # un dummy controller server to test

## Benchmark

see mhxie/esbench

## Go Serverless

    # install terraform and aws-cli
    # configure your AWS credentials
    aws configure
    cd deploy && terraform apply
    # type Y after confirmation
    # run a functional test
    aws lambda invoke \
      --function-name test-cli \
      --payload '{
        "addr": "10.0.1.208:9999",
        "start": 0.0,
        "length": 4096,
        "iops": 1000,
        "duration": 10,
        "batch": 1,
        "latency_slo": 200,
        "rw_ratio": 100,
        "seq": 1}' \
      outputfile.txt

# Development

```
# Ensure you're in the Poetry shell
poetry shell

# Build the package (wheel and source distribution)
poetry build
```

# Dockerized Development
```
# Build the Docker image
docker build -f docker/Dockerfile.dev -t asyncreflex:dev .

# run mock servers
docker run -p 8888:8888 -it asyncreflex:dev python ./mock/dummy_storage.py
docker run -p 9999:9999 -it asyncreflex:dev python ./mock/dummy_controller.py

# run client
docker run --net=host -it asyncreflex:dev
```

## TODO

- optimize buffer management performance
- use flow control to support QoS
- support more job policy patterns
- improve the metadata management design

## Future Work

- support put and get objects through file system
- support more programming languages (c++ and rust)
