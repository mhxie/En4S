## En4S: Enabling SLOs in Serverless Storage Systems

### Introduction
```
❯ tree -L 1
.
├── app                    # Example application code
├── build_functions.sh     # Build functions
├── build_layers.sh        # Build layers for applications (e.g. opencv)
├── build_lib.sh           # Build ephemeral storage client lib
├── controller             # Controller code
├── deploy                 # Terraform deployment scripts
├── function_docker        # Dockerfile for functions
├── layer_docker           # Dockerfile for layers
├── lib                    # En4S client lib
├── lib_docker             # Dockerfile for lib
├── README.md              # Project documentation
└── storage                # Storage server code
```

### Instructions
1. Build your own server and controller AMI images.
```
# see /storage/README.md
# see /controller/README.md
```
2. Build layers, libs, and functions.
```
./build_layers.sh
./build_lib.sh
# build our example applications
./build_functions.sh
```

3. Deploy the servers and functions to the cloud.
```
# see /deploy/README.md
# you may build and deploy your own workload generator to test the system
```