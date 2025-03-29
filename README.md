# Streaming Prototype

## Overview
The `streaming_prototype` project consists of two services, **Service A** and **Service B**, that work together. This guide provides instructions to set up and run the project.

## Installation

### 1. Clone the Repository
```sh
git clone https://github.com/mukundvijay123/streaming_prototype.git
cd streaming_prototype
```

### 2. Setup Instructions

#### Windows (Using Git Bash)
```sh
source setup.win.sh
```

#### Linux
```sh
source setup.linux.sh
```

## Running the Services

After completing the setup, open **two separate terminals** to run each service.

### Running Service A
```sh
cd service-a
# Activate virtual environment
source env/bin/activate   # Linux
source env/Scripts/activate  # Windows

# Start the service
python service-a.py
```

### Running Service B
```sh
cd service-b
# Activate virtual environment
source env/bin/activate   # Linux
source env/Scripts/activate  # Windows

# Start the service
python service-b.py
```

## Notes
- Ensure that all dependencies are installed as part of the setup script.
- Run both services in separate terminals for proper functionality.

---


