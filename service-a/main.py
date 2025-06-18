import subprocess
import signal
import time
import sys

# Paths
JAR_PATH = "jarFiles/calcite.server-1.0.0-all.jar"
PYTHON_SCRIPT = "service-a.py"
ENV_PYTHON = "env/Scripts/python.exe"
LOG_FILE = "jarFiles/out.log"

# Global process handles
java_process = None
python_process = None

def cleanup(signum, frame):
    print("\n[Shutdown] Caught Ctrl+C, terminating processes...")

    if java_process and java_process.poll() is None:
        java_process.terminate()
        print("[Shutdown] Java server terminated.")
    
    if python_process and python_process.poll() is None:
        python_process.terminate()
        print("[Shutdown] Python script terminated.")

    sys.exit(0)

def main():
    global java_process, python_process

    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, cleanup)

    # Start the Java JAR server and log output
    with open(LOG_FILE, "w") as logfile:
        print("[Start] Launching Java HTTP server...")
        java_process = subprocess.Popen(
            ["java", "-jar", JAR_PATH],
            stderr=subprocess.STDOUT
        )

        # Wait a few seconds for the server to initialize
        time.sleep(5)

        print("[Start] Launching Python script...")
        python_process = subprocess.Popen(
            ["python", PYTHON_SCRIPT]
        )

        # Wait for both processes (or until Ctrl+C)
        java_process.wait()
        python_process.wait()

if __name__ == "__main__":
    main()
