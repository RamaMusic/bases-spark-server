from pyspark.sql import SparkSession
import socket
import sys
import io
import contextlib
import traceback
import importlib.util

# Configuration
HOST = '0.0.0.0'
PORT = 9999
LAB_FILE_PATH = "/bases/lab.py"
CSV_DATA_PATH = "/bases/data.csv"

def load_and_run_lab(spark, sc, df, rdd):
    """Reloads lab.py and executes the run function, capturing output."""
    output_buffer = io.StringIO()
    
    try:
        # Redirect stdout and stderr to capture output
        with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(output_buffer):
            print("--- Executing lab.py ---")
            
            # Reload the module
            spec = importlib.util.spec_from_file_location("lab", LAB_FILE_PATH)
            lab_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(lab_module)
            
            if hasattr(lab_module, 'run'):
                lab_module.run(spark, sc, df, rdd)
            else:
                print("Error: 'run(spark, sc, df, rdd)' function not found in lab.py")
                
            print("--- Execution Finished ---")
            
    except Exception:
        # Capture the full traceback if something goes wrong
        output_buffer.write("\n*** EXCEPTION DURING EXECUTION ***\n")
        traceback.print_exc(file=output_buffer)
        
    return output_buffer.getvalue()

def main():
    print("Initializing Spark Session...", flush=True)
    spark = SparkSession.builder \
        .appName("SparkServer") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    csv_path = CSV_DATA_PATH
    print(f"Loading data from {csv_path}...", flush=True)
    
    try:
        # Load and Cache Data
        df = spark.read.csv(csv_path, header=False, inferSchema=True)
        # df.cache() # Disabled to save memory
        print(f"DataFrame loaded. Count: {df.count()}", flush=True)
        
        rdd = sc.textFile(csv_path)
        rdd.cache()
        print(f"RDD cached. Count: {rdd.count()}", flush=True)
        
    except Exception as e:
        print(f"Error loading data: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    # Start Socket Server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen()
        print(f"\nServer listening on {HOST}:{PORT}...", flush=True)
        print("Ready to receive execution requests.", flush=True)
        
        # Signal readiness for Docker Healthcheck
        with open("/tmp/ready", "w") as f:
            f.write("ready")
        
        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connection from {addr}", flush=True)
                # We don't really need to read data from client, just the connection is the trigger.
                # But we could read a 'GO' command if we wanted.
                
                # Run the code
                result_output = load_and_run_lab(spark, sc, df, rdd)
                
                # Send result back
                conn.sendall(result_output.encode('utf-8'))
                print("Result sent to client.", flush=True)

if __name__ == "__main__":
    main()
