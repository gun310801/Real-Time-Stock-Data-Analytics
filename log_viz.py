##viz_logs

import streamlit as st
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Thread
import pandas as pd
import json

log_file_path = 'consumer1/logfile.log'

# Initialize log containers
microbatch_lines = []
error_lines = []
warn_lines = []
combined_table = pd.DataFrame(columns=[
    'timestamp', 'numInputRows', 'inputRowsPerSecond', 'processedRowsPerSecond',
    'duration_addBatch', 'duration_commitOffsets', 'duration_getBatch', 
    'duration_latestOffset', 'duration_queryPlanning', 'duration_triggerExecution', 'duration_walCommit'
])
table3 = pd.DataFrame(columns=[
    'description', 'startOffset', 'endOffset', 'latestOffset', 
    'numInputRows', 'inputRowsPerSecond', 'processedRowsPerSecond', 
    'avgOffsetsBehindLatest', 'maxOffsetsBehindLatest', 'minOffsetsBehindLatest'
])

def process_log_entry(log_entry):
    # Extract the JSON part after 'Streaming query made progress'
    json_start_index = log_entry.find('{')
    if json_start_index != -1:
        json_string = log_entry[json_start_index:]
        # Load the JSON string
        data = json.loads(json_string)

        # Extract data for the combined table
        combined_data = {
            "timestamp": data.get("timestamp", ""),
            "numInputRows": data.get("numInputRows", ""),
            "inputRowsPerSecond": data.get("inputRowsPerSecond", ""),
            "processedRowsPerSecond": data.get("processedRowsPerSecond", ""),
            "duration_addBatch": data.get("durationMs", {}).get("addBatch", ""),
            "duration_commitOffsets": data.get("durationMs", {}).get("commitOffsets", ""),
            "duration_getBatch": data.get("durationMs", {}).get("getBatch", ""),
            "duration_latestOffset": data.get("durationMs", {}).get("latestOffset", ""),
            "duration_queryPlanning": data.get("durationMs", {}).get("queryPlanning", ""),
            "duration_triggerExecution": data.get("durationMs", {}).get("triggerExecution", ""),
            "duration_walCommit": data.get("durationMs", {}).get("walCommit", "")
        }

        # Extract data for the third table
        sources_data = data.get("sources", [{}])[0]  # Assuming there's only one source
        
        start_offset = sources_data.get("startOffset", {})
        end_offset = sources_data.get("endOffset", {})
        latest_offset = sources_data.get("latestOffset", {})
        metrics = sources_data.get("metrics", {})

        table3_data = {
            "description": sources_data.get("description", ""),
            "startOffset": start_offset.get("symbol_topic", {}).get("0", "") if isinstance(start_offset, dict) else "",
            "endOffset": end_offset.get("symbol_topic", {}).get("0", "") if isinstance(end_offset, dict) else "",
            "latestOffset": latest_offset.get("symbol_topic", {}).get("0", "") if isinstance(latest_offset, dict) else "",
            "numInputRows": sources_data.get("numInputRows", ""),
            "inputRowsPerSecond": sources_data.get("inputRowsPerSecond", ""),
            "processedRowsPerSecond": sources_data.get("processedRowsPerSecond", ""),
            "avgOffsetsBehindLatest": metrics.get("avgOffsetsBehindLatest", "") if isinstance(metrics, dict) else "",
            "maxOffsetsBehindLatest": metrics.get("maxOffsetsBehindLatest", "") if isinstance(metrics, dict) else "",
            "minOffsetsBehindLatest": metrics.get("minOffsetsBehindLatest", "") if isinstance(metrics, dict) else ""
        }

        return combined_data, table3_data
    else:
        return None, None
def log_update(log_text):
    global microbatch_lines
    global error_lines
    global warn_lines
    global combined_table, table3

    # Split the log text into individual lines
    log_lines = log_text.split('\n')
    
    # Initialize variables to store the current log block and its timestamp
    current_block = ""

    # Iterate over each line in the log text
    for line in log_lines:
        # Check if the line starts with a timestamp pattern (assuming it starts with "2024")
        if line.startswith("2024"):
            # If we have a current block, append it to the appropriate list
            if current_block:
                if 'Streaming query made progress' in current_block:
                    microbatch_lines.append(current_block)
                    table1_data, table3_data = process_log_entry(current_block)
                    if table1_data:
                        combined_table = combined_table.append(table1_data, ignore_index=True)
                    # if table2_data:
                    #     table2 = table2.append(pd.DataFrame(table2_data.items(), columns=["Metric", "Value"]), ignore_index=True)
                    if table3_data:
                        table3 = table3.append(table3_data, ignore_index=True)
                elif 'ERROR' in current_block:
                    error_lines.append(current_block)
                elif 'WARN' in current_block:
                    warn_lines.append(current_block)
            # Start a new block with the current line
            current_block = line
        else:
            # If the line does not start with the expected timestamp pattern, add it to the current block
            current_block += "\n" + line
    
    # Append the last block after the loop ends
    if current_block:
        if 'Streaming query made progress' in current_block:
            microbatch_lines.append(current_block)
            table1_data,  table3_data = process_log_entry(current_block)
            if table1_data:
                combined_table = combined_table.append(table1_data, ignore_index=True)
            # if table2_data:
            #     table2 = table2.append(pd.DataFrame(table2_data.items(), columns=["Metric", "Value"]), ignore_index=True)
            if table3_data:
                table3 = table3.append(table3_data, ignore_index=True)
        elif 'ERROR' in current_block:
            error_lines.append(current_block)
        elif 'WARN' in current_block:
            warn_lines.append(current_block)

class LogHandler(FileSystemEventHandler):
    def __init__(self, log_file):
        self.log_file = log_file
        self.file = open(log_file, 'r')
        self.file.seek(0, os.SEEK_END)

    def on_modified(self, event):
        if event.src_path == self.log_file:
            lines = self.file.read()
            if lines:
                log_update(lines)

def start_monitoring(log_file):
    event_handler = LogHandler(log_file)
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(log_file), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    st.title('Real-Time Log Viewer')

    st.header('MicroBatchProcessing Metadata')
    microbatch_container = st.empty()
    st.header('Source Metadata')
    source_container = st.empty()
    # st.header('Duration Metric Metadata')
    # duration_metric_container = st.empty()
    st.header('ERROR Logs')
    error_container = st.empty()

    st.header('WARN Logs')
    warn_container = st.empty()

    # Function to be run in a separate thread
    def monitor_logs():
        start_monitoring(log_file_path)

    # Start the log monitoring in a separate thread
    log_thread = Thread(target=monitor_logs)
    log_thread.start()

    # Main loop to update the Streamlit UI
    while True:
        time.sleep(1)
        # Update the UI from the main thread
        if not combined_table.empty:
            with microbatch_container:
                st.write("Table 1: MicroBatchProcessing Logs")
                st.dataframe(combined_table)
            # with duration_metric_container:
            #     st.write("Table 2: Duration Metrics")
            #     st.dataframe(table2)
            with source_container:
                st.write("Table 3: Source Metrics")
                st.dataframe(table3)
        if error_lines:
            with error_container:
                st.write("ERROR LOGS")
                st.write('\n'.join(error_lines), unsafe_allow_html=True)
                error_lines = []
        if warn_lines:
            with warn_container:
                st.write("WARN LOGS")
                st.write('\n'.join(warn_lines), unsafe_allow_html=True)
                warn_lines = []
