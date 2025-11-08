import pathway as pw
import os
import json

# Ensure the output directory exists
output_dir = './processed_output'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Schema for the events.csv file
class EventSchema(pw.Schema):
    PROSPECTID: int
    timestamp: pw.DateTimeUtc
    action_type: str
    payload: str  # Assuming payload is a string containing JSON data

# Schema for the transactions.csv file
class TransactionSchema(pw.Schema):
    PROSPECTID: int
    t: int
    timestamp: pw.DateTimeUtc
    time_since_recent_payment: int
    time_since_first_deliquency: int
    # Add other fields based on your CSV structure
    # ...

# Read events and transactions CSV files as streams
events_stream = pw.io.csv.read(
    './csv_input/events.csv',
    schema=EventSchema,
    timestamp_column='timestamp'
)

transactions_stream = pw.io.csv.read(
    './csv_input/transactions.csv',
    schema=TransactionSchema,
    timestamp_column='timestamp'
)

# Example Transformation - We can process the payload (JSON) in action events
def parse_payload(payload):
    try:
        return json.loads(payload)  # Parse the payload as a JSON string
    except Exception as e:
        return {}  # Return empty dict if parsing fails

# Join the two data streams on PROSPECTID and timestamp (you can adjust based on needs)
joined_stream = events_stream.join(
    transactions_stream, 
    on=["PROSPECTID", "timestamp"], 
    how="inner"
).select(
    # Select relevant fields after joining
    event_id=pw.this.PROSPECTID,
    event_timestamp=pw.this.timestamp,
    action_type=pw.this.action_type,
    event_payload=pw.apply(parse_payload, pw.this.payload),
    # Other fields from transaction stream
    time_since_recent_payment=pw.this.time_since_recent_payment,
    time_since_first_deliquency=pw.this.time_since_first_deliquency,
    # Add more fields as necessary
)

# Write the joined and transformed data to an output file
pw.io.jsonlines.write(joined_stream, './processed_output/output.jsonl')

# Running the Pathway pipeline
if __name__ == "__main__":
    pw.run()
