FROM mageai/mageai:latest

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy data generator and schema
COPY sampledata.py /app/sampledata.py
COPY schema.sql /app/schema.sql
