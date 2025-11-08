# Use Pathway's base image
FROM pathwaycom/pathway:latest

# Set working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project directory into the container
COPY . .

# Command to run the main Python script
CMD ["python", "main.py"]
