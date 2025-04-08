# Step 1: Use an official Python runtime as a parent image
FROM python:3.11.1-slim

# Step 2: Set the working directory in the container
WORKDIR /app

# Step 3: Copy the current directory contents into the container at /app
COPY . /app

# Step 4: Install any dependencies (from requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Make port 5000 available for Flask (or whatever port your app uses)
EXPOSE 5000

# Step 6: Run the Python script when the container launches
CMD ["python", "auto-sto.py"]