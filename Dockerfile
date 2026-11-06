FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ffmpeg pulseaudio pulseaudio-utils build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run pulseaudio as root in headless mode before starting python
CMD pulseaudio -D --exit-idle-time=-1 --system --disallow-exit && python3 main.py
