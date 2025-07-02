FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
  wget curl unzip fonts-liberation libnss3 libatk-bridge2.0-0 \
  libxss1 libasound2 libgtk-3-0 libgbm-dev

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install playwright && playwright install --with-deps webkit

COPY . .

CMD ["python", "app.py"]
