FROM python:3.6.8-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY lib ./lib
CMD [ "python", "-u", "./main.py" ]
