FROM python:3.6.8-slim

RUN apt-get update && apt-get install -y curl

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./main.py
COPY lib ./lib

RUN mkdir -p netatmo_data
COPY netatmo.sh ./netatmo.sh
RUN chmod +x netatmo.sh

CMD [ "python", "-u", "./main.py" ]
