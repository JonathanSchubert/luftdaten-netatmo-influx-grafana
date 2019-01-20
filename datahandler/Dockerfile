FROM python:3.6.8-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY get_data.py ./get_data.py

CMD [ "python", "-u", "./get_data.py" ]
