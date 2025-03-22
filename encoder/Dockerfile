FROM python:3.12-slim

WORKDIR /encoder
RUN apt update && apt install -y ffmpeg
ADD requirements.txt .
RUN pip install -r requirements.txt
ADD . .
CMD /bin/bash
