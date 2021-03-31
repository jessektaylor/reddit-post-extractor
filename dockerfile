FROM python:3.6
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
COPY requirements.txt /code/
RUN pip install -r requirements.txt
RUN cd /usr/
RUN python3 -m textblob.download_corpora
RUN cd /code/
COPY . /code/
CMD python3 reddit-extracting-posts.py

