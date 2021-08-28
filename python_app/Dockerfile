FROM python:3.8
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
# COPY app.py app.py
COPY utils utils
COPY file_uploader.py file_uploader.py
CMD ["python", "-u", "file_uploader.py"]
# CMD ["python", "-u", "app.py"]