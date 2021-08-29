FROM python:3.8
WORKDIR /code
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
# COPY app.py app.py
COPY utils utils
COPY main.py main.py
CMD ["python", "-u", "main.py"]
