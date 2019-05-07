FROM python:3.7.3
MAINTAINER Gregory Ganley <gganley@student.bridgew.edu>
COPY . /app
WORKDIR /app
RUN pip install --trusted-host pypi.python.org -r requirements-dev.txt
RUN pip install tzlocal
RUN pip install pytz
RUN apt-get update && apt-get install -y mysql-server && rm -rf /var/lib/apt
ENV PYTHONDONTWRITEBYTECODE 1
CMD ./wait_for_it.sh && pytest -s -v
