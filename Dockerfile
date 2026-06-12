FROM python:3.14-alpine as base

WORKDIR /app

RUN apk add --no-cache gcc musl-dev linux-headers librdkafka-dev

ADD requirements.txt .

RUN pip install --root-user-action ignore --no-cache-dir -r requirements.txt

COPY configs/ ./configs/
COPY kafi/ ./kafi/

CMD ["/bin/sh", "-c", "trap 'exit 0' TERM INT; while true; do sleep 3600; done"]
