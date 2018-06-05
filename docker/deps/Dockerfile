FROM alpine:3.7

WORKDIR /root
COPY requirements.txt .
COPY compiler/requirements.txt compiler/requirements.txt

RUN apk update && apk upgrade \
  && apk add ca-certificates \
  && apk add build-base \
  && apk add git \
  && apk add python3 \
  && apk add python3-dev \
  && pip3 install --upgrade pip setuptools \
  && pip3 install -r requirements.txt \
  && pip3 install awscli \
  && apk del build-base \
  && apk del git \
  && apk del python3-dev \
  && rm -rf /var/cache/apk/* \
  && rm requirements.txt compiler/requirements.txt
