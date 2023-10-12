FROM library/debian:12

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
      apt-get --assume-yes install \
        build-essential \
        git \
        librdkafka-dev \
        python3 \
        python3-confluent-kafka \
        python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip install "git+https://github.com/EclipseTrading/ksnap.git" \
      --break-system-packages
