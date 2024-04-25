FROM mobilitydb/mobilitydb:16-3.4-1.1-beta1

# Set maintainer (consider using LABEL instead of MAINTAINER as it's deprecated)
LABEL maintainer="leonvillapun@gmail.com"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git curl gnupg build-essential tree vim cmake postgresql-server-dev-15 libproj-dev libjson-c-dev libgsl-dev libgeos-dev postgis \
    python3 python3-pip \
    && pip3 install --upgrade pip \
    && curl -fL https://apt.corretto.aws/corretto.key | gpg --batch --import \
    && gpg --batch --export '6DC3636DAE534049C8B94623A122542AB04F24E3' > /usr/share/keyrings/corretto.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/corretto.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
    && apt-get update \
    && apt-get install -y java-21-amazon-corretto-jdk

RUN rm -R /usr/local/src/MobilityDB

# Install MobilityDB
RUN git clone https://github.com/MobilityDB/MobilityDB.git -b v1.1.0 --depth 1 /usr/local/src/MobilityDB \
    && mkdir -p /usr/local/src/MobilityDB/build \
    && cd /usr/local/src/MobilityDB/build \
    && cmake -DMEOS=ON .. \
    && make -j$(nproc) \
    && make install

# Clone and set up PysparkMobility
RUN git clone https://github.com/Action52/PysparkMobility.git /data/PysparkMobility

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

WORKDIR /data/PysparkMobility

# Configure JupyterLab
RUN jupyter lab --generate-config \
    && echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_lab_config.py \
    && echo "c.NotebookApp.password = '`python3 -c "from notebook.auth import passwd; print(passwd('docker'))"`'" >> ~/.jupyter/jupyter_lab_config.py \
    && echo "c.NotebookApp.allow_root = True" >> ~/.jupyter/jupyter_lab_config.py

EXPOSE 8888

CMD ["python3", "-m", "jupyter", "lab", "--allow-root", "--no-browser", "--ip=0.0.0.0"]
