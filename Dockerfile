FROM ubuntu:18.04


RUN apt-get update
RUN apt update
RUN apt-get install -y python3-pip python3-dev git
RUN cd /usr/local/bin
RUN ln -s /usr/bin/python3 python
RUN pip3 install --upgrade pip
RUN pip3 install pymongo kafka-python opencv-python pytz
RUN pip3 install Pillow
RUN apt install -y libgl1-mesa-glx

RUN git clone https://github.com/leonardoasch/LEO-LABIC-CPU-Docker.git app
RUN echo "10.0.10.11 bigdata_cpu01" >> /etc/hosts

CMD ["python3","./app/main.py"]
