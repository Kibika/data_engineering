exit
wget https://repo.anaconda.com/archive/Anaconda3-2022.10-Linux-x86_64.sh
bash Anaconda3-2022.10-Linux-x86_64.sh
ls
sudo apt-get update
sudo apt-get install docker.io
docker run hello-world
sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart
docker run hello-world
git clone https://github.com/Kibika/data_engineering.git
mkdir bin
cd bin
wget https://github.com/docker/compose/releases/download/v2.16.0/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose
cd
nano bash.rc
ls
nano .bashrc
source .bashrc
which docker-compose
pip install pgcli
cd .gc
ls
cd .
cd ..
export GOOGLE_APPLICATION_CREDENTIALS=.~/.gc/second-chariot-375510-cf05a71f2b3e.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/second-chariot-375510-cf05a71f2b3e.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
shutdown
sudo shutdown now
mkdir spark
cd spark
wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz
rm openjdk-11.0.2_linux-x64_bin.tar.gz
ls
export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
which java
java --version
wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
tar xzfv spark-3.3.0-bin-hadoop3.tgz
tar xzfv spark-3.3.1-bin-hadoop3.tgz
rm spark-3.3.1-bin-hadoop3.tgz
export SPARK_HOME="${HOME}/spark/spark-3.3.1-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
nano .bashrc
cd ..
nano .bashrc
source .bashrc
which java
which pyspark
ls
mkdir notebooks
cd notebooks
ls
cd spark
ls
cd spark-3.3.1-bin-hadoop3
ls
cd python
ls
cd lib
ls
cd notebooks
ls
cd ..
nano .bashrc
source .bashrc
ls
cd notebooks
ls
ls -lh fhvhv/2021/01/
cd ..
shutdown now
ls
cd notebooks
jupyter notebook
ls
cd ..
rm -rf data
ls
rm -rf week_5
ls
cd notebooks
jupyter notebook
ls
cd data_engineering
code .
cd ..
mkdir week_5
cd notebooks
ls
shutdown
ls
cd notebooks
ls
rm -rf data
cd ..
rm -rf data
vim download_data.sh
ls
cd notebooks
ls
vim download_data.sh
./dowload_data.sh green 2020
chmod +x download_data.sh
./download_data.sh green 2020
./download_data.sh green 2021
./download_data.sh yellow 2020
./download_data.sh yellow 2021
ls
clear
ls
cd notebooks
ls
cd data
ls
cd raw
ls
cd ..
cd data
ls
cd raw
ls
cd ..
cd notebooks
clear
jupyter notebooks
jupyter notebook
ls
cd notebooks
jupyter notebook
cd notebooks
jupyter notebok
jupyter notebook
cd notebooks
jupyter notebook
nano bash.rc
ls
nano .bashrc
source .bashrc
ls
cd .gc
ls
cd ..
pwd
cd notebooks
ls
cd data
ls
gsutil -m cp -r gs://de-zoomcamp-375510-cf05a71f2b3e/pq
gsutil -m cp -r pq gs://de-zoomcamp-375510-cf05a71f2b3e/pq
cd notebook
cd notebooks
cd data
ls
gsutil -m cp -r pq gs://de-zoomcamp-375510-cf05a71f2b3e/pq
cd ..
cd notebooks/data
gsutil -m cp -r pq gs://de-zoomcamp-375510-cf05a71f2b3e/pq
cd notebooks/data
gsutil -m cp -r pq gs://de-zoomcamp-375510-cf05a71f2b3e/pq
mkdir lib
rm -rf lib
cd ..
mkdir lib
cd lib
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
ls
cd ..
cd notebooks
jupyter notebook
