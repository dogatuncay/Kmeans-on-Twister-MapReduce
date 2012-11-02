Distributed kmeans program based on Twister-0.9.
Author: Fei Teng/Doga Tuncay
Data  : 11/9/2011

How to run twister kmeans:

prerequisites:

1. Start apach-activemq-5.4.2

2. Configure and start twister-0.9

3. Create kmeans data directory under $TWISTER_HOME/data

4. run genData script (located in $TWISTER_HOME/sample/kmeans/bin) according to the format:
./gen_data.sh [init clusters file][num of clusters][vector length][sub dir][data file prefix][num of files to generate][num of data points]

5. create partition file (located in $TWISTER_HOME/bin) like this:
./create_partition_file.sh [common directory][file filter][partition file]

6. run kmeansClustering (located in $TWISTER_HOME/samples/kmeans/bin)
./run_kmeans.sh [init clusters file][number of map tasks][partition file]

7. redirect the standard output and collect statistical info.
