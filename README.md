# YaMR - Yet Another Map Reduce

Yet Another Map Reduce is an implementation of the core components of Hadoop's Map Reduce Framework.


## Commands for Execution

Clone the repository
```
git clone https://github.com/Projects-EC-2022/BD2_018_023_041_058.git
```

On a shell, navigate to `YaMR` and run the following command
```
cd YaMR
python client.py 3 input_files/input_1.txt mapper.py reducer.py
```

To provide a custom configuration, replace the number of worker nodes, input file, mapper file and reducer file.
