#include "mapreduce.h"

void MR_Emit(char *key, char *value) {
    
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {

}

unsigned long MR_SortedPartition(char *key, int num_partitions) {

}

void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition, int num_partitions) {

}