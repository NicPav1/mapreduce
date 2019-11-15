#include "mapreduce.h"
#include "stdlib.h"

// Used to store key-value pairs?
typedef struct 
{
    char key;
    char value;
    key_value *next;
} key_value;

key_value *head = NULL;

void MR_Emit(char *key, char *value) {
    
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {

}

void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition, int num_partitions) {

    if (!(argc > 1)) {
        exit(1);
    }

    for (int i = 1; i < argc; i++) {
        (*map)(argv[i]);
    }
}