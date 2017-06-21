/*
 ============================================================================
 Name        : test4.cpp
 Author      : at
 Version     : 2.0
 Copyright   : Copyright University of Luxembourg
 Description : MPICH Large Dataflow
 ============================================================================
 */
#include <math.h> 
#include <mpi.h>
#include <iostream>
#include <random>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <vector>

using namespace std;

//Global Variables
long long receivedBy1;
int slaveNode = 0;
int counter;

void* testFunc(void* p){

	//Message Passing Configuration Code
	int dest, source, rank, world_size;
	int tag=1;
	MPI_Init(NULL, NULL);
	// Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	// Get the rank of the process
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Status stat;
	MPI_Request request;
	request = MPI_REQUEST_NULL;

	//Data Generation Configuration Code
	int arraySize = 1000;
	int* msgOut = (int*)malloc(sizeof(int) * arraySize);
	int* msgIn = (int*)malloc(sizeof(int) * arraySize);
	//Large Random number generator
	random_device rd;
	default_random_engine generator(rd());
	uniform_int_distribution<int unsigned> distribution(0, 2147483646);
	srand (time(0));

	try{
		while(true){
			if(rank ==0){
				slaveNode = 0;
				dest=1;
				source=1;

				//Generating Data
				for(int i=0; i< arraySize ; i++){
					msgOut[i] = distribution(generator);
				}

				//Sending Data - Non Blocking
				MPI_Isend(msgOut, arraySize, MPI_INT, dest, tag, MPI_COMM_WORLD, &request);

			}
			else if(rank ==1){
				slaveNode = 1;
				dest=0;
				source=0;

				//Receiving Data - Non Blocking
				MPI_Irecv(msgIn, arraySize, MPI_INT, source, tag, MPI_COMM_WORLD, &request);
				//Counting the number of messages received by master
				counter++;

				receivedBy1 = receivedBy1+ (sizeof(int)*arraySize);

			}
			//Blocks until transfer complete
			//Try mpi_test
			MPI_Wait(&request, &stat);
		}//While
	}
	catch(MPI:: Exception & e) {
		cout<< e.Get_error_string();
	}
	MPI_Finalize();
	return NULL;
}

int main(int argc, char *argv[]) {

	pthread_t threadxx;
	pthread_create(&threadxx, NULL, &testFunc, NULL);
	while(true){
		sleep(1);
		if(slaveNode == 1){
			cout << "Total byte size of data received by node 1: "<< receivedBy1 <<", number of recv: "<<counter<<"\n";
		}
	}
	pthread_exit(NULL);

	return 0;
}

