/*
 ============================================================================
 Name        : test300617.cpp
 Author      : A Tawakuli
 Supervisor  : Prof. Dr. Theobald
 Version     : 8.0
 Copyright   : Copyright University of Luxembourg
 Description : MPICH Large Dataflow (> 100 MB per sec) From master node to slave node run on local machine and external clusters
 ============================================================================
 */

#include <mpi.h>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

#define ARRAYSIZE    200000

//Global Variables
long sentBy0, receivedBy1;
int slaveNode = 0;
long  counter, sentCounter;
double t1, t2, timeLapse;

void* testFunc(void* p){

	//MPI Initialization Code
	int dest, source, rank, world_size;
	int tag=1;
	MPI_Init(NULL, NULL);
	// Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	// Get the rank of the process
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Status Stat;

	//Data Generation Code
	int arraySize = ARRAYSIZE;
	char msgOut[arraySize], msgIn[arraySize];
	for(int i=0; i< arraySize ; i++){
		msgOut[i] = 'x';
	}

	try{
		while(true){
			//Master Node
			if(rank ==0){
				slaveNode = 0;
				dest=1;
				source=1;
				t1 = MPI_Wtime();
				//sending
				MPI_Send(&msgOut, arraySize, MPI_CHAR, dest, tag, MPI_COMM_WORLD);
				t2 = MPI_Wtime();
				timeLapse = t2-t1;
				sentBy0 = sentBy0 + sizeof(msgOut);
				sentCounter++;
			}
			else if(rank ==1){
				slaveNode = 1;
				dest=0;
				source=0;
				//receiving
				int rx = MPI_Recv(&msgIn, arraySize, MPI_CHAR, source, tag, MPI_COMM_WORLD, &Stat);
				if(rx==0){
					//counting the number of messages received
					counter++;
					//Summing the amount of data sent
					receivedBy1 = receivedBy1+ sizeof(msgIn);
				}
			}
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
			cout << "Total data received by node 1: "<< (long)receivedBy1/1000000 <<" MB/s, total number of messages received: "<<counter<<" per second\n";
		}
		else{
			cout << "Total data sent by node 0: "<< (long)sentBy0/1000000 <<" MB/s, total number of messages sent: "<<sentCounter<<" per second || Time it took to send the last message: "<<timeLapse*1000000<<" Microseconds \n";
		}
	}
	pthread_exit(NULL);

	return 0;
}
