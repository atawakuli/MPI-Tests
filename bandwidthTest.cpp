/*
 ============================================================================
 Name        : bandwidthTest.cpp
 Author      : A Tawakuli
 Supervisor  : Prof. Dr. Theobald
 Version     : 10.0
 Copyright   : Copyright University of Luxembourg
 Description : MPICH Large Dataflow (> 3 GB/s) From master node to slave node runs on local machine and external clusters
 ============================================================================
 */

#include <mpi.h>
#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

#define ARRAYSIZE    400000

//Global Variables
long sentBy0, receivedBy1, sentBy0PerSec, receivedBy1PerSec;
int slaveNode = 0;
long  counter, sentCounter;
double t1, t2, timeLapse;
int lastSize;
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
			sentBy0PerSec = sentBy0PerSec + sizeof(msgOut);
			sentCounter++;
		}
		else if(rank ==1){
			slaveNode = 1;
			dest=0;
			source=0;
			int rx = MPI_Recv(&msgIn, arraySize, MPI_CHAR, source, tag, MPI_COMM_WORLD, &Stat);
			if(rx==0){
				//counting the number of messages received
				counter++;
				//Summing the amount of data sent
				int bufsize = arraySize * sizeof(char);
				receivedBy1 = receivedBy1+ bufsize;
				receivedBy1PerSec = receivedBy1PerSec + sizeof(msgIn);
			}
		}
	}//While

	MPI_Finalize();
	return NULL;
}

int main(int argc, char *argv[]) {

	pthread_t threadxx;
	pthread_create(&threadxx, NULL, &testFunc, NULL);
	while(true){
		sleep(1);
		if(slaveNode == 1){
			cout << "Data received by node 1 per second: "<<(long) receivedBy1PerSec/1000000 <<" MB/s, Total data received (accumulation): "<<(long)receivedBy1/1000000 <<" MB, total number of messages received: "<<counter<<" per second\n";
			receivedBy1PerSec = 0;
		}
		else{
			cout << "Data sent by node 0 per second: "<<(long) sentBy0PerSec/1000000<<" MB/s, Total data received (accumulation): "<<(long)sentBy0/1000000 <<" MB/s, total number of messages sent: "<<sentCounter<<" per second || Time it took to send the last message: "<<timeLapse*1000000<<" Microseconds \n";
			sentBy0PerSec = 0;
		}

	}
	pthread_exit(NULL);
	return 0;
}
