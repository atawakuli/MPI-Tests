/*
 ============================================================================
 Name        : binaryDataPartitioning.cpp
 Author      : at
 Version     : 30.0
 Copyright   : at
 Description : Create a binary file with random integers and partition the data via sharding
 ============================================================================
 */
#include <math.h> 
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
using namespace std;

int main(int argc, char *argv[]) {
	
	//////////////////////////////////////////////////////////////////////
	// 1) Generating a binary file with random integers
	////////////////////////////////////////////////////////////////////
	//The code handles odd and even number of elements
	//Can generate int range of numbers	
	int numberOfElements = 21;	
	ofstream dataSource;
	srand(time(NULL));
	int counter = 1;
	dataSource.open("dataInt.bin", ios::binary | ios::out);
	while(counter <= numberOfElements){
		//range of values generated: 0 to 1000
		int x = rand()%1000+1;
		dataSource.write((char*) &x, sizeof(int));
		counter++;
	}
	dataSource.close();


	/////////////////////////////////////////////////////////////////
	// 2) Reading the binary data from the binary file
	////////////////////////////////////////////////////////////////
	ifstream dataSource2;
	dataSource2.open("dataInt.bin", ios::binary | ios::in);
	int y;
	do {
		dataSource2.read((char*) &y, sizeof(y));
		cout <<y<<" ";
	}while(dataSource2.peek() != EOF);
	cout <<"\n";

	//Getting the length of the file
	dataSource2.seekg(0, ios::end);
	int fileLength = dataSource2.tellg();

	//returning back to the beginning of the file
	dataSource2.seekg(0, ios::beg);

	//Create a buffer equal to the length of the file i.e for ten integers, the length is 40
	char *buffer = new char[fileLength];

	//Load the file's content to the buffer
	//Binary data loaded
	dataSource2.read(buffer, fileLength);

	//number of slave nodes to which the sharded data are sent
	int n = 2;

	//////////////////////////////////////////////////////////////
	//3) Sharding
	/////////////////////////////////////////////////////////////
	//even
	int shardsLength1 = ceil((double)numberOfElements/n)*4;
	//odd
	int shardsLength2 = floor((double)numberOfElements/n)*4;
	char *shardedBuf1 = new char[shardsLength1];
	char *shardedBuf2 = new char[shardsLength2];
	int j = 0;
	int k = 0;
	for(int i=0; i<fileLength-3; i+=4){
		//calculating the index of the integer (4 bytes) not a byte
		int tmpIndex = i/4;
		if(tmpIndex%n==0){
			shardedBuf1[j] = buffer[i];
			shardedBuf1[j+1] = buffer[i+1];
			shardedBuf1[j+2] = buffer[i+2];
			shardedBuf1[j+3] = buffer[i+3];
			j+=4;
		}
		else{
			shardedBuf2[k] = buffer[i];
			shardedBuf2[k+1] = buffer[i+1];
			shardedBuf2[k+2] = buffer[i+2];
			shardedBuf2[k+3] = buffer[i+3];
			k+=4;
		}
	}
	cout <<"\n";
	cout<<"First sharded buffer: \n";
	for(int z=0; z<shardsLength1-3; z+=4){
		int theInt;
		theInt = (theInt << 8) + (unsigned char)shardedBuf1[z+3];
		theInt = (theInt << 8) + (unsigned char)shardedBuf1[z+2];
		theInt = (theInt << 8) + (unsigned char)shardedBuf1[z+1];
		theInt = (theInt << 8) + (unsigned char)shardedBuf1[z];
		cout<<theInt<<"\n";
	}

	cout<<"Second sharded buffer: \n";
	for(int m=0; m<shardsLength2-3; m+=4){
		int theInt;
		theInt = (theInt << 8) + (unsigned char)shardedBuf2[m+3];
		theInt = (theInt << 8) + (unsigned char)shardedBuf2[m+2];
		theInt = (theInt << 8) + (unsigned char)shardedBuf2[m+1];
		theInt = (theInt << 8) + (unsigned char)shardedBuf2[m];
		cout<<theInt<<"\n";
	}
	return 0;
}

