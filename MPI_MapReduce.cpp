/*
 ============================================================================
 Name        : MPI_MapReduce.cpp
 Author      : at
 Supervisor  : Prof. Dr. Martin Theobald
 Version     : 14.0
 Copyright   : University of Luxembourg
 Description : Map and Reduce Simulation using MPI (parallelism within one operation)
 ============================================================================
 */

#include <mpi.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

using namespace std;

long MESSAGE_SIZE = 100000, sent = 0, received = 0;

typedef struct pthread_p {
    int fromRank; // rank from where incoming message is expected
    char* inMessageBuffer;
    int windowSize; // maximum window size in int's
    int received; // number of bytes received
    long partialSumOfSquares; // the result of the computation
} pthread_p;

class MPI_MapReduce {

  protected:

    //Number of elements per window
    int windowSize;

  public:

    MPI_MapReduce();

  public:

    // Write the file with 'numberOfElements' int values
    static void writeBinaryFile(int numberOfElements) {

      int sizeOfElement = sizeof(int);
      srand(time(NULL));

      ofstream dataSource;
      dataSource.open("../dataInt.bin", ios::binary | ios::out);
      dataSource.write((char*) &numberOfElements, sizeOfElement); //first value in file indicates length

      for (int i = 0; i < numberOfElements; i++) {
        int val = i; //rand();
        dataSource.write((char*) &val, sizeOfElement);
        //cout << "WRITE VALUE: " << val << endl;
      }

      dataSource.close();
      //cout << numberOfElements * sizeOfElement << "\tBYTES WRITTEN.\n";
    }

    // Read the file (size must be multiple of 'sizeOfElement')
    static int readBinaryFile(char* buffer) {

      int sizeOfElement = sizeof(int);
      int bufferSize = 0; // size of buffer to be returned

      ifstream dataSource;
      dataSource.open("../dataInt.bin", ios::binary | ios::in);

      if (dataSource) {

        dataSource.read(&buffer[0], sizeOfElement); // first value contains length
        bufferSize = decode(&buffer[0]); // so get length of file first
        dataSource.read(&buffer[sizeOfElement], bufferSize * sizeOfElement); // then read rest of file in one chunck

      } else {

        char szTmp[1024];
        getcwd(szTmp, sizeof(szTmp));
        cout << "Problem with opening the binary file: " << szTmp << endl;

      }

      dataSource.close();

      return bufferSize;
    }

    //Converts a binary array of chars to an array of integers
    static int deserialize(char* binData, int* intData) {
      int sizeOfElement = sizeof(int);
      int intDataLength = decode(&binData[0]);
      //cout << "DESERIALIZE LENGTH: " << intDataLength << endl;
      int charLength = (intDataLength + 1) * sizeOfElement;
      int k = 0;
      for (int i = 4; i < charLength - 3; i += 4) {
        int value = decode(&binData[i]);
        //cout << "DESERIALIZE VALUE: " << value << endl;
        intData[k] = value;
        k++;
      }
      return intDataLength;
    }

    //Converts an array of integers to an array of chars
    void serialize(char* binData, int* intData, int intDataLength) {
      encode(&binData[0], intDataLength);
      //cout << "SERIALIZE LENGTH: " << intDataLength << endl;
      int k = 4;
      for (int i = 0; i < intDataLength; i++) {
        encode(&binData[k], intData[i]);
        k += 4;
        //cout << "SERIALIZE VALUE: " << intData[i] << endl;
      }
    }

    static int decode(char* chars) {
      unsigned int value = 0;
      value = (value << 8) + (unsigned char) chars[3];
      value = (value << 8) + (unsigned char) chars[2];
      value = (value << 8) + (unsigned char) chars[1];
      value = (value << 8) + (unsigned char) chars[0];
      return value;
    }

    static void encode(char* chars, int value) {
      int k = 0;
      chars[k++] = (unsigned char) value & 0xFF;
      chars[k++] = (unsigned char) (value >> 8) & 0xFF;
      chars[k++] = (unsigned char) (value >> 16) & 0xFF;
      chars[k++] = (unsigned char) (value >> 24) & 0xFF;
    }

    static void partition(int** partitionArrays, int* partitionSizes, int* intBuffer, int intBufferSize, int n) {

      // Initialize return arrays
      for (int i = 0; i < n; i++) {
        partitionArrays[i] = new int[intBufferSize / n];
        partitionSizes[i] = 0;
      }

      // Fill the partitions
      for (int i = 0; i < intBufferSize; i++) {
        int val = intBuffer[i];
        int pos = partitionSizes[val % n];
        //cout << "IDX: " << val % n << endl;
        partitionArrays[val % n][pos] = val;
        partitionSizes[val % n]++;
      }

      // Check partitions
      int total = 0;
      for (int i = 0; i < n; i++) {
        cout << "PARTITION " << i << " SIZE: " << partitionSizes[i] << "x" << sizeof(int) << "=" << partitionSizes[i] * sizeof(int) << " BYTES \n";
        total += partitionSizes[i] * sizeof(int);
      }
      cout << "TOTAL PARTITION SIZE: " << total << " BYTES\n";
    }

    static long sum(int* intData, int arrLength) {
      long sum = 0;
      for (int i = 0; i < arrLength; i++) {
        sum = sum + (unsigned int) intData[i];
      }
      return sum;
    }

    static void square(int* intData, int arrLength) {
      for (int i = 0; i < arrLength; i++) {
        intData[i] = (unsigned int) intData[i] * intData[i];
      }
    }

    static void* mpiReduceFunc(void* params) {

      int tag = 1, received = 0;
      int fromRank = ((pthread_p*) params)->fromRank;
      int windowSize = ((pthread_p*) params)->windowSize;
      char* inMessageBuffer = ((pthread_p*) params)->inMessageBuffer;

      MPI_Status status;

      MPI_Recv(inMessageBuffer, MESSAGE_SIZE, MPI_CHAR, fromRank, tag, MPI_COMM_WORLD, &status);
      cout << "+++++ THREAD RECEIVED FROM:\t" << fromRank << endl;
      received += MESSAGE_SIZE;

      int* partition = new int[windowSize];
      int partitionSize = deserialize(inMessageBuffer, partition);
      long partialSumOfSquares = sum(partition, partitionSize);

      cout << "+++++ PARTIAL SUM OF SQUARES:\t" << partialSumOfSquares << endl;
      ((pthread_p*) params)->partialSumOfSquares = partialSumOfSquares;
      ((pthread_p*) params)->received = received;

      return NULL;
    } // End of Reducer thread

    void* mpiMapFunc() {

      int worldSize, rank, tag = 1, level;

      MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &level);

      // Get the size and ranks of all processes
      MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);

      int n = worldSize - 1; // rank 0 always reserved as master, rest as slaves

      if (rank == 0) { // Master node

        cout << "WORLD: " << worldSize << ", " << n << " WORKER(S).\n";

        char* binBuffer = new char[(windowSize + 1) * sizeof(int)]; // always reserve binary buffer to maximum size (plus one for length indicator)
        int* intBuffer = new int[windowSize]; // also reserve integer buffer to maximum size
        int intBufferSize = 0; // keep actual size in separate variable

        // Get the binary data
        //writeBinaryFile(windowSize);
        intBufferSize = readBinaryFile(binBuffer); // this is the window in binary format

        // Local buffers
        char** outMessageBuffers = new char*[n]; // n outgoing message buffers
        char** inMessageBuffers = new char*[n]; // n incoming message buffers

        int** partitions = new int*[n]; // n partitions
        int* partitionSizes = new int[n]; // n sizes

        // Deserialize and partition the binary data
        deserialize(binBuffer, intBuffer); // this is the window in integer format
        partition(partitions, partitionSizes, intBuffer, intBufferSize, n); // these are the partitions of the window in integer format

        MPI_Request reqs[n];
        MPI_Status stats[n];

        pthread_p params[n];
        pthread_t threads[n];

        long totalSumOfSquares = 0;

        sleep(1); // let all workers initialize

        for (int i = 0; i < n; i++) {
          inMessageBuffers[i] = new char[MESSAGE_SIZE];
          params[i].fromRank = i + 1;
          params[i].windowSize = windowSize;
          params[i].inMessageBuffer = inMessageBuffers[i];
          pthread_create(&threads[i], NULL, &mpiReduceFunc, (void*) &params[i]);
        }

        sleep(1);

        cout << "ALL REDUCER THREADS STARTED." << endl;

        // Send partitions to Mapper processes in non-blocking manner
        for (int i = 0; i < n; i++) {

          outMessageBuffers[i] = new char[MESSAGE_SIZE];
          serialize(outMessageBuffers[i], partitions[i], partitionSizes[i]);

          MPI_Isend(outMessageBuffers[i], MESSAGE_SIZE, MPI_CHAR, i + 1, tag, MPI_COMM_WORLD, &reqs[i]);
          cout << "_________ ASYNC-SENT TO WORKER: \t" << i + 1 << " -> " << MESSAGE_SIZE << " BYTES.\n";
          sent += MESSAGE_SIZE;
        }

        MPI_Waitall(n, reqs, stats);

        cout << "ALL MESSAGES SENT TO WORKERS." << endl;

        // Thread synchronization
        for (int i = 0; i < n; i++) {
          pthread_join(threads[i], NULL);
          totalSumOfSquares += params[i].partialSumOfSquares;
          received += params[i].received;
        }

        cout << "TOTAL SUM OF SQUARES: " << totalSumOfSquares << endl;

      } else { // Slave nodes

        MPI_Status status;

        char* inMessageBuffer = new char[MESSAGE_SIZE]; // fixed-size incoming message buffer
        char* outMessageBuffer = new char[MESSAGE_SIZE]; // fixed-size outgoing message buffer

        MPI_Recv(inMessageBuffer, MESSAGE_SIZE, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &status);
        cout << "_________ RECEIVED BY WORKER: \t" << rank << " -> " << MESSAGE_SIZE << " BYTES.\n";
        received += MESSAGE_SIZE;

        int* partition = new int[windowSize]; // also reserve integer buffer to maximum size
        int partitionSize = deserialize(inMessageBuffer, partition); // keep actual size in separate variable
        square(partition, partitionSize);
        serialize(outMessageBuffer, partition, partitionSize);

        if (rank == 1) // simulate high workload on rank 1 only
          sleep(1);

        MPI_Send(outMessageBuffer, MESSAGE_SIZE, MPI_CHAR, 0, tag, MPI_COMM_WORLD);
        cout << "_________ SENT BY WORKER: \t" << rank << " -> " << MESSAGE_SIZE << " BYTES.\n";
        sent += MESSAGE_SIZE;
      }

      sleep(1);

      cout << "RANK " << rank << " FINISHED: " << sent << " BYTES SENT, " << received << " BYTES RECEIVED.\n";

      MPI_Finalize();
      return NULL;
    } // End of Map function
};

MPI_MapReduce::MPI_MapReduce() {
  windowSize = 1000;
  if ((windowSize + 1) * sizeof(int) > MESSAGE_SIZE)
    cout << "WARNING: MESSAGE SIZE SET SMALLER THAN WINDOW BUFFER -- PROGRAM MAY CRASH!\n";
}

int main(int argc, char* argv[]) {

  MPI_MapReduce testObj;
  testObj.mpiMapFunc();

  return 0;
}
