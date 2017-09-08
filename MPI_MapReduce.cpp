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

long MESSAGE_SIZE = 262148, SENT = 0, RECEIVED = 0;
bool ACTIVE = false;

typedef struct pthread_p {
    char* inMessageBuffer; // incoming message buffer provided by master
    int* partition; // incoming partition buffer provided by master
    int fromRank; // rank from where incoming message is expected
    int received; // number of bytes received
    long partialSumOfSquares; // the result of the computation
} pthread_p;

class MPI_MapReduce {

  protected:

    static const int windowSize = 1048576; // max. number of elements per window: (1048576+16)/16workers*4bytes = 262148bytes/worker

    // MPI configuration
    int worldSize; // number of nodes in the cluster
    int rank; // rank of the current node

    // Window with input data
    int* intBuffer; // deserialized elements in window
    int intBufferSize; // actual number of elements per window

    // Local message buffers (binary)
    char** outMessageBuffers; // static outgoing message buffers
    char** inMessageBuffers; // static incoming message buffers

    // Local partition buffers (ints)
    int** partitions; // static partition buffers
    int* partitionSizes; // partition sizes

    // Requests & statuses
    MPI_Request* reqs;
    MPI_Status* stats;

    // Threads & parameters
    pthread_t* threads;
    pthread_p* params;

  public:

    MPI_MapReduce(int worldSize, int rank) {

      this->worldSize = worldSize;
      this->rank = rank;

      if (rank == 0) {

        int n = worldSize - 1; // number of worker nodes

        // Write binary file with some sample values to simulate window
        //writeBinaryFile(windowSize);

        // Read and deserialize the binary window into integer format
        char binBuffer[(windowSize + 1) * sizeof(int)];
        intBuffer = new int[windowSize];
        intBufferSize = readBinaryFile(binBuffer);
        deserialize(binBuffer, intBuffer);

        // Local message buffers (binary)
        outMessageBuffers = new char*[n];
        inMessageBuffers = new char*[n];

        // Local partition buffers (ints)
        partitions = new int*[n];
        partitionSizes = new int[n];

        // Requests & statuses
        reqs = new MPI_Request[n];
        stats = new MPI_Status[n];

        // Threads & parameters
        threads = new pthread_t[n];
        params = new pthread_p[n];

        // Individual buffers
        for (int i = 0; i < n; i++) {
          outMessageBuffers[i] = new char[MESSAGE_SIZE];
          inMessageBuffers[i] = new char[MESSAGE_SIZE];
          partitions[i] = new int[windowSize / n]; // cautions: assumes uniform partition sizes!
        }

        //cout << "MASTER INIT DONE." << endl;

      } else {

        // Local message buffers (binary)
        outMessageBuffers = new char*[1];
        inMessageBuffers = new char*[1];
        outMessageBuffers[0] = new char[MESSAGE_SIZE];
        inMessageBuffers[0] = new char[MESSAGE_SIZE];

        // Local partition buffers (ints)
        partitions = new int*[1];
        partitions[0] = new int[windowSize];
        partitionSizes = new int[1];

        // Requests & statuses
        reqs = new MPI_Request[1];
        stats = new MPI_Status[1];

        //cout << "SLAVE " << rank << " INIT DONE." << endl;
      }
    }

    ~MPI_MapReduce() {

      if (rank == 0) {

        delete[] intBuffer;

        // All local buffers
        for (int i = 0; i < worldSize - 1; i++) {
          delete[] outMessageBuffers[i];
          delete[] inMessageBuffers[i];
          delete[] partitions[i];
        }
        delete[] outMessageBuffers;
        delete[] inMessageBuffers;
        delete[] partitions;
        delete[] partitionSizes;

        // Requests & statuses
        delete[] reqs;
        delete[] stats;

        // Threads & parameters
        delete[] threads;
        delete[] params;

      } else {

        // All local buffers
        delete[] outMessageBuffers[0];
        delete[] inMessageBuffers[0];
        delete[] partitions[0];
        delete[] outMessageBuffers;
        delete[] inMessageBuffers;
        delete[] partitions;
        delete[] partitionSizes;
        delete[] stats;
        delete[] reqs;

      }
    }

    // Write the file with 'numberOfElements' int values
    static void writeBinaryFile(int numberOfElements) {

      int sizeOfElement = sizeof(int);
      srand(time(NULL));

      ofstream dataSource;
      dataSource.open("./data.bin", ios::binary | ios::out);
      dataSource.write((char*) &numberOfElements, sizeOfElement); //first value in file indicates length

      for (int i = 0; i < numberOfElements; i++) {
        int val = i; //rand();
        dataSource.write((char*) &val, sizeOfElement);
        //cout << "WRITE VALUE: " << val << endl;
      }

      dataSource.close();
      //cout << (numberOfElements + 1) * sizeOfElement << "\tBYTES WRITTEN.\n";
    }

    // Read the file (size must be multiple of 'sizeOfElement')
    static int readBinaryFile(char* buffer) {

      int sizeOfElement = sizeof(int);
      int bufferSize = 0; // size of buffer to be returned

      ifstream dataSource;
      dataSource.open("./data.bin", ios::binary | ios::in);

      if (dataSource) {

        dataSource.read(&buffer[0], sizeOfElement); // first value contains length
        bufferSize = decode(&buffer[0]); // so get length of file first
        dataSource.read(&buffer[sizeOfElement], bufferSize * sizeOfElement); // then read rest of file in one chunck
        //cout << (bufferSize + 1) * sizeOfElement << "\tBYTES READ.\n";

      } else {

        char szTmp[1024];
        getcwd(szTmp, sizeof(szTmp));
        cout << "Problem with opening the binary file: " << szTmp << endl;

      }

      dataSource.close();

      return bufferSize;
    }

    // Converts a binary array of chars to an array of integers
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

    // Converts an array of integers to an array of chars
    static void serialize(char* binData, int* intData, int intDataLength) {
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
        partitionSizes[i] = 0;
      }

      // Fill the partitions
      for (int i = 0; i < intBufferSize; i++) {
        int val = intBuffer[i];
        int idx = partitionSizes[val % n];
        partitionArrays[val % n][idx] = val;
        partitionSizes[val % n]++;
      }

      // Check partitions
      int total = 0;
      for (int i = 0; i < n; i++) {
        cout << "PARTITION " << i << " SIZE: " << partitionSizes[i] << "x" << sizeof(int) << "=" << partitionSizes[i] * sizeof(int) << " BYTES \n";
        total += partitionSizes[i] * sizeof(int);
        if ((partitionSizes[i] + 1) * sizeof(int) > MESSAGE_SIZE)
          cout << "WARNING: PARTITION SIZE SET LARGER THAN MESSAGE BUFFER -- PROGRAM MAY CRASH!\n";
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

      int tag = 1;

      // Local message buffer & rank from where to receive message
      char* inMessageBuffer = ((pthread_p*) params)->inMessageBuffer;
      int* partition = ((pthread_p*) params)->partition;
      int fromRank = ((pthread_p*) params)->fromRank;

      MPI_Status status;
      MPI_Recv(inMessageBuffer, MESSAGE_SIZE, MPI_CHAR, fromRank, tag, MPI_COMM_WORLD, &status);
      //cout << "+++++++++ REDUCER RECEIVED FROM: " << fromRank << endl;

      // Manage local partition buffer
      int partitionSize = deserialize(inMessageBuffer, partition);
      long partialSumOfSquares = sum(partition, partitionSize);
      //cout << "+++++++++ PARTIAL SUM OF SQUARES: " << partialSumOfSquares << endl;

      ((pthread_p*) params)->partialSumOfSquares = partialSumOfSquares;
      ((pthread_p*) params)->received = MESSAGE_SIZE;

      return NULL;
    } // End of Reducer thread

    void* mpiMapFunc() {

      int tag = 1, n = worldSize - 1; // rank 0 always reserved as master, rest as slaves

      if (rank == 0) { // Master node

        cout << "MAPREDUCE USING 1 MASTER & " << n << " WORKER(S).\n";

        partition(partitions, partitionSizes, intBuffer, intBufferSize, n); // these are the partitions of the window in integer format

        // Start Reducer threads and send partitions to Mapper processes in non-blocking manner
        for (int i = 0; i < n; i++) {
          params[i].fromRank = i + 1;
          params[i].inMessageBuffer = inMessageBuffers[i];
          params[i].partition = partitions[i];
          pthread_create(&threads[i], NULL, &mpiReduceFunc, (void*) &params[i]);

          serialize(outMessageBuffers[i], partitions[i], partitionSizes[i]);

          MPI_Isend(outMessageBuffers[i], MESSAGE_SIZE, MPI_CHAR, i + 1, tag, MPI_COMM_WORLD, &reqs[i]);
          SENT += MESSAGE_SIZE;
          //cout << "_________ SENT TO WORKER: " << i + 1 << " -> " << MESSAGE_SIZE << " BYTES.\n";
        }

        //MPI_Waitall(n, reqs, stats); // MPI synchronization
        //cout << "ALL MESSAGES SENT TO WORKERS." << endl;

        // Thread synchronization
        long totalSumOfSquares = 0;
        for (int i = 0; i < n; i++) {
          pthread_join(threads[i], NULL); // thread synchronization
          totalSumOfSquares += params[i].partialSumOfSquares;
          RECEIVED += params[i].received;
        }

        cout << "TOTAL SUM OF SQUARES: " << totalSumOfSquares << endl;

      } else { // Slave nodes

        MPI_Recv(inMessageBuffers[0], MESSAGE_SIZE, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &stats[0]);
        RECEIVED += MESSAGE_SIZE;
        //cout << "_________ RECEIVED BY WORKER: " << rank << " -> " << MESSAGE_SIZE << " BYTES.\n";

        partitionSizes[0] = deserialize(inMessageBuffers[0], partitions[0]); // keep actual size in separate variable
        square(partitions[0], partitionSizes[0]);
        serialize(outMessageBuffers[0], partitions[0], partitionSizes[0]);

        MPI_Send(outMessageBuffers[0], MESSAGE_SIZE, MPI_CHAR, 0, tag, MPI_COMM_WORLD);
        SENT += MESSAGE_SIZE;
        //cout << "_________ SENT BY WORKER: " << rank << " -> " << MESSAGE_SIZE << " BYTES.\n";
      }

      //cout << "RANK " << rank << " FINISHED." << endl;

      return NULL;
    } // End of Map function
};

void* timerFunc(void* params) {
  int i = 1;
  while (ACTIVE) {
    sleep(1);
    cout << "MASTER: " << (SENT / i) << " BYTES/SEC SENT, " << (RECEIVED / i) << " BYTES/SEC RECEIVED." << endl;
    i++;
  }
  return NULL;
}

int main(int argc, char* argv[]) {

  int level, worldSize, rank;

  // Initialize the MPI environment
  MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &level);

  // Get the size and ranks of all processes
  MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  ACTIVE = true;

  if (rank == 0) {
    pthread_t thread;
    pthread_create(&thread, NULL, &timerFunc, NULL);
  }

  MPI_MapReduce testObj(worldSize, rank);
  for (int i = 0; i < 1000; i++)
    testObj.mpiMapFunc();

  ACTIVE = false;

  // Close the MPI environment
  MPI_Finalize();

  return 0;
}
