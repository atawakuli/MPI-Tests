#include <mpi.h>
#include <math.h>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>

using namespace std;

typedef struct pthread_p {
    int fromRank; // rank from where incoming message is expected
} pthread_p;

void* mpiReceiver(void* params) {

  int fromRank = ((pthread_p*) params)->fromRank;

  int inMessage;

  MPI_Status status;
  MPI_Recv(&inMessage, 1, MPI_INT, fromRank, 123, MPI_COMM_WORLD, &status);
  cout << "+++++ THREAD RECEIVED FROM: " << fromRank << " -> " << inMessage << endl;

  return NULL;
}

int main(int argc, char *argv[]) {

  int world, rank, level;

  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &level);
  MPI_Comm_size(MPI_COMM_WORLD, &world);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  int n = world - 1;

  cout << "WORLD: " << world << ", RANK " << rank << " INIT.\n";

  if (rank == 0) {

    sleep(1); // let all slaves initialize

    pthread_p params[n];
    pthread_t threads[n];

    for (int i = 0; i < n; i++) {
      params[i].fromRank = i + 1;
      pthread_create(&threads[i], NULL, &mpiReceiver, (void*) &params[i]);
    }

    sleep(1); // all threads initialized

    int outMessages[n];

    MPI_Request reqs[n];
    MPI_Status stats[n];

    for (int i = 0; i < n; i++) {
      outMessages[i] = i + 1;
      MPI_Isend(&outMessages[i], 1, MPI_INT, i + 1, 123, MPI_COMM_WORLD, &reqs[i]);
      cout << "MASTER SENT TO SLAVE " << i + 1 << ":\t" << outMessages[i] << endl;
    }

    MPI_Waitall(n, reqs, stats); // wait for all messages to be sent

    for (int i = 0; i < n; i++) {
      pthread_join(threads[i], NULL);
    }

    sleep(1);

  } else {

    int inMessage;
    int outMessage;

    MPI_Status stat;

    // on each slave it is just fine to use a synchronous receive/send pair
    MPI_Recv(&inMessage, 1, MPI_INT, 0, 123, MPI_COMM_WORLD, &stat);
    cout << "_____ SLAVE " << rank << " RECEIVED:\t" << inMessage << endl;

    if (rank == 1) // simulate high workload on slave #1 only
      sleep(1);

    outMessage = inMessage * 10;

    MPI_Send(&outMessage, 1, MPI_INT, 0, 123, MPI_COMM_WORLD);
    cout << "_____ SLAVE " << rank << " SENT:\t" << outMessage << endl;
  }

  sleep(1);

  cout << "RANK " << rank << " FINALIZE." << endl;

  MPI_Finalize();
  return 0;
}

