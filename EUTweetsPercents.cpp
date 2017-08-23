/*
 ============================================================================
 Name        : EUTweetsSums.c
 Supervisor	 : Prof. Dr. Martin Theobald
 Author      : at
 Version     :
 Copyright   : University of Luxembourg
 Description : Implementation of usecase 2 (Asynchronous).
 ============================================================================
 */
#include <mpi.h>
#include <math.h>
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <curl/curl.h>
#include <oauth.h>
#include <string.h>
#include <sstream>

using namespace std;

//Global Variables visible within a node and its threads
static const int WIN_SIZE=100;
static string window;
static int windowPointer, windowCounter, tweetCount;
static long maxTweet, gCount, fCount, aCount;
static int worldSize, rank, tag = 1, level, n, tag2 = 2, tag3 = 3;
double t1, t2;
typedef struct pthread_p {
    int fromRank; // rank from where incoming message is expected
} pthread_p;
static MPI_Request reqs[3];
static MPI_Status stats[3];
static pthread_p params[3];
static pthread_t threads[3];

class Usecase1{
public:
	Usecase1();

	void* mpiFunc(){
		//MPI Initialization
		MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &level);
		// Get the size and ranks of all processes
		MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		n = worldSize - 1; // rank 0 always reserved as master, rest as slaves
		//Master
		if(rank == 0){
			for (int i = 0; i < n; i++) {
				params[i].fromRank = i + 1;
				pthread_create(&threads[i], NULL, &aSyncMasterReceive, (void*) &params[i]);
			}
			connectToTweeter();
		}
		//Slaves
		else{
			MPI_Status status;
			int size;
			while(true){
				MPI_Probe(0, tag, MPI_COMM_WORLD, &stats[rank-1]);
				MPI_Get_count(&stats[rank-1], MPI_CHAR, &size);
				char *msgIn = new char[size];
				MPI_Recv(msgIn, size, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &status);
				string tmp(msgIn);
				//need to do "clean up" when "new" used
				delete[] msgIn;
				long * results;
				double percent;
				MPI_Request sReq;
				switch(rank){
				case 1:{
					//First Stage of Usecase2
					results = partition(tmp, "DE");
					//Second Stage of Usecase2
					MPI_Isend(&results[0], 1, MPI_LONG, 0, tag2, MPI_COMM_WORLD, &sReq);
					percent = percentage(results);
					MPI_Send(&percent, 1, MPI_DOUBLE, 0, tag3, MPI_COMM_WORLD);
					break;
				}
				case 2:{
					sleep(2);
					results = partition(tmp, "FR");
					MPI_Isend(&results[0], 1, MPI_LONG, 0, tag2, MPI_COMM_WORLD, &sReq);
					percent = percentage(results);
					MPI_Send(&percent, 1, MPI_DOUBLE, 0, tag3, MPI_COMM_WORLD);
					break;
				}
				case 3:{
					results = partition(tmp, "AD");
					MPI_Isend(&results[0], 1, MPI_LONG, 0, tag2, MPI_COMM_WORLD, &sReq);
					percent = percentage(results);
					MPI_Send(&percent, 1, MPI_DOUBLE, 0, tag3, MPI_COMM_WORLD);
					break;
					}
				}
			}
		}
		MPI_Finalize();
		return NULL;
	}

private:
	//Master Function - main thread
	 static void connectToTweeter(){
			char* cSignedUrl;
			//Getting Tweets that consist of geographic terms (Germany, France, Andorra) because location data collected by Twitter are not included in the stream data.
			const char* URL = "https://stream.twitter.com/1.1/statuses/filter.json?track=germany,france,andorra,deutschland";//"https://stream.twitter.com/1.1/statuses/sample.json";//
			//Need to be placed somewhere else
			const char* CKEY = "VRL9akJJKzpBoxNFJDIL6qyX7";
			const char* CSEC = "fPwON0WvF2Pjf2D0avAimEnJFN4IpbGblkkVLFbzFsmIScPoN9";
			const char* TKEY = "897027147059146752-ZprY3BaR55ztDDn1wcqTrSrYAVPEhS3";
			const char* TSEC = "ljtezjDpaeepb5KE3vJhjzj3zOOJstnPfqNYCX6UIUdiD";
			string tweets;
			CURL* curl = curl_easy_init();
			cSignedUrl = oauth_sign_url2(URL, NULL, OA_HMAC, "GET", CKEY, CSEC, TKEY, TSEC);
			curl_easy_setopt(curl, CURLOPT_URL, cSignedUrl);
			curl_easy_setopt(curl, CURLOPT_USERAGENT, "user-string");
			curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1);
			curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, processTweets);
			curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&tweets);
			int error = curl_easy_perform(curl);
			//Check if curl execution is OK
			if(error != CURLE_OK)
			{
				wcout << "\n[ERROR]: curl_easy_perform() failed: "<< error <<endl;
			}
			// cURL Cleanup
			curl_easy_cleanup(curl);
			curl_global_cleanup();
	}

	//Master Function - main thread
	static size_t processTweets(char* ptr, size_t size, size_t nmemb, string* stream) {
		int iRealSize = size * nmemb;
		stream->append(ptr, iRealSize);
		string str = *stream;
		//Some callbacks will be empty (according to twitter)
		if(!str.empty()){
			istringstream tweetStream(str);
			string tweet;
			//Get Tweets per callback. According to Twitter, tweets are separated by \n
			while(getline(tweetStream, tweet, '\n')) {
				tweetCount++;
				windowing(tweet);
			}
		}
		return iRealSize;
	}

	//Master Function - main thread
	static void windowing(string tweet){
		if(windowPointer >= WIN_SIZE){
			//Ship-off window for partitioning
			shipOff(window);
			windowCounter++;
			windowPointer = 0;
			maxTweet = 0;
			window.clear();
		}
		window = window+tweet+"\n";
		windowPointer++;
	}

	//Master Function - main thread
	static void shipOff(string window){
		for (int i = 0; i < n; i++) {
			const char* tmp  = window.c_str();
			t1 = MPI_Wtime();
			MPI_Isend(tmp, strlen(tmp), MPI_CHAR, i + 1, tag, MPI_COMM_WORLD, &reqs[i]);
		}
		//barrier
		//will stop sending until all receive
		//iteration dataflow synchronous
		//dataflow within the DAG asynchronous
		MPI_Waitall(n, reqs, stats); // wait for all messages to be sent
		//This is possible resolution to make iterations asynchronous, however adds segmentation error --> buffers not empty while sends accummulate.
		/*int index;
		MPI_Waitany(n, reqs, &index, stats);*/
	}

	//Master Function - side threads
	static void* aSyncMasterReceive(void* params) {
	  int fromRank = (int)((pthread_p*) params)->fromRank;
	  int test = fromRank;
	  long inMessage;
	  double percent;
	  MPI_Status status1, status2;
	  MPI_Request rReq1, rReq2;
	  while(true){
		  //Receiving window Count
		  MPI_Irecv(&inMessage, 1, MPI_LONG, test, tag2, MPI_COMM_WORLD, &rReq1);
		  //Receiving window Percentage
		  MPI_Irecv(&percent, 1, MPI_DOUBLE, test, tag3, MPI_COMM_WORLD, &rReq2);
		  //Rank1 handles Germany
		  if(test==1){
			  long oldCount = gCount;
			  static long results[2];
			  results[0] = oldCount;
			  results[1] = tweetCount;
			  double globalPercent = percentage(results);
			  MPI_Wait(&rReq1, &status1);
			  gCount = gCount+inMessage;
			  cout<<"Germany's Thread:\n";
			  cout<<"Previous global tweets count: "<<oldCount<<" || received "<<inMessage<<" new tweets || new total count: "<<gCount<<endl;
			  MPI_Wait(&rReq2, &status2);
			  t2 = MPI_Wtime();
			  cout<<"Window percent: "<<percent<<" || Global Percent (before adding window count): "<<globalPercent<<" || Window percentage received is "<<((globalPercent<percent) ? "higher" : "lower/same")<<" than global percentage.\n";
			  double tElapsed= t2-t1;
			  cout<<"Pipeline duration from master window send: "<<tElapsed<<" seconds.\n";
			  cout<<"////////////////////////////////////////////////////////////////////////////\n";
		  }
		  //Rank2 handles France
		  else if(test==2){
			  long oldCount = fCount;
			  static long results[2];
			  results[0] = oldCount;
			  results[1] = tweetCount;
			  double globalPercent = percentage(results);
			  MPI_Wait(&rReq1, &status1);
			  fCount = fCount+inMessage;
			  cout<<"France's Thread:\n ";
			  cout<<"Previous global tweets count: "<<oldCount<<" || received "<<inMessage<<" new tweets || new total count: "<<fCount<<endl;
			  MPI_Wait(&rReq2, &status2);
			  t2 = MPI_Wtime();
			  cout<<"Window percent: "<<percent<<" || Global Percent (before adding window count): "<<globalPercent<<" || Window percentage received is "<<((globalPercent<percent) ? "higher" : "lower/same")<<" than global percentage.\n";
			  double tElapsed= t2-t1;
			  cout<<"Pipeline duration from master window send: "<<tElapsed<<" seconds.\n";
			  cout<<"////////////////////////////////////////////////////////////////////////////\n";
		  }
		  //Rank3 handles Andorra
		  else if(test==3){
			  long oldCount = aCount;
			  static long results[2];
			  results[0] = oldCount;
			  results[1] = tweetCount;
			  double globalPercent = percentage(results);
			  MPI_Wait(&rReq1, &status1);
			  aCount = aCount+inMessage;
			  cout<<"Andorra's Thread:\n";
			  cout<<"Previous global tweets count: "<<oldCount<<" || received "<<inMessage<<" new tweets || new total count: "<<aCount<<endl;
			  MPI_Wait(&rReq2, &status2);
			  t2 = MPI_Wtime();
			  cout<<"Window percent: "<<percent<<" || Global Percent (before adding window count): "<<globalPercent<<" || Window percentage received is "<<((globalPercent<percent) ? "higher" : "lower/same")<<" than global percentage.\n";
			  double tElapsed= t2-t1;
			  cout<<"Pipeline duration from master window send: "<<tElapsed<<" seconds.\n";
			  cout<<"////////////////////////////////////////////////////////////////////////////\n";
		  }
		  else{
			  cout<<"Handling three countries only Germany, France and Andorra. \n";
		  }
	  }
	  return NULL;
	}

	//Slave Function - Location based partitioning with total Tweets count.
	static long * partition(string window, string country)
	{
		long total = 0;
		long count = 0;
		static long results[2];
		string tweet;
		istringstream tweetStream(window);
		while(getline(tweetStream, tweet, '\n')) {
			total++;
			if(country == "DE"){
				if(tweet.find("germany")!=string::npos || tweet.find("deutschland")!=string::npos || tweet.find("Deutschland")!=string::npos || tweet.find("Germany")!=string::npos || tweet.find("GERMANY")!=string::npos || tweet.find("DEUTSCHLAND")!=string::npos){
					count++;
				}
			}
			else if(country == "FR"){
				if(tweet.find("france")!=string::npos || tweet.find("FRANCE")!=string::npos || tweet.find("France")!=string::npos ){
					count++;
				}
			}
			else if(country == "AD"){
				if(tweet.find("andorra")!=string::npos || tweet.find("Andorra")!=string::npos || tweet.find("ANDORRA")!=string::npos){
					count++;
				}
			}
			else{
				count = -1;
			}
		}
		results[0] = count;
		results[1] = total;
		return results;
	}

	static double percentage(long * results){
		if(results[0]==0 || results[1]==0 ){
			return 0;
		}
		else{
			return ((double)results[0]/(double)results[1])*(double)100;
		}
	}
};

//class constructor
Usecase1::Usecase1(){
	window.clear();
	windowPointer = 0;
	windowCounter = 0;
	tweetCount = 0;
	maxTweet = 0;
	gCount = 0;
	fCount = 0;
	aCount = 0;
}

int main(int argc, char *argv[]) {
	Usecase1 test;
	test.mpiFunc();
	//MPI::Finalize();
	return 0;
}
