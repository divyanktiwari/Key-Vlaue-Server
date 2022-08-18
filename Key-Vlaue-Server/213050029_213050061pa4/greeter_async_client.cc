
/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <time.h>
#include <iomanip> 
#include <vector>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>	


#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::PutReply;
using helloworld::PutRequest;
using helloworld::GetReply;
using helloworld::GetRequest;
using helloworld::DelReply;
using helloworld::DelRequest;
using namespace std;
using grpc::ClientAsyncResponseReader;	
using grpc::CompletionQueue;	

class GreeterClient {
 public:
  explicit GreeterClient(std::shared_ptr<Channel> channel)
      : stub_(Greeter::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  vector<pair<int, string>> Put( const std::string& key, const std::string& value) {
    // Data we are sending to the server.
    PutRequest prequest;
   
    prequest.set_key(key);
    prequest.set_value(value);
    

    // Container for the data we expect from the server.
    PutReply preply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the
    // gRPC runtime.
    CompletionQueue cq;

    // Storage for the status of the RPC upon completion.
    Status status;

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    std::unique_ptr<ClientAsyncResponseReader<PutReply> > rpc(
        stub_->PrepareAsyncPut(&context, prequest, &cq));

    // StartCall initiates the RPC call
    rpc->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&preply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    GPR_ASSERT(cq.Next(&got_tag, &ok));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    GPR_ASSERT(got_tag == (void*)1);
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    GPR_ASSERT(ok);

    // Act upon the status of the actual RPC.

    // Act upon its status.
  if (status.ok()) {
      // return atoi(preply.status()) + ":" + preply.message();
      vector<pair<int, string>> vec = {{preply.status(), preply.message()}};
      return vec;
    } else {
      // std::cout << status.error_code() << ": " << status.error_message()
      //           << std::endl;
      // // return "RPC failed";
      // return 1;
      vector<pair<int, string>> vec = {{preply.status(), preply.message()}};
      return vec;
    }
  }
   vector<pair<int, string>> Get(const std::string& key) {
    // Data we are sending to the server.
    GetRequest grequest;
   
    grequest.set_key(key);
    

    // Container for the data we expect from the server.
    GetReply greply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the	
    // gRPC runtime.	
    CompletionQueue cq;	
    // Storage for the status of the RPC upon completion.	
    Status status;	

    // stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    std::unique_ptr<ClientAsyncResponseReader<GetReply> > rpc(
        stub_->PrepareAsyncGet(&context, grequest, &cq));

    // StartCall initiates the RPC call
    rpc->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&greply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    GPR_ASSERT(cq.Next(&got_tag, &ok));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    GPR_ASSERT(got_tag == (void*)1);
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    GPR_ASSERT(ok);

    // Act upon the status of the actual RPC.

    // Act upon its status.

    // Act upon its status.
   if (status.ok()) {
      vector<pair<int, string>> vec = {{greply.status(), greply.message()}};
      return vec;
    } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      vector<pair<int, string>> vec = {{400, "Connection Failed!!!"}};
      return vec;
    }
  }
vector<pair<int, string>> Del(const std::string& key) {
    // Data we are sending to the server.
    DelRequest drequest;

    drequest.set_key(key);
  

    // Container for the data we expect from the server.
    DelReply dreply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The producer-consumer queue we use to communicate asynchronously with the	
    // gRPC runtime.	
    CompletionQueue cq;	
    // Storage for the status of the RPC upon completion.	
    Status status;	

    /// stub_->PrepareAsyncSayHello() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    std::unique_ptr<ClientAsyncResponseReader<DelReply> > rpc(
        stub_->PrepareAsyncDel(&context, drequest, &cq));

    // StartCall initiates the RPC call
    rpc->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the integer 1.
    rpc->Finish(&dreply, &status, (void*)1);
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    // The return value of Next should always be checked. This return value
    // tells us whether there is any kind of event or the cq_ is shutting down.
    GPR_ASSERT(cq.Next(&got_tag, &ok));

    // Verify that the result from "cq" corresponds, by its tag, our previous
    // request.
    GPR_ASSERT(got_tag == (void*)1);
    // ... and that the request was completed successfully. Note that "ok"
    // corresponds solely to the request for updates introduced by Finish().
    GPR_ASSERT(ok);

    // Act upon the status of the actual RPC.

    // Act upon its status.

  if (status.ok()) {
      vector<pair<int, string>> vec = {{dreply.status(), dreply.message()}};
      return vec;    
      } else {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      vector<pair<int, string>> vec = {{400, "Connection Failed!!!"}};
      return vec;
    }
  }
 private:
  std::unique_ptr<Greeter::Stub> stub_;
};

int main(int argc, char** argv) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint specified by
  // the argument "--target=" which is the only expected argument.
  // We indicate that the channel isn't authenticated (use of
  // InsecureChannelCredentials()).

  ifstream in_file("../../config.txt");

  std::string target_str;
  
 getline(in_file, target_str);
    // in_file.close();
  GreeterClient greeter(
      grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
      
  std::string key;
  std::string value;
  
  std::string line;
   std::string mode;
   std::string cmd;
   std::string choice;
  std::string flname;
  using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    auto t1 = high_resolution_clock::now();
        auto t2 = high_resolution_clock::now();
  auto ms_int=duration_cast<milliseconds>(t2 - t1);
  auto total_time=duration_cast<milliseconds>(t2 - t1);
   auto t3 = high_resolution_clock::now();
      sleep(1);
        auto t4 = high_resolution_clock::now();
 auto mintime=duration_cast<milliseconds>(t4 - t3);;
  auto maxtime=duration_cast<milliseconds>(t2 - t1);;

 


  cout<<"Enter BATCH for batch mode and INTER for interactive mode::  ";
   cin >> mode;
   
   if(mode=="BATCH"){
   
 //    cout<<"Enter file name::  ";
   //  cin >> flname;
     
     ifstream infile("../../cmd.txt");
    
    int count=0;

     
     while(getline(infile,line)){
       count++;
         int i=0;
         cmd="";
         key="";
         value="";

         char *token = strtok(&line[0], " ");
   
    
    if(token!= NULL){
      cmd=token;
       token = strtok(NULL, " ");
    }
    if(token!= NULL){
      key=token;
       token = strtok(NULL, " ");
    }
   if(token!= NULL){
      value=token;
       token = strtok(NULL, " ");
    }
   
       if(cmd=="GET"){
         auto t1 = high_resolution_clock::now();
       
         auto res = greeter.Get(key); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          ms_int = duration_cast<milliseconds>(t2 - t1);
    
    
         std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
        
         
      }
      else if(cmd=="PUT" || cmd=="DEL"){
         if(cmd=="PUT"){
           auto t1 = high_resolution_clock::now();
       
         auto res = greeter.Put(key,value); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          ms_int = duration_cast<milliseconds>(t2 - t1);
            //std::cout << reply << std::endl;
            std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
            
         }
         else{
           auto t1 = high_resolution_clock::now();
       
         auto res = greeter.Del(key); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          ms_int = duration_cast<milliseconds>(t2 - t1);
          std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
       
         }

      }
         
      if(ms_int.count()>maxtime.count()){
       maxtime=ms_int;
      
      }
      if(ms_int.count()<mintime.count()){
      mintime=ms_int;
      }

        total_time+=ms_int;
     }
      total_time=total_time/count;

     cout << "maximum time for the batch : " << fixed << maxtime.count();
    cout << " milisec " << endl;
    cout << "minimum time for the batch : " << fixed << mintime.count();
    cout << " milisec " << endl;
   cout << "average time for the batch : " << fixed  << total_time.count();
    cout << " milisec " << endl;

   

    
     infile.close();

   }
   else if(mode=="INTER"){
      while(1){
      
      cout<< "Enter PUT , GET or DEL to select operation::  ";
      cin >> cmd;
      if(cmd=="GET"){
         cout<< "Enter key :: ";
         cin >> key;
          auto t1 = high_resolution_clock::now();
          
         auto res = greeter.Get(key); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          auto ms_int = duration_cast<milliseconds>(t2 - t1);

  
    
         std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
            cout << "TIME TAKEN : " << fixed << ms_int.count();
             cout << " milisec " << endl;
      }
      else if(cmd=="PUT" || cmd=="DEL"){
        if(cmd=="PUT"){
        cout<< "Enter key :: ";
         cin >> key;
         cout<< "Enter value :: ";
         cin >> value;
        }
        else{
          cout<< "Enter key :: ";
         cin >> key;
        }
         if(cmd=="PUT"){
                     auto t1 = high_resolution_clock::now();
          
         auto res = greeter.Put(key,value); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          auto ms_int = duration_cast<milliseconds>(t2 - t1);

            //std::cout << reply << std::endl;
            std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
            cout << "TIME TAKEN : " << fixed << ms_int.count();
             cout << " milisec " << endl;
         }
         else{
          auto t1 = high_resolution_clock::now();
          
         auto res = greeter.Del(key); // The actual RPC call!
        auto t2 = high_resolution_clock::now();
      
          auto ms_int = duration_cast<milliseconds>(t2 - t1);
          std::cout << "RESULT :: " << res[0].first << res[0].second << std::endl;
          cout << "TIME TAKEN : " << fixed << ms_int.count();
             cout << " milisec " << endl;
         }

      }
      else{
        cout<<"WRONG INPUT"<<endl;
      }
     
       cout<<"Enter YES to continue or NO to exit::  ";
   cin>> choice;
   if(choice=="YES"){
     continue;
   }
   else{
     cout<<"client exited as YES not entered "<<endl;
     return 0;
   }
    
}
   }

   
   else{
     cout<<"WRONG INPUT"<<endl;
   }


   
  return 0;
}

