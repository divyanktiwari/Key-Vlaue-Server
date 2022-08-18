
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <fstream>
#include <vector>
#include <list>
#include <unistd.h>
#include <mutex>
#include <chrono>


#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/helloworld.grpc.pb.h"
#else
#include "helloworld.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using helloworld::Greeter;
using helloworld::PutReply;
using helloworld::PutRequest;
using helloworld::GetReply;
using helloworld::GetRequest;
using helloworld::DelReply;
using helloworld::DelRequest;
using namespace std;


#define file_size 10000
#define max_files 13
string port;
  int flag_cl=0;
string cache_type;
string cache_size1;
int cache_size;
string threadpool;
fstream update_logs_file;
auto start_time = std::chrono::high_resolution_clock::now();
string replacement_policy;
int cache_counter=0;


typedef struct file_t{
    string file_name;
    fstream f_handle;
}file_tab;
file_tab file_table[13];

typedef struct CACHE_d{
  std::string key, value;
  int readers_count=0;
  // chrono::duration_cast<std::chrono::nanoseconds> LRU;
  std::atomic<unsigned long long> LRU;
  std::atomic<int> LFU;
  bool dirty;
  mutex writer_ready, readers_present, mutex_lock;
  // mutex policy_lock;
}CACHE_def;

typedef struct INDEX_d{
  std::string key;
  int File_name, Line_num;
}INDEX_def;

CACHE_def *CACHE;
std::unordered_map<string, int> CACHE_MAP;
mutex CACHE_mutex_lock;//INDEX_writer_ready, INDEX_readers_present,


vector <vector <int>> BITMAP;
// vector<INDEX_def> INDEX ;
mutex BITMAP_mutex_lock;
std::unordered_map<string, INDEX_def> INDEX;
mutex INDEX_mutex_lock;//INDEX_writer_ready, INDEX_readers_present,
// int INDEX_readers_count = 0;

string file_op(string file_name, int line_num, string key, string value, string operation)
{
    int i;
    for(i=0; i<max_files ; i++)
    {
        // cout << file_table[i].file_name << " = " << file_name << endl;
        if(file_table[i].file_name == file_name)
        {
            break;
        }
    }
    // fstream f_handle = file_table[i].f_handle;
    if(operation == "INSERT")
    {
        if(file_name != "update_logs.txt" && file_name != "server_logs.txt") 
        {
          // cout << "Inside INSERT for file :" << file_name << "line : " << line_num << endl;
          file_table[i].f_handle.seekp (515*line_num , ios::beg);
          // cout << file_table[i].f_handle.tellp() <<endl;
          string keyval = key + ":" + value + "~" + string(514-key.length()-value.length()-1 , '~') ;
          // cout << string(keyval).length() << keyval<<endl;
          const char *keyval_chars = keyval.c_str();
          // cout << keyval_chars << endl;
          file_table[i].f_handle.write(keyval_chars, 514);
          file_table[i].f_handle << endl;
          return "Inserted!";
        }
        else if(file_name == "update_logs.txt")
        {
          file_table[i].f_handle << key + string(":") + value << endl;
          return "Inserted!";
        }
        else{
          file_table[i].f_handle << key << endl;
          return "Inserted!";
        }
    }
    else if(operation == "DELETE")
    {
        // cout << "Inside DELETE for file :" << file_name << "line : " << line_num << endl;
        file_table[i].f_handle.seekp (515*line_num);
        // cout << file_table[i].f_handle.tellp() <<endl;
        string keyval = string(514, '~');
        // cout << string(keyval).length() << keyval<<endl;
        file_table[i].f_handle.write (keyval.c_str(), 514);
        return "Deleted!";
    }
    else
    {
        // cout << "Inside READ for file : " << file_name << "line : " << line_num << " index : " << i << endl;
        file_table[i].f_handle.seekg(515*line_num);
        char memblock [515];
        file_table[i].f_handle.read (memblock, 513);
        string keyval = string(memblock);
        keyval = keyval.substr(0, keyval.find("~")).substr(keyval.find(":")+1,keyval.length());
        return keyval;
    }
}


void server_initialise()
{
    string file_name;
    string key_val;
    int j=0;
    int i=1;

    for(i=1; i < 10; i++)//Assuming a max of 10 files
    {
        file_name = "file_" + to_string(i);
        ifstream ip_file("../../"+file_name);

        // 
        file_table[i+3].file_name = "file_" + to_string(i);
        file_table[i+3].f_handle.open("../../"+file_name, ios::in | ios::out );
        // 

        if(file_table[i+3].f_handle.fail())
            {
                //File does not exist code here
               // cout << "File " << file_name << " does not exist here" << endl;
                break;
            }
        else
        {
            // cout << "File " << file_name << " Exist" << endl;
            j=0;
            std::vector <int> Row;
            while (getline (ip_file, key_val)) 
            {
                // Add record mapping to INDEX and update bitmap
                if(key_val.substr(0,1) != "~")
                {
                    // cout << "REALLY????" << endl;
                    Row.push_back(1);
                    INDEX_def record;
                    record.key=key_val.substr(0, key_val.find("~")).substr(0,key_val.find(":"));
                    record.File_name= i ;
                    record.Line_num= j ;
                    // INDEX.push_back(record);

                    INDEX[record.key] = record;

                    if(cache_counter < cache_size)
                    {
                      /* // std::string key, value, LRU, LFU, dirty;
                      // cout << "Came in for " << key_val;
                      CACHE_def cache_record;
                      cache_record.key = record.key;
                      cache_record.value = key_val.substr(key_val.find(":")+1, key_val.length());
                      cache_record.LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
                      cache_record.LFU = 1;
                      cache_record.dirty = false;

                      CACHE.emplace(CACHE.begin(), cache_record);
                       */
                      CACHE_MAP[record.key] = cache_counter;
                      CACHE[cache_counter].key = record.key;
                      CACHE[cache_counter].value = key_val.substr(0, key_val.find("~")).substr(key_val.find(":")+1,key_val.length());;
                      CACHE[cache_counter].LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
                      CACHE[cache_counter].LFU = 1;
                      CACHE[cache_counter].dirty = false;
                      cache_counter++;
                    }
                }
                else
                {
                    Row.push_back(0);
                }
                j++;   
            }
            for(int t= Row.size(); t < file_size; t++)
            {
                Row.push_back(0);
            }
            BITMAP.push_back(Row);
            
        }
    }

    for(; cache_counter < cache_size; cache_counter++)
            {
                /* CACHE_def cache_record;
                cache_record.key = "";
                cache_record.value = "";
                cache_record.LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
                cache_record.LFU = 0;
                cache_record.dirty = false;

                CACHE.push_back(cache_record); */
                
                CACHE[cache_counter].key = "";
                CACHE[cache_counter].value = "";
                CACHE[cache_counter].LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
                CACHE[cache_counter].LFU = 0;
                CACHE[cache_counter].dirty = false;
            }


    if(i==1)
    {
        // no input files. 
        // Create first file and initialse bitmap
        // ofstream cur_file;
        // cur_file.open("../../" + string("file_1"), fstream::out);


        file_table[i+3].file_name = "file_" + to_string(i);
        file_table[i+3].f_handle.open("../../" + string("file_1"),  ios::in | ios::out | ios::trunc);

        std::vector <int> Row;
        for(int i=0; i<file_size; i++)
        {
                Row.push_back(0);
                // cur_file << "";
        }
        BITMAP.push_back(Row);

        // cout << "INDEX Size" << INDEX.size() << endl;
    }

    // Reading and updating updates logs remaining from last server run
    // ifstream log_file("../../" + string("update_logs.txt"));


    // update_logs_file.open("../../update_logs.txt", ios::in);

    file_table[2].file_name = "update_logs.txt";
    file_table[2].f_handle.open("../../update_logs.txt", ios::in | ios::out );

    if(file_table[2].f_handle)
    {  
      // cout<<"------------------------- YES WE GONNA UPDATEEEEE!" << endl;
      while (getline (file_table[2].f_handle, key_val)) 
      {
          // cout << key_val.substr(0, key_val.find(":")) << endl;
          if(key_val.length()>1)
          {
              /* for (int i=0; i < INDEX.size(); i++)
              {
                  if (INDEX[i].key == key_val.substr(0, key_val.find(":")))
                  {
                      // cout << "[[[]]]]" << INDEX[i].key << " ==== " << key_val.substr(0, key_val.find(":")) << endl;
                      // update_file(key_val.substr(0, key_val.find(":")), key_val.substr(key_val.find(":")+1, key_val.length()), "file_" +  to_string(INDEX[i].File_name), INDEX[i].Line_num);
                      file_op("file_" +  to_string(INDEX[i].File_name), INDEX[i].Line_num, key_val.substr(0, key_val.find(":")), key_val.substr(key_val.find(":")+1, key_val.length()), "INSERT");
                  }
              } */

              if( INDEX.find(key_val.substr(0, key_val.find(":"))) != INDEX.end())
              {
                      file_op("file_" +  to_string(INDEX[key_val.substr(0, key_val.find(":"))].File_name), INDEX[key_val.substr(0, key_val.find(":"))].Line_num, key_val.substr(0, key_val.find(":")), key_val.substr(key_val.find(":")+1, key_val.length()), "INSERT");
              }
              else
              {
                string key = key_val.substr(0, key_val.find(":"));
                string value = key_val.substr(key_val.find(":")+1, key_val.length());
                bool flag = false;
                for (int i = 0; i < BITMAP.size(); i++)
                {
                  for(int j=0; j<BITMAP[i].size() ; j++)
                  {
                    if (BITMAP[i][j]==0)
                    {
                      BITMAP[i][j] = 1;
                      flag= true;
                      file_op("file_" + to_string(i+1), j , key , value , "INSERT");
                      
                      INDEX_def record;
                      record.key=key;
                      record.File_name= i+1 ;
                      record.Line_num= j ;
                      INDEX[record.key] = record;
                      // cout << "INDEX should have stuff inserted here for key:"<<key <<" File_name: "<< record.File_name<<" Line_num"<< record.Line_num << endl;
                      break;
                    }
                  }
                }
            
                if(!flag)
                {
                  file_table[BITMAP.size()+1+3].file_name = "file_" + to_string(BITMAP.size()+1);
                  file_table[BITMAP.size()+1+3].f_handle.open("../../file_" + to_string(BITMAP.size()+1), ios::in | ios::out | ios::trunc);
                  // cout << "File_name opened : " << file_table[BITMAP.size()+1+3].file_name << endl;
                  INDEX_def record;
                  record.key=key;
                  record.File_name= BITMAP.size() + 1;
                  record.Line_num= 0 ;
                  INDEX[record.key] = record;
                  std::vector <int> Row;
                  Row.push_back(1);
                  for(int i=1; i<file_size; i++)
                  {
                          Row.push_back(0);
                          // cur_file << "" << endl;
                  }
                  BITMAP.push_back(Row);
                  
                  file_op("file_" + to_string(BITMAP.size()), 0 , key , value , "INSERT");
                  }
                  if(CACHE_MAP.count(key_val.substr(0, key_val.find(":"))) > 0)
                  {
                    CACHE[CACHE_MAP.count(key_val.substr(0, key_val.find(":")))].value = key_val.substr(key_val.find(":")+1, key_val.length());
                  }
                }
              /* for (int i=0; i < cache_size; i++)
              {
                  if (CACHE[i].key == key_val.substr(0, key_val.find(":")))
                  {
                      // cout << "[[[]]]]" << INDEX[i].key << " ==== " << key_val.substr(0, key_val.find(":")) << endl;
                      // update_file(key_val.substr(0, key_val.find(":")), key_val.substr(key_val.find(":")+1, key_val.length()), "file_" +  to_string(INDEX[i].File_name), INDEX[i].Line_num);
                      CACHE[i].value = key_val.substr(key_val.find(":")+1, key_val.length());
                      
                      // CACHE.push_back(cache_record);
                  }
              } */

          }
      }
      file_table[2].f_handle.close();
      file_table[2].f_handle.open("../../update_logs.txt", ios::in | ios::out | ios::trunc);
      // update_logs_file.close();
    }
    else
    {
      file_table[2].f_handle.open("../../update_logs.txt", ios::in | ios::out | ios::trunc);
    }
    
}

void write_to_CACHE(std::string key, std::string value)
{
  CACHE_mutex_lock.lock();
  int cache_count = CACHE_MAP.count(key);
  int c_ind = CACHE_MAP[key];
  CACHE_mutex_lock.unlock();

  if(cache_count)
  {
    // Entry Section
    CACHE[c_ind].writer_ready.lock();
    CACHE[c_ind].readers_present.lock();
    // Critical section
    CACHE[c_ind].key = key;
    CACHE[c_ind].value = value;
    
    // // Exit Section
    CACHE[c_ind].writer_ready.unlock();
    CACHE[c_ind].readers_present.unlock();
    return;
  }
  /* for (int i=0; i < cache_size; i++)
  {
    if (CACHE[i].key == "")
    {
      // // Entry Section
      CACHE[i].writer_ready.lock();
      CACHE[i].readers_present.lock();
      // Critical section
      CACHE[i].key = key;
      CACHE[i].value = value;
      
      // // Exit Section
      CACHE[i].writer_ready.unlock();
      CACHE[i].readers_present.unlock();
      return;

    }
  } */

  
  // Write replacement policy code here
  int index_to_replace = 0;

  if (replacement_policy == "LFU")
  {
    int min = CACHE[0].LFU;
    int min_index = 0;
    for (int i=1; i < cache_size; i++)
    {
      // cout << " min : " << min << "LFU val : " << CACHE[i].LFU << endl;
      if (min > CACHE[i].LFU)
      {
        min = CACHE[i].LFU;
        min_index = i;
      }
    }
    index_to_replace = min_index;
  }

  else
  {
    int i;
    long min = CACHE[0].LRU;
    int min_index = 0;
    for (int i=1; i < cache_size; i++)
    {
      // cout << " min : " << min << "LFU val : " << CACHE[i].LRU << endl;
      if ( min > CACHE[i].LRU)
      {
        min = CACHE[i].LRU;
        min_index = i;
      }
    }
    index_to_replace = min_index;
  }

  // cout << "Replacing entry:" << index_to_replace+1 << endl;
  // For now replacing first entry. handling dirty page also
  
  if(CACHE[index_to_replace].dirty == true)
  {
    // Serach in index for location:
    /* for (int i=0; i < INDEX.size(); i++)
    {
      if (INDEX[i].key == CACHE[index_to_replace].key)
      {
        // update_file(CACHE[index_to_replace].key, CACHE[index_to_replace].value, "file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);
        file_op("file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num, CACHE[index_to_replace].key, CACHE[index_to_replace].value, "INSERT");

      }
    } */
    if(INDEX.find(CACHE[index_to_replace].key) != INDEX.end())
    {
        file_op("file_" + to_string(INDEX[CACHE[index_to_replace].key].File_name), INDEX[CACHE[index_to_replace].key].Line_num, CACHE[index_to_replace].key, CACHE[index_to_replace].value, "INSERT");
    }
  }

  // Entry Section
  CACHE[index_to_replace].writer_ready.lock();
  CACHE[index_to_replace].readers_present.lock();
  
  // Critical section
  CACHE[index_to_replace].key = key;
  CACHE[index_to_replace].value = value;
  CACHE[index_to_replace].dirty = false;
  CACHE[index_to_replace].LFU = 1;
  CACHE[index_to_replace].LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();

  
  // // Exit Section
  CACHE[index_to_replace].writer_ready.unlock();
  CACHE[index_to_replace].readers_present.unlock();
}

void print_meta()
{
  
/*
  INDEX_mutex_lock.lock();
  for (auto x : INDEX)
  {
      cout << x.second.key << " file : " <<x.second.File_name << " line : " <<x.second.Line_num << endl;
  }
  INDEX_mutex_lock.unlock();
*/

 // cout << "Printing CACHE" << endl;
  for(int i=0; i< cache_size; i++)
  {
      // Reader's Entry section
      CACHE[i].writer_ready.lock();
      CACHE[i].writer_ready.unlock();
      CACHE[i].mutex_lock.lock();
        CACHE[i].readers_count++;
        if(CACHE[i].readers_count==1)
        {
          CACHE[i].readers_present.lock();
        }
      CACHE[i].mutex_lock.unlock();
      
      // Critical Section
     // cout << "Key : " << CACHE[i].key << " Value : " <<CACHE[i].value << " LRU : " <<CACHE[i].LRU << " LFU : " <<CACHE[i].LFU << " dirty : " <<CACHE[i].dirty << endl;
  if(CACHE[i].key!="")
  file_op("server_logs.txt",0,CACHE[i].key ,"", "INSERT");
      // Exit Section
      CACHE[i].mutex_lock.lock();
        CACHE[i].readers_count--;
        if(CACHE[i].readers_count==0)
        {
          CACHE[i].readers_present.unlock();
        }
      CACHE[i].mutex_lock.unlock();
  }
  /*
  BITMAP_mutex_lock.lock();
  cout << "Printing BITMAP" << endl;
  for (int i = 0; i < BITMAP.size(); i++) 
  {
      for (int j = 0; j < BITMAP[i].size(); j++)
          cout << BITMAP[i][j] << " ";
      cout << endl;
  }
  BITMAP_mutex_lock.unlock();
  */
}

void write_to_log(string key, string val)
{
  update_logs_file.open("../../update_logs.txt", ios::app);
  if (update_logs_file)
  {
    // cout << "Opened logs file." << endl;
    update_logs_file << key + ":" + val << endl;
    update_logs_file.close();
  }
  else
  {
    cout << "FAILED to open logs file!!!!!" << endl;
  }
}

std::string PUT (std::string key, std::string value)
{
  // sleep(2);
  // cout << "Inside PUT for key : " << key << " val : " << value << endl;
  // return "Returning from PUT";
  //Search in CACHE first
  CACHE_mutex_lock.lock();
  int cache_count = CACHE_MAP.count(key);
  int c_ind = CACHE_MAP[key];
  CACHE_mutex_lock.unlock();

  // for (int i=0; i < cache_size; i++)
  // {
    if (cache_count)
    {
        // std::cout << "YESSSS Found value in CACHE hence updating it here only" << endl;
        int i = c_ind;
        // // Writer's Entry Section
        CACHE[i].writer_ready.lock();
        CACHE[i].readers_present.lock();

        // Critical section

        CACHE[i].value = value;
        CACHE[i].dirty = true;
        CACHE[i].LFU++;
        // append_to_log(key,val);
        // if(update_logs_file)
        //   cout<< endl << endl << endl << "WOooooooowwww"<<endl;
        // else
        //     cout<< endl << endl << endl << "NOooooooooo"<<endl;

        // update_logs_file << CACHE[i].key + ":" + CACHE[i].value;
        // update_logs_file << ":" ;
        // write_to_log(CACHE[i].key, CACHE[i].value);
        file_op("update_logs.txt",0,key, value, "INSERT");

        if(replacement_policy=="LRU")
        {
          // auto cache_entry = CACHE[i];
          // CACHE.erase(CACHE.begin()+i);
          // CACHE.push_back(cache_entry);
          CACHE[i].LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
        }

        // Exit Section
        CACHE[i].writer_ready.unlock();
        CACHE[i].readers_present.unlock();
        return "200:";
    }
  // }
  /* for (int i=0; i < INDEX.size(); i++)
  {
    if (INDEX[i].key == key)
    {
        std::cout << ":(( Found value in INDEX hence updating it file" << endl;
        // update_file(key, value, "file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);
        
        file_op("file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num, key, value, "INSERT");
        write_to_CACHE(key, value);  //---- Here we write to cache
        
        return "200:";
    }
  } */


  INDEX_mutex_lock.lock();
    bool exec = INDEX.find(key) != INDEX.end();
    int file_num, line_num;
    if(exec)
    {
      file_num=INDEX[key].File_name;
      line_num=INDEX[key].Line_num;
    }
  INDEX_mutex_lock.unlock();
  
  if(exec)
  {
      // std::cout << ":(( Found value in INDEX hence updating it file" << endl;
      file_op("file_" + to_string(file_num), line_num, key, value, "INSERT");
      write_to_CACHE(key, value);  //---- Here we write to cache
      return "200:";
  }

    // std::cout << "A Simple WRITE" << endl;
    BITMAP_mutex_lock.lock();
    
    for (int i = 0; i < BITMAP.size(); i++)
    {
      for(int j=0; j<BITMAP[i].size() ; j++)
      {
        if (BITMAP[i][j]==0)
        {
          BITMAP[i][j] = 1;
          BITMAP_mutex_lock.unlock();

          // cout << "Found empty place in file : file_"<<i+1 << " line : "<< j+1 <<endl;
          
          // update_file(key, value, "file_" + to_string(i+1), j)
          if(file_op("file_" + to_string(i+1), j , key , value , "INSERT") == "Inserted!")
          {
            // cout<<"Wrtie  to file successfull." << endl;
          }
          else
          {
            // cout<<"Wrtie  to file FAILED!!!!" << endl;
          }
          INDEX_def record;
          record.key=key;
          record.File_name= i+1 ;
          record.Line_num= j ;
          
          INDEX_mutex_lock.lock();          
          INDEX[record.key] = record;
          INDEX_mutex_lock.unlock();
          
          write_to_CACHE(key, value);  //---- Here we write to cache
          return "200:";
        }
      }
    }
    BITMAP_mutex_lock.unlock();

    // Gotta create a new file and add entry in bit-vector
    // cout << "Gotta create a new file and add entry in bit-vector" << endl;
    // ofstream cur_file;
    // cur_file.open("../../file_" + to_string(BITMAP.size()+1), fstream::out);

    file_table[BITMAP.size()+1+3].file_name = "file_" + to_string(BITMAP.size()+1);
    file_table[BITMAP.size()+1+3].f_handle.open("../../file_" + to_string(BITMAP.size()+1), ios::in | ios::out | ios::trunc);
    // cout << "File_name opened : " << file_table[BITMAP.size()+1+3].file_name << endl;
    INDEX_def record;
    record.key=key;
    record.File_name= BITMAP.size() + 1;
    record.Line_num= 0 ;
    
    // INDEX.push_back(record);

    INDEX_mutex_lock.lock();          
    INDEX[record.key] = record;
    INDEX_mutex_lock.unlock();
    

    std::vector <int> Row;
    Row.push_back(1);
    for(int i=1; i<file_size; i++)
    {
            Row.push_back(0);
            // cur_file << "" << endl;
    }

    BITMAP_mutex_lock.lock();
    BITMAP.push_back(Row);
    BITMAP_mutex_lock.unlock();          

    // cur_file.close();
    // update_file(key,value,"file_" + to_string(BITMAP.size()),0);
    
    file_op("file_" + to_string(BITMAP.size()), 0 , key , value , "INSERT");
    // cout << "INSERTING IN file : " << "file_" + to_string(BITMAP.size()) << "Line : " << 0 << endl;



    return "200:";
}

std::string GET (std::string key)
{
  // sleep(2);
  // cout << "Inside GET for key : " << key  << endl;
  // return "Returning from GET";
  CACHE_mutex_lock.lock();
  int cache_count = CACHE_MAP.count(key);
  int c_ind = CACHE_MAP[key];
  CACHE_mutex_lock.unlock();
  // for (int i=0; i < cache_size; i++)
  // {
    if (cache_count)
    {
        // std::cout << "[[[CCC]]]YESSSS Found value in CACHE  only" << endl;
        int i = c_ind;
        string return_value;
        // Reader's Entry section
        CACHE[i].writer_ready.lock();
        CACHE[i].writer_ready.unlock();
        CACHE[i].mutex_lock.lock();
            CACHE[i].readers_count++;
            if(CACHE[i].readers_count==1)
            {
                CACHE[i].readers_present.lock();
            }
        CACHE[i].mutex_lock.unlock();

        // Critical Section
        // ONCE again must check if we blocked on a writer who has deleted this key or this cache entry has been swapped out
        //If so we go try to look for it in index if possible.
        if(CACHE[i].key == key)
        {
          return_value = CACHE[i].value;
        }

        //Exit Section
        CACHE[i].mutex_lock.lock();
            CACHE[i].readers_count--;
            if(CACHE[i].readers_count==0)
            {
                CACHE[i].readers_present.unlock();
            }
        CACHE[i].mutex_lock.unlock();

        if(replacement_policy=="LRU")
        {
          // auto cache_entry = CACHE[i];
          // CACHE.erase(CACHE.begin()+i);
          // CACHE.push_back(cache_entry);
          CACHE[i].LRU = std::chrono::duration_cast<std::chrono::microseconds>(chrono::high_resolution_clock::now()-start_time).count();
        }
        else
        {
          CACHE[i].LFU++;
        }

        return "200:" + return_value;
    }
  // }


  /* for (int i=0; i < INDEX.size(); i++)
  {
    // cout << "INDEX[i].key == !"<< INDEX[i].key << "! key == !" << key << "!" << endl;
    if (INDEX[i].key == key)
    {
        std::cout << ":(( Found value in INDEX hence have to fetch from file" << endl;
        // Fetch value from file
        // std::string value = get_value_from_file("file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);

        std::string value = file_op("file_" + to_string(INDEX[i].File_name),  INDEX[i].Line_num , "", "", "READ");
        write_to_CACHE(key, value);  //---- Here we find a position in cache, handle dirty page then write
        return "200:" +  value;
    }
  } */


  INDEX_mutex_lock.lock();
  bool exec = INDEX.find(key) != INDEX.end();
  int file_num, line_num;
  if(exec)
  {
    file_num=INDEX[key].File_name;
    line_num=INDEX[key].Line_num;
  }
  INDEX_mutex_lock.unlock();
  
  if(exec)
  {
        // std::cout << ":(( Found value in INDEX hence have to fetch from file" << endl;
        // Fetch value from file
        // std::string value = get_value_from_file("file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);

        std::string value = file_op("file_" + to_string(file_num),  line_num , "", "", "READ");
        write_to_CACHE(key, value);  //---- Here we find a position in cache, handle dirty page then write
        return "200:" +  value;
  }
  
  return "400:KEY NOT EXIST";
}

std::string DEL (std::string key)
{
  // sleep(2);
  // cout << "Inside DEL for key : " << key << endl;
  // return "Returning from DEL";
  
  CACHE_mutex_lock.lock();
  int cache_count = CACHE_MAP.count(key);
  int c_ind = CACHE_MAP[key];
  if(cache_count){
    CACHE_MAP.erase(key);
  }
  CACHE_mutex_lock.unlock();
  // for (int i=0; i < cache_size; i++)
  // {
    if (cache_count)
    {
        int i = c_ind;
        // Set key in cache as empty signifying empty position
        // Writer's Entry Section
        CACHE[i].writer_ready.lock();
        CACHE[i].readers_present.lock();

        // Critical section        
        CACHE[i].key = "";
        CACHE[i].LFU = 0;

        // // Exit Section
        CACHE[i].writer_ready.unlock();
        CACHE[i].readers_present.unlock();
    }
  // }
  /* for (int i=0; i < INDEX.size(); i++)
  {
    if (INDEX[i].key == key)
    {
        BITMAP[INDEX[i].File_name - 1][INDEX[i].Line_num] = 0;
        // update_file("","", "file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);

        file_op("file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num , "" , "" , "DELETE");
        
        // deleting from INDEX
        auto it = INDEX.begin();
        INDEX.erase(it+i);
        return "200:";
    }
  } */

  INDEX_mutex_lock.lock();
  bool exec = INDEX.find(key) != INDEX.end();
  int File_name, Line_num;
  if(exec)
  {
    File_name=INDEX[key].File_name;
    Line_num=INDEX[key].Line_num;
  }
  INDEX_mutex_lock.unlock();

  if(exec)
  {
      BITMAP_mutex_lock.lock();
      BITMAP[INDEX[key].File_name - 1][INDEX[key].Line_num] = 0;
      BITMAP_mutex_lock.unlock();
      // update_file("","", "file_" + to_string(INDEX[i].File_name), INDEX[i].Line_num);

      file_op("file_" + to_string(File_name), Line_num , "" , "" , "DELETE");
      
      // deleting from INDEX
      // auto it = INDEX.begin();
      INDEX_mutex_lock.lock();
      INDEX.erase(key);
      INDEX_mutex_lock.unlock();

      return "200:";
  }
  
  return "400:KEY NOT EXIST";
}


class ServerImpl final {
 public:
 // typedef enum {put,get,del}type_;
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    //cq_->Shutdown();
    for (const auto& cq : compl_queues_)
        cq->Shutdown();

  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address(port);

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.
   // cq_ = builder.AddCompletionQueue();
    for(auto i{0}; i<stoi(threadpool); i++)
        compl_queues_.emplace_back(builder.AddCompletionQueue());
    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    file_op("server_logs.txt",0,"server comes up successfully", "", "INSERT");
    std::cout << "Server listening on " << server_address << std::endl;
     std::vector<std::thread> threads;
    for(auto i{0}; i<stoi(threadpool); i++)
        threads.emplace_back(std::thread(&ServerImpl::HandleRpcs, this, compl_queues_[i].get()));
    for(auto i{0}; i<threads.size(); i++)
        threads[i].join();
    // Proceed to the server's main loop.
   // HandleRpcs();
  }

 private:
  // Class encompasing the state and logic needed to serve a request.
  class CallData {
   public:
    // Take in the "service" instance (in this case representing an asynchronous
    // server) and the completion queue "cq" used for asynchronous communication
    // with the gRPC runtime.
   
    CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq,string type)
        : service_(service), cq_(cq), presponder_(&ctx_),gresponder_(&ctx_),dresponder_(&ctx_), status_(CREATE),type_(type) {
      // Invoke the serving logic right away.
      Proceed();
    }

    void Proceed() {
     
      if (status_ == CREATE) {
        // Make this instance progress to the PROCESS state.
       // write_to_slog("server accepted a client connection");
        status_ = PROCESS;

        // As part of the initial CREATE state, we *request* that the system
        // start processing SayHello requests. In this request, "this" acts are
        // the tag uniquely identifying the request (so that different CallData
        // instances can serve different requests concurrently), in this case
        // the memory address of this CallData instance.

        if(type_=="put"){
        
        service_->RequestPut(&ctx_, &prequest_, &presponder_, cq_, cq_,
                                  this);
        }
        else if(type_=="get"){
          service_->RequestGet(&ctx_, &grequest_, &gresponder_, cq_, cq_,
                                  this);
        }
        else{
               
               service_->RequestDel(&ctx_, &drequest_, &dresponder_, cq_, cq_,
                                  this);

        }
      } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while we process
        // the one for this CallData. The instance will deallocate itself as
        // part of its FINISH state.

        new CallData(service_, cq_,type_);
        // string ckeys="";
        // The actual processing.
        if(type_== "put"){
        // std::string prefix("PUT Request received.");
        // cout << "PUT Request received." << endl;
        if(string(prequest_.key()).length()==0 || string(prequest_.value()).length() == 0 || string(prequest_.key()).length()>256 || string(prequest_.value()).length()>256)
        {
          preply_.set_message("Incorrect argument length passed.");
          preply_.set_status(400);
       

        }
        else{
          string return_val = PUT(prequest_.key(),prequest_.value());
        //  write_to_slog("server served a PUT request with key:"+prequest_.key()+", value :"+prequest_.value()+"and returned : "+return_val);
          file_op("server_logs.txt",0,"server served a PUT request with key:"+prequest_.key()+", value :"+prequest_.value()+"and returned : "+return_val, "", "INSERT");
         /*
          for(int i=0;i<cache_size;i++){
            if(CACHE[i].key!=""){
              ckeys=ckeys+CACHE[i].key+",";
            }
          }
     // write_to_slog("cache content :: "+ ckeys);
          file_op("server_logs.txt",0,"cache content :: "+ ckeys, "", "INSERT");
          */
         print_meta();
          int status = stoi(return_val.substr(0, return_val.find(":")));
          string reply = return_val.substr(return_val.find(":")+1, return_val.length());
          preply_.set_message(reply);
          preply_.set_status(status);

          // cout << "PUT returned : " << return_val << endl;
         // print_meta();
          // std::cout << "Arguments recieved " << prequest_.name() << prequest_.key() << prequest_.value() << prequest_.type() << std::endl;
        }
        }
        else if(type_=="get"){
          if(string(grequest_.key()).length()==0 || string(grequest_.key()).length() > 256)
          {
            greply_.set_message( "Incorrect argument length passed.");
            greply_.set_status(400);

          }
          // std::string prefix("Hello ");
          else
          {
            string return_val = GET(grequest_.key());
          //  write_to_slog("server served a GET request with key:"+grequest_.key()+"and returned : "+return_val);
            file_op("server_logs.txt",0,"server served a GET request with key:"+grequest_.key()+"and returned : "+return_val, "", "INSERT");
            int status = stoi(return_val.substr(0, return_val.find(":")));
            string reply = return_val.substr(return_val.find(":")+1, return_val.length());
            print_meta();
            greply_.set_message(reply);
            greply_.set_status(status);

            // cout << GET(grequest_.key());
           // print_meta();
          }
        }
        else{
          if(string(drequest_.key()).length()==0 || string(drequest_.key()).length() > 256)
          {
            dreply_.set_message ("Incorrect argument length passed.");
            dreply_.set_status(400);
          }
          else
          {
              string return_val = DEL(drequest_.key());
           //   write_to_slog("server served a DEL request with key:"+prequest_.key()+"and returned : "+return_val);
                file_op("server_logs.txt",0,"server served a DEL request with key:"+prequest_.key()+"and returned : "+return_val, "", "INSERT");
              int status = stoi(return_val.substr(0, return_val.find(":")));
              string reply = return_val.substr(return_val.find(":")+1, return_val.length());
              print_meta();
              dreply_.set_message(reply);
              dreply_.set_status(status);

              //print_meta();
          }
        }
        // And we are done! Let the gRPC runtime know we've finished, using the
        // memory address of this instance as the uniquely identifying tag for
        // the event.
       // if (counter++%2==1)
       //     std::this_thread::sleep_for(std::chrono::seconds(10));
        status_ = FINISH;
        if(type_=="put"){
        presponder_.Finish(preply_, Status::OK, this);
        }
        else if(type_=="get"){
               gresponder_.Finish(greply_, Status::OK, this);
        }
        else{

        dresponder_.Finish(dreply_, Status::OK, this);
        }
      } else {
        GPR_ASSERT(status_ == FINISH);
        // Once in the FINISH state, deallocate ourselves (CallData).
        delete this;
      }
    }

   private:
    // The means of communication with the gRPC runtime for an asynchronous
    // server.
    Greeter::AsyncService* service_;
    // The producer-consumer queue where for asynchronous server notifications.
    ServerCompletionQueue* cq_;
    // Context for the rpc, allowing to tweak aspects of it such as the use
    // of compression, authentication, as well as to send metadata back to the
    // client.
    ServerContext ctx_;
      string type_;
    // What we get from the client.
    PutRequest prequest_;
    // What we send back to the client.
    PutReply preply_;
     GetRequest grequest_;
    // What we send back to the client.
    GetReply greply_;
    DelRequest drequest_;
    // What we send back to the client.
    DelReply dreply_;
    
    // The means to get back to the client.
    ServerAsyncResponseWriter<PutReply> presponder_;
    ServerAsyncResponseWriter<GetReply> gresponder_;
    ServerAsyncResponseWriter<DelReply> dresponder_;

    // Let's implement a tiny state machine with the following states.
    enum CallStatus { CREATE, PROCESS, FINISH };
    CallStatus status_;  // The current serving state.
  };

  // This can be run in multiple threads if needed.
  void HandleRpcs(grpc::ServerCompletionQueue* cq_) {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&service_, cq_,"put");
    new CallData(&service_, cq_,"get");
    new CallData(&service_, cq_,"del");

    void* tag;  // uniquely identifies a request.
    bool ok;
  
    
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(cq_->Next(&tag, &ok));
      if(flag_cl==0){
      flag_cl=1;
    file_op("server_logs.txt",0,"server accepts a client connection", "", "INSERT");
    }
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
    }
  }

  //std::unique_ptr<ServerCompletionQueue> cq_;
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> compl_queues_;
  Greeter::AsyncService service_;
  std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
  
  file_table[0].f_handle.open("../../config.txt", ios::in);
  file_table[0].file_name = "config.txt";
  file_table[1].f_handle.open("../../server_logs.txt", ios::in | ios::out | ios::trunc);
  file_table[1].file_name = "server_logs.txt";
    
  
  if(file_table[0].f_handle.fail())
            {
                //File does not exist code here
                cout << "config file does not exist" << endl;
                
            }
  
  getline(file_table[0].f_handle,port);
  getline(file_table[0].f_handle,cache_type);
  getline(file_table[0].f_handle,cache_size1);
  getline(file_table[0].f_handle,threadpool);

  CACHE_def new_CACHE[stoi(cache_size1)];
  cache_size=stoi(cache_size1);
  replacement_policy = cache_type;
  CACHE = new_CACHE;

  server_initialise();


  ServerImpl server;
  server.Run();

  return 0;
}
