#include <map>
#include <deque>
#include <iostream>

#include "queuefactory.h"

class Queue;

typedef std::deque<Msg*> MsgQueue;
typedef std::map<size_t, Queue*> QueueMap;


class Queue{
  QStat stats;
  std::list<uint64_t> files;
  MsgQueue* hq = new MsgQueue();  // head queue
  MsgQueue* tq = new MsgQueue();  // tail queue

  uint64_t popCount{0};
  uint64_t pushCount{0};

  bool SaveTqToFile();
  bool LoadHqFromFile();
  bool SaveQToFile(MsgQueue* q, uint64_t ts);

public:
  char* name;
  char* dbPath;
  Queue(char* _name, char* _dbPath);
  Msg* Pop();
  bool Push(Msg* msg);
  void DumpToDisk();
  QStat* GetStats();
};

class QueueFactoryImpl : QueueFactory{
  char* dbPath;
  QueueMap qmap;
  std::list<Queue*> queues;
  Queue* GetQ(char* name, bool create = false);
public:
  QueueFactoryImpl(char* dbPath);
  Msg* Pop(char* name);
  bool Push(char* name, Msg* msg);
  void DumpToDisk();
  std::list<QStat*> GetStats();
};
