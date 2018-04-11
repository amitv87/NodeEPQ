#include <log.h>
#include <glob.h>
#include <limits>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "queue.h"

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

int LOG_LEVEL = L_TRC;
bool COLOR_LOG = true;

const size_t MAX_Q_SIZE = 1000000;
const uint8_t MSG_HEADER_SIZE = sizeof(uint16_t);

void _mkdir(char *dir) {
  char tmp[1024];
  char *p = NULL;
  size_t len;

  snprintf(tmp, sizeof(tmp),"%s",dir);
  len = strlen(tmp);
  if(tmp[len - 1] == '/') tmp[len - 1] = 0;
  for(p = tmp + 1; *p; p++){
    if(*p == '/'){
      *p = 0;
      mkdir(tmp, S_IRWXU);
      *p = '/';
    }
  }
  mkdir(tmp, S_IRWXU);
}

size_t djb_hash(const char* cp){
  size_t hash = 5381;
  while (*cp) hash = 33 * hash ^ (unsigned char) *cp++;
  return hash;
}

void FreeMsg(Msg* msg){
  free(msg);
}

uint16_t GetSize(Msg* msg){
  return ((uint16_t*)(msg))[0];
}

unsigned char* GetData(Msg* msg){
  return msg + MSG_HEADER_SIZE;
}

Msg* NewMsg(uint16_t size, unsigned char* _data){
  Msg* msg = (Msg*)malloc(sizeof(Msg) * (size + MSG_HEADER_SIZE));
  memcpy(msg, &size, MSG_HEADER_SIZE);
  memcpy(msg + MSG_HEADER_SIZE, _data, size);
  return msg;
}

bool Queue::SaveQToFile(MsgQueue* q, uint64_t ts, bool clear){
  std::ostringstream fname;
  fname << dbPath << name << "_" << ts;
  FILE* pFile = fopen(fname.str().c_str(), "wb");

  uint32_t numMessage = 0;
  uint64_t stts = getTS();

  Msg* msg;

  if(clear){
    while(q->size() > 0){
      msg = q->front();
      q->pop_front();
      fwrite(msg, 1, MSG_HEADER_SIZE + GetSize(msg), pFile);
      numMessage += 1;
      FreeMsg(msg);
    }
  }
  else{
    MsgQueue::iterator it = q->begin();
    while (it != q->end()){
      msg = *it;
      fwrite(msg, 1, MSG_HEADER_SIZE + GetSize(msg), pFile);
      numMessage += 1;
      it += 1;
    }
  }

  fclose(pFile);
  LOG(L_MSG) << "q " << name << ": dumped " << numMessage << " messages to " << ts << " in " << getTimeDiff(&stts) << " ms";
  return true;
}

bool Queue::LoadQFromFile(MsgQueue* q, uint64_t fileNo){
  bool rc = false;

  std::ostringstream fname;
  fname << dbPath << name << "_" << fileNo;

  struct stat buffer;
  if(stat(fname.str().c_str(), &buffer) == 0){
    size_t fileSize = buffer.st_size;

    unsigned char* buff = (unsigned char*) malloc (sizeof(unsigned char)*fileSize);

    if(!buff){
      LOG(L_FAT) << "alloc failed";
      goto end;
    }

    unsigned char* orgBuff = buff;

    uint64_t ts = getTS();

    FILE* pFile = fopen(fname.str().c_str(), "rb");
    long long bytesRead = fread(buff, 1, fileSize, pFile);
    if(bytesRead != buffer.st_size){
      LOG(L_ERR) << fname.str() << " size mismatch, " << " bytesRead: " << bytesRead << ", against fileSize: " << fileSize;
    }

    uint32_t numMessage = 0;

    while(bytesRead > 0){
      if(bytesRead <= MSG_HEADER_SIZE) break;
      uint16_t size = ((uint16_t*)(buff))[0];
      buff += MSG_HEADER_SIZE;
      bytesRead -= MSG_HEADER_SIZE;
      if(bytesRead < size) break;
      Msg* msg = NewMsg(size, buff);
      buff += size;
      bytesRead -= size;
      q->push_back(msg);
      numMessage += 1;
    }

    remove(fname.str().c_str());

    free(orgBuff);
    fclose(pFile);

    LOG(L_MSG) << "q " << name << ": loaded " << numMessage << " messages from "  << fileNo  << " in " << getTimeDiff(&ts) << " ms";

    rc = numMessage > 0 ? true : false;
  }
  else{
    FBEG << fname.str() << " not found";
  }
  end:
  return rc;
}

bool Queue::LoadHqFromFile(){
  bool rc = false;
  if(files.size() > 0){
    uint64_t fileNo = files.front();
    files.pop_front();
    rc = LoadQFromFile(hq, fileNo);
  }
  return rc;
}

bool Queue::LoadTqFromFile(){
  bool rc = false;
  if(files.size() > 0){
    uint64_t fileNo = files.back();
    files.pop_back();
    rc = LoadQFromFile(tq, fileNo);
  }
  return rc;
}

Queue::Queue(char* _name, char* _dbPath){
  FBEG;
  name = new char[strlen(_name) + 1];
  memset(name, 0, sizeof(strlen(_name) + 1));
  strcpy(name, _name);

  dbPath = _dbPath;

  glob_t globbuf;

  stats.qname = name;

  std::ostringstream fname;
  fname << dbPath << name << "_*";

  if(!glob(fname.str().c_str(), 0, NULL, &globbuf)){
    for(size_t i = 0; i < globbuf.gl_pathc; i++){
      std::string dumpFileName = globbuf.gl_pathv[i];
      std::string tsStr = dumpFileName.substr(fname.str().length() - 1);
      if(!tsStr.empty()){
        uint64_t fts = std::strtoul(tsStr.c_str(), 0 , 10);
        if(fts > 0){
          files.push_back(fts);
          LOG(L_MSG) << "q " << name << ": dump file with ts: " << fts << " found";
        }
      }
    }
    globfree(&globbuf);
  }
  else
    LOG(L_MSG) << "q " << name << ": no dump files found";

  files.sort();
  files.unique();

  while(LoadHqFromFile() && hq->size() < MAX_Q_SIZE / 10);

  LoadTqFromFile();

  FEND;
}

void Queue::DumpToDisk(){
  SaveQToFile(hq, 1, false);
  SaveQToFile(tq, std::numeric_limits<uint64_t>::max(), false);
}

bool Queue::Push(Msg* msg){
  pushCount += 1;
  if(unlikely(tq->size() >= MAX_Q_SIZE)){
    if(hq->size() == 0 && files.size() == 0){
      MsgQueue* _q = hq;
      hq = tq;
      tq = _q;
    }
    else{
      MsgQueue* _q = tq;
      tq = new MsgQueue();
      uint64_t ts = getTS();
      SaveQToFile(_q, ts);
      delete _q;
      files.push_back(ts);
    }
  }
  tq->push_back(msg);
  return true;
}

Msg* Queue::Pop(){
  Msg* msg = nullptr;
  if(hq->size() > 0){
  popFromHQ:
    msg = hq->front();
    hq->pop_front();
  }
  else if(LoadHqFromFile()){
    goto popFromHQ;
  }
  else if(tq->size() > 0){
    msg = tq->front();
    tq->pop_front();
  }
  if(msg) popCount += 1;
  return msg;
}

QStat* Queue::GetStats(){
  stats.hqSize = hq->size();
  stats.tqSize = tq->size();
  stats.popCount = popCount;
  stats.pushCount = pushCount;
  stats.files = files.size();

  popCount = 0;
  pushCount = 0;
  return &stats;
}

QueueFactory* QueueFactory::qFactory = nullptr;

QueueFactory* QueueFactory::GetQueueFactory(char* dbPath){
  if(!qFactory){
    FBEG;
    qFactory = (QueueFactory*)new QueueFactoryImpl(dbPath);
    FEND;
  }
  return qFactory;
}

void QueueFactory::SetLogLevel(int l, bool c){
  _SetLogLevel(l,c);
}

QueueFactoryImpl::QueueFactoryImpl(char* _dbPath){
  size_t len = strlen(_dbPath);
  dbPath = new char[len + 2];
  memset(dbPath, 0, sizeof(len + 2));
  strcpy(dbPath, _dbPath);
  if(len > 0){
    if(dbPath[len - 1] != '/') dbPath[len] = '/';
    _mkdir(dbPath);
  }
  LOG(L_MSG) << "QueueFactory initialised with path: " << dbPath;
}

Queue* QueueFactoryImpl::GetQ(char* name, bool create){
  size_t hash = djb_hash(name);
  Queue* q = qmap[hash];
  if(!q && create){
    q = new Queue(name, dbPath);
    qmap[hash] = q;
  }
  if(q){
    if(strcmp(name, q->name) != 0){
      LOG(L_FAT) << "hash collision found names: " << name << " " << q->name << ", hash: " << hash;
    }
  }
  return q;
}

bool QueueFactoryImpl::Push(char* name, Msg* msg){
  Queue* q = GetQ(name, true);
  if(q) return q->Push(msg);
  return false;
}

Msg* QueueFactoryImpl::Pop(char* name){
  Queue* q = GetQ(name, true);
  Msg* msg = nullptr;
  if(q) msg = q->Pop();
  return msg;
}

std::list<QStat*> QueueFactoryImpl::GetStats(){
  std::list<QStat*> stats;
  for(const auto& any : qmap){
    Queue *q = any.second;
    stats.push_back(q->GetStats());
  }
  return stats;
}

void QueueFactoryImpl::DumpToDisk(){
  FBEG;
  for(const auto& any : qmap){
    Queue *q = any.second;
    q->DumpToDisk();
  }
  FEND;
}
