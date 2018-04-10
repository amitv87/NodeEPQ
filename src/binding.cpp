#include <v8.h>
#include <nan.h>
#include <node.h>
#include "queuefactory.h"

using namespace v8;
using namespace Nan;

#define NAN_SET_METHOD(name,mehtod) Nan::Set(target, Nan::New(name).ToLocalChecked(), Nan::GetFunction(Nan::New<FunctionTemplate>(mehtod)).ToLocalChecked())

QueueFactory* qf = nullptr;

NAN_METHOD(init){
  if(qf) return;
  Nan::Utf8String configPath(info[0]);
  qf = QueueFactory::GetQueueFactory(*configPath);
}

NAN_METHOD(Pop){
  Nan::Utf8String qname(info[0]);
  Msg* msg = qf->Pop(*qname);
  if(msg){
    info.GetReturnValue().Set(Nan::CopyBuffer((char*)GetData(msg), GetSize(msg)).ToLocalChecked());
    FreeMsg(msg);
  }
}

NAN_METHOD(Push){
  Nan::Utf8String qname(info[0]);
  Local<Object> data = info[1]->ToObject();
  Msg* msg = NewMsg(node::Buffer::Length(data), (unsigned char*)node::Buffer::Data(data));
  info.GetReturnValue().Set(qf->Push(*qname, msg));
}

NAN_METHOD(GetStats){
  std::list<QStat*> lst = qf->GetStats();
  v8::Isolate *isolate = info.GetIsolate();
  Local<Array> result_list = Array::New(isolate, lst.size());
  int i = 0;
  for(std::list<QStat*>::iterator it=lst.begin(); it != lst.end(); ++it){
    QStat* stat = *it;
    Local<Object> obj = Nan::New<Object>();
    obj->Set(String::NewFromUtf8(isolate, "qname"), String::NewFromUtf8(isolate, stat->qname));
    obj->Set(String::NewFromUtf8(isolate, "files"), Integer::New(isolate, stat->files));
    obj->Set(String::NewFromUtf8(isolate, "hqSize"), Integer::New(isolate, stat->hqSize));
    obj->Set(String::NewFromUtf8(isolate, "tqSize"), Integer::New(isolate, stat->tqSize));
    obj->Set(String::NewFromUtf8(isolate, "popCount"), Integer::New(isolate, stat->popCount));
    obj->Set(String::NewFromUtf8(isolate, "pushCount"), Integer::New(isolate, stat->pushCount));
    result_list->Set(i, obj);
    i += 1;
  }
  info.GetReturnValue().Set(result_list);
}

NAN_METHOD(SetLogLevel){
  int level = info[0]->NumberValue();
  bool color = info[1]->BooleanValue();
  qf->SetLogLevel(level, color);
}

NAN_METHOD(DumpToDisk){
  qf->DumpToDisk();
}

NAN_MODULE_INIT(InitAll){
  NAN_SET_METHOD("init", init);
  NAN_SET_METHOD("pop", Pop);
  NAN_SET_METHOD("push", Push);
  NAN_SET_METHOD("getStats", GetStats);
  NAN_SET_METHOD("dumpToDisk", DumpToDisk);
  NAN_SET_METHOD("setLogLevel", SetLogLevel);
}

NODE_MODULE(node_cv, InitAll);
