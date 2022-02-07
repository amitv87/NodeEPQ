#include <v8.h>
#include <nan.h>
#include <node.h>
#include "queuefactory.h"

using namespace v8;
using namespace Nan;

#define STR_NUM(key, val) ctx, Nan::New(key).ToLocalChecked(), Nan::New((double) val)
#define STR_STR(key, val) ctx, Nan::New(key).ToLocalChecked(), Nan::New(val).ToLocalChecked()
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
  Local<Object> data = info[1]->ToObject(info.GetIsolate()->GetCurrentContext()).ToLocalChecked();
  Msg* msg = NewMsg(node::Buffer::Length(data), (unsigned char*)node::Buffer::Data(data));
  info.GetReturnValue().Set(qf->Push(*qname, msg));
}

NAN_METHOD(GetStats){
  std::list<QStat*> lst = qf->GetStats();
  v8::Isolate *isolate = info.GetIsolate();
  Local<Array> result_list = Array::New(isolate, lst.size());
  int i = 0;
  auto ctx = info.GetIsolate()->GetCurrentContext();
  for(std::list<QStat*>::iterator it=lst.begin(); it != lst.end(); ++it){
    QStat* stat = *it;
    Local<Object> obj = Nan::New<Object>();
    obj->Set(STR_STR("qname", stat->qname));
    obj->Set(STR_NUM("files", stat->files));
    obj->Set(STR_NUM("hqSize", stat->hqSize));
    obj->Set(STR_NUM("tqSize", stat->tqSize));
    obj->Set(STR_NUM("popCount", stat->popCount));
    obj->Set(STR_NUM("pushCount", stat->pushCount));
    result_list->Set(ctx, i, obj);
    i += 1;
  }
  info.GetReturnValue().Set(result_list);
}

NAN_METHOD(SetLogLevel){
  auto ctx = info.GetIsolate()->GetCurrentContext();
  int level = info[0]->NumberValue(ctx).FromJust();
  #if NODE_MAJOR_VERSION > 10
  bool color = info[1]->BooleanValue(info.GetIsolate());
  #else
  bool color = info[1]->BooleanValue();
  #endif
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
