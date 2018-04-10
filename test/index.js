var epq = require('..');

epq.setLogLevel(1, true);
epq.init('db');

setInterval(()=>{
  console.log(epq.getStats());
}, 1000);

function exitHandler(src, err){
  epq.dumpToDisk();
  console.log('exit src', src, err);
  process.exit();
}

process.on('SIGHUP', exitHandler.bind(null, 'SIGHUP'));
process.on('SIGINT', exitHandler.bind(null, 'SIGINT'));
process.on('SIGTERM', exitHandler.bind(null, 'SIGTERM'));
process.on('uncaughtException', exitHandler.bind(null, 'uncaughtException'));

function testFunctionality(){
  var i = 0;
  function getBuf(){
    return new Buffer((i++).toString());
  }

  function getData(count){
    var data;
    if(count > 0){
      data = [];
      for(var i = 0; i < count; i++){
        var buff = getBuf();
        console.log(buff.toString());
        data.push(buff)
      }
    }
    else{
      data = getBuf();
      console.log(data.toString());
    }
    return data;
  }

  function printData(data){
    if(data instanceof Array){
      console.log('got', data.length, 'elements');
      data.forEach((buff)=>{
        console.log(buff.toString());
      })
    }
    else if(data instanceof Buffer){
      console.log('got 1 element');
      console.log(data.toString());
    }
    else
      console.log('got none');
  }

  function insert(op, count){
    // count = count > 0 ? count : 1;
    console.log(op, count);
    for(var i = 0; i < count; i++) epq[op]('scy', getData());
  }

  function remove(op, count){
    // count = count > 0 ? count : 1;
    console.log(op, count);
    for(var i = 0; i < count; i++)  printData(epq[op]('scy', count));
  }

  insert('push', 5);
  // insert('unshift', 5);
  insert('push', 5);
  // insert('unshift', 5);
  remove('pop', 5);
  // remove('shift', 5);
  // remove('pop', 5);
  // remove('shift', 5);
  exitHandler('test code');
}

function benchmarkRaw(){
  var COUNT = 2000000 - 1;

  function push(){
    epq.push('scy', new Buffer('auto events = any_cast<List>(q1.get_property_value<util::Any>auto events = any_cast<List>'));
  }

  function pop(){
    return epq.pop('scy');
  }

  var i = 0;
  var ts = Date.now();
  while(i < COUNT){
    push();
    i += 1;
  }
  var elapsed = Date.now() - ts;
  console.log('===> push stats', elapsed, i / elapsed);

  i = 0;
  var ts = Date.now();
  while(pop()){
    i += 1;
  }
  var elapsed = Date.now() - ts;
  console.log('===> pop stats', elapsed, i / elapsed);
  exitHandler('test code');
}

function benchmarkRealistic(){
  var i = 0;
  var COUNT = 2000001;
  var ts = Date.now();
  function push(){
    i += 1;
    var now = Date.now();
    epq.push('scy', new Buffer('hello' + now));
    if(i < COUNT)
      setImmediate(push);
    else{
      var elapsed = Date.now() - ts;
      console.log('===> push stats', elapsed, COUNT / elapsed);

      i = 0;
      setTimeout(()=>{
        ts = Date.now();
        pop();
      }, 1200);
    }
  }

  var popped = 0;

  function pop(){
    i += 1;
    var obj = epq.pop('scy');
    if(obj){
      popped += 1;
      setImmediate(pop);
    }
    else{
      var elapsed = Date.now() - ts;
      console.log('===> pop stats', elapsed, popped / elapsed);
      exitHandler('test code');
    }
  }
  // pop();
  push();
}


testFunctionality();
// benchmarkRaw();
// benchmarkRealistic();