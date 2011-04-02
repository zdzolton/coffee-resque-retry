{puts,inspect} = require 'sys'
vows = require 'vows'
assert = require 'assert'
resqueConnect = require('coffee-resque').connect

redisHostAndPort = host: 'localhost', port: 6379
testQueueName = 'coffee-resque-retry'

enqueueTask = (jobName, args) ->
  resque = resqueConnect redisHostAndPort
  resque.enqueue testQueueName, jobName, args
  resque.end()

{createWorker,watcher,WorkerWithRetry} = require '../src/index'
resqueForWorker = resqueConnect redisHostAndPort

worker = null
startTime = null

jobs =
  bad:
    retry_limit: 2
    func: (x, cb) ->
      {callee} = arguments
      callee.count or= 0
      callee.count++
      cb new Error 'fail'
  bad2:
    retry_limit: 1
    retry_delay: 3
    func: (_, cb) -> cb new Error 'NO!!'

vows.describe('coffee-resque failure retry')

  .addBatch
    'initialize worker object':
      topic: ->
        worker = createWorker resqueForWorker, testQueueName, jobs
        true  # HACK
    
      'should return an instance of WorkerWithRetry': ->
        assert.isTrue worker instanceof WorkerWithRetry

  .addBatch
    'set up an error callback that looks for the "bad" job':
      topic: ->
        worker.on 'error', (err, work, queue, job) =>
          # puts "error"
          {callee} = arguments
          callee.badCount or= 0
          @callback() if job.class is 'bad' and ++callee.badCount is 2
        return
      
      'should have ran six times': ->
        assert.equal jobs.bad.func.count, 6
  
    'set up an error callback that looks for the "bad2" job':
      topic: ->
        worker.on 'error', (_e, _w, _q, job) =>
          @callback() if job.class is 'bad2'
        return
    
      'should be at least 3 seconds later': ->
        assert.isTrue (new Date) - startTime >= 3000
    
    'start the mouse trap': ->
      enqueueTask 'bad', ['abc123']
      enqueueTask 'bad', ['098zyx']
      enqueueTask 'bad2', ['quux']
      watcher.start redisHostAndPort
      worker.start()
      startTime = new Date
    
    "starting extra times": ->
      assert.doesNotThrow (-> watcher.start redisHostAndPort), Error
  
  .addBatch
    'terminate test suite':
      topic: ->
        watcher.stop()
        worker.end -> resqueForWorker.end()
        true
      
      ok: (v) -> assert.ok v
      
      "stopping extra times": ->
        assert.doesNotThrow (-> watcher.stop()), Error

  .export module
