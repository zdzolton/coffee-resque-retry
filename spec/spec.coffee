{puts,inspect} = require 'sys'
vows = require 'vows'
assert = require 'assert'
resqueRetry = require '../src/index'

resque = require('coffee-resque').connect host: 'localhost', port: 6379

worker = null
badCount = 0
badErrorCBCount = 0
bad2Count = 0

jobs =
  bad:
    retry_limit: 2
    func: (cb) ->
      badCount++
      cb new Error 'fail'
  bad2:
    retry_limit: 1
    func: (cb) ->
      bad2Count++
      cb new Error 'NO!!'

vows.describe('coffee-resque failure retry')

  .addBatch
    'initialize worker object':
      topic: ->
        worker = resqueRetry.createWorker resque, 'coffee-resque-retry', jobs
        true
      
      'should not fail': ->
        assert.isNotNull worker
        assert.isObject worker
      
      'set up an error callback':
        topic: ->
          worker.on 'error', (err, work, queue, job) =>
            @callback() if job.class is 'bad' and ++badErrorCBCount is 2
          return
        
        'should have ran six times': ->
          assert.equal badCount, 6
      
      'start the mouse trap': ->
        resque.enqueue 'coffee-resque-retry', 'bad', []
        resque.enqueue 'coffee-resque-retry', 'bad', []
        resque.enqueue 'coffee-resque-retry', 'bad2', []
        worker.start()

  .addBatch
    'terminate test suite':
      topic: ->
        worker.end -> resque.end()
        true
      
      ok: (v) -> assert.ok v

  .export module
