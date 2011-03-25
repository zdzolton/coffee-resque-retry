{puts,inspect} = require 'sys'
vows = require 'vows'
assert = require 'assert'
resqueRetry = require '../src/index'

resque = require('coffee-resque').connect host: 'localhost', port: 6379

worker = null

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
    func: (cb) ->
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
            {callee} = arguments
            callee.badCount or= 0
            @callback() if job.class is 'bad' and ++callee.badCount is 2
          return
        
        'should have ran six times': ->
          assert.equal jobs.bad.func.count, 6
      
      'start the mouse trap': ->
        resque.enqueue 'coffee-resque-retry', 'bad', ['a']
        resque.enqueue 'coffee-resque-retry', 'bad', ['b']
        resque.enqueue 'coffee-resque-retry', 'bad2', []
        worker.start()

  .addBatch
    'terminate test suite':
      topic: ->
        worker.end -> resque.end()
        true
      
      ok: (v) -> assert.ok v

  .export module
