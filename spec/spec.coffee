{puts,inspect} = require 'sys'
vows = require 'vows'
assert = require 'assert'
resqueRetry = require '../src/index'

resque = require('coffee-resque').connect host: 'localhost', port: 6379

worker = null
badCount = 0

jobs =   
  bad:
    retry_limit: 2
    func: (cb) ->
      badCount++
      cb new Error 'fail'

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
          worker.on 'error', => @callback null, arguments
          return
        
        'should have ran three times': (args) ->
          assert.equal badCount, 3
      
      'start the mouse trap': ->
        resque.enqueue 'coffee-resque-retry', 'bad', []
        worker.start()

  .addBatch
    'terminate test suite':
      topic: ->
        worker.end -> resque.end()
        true
      
      ok: (v) -> assert.ok v

  .export module
