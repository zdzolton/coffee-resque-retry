{puts,inspect}  = require 'sys'
coffeeResque    = require 'coffee-resque'
exports.watcher = require './scheduled-task-watcher'

exports.createWorker = (connection, queues, jobsWithRetry) ->
  callbacks = getCallbacksObject jobsWithRetry
  new exports.WorkerWithRetry connection, queues, callbacks, jobsWithRetry

workerPrototype = coffeeResque.Worker::

coffeeResque.Connection::enqueueIn = (queue, jobName, numberOfSecondsFromNow, args) ->
  enqueueIn @redis, numberOfSecondsFromNow, queue, jobName, args

coffeeResque.Connection::enqueueAt = (queue, jobName, dateToExecute, args) ->
  enqueueAt @redis, dateToExecute.valueOf(), queue, jobName, args

class exports.WorkerWithRetry extends coffeeResque.Worker
  constructor: (connection, queues, jobs, @jobsWithRetry) ->
    coffeeResque.Worker.apply @, [connection, queues, jobs]
  
  end: (cb) ->
    @removeAllListeners 'poll'
    @removeAllListeners 'job'
    @removeAllListeners 'error'
    @removeAllListeners 'success'
    super cb
  
  perform: (job) ->
    return unless @running
    key = redisRetryKey job
    @redis.setnx key, -1, (err, res) =>
      unless err? or not @running
        @redis.incr key, (err, res) =>
          unless err? or not @running
            workerPrototype.perform.apply @, [job]
  
  succeed: (result, job) ->
    return unless @running
    @redis.del redisRetryKey job, (err, res) =>
      unless err? or not @running
        workerPrototype.succeed.apply @, [result, job]
  
  fail: (error, job) ->
    return unless @running
    @getRetryAttempt job, (err, retryAttempt) =>
      unless err? or not @running
        limit = @jobsWithRetry[job.class]?.retry_limit or 0
        if retryAttempt < limit then @tryAgain job
        else
          key = redisRetryKey job
          @redis.del key, (err, res) =>
            unless err? or not @running
              workerPrototype.fail.apply @, [error, job]
  
  tryAgain: (job) ->
    retryDelay = @jobsWithRetry[job.class]?.retry_delay or 0
    if retryDelay <= 0
      coffeeResque
        .connect(@redis)
        .enqueue @queue, job.class, job.args
    else enqueueIn @redis, retryDelay, @queue, job.class, job.args

  getRetryAttempt: (job, cb) ->
    @redis.get redisRetryKey(job), cb

getCallbacksObject = (jobsWithRetry) ->
  jobs = {}
  for name, {func} of jobsWithRetry when typeof func is 'function'
    jobs[name] = func
  jobs

redisRetryKey = (job) ->
  name = job.class
  {args} = job
  ['resque-retry', name].concat(identifier args).join(":").replace /\s/g, ''

identifier = (args) ->
  types = ['string', 'number', 'boolean']
  (a.toString().slice 0, 40 for a in args when typeof a in types)
    .slice(0, 4)
    .join '-'

enqueueIn = (redis, numberOfSecondsFromNow, queue, jobName, args) ->
  timestamp = (new Date).valueOf() + numberOfSecondsFromNow * 1000
  enqueueAt redis, timestamp, queue, jobName, args

enqueueAt = (redis, timestamp, queue, jobName, args) ->
  delayedPush redis, timestamp, {queue, class: jobName, args}

delayedPush = (redis, timestamp, item) ->
  redis.rpush "delayed:#{timestamp}", JSON.stringify item
  redis.zadd 'delayed_queue_schedule', timestamp, timestamp
