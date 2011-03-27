{puts,inspect} = require 'sys'
exports.watcher = require './scheduled-task-watcher'

exports.createWorker = (resque, queue, jobsWithRetry) ->
  worker = resque.worker queue, getRegularJobsObject jobsWithRetry
  decorate worker, jobsWithRetry
  worker

getRegularJobsObject = (jobsWithRetry) ->
  jobs = {}
  for name, job of jobsWithRetry when typeof job.func is 'function'
    jobs[name] = job.func
  jobs

decorate = (worker, jobsWithRetry) ->
  worker.jobsWithRetry = jobsWithRetry
  worker.fail = overrideFail worker
  worker.on 'job', (worker, queue, job) ->
    beforePerformRetry worker, job
  worker.on 'success', (worker, queue, job, result) ->
    afterPerformRetry worker, job

overrideFail = (worker) ->
  originalFailFunc = worker.fail
  {redis} = worker
  (error, job) ->
    getRetryAttempt worker, job, (err, retryAttempt) ->
      unless err?
        # if we're gonna retry, let's suppress the failure logging
        if retryAttempt < getRetryLimit worker, job
          retryFailure worker, job, err
        else
          redis.del redisRetryKey job
          originalFailFunc.apply worker, [error, job]

getJobWithRetry = (worker, job) ->
  for name, jobDef of worker.jobsWithRetry
    return jobDef if name is job.class

getRetryLimit = (worker, job) ->
  getJobWithRetry(worker, job)?.retry_limit or 0

getRetryDelay = (worker, job) ->
  getJobWithRetry(worker, job)?.retry_delay or 0

getRetryAttempt = (worker, job, cb) -> worker.redis.get redisRetryKey(job), cb

identifier = (args) ->
  types = ['string', 'number', 'boolean']
  (a.toString().slice 0, 40 for a in args when typeof a in types)
    .slice(0, 4)
    .join '-'

redisRetryKey = (job) ->
  name = job.class
  {args} = job
  ['resque-retry', name]
    .concat(identifier args)
    .join(":")
    .replace /\s/g, ''

beforePerformRetry = (worker, job) ->
  {redis} = worker
  key = redisRetryKey job
  redis.setnx key, -1  # default to -1 if not set.
  redis.incr key

retryFailure = (worker, job, err) ->
  {redis} = worker
  getRetryAttempt worker, job, (err, retryAttempt) ->
    unless err?
      if retryAttempt < getRetryLimit worker, job
        tryAgain redis, worker, job
      else redis.del redisRetryKey job

tryAgain = (redis, worker, job) ->
  retryDelay = getRetryDelay worker, job
  if retryDelay <= 0
    require('coffee-resque')
      .connect({redis})
      .enqueue worker.queue, job.class, job.args
  else enqueueIn redis, retryDelay, worker.queue, job.class, job.args

afterPerformRetry = (worker, job) ->
  worker.redis.del redisRetryKey job

enqueueIn = (redis, numberOfSecondsFromNow, queue, jobName, args) ->
  timestamp = (new Date).valueOf() + numberOfSecondsFromNow * 1000
  enqueueAt redis, timestamp, queue, jobName, args

enqueueAt = (redis, timestamp, queue, jobName, args) ->
  delayedPush redis, timestamp, {queue, class: jobName, args}

delayedPush = (redis, timestamp, item) ->
  redis.rpush "delayed:#{timestamp}", JSON.stringify item
  redis.zadd 'delayed_queue_schedule', timestamp, timestamp
