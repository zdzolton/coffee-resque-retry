{puts,inspect} = require 'sys'

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
    key = redisRetryKey job
    redis.get key, (err, retryAttempt) ->
      unless err?
        # if we're gonna retry, let's suppress the failure logging
        if retryAttempt < getRetryLimit worker, job
          retryFailure worker, job, err
        else
          redis.del key
          originalFailFunc.apply worker, [error, job]

getRetryLimit = (worker, job) ->
  rv = 0
  for name, jobDef of worker.jobsWithRetry
    if name is job.class
      rv = jobDef.retry_limit or 0
      break
  rv

identifier = (args) ->
  types = ['string', 'number', 'boolean']
  (a.toString().slice 0, 40 for a in args when typeof a in types)
    .slice(0, 4)
    .join '-'

redisRetryKey = (job) ->
  name = job.class
  {args} = job
  ['resque-retry', name].concat(identifier args).join(":").replace /\s/g, ''

beforePerformRetry = (worker, job) ->
  {redis} = worker
  key = redisRetryKey job
  redis.setnx key, -1  # default to -1 if not set.
  redis.incr key

retryFailure = (worker, job, err) ->
  {redis} = worker
  key = redisRetryKey job
  redis.get key, (err, retryAttempt) ->
    unless err?
      if retryAttempt < getRetryLimit worker, job
        resque = require('coffee-resque').connect {redis}
        resque.enqueue worker.queue, job.class, job.args
      else redis.del key

afterPerformRetry = (worker, job) ->
  worker.redis.del redisRetryKey job
