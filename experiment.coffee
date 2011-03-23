{puts,inspect} = require 'sys'

resque = require('coffee-resque').connect host: 'localhost', port: 6379

# Code adapater from the Ruby version of resque-scheduler and resque-scheduler
#=START=
RETRY_LIMIT = 3

identifier = (args) ->
  types = ['string', 'number', 'boolean']
  (a.toString().slice 0, 40 for a in args when typeof a in types)
    .slice(0, 4)
    .join '-'

redisRetryKey = (job) ->
  name = job.class
  {args} = job
  ['resque-retry', name].concat(identifier args).join(":").replace /\s/g, ''

failureKey = (retryKey) -> "failure_#{retryKey}"

retryFailureCleanup = (job) -> resque.redis.del failureKey redisRetryKey job

beforePerformRetry = (job) ->
  key = redisRetryKey job
  resque.redis.setnx key, -1  # default to -1 if not set.
  resque.redis.incr key

onFailureRetry = (err, queue, job) ->
  key = redisRetryKey job
  puts "onFailureRetry() - #{key}"
  resque.redis.get key, (err, retryAttempt) ->
    puts "retryAttempt: #{retryAttempt}"
    unless err?
      if retryAttempt < RETRY_LIMIT
        resque.enqueue queue, job.class, job.args
      else resque.redis.del key

afterPerformRetry = (job) -> resque.redis.del redisRetryKey job
#=END=

worker = resque.worker 'coffee-resque-retry',
  good: (arg, cb) -> cb true
  
  bad:  (arg, cb) ->
    # # Let's make this succeed the third time around
    # if worker.failCount is 2 then cb true
    # else cb new Error 'fail'
    
    # Always fail
    cb new Error 'fail'
  
  ugly: (arg, cb) -> # don't call callback?!

# Let's override the fail behavior:
worker.failCount = 0
originalFailFunc = worker.fail

worker.fail = (err, job) ->
  @failCount++
  # if we're gonna retry, let's suppress the failure logging
  if @failCount <= RETRY_LIMIT then onFailureRetry err, @queue, job
  else
    resque.redis.del redisRetryKey job
    originalFailFunc.apply @, [err, job]

# some global event listeners

# Triggered every time the Worker polls.
worker.on 'poll', (worker, queue) ->
  puts "POLLING"

# Triggered before a Job is attempted.
worker.on 'job', (worker, queue, job) ->
  puts "STARTING JOB - #{worker._name}"
  beforePerformRetry job

# Triggered every time a Job errors.
worker.on 'error', (err, worker, queue, job) ->
  puts "ERROR: #{err}"
  
  # Retry data -- not sure quite when to do this
  # :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
  # :payload   => payload,
  # :exception => exception.class.to_s,
  # :error     => exception.to_s,
  # :backtrace => Array(exception.backtrace),
  # :worker    => worker.to_s,
  # :queue     => queue

# Triggered on every successful Job run.
worker.on 'success', (worker, queue, job, result) ->
  puts "SUCCESS"
  afterPerformRetry job
  
worker.start()

resque.enqueue 'coffee-resque-retry', 'bad', ["whatever"]
