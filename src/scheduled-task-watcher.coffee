{puts,inspect} = require 'sys'

redis = intervalID = null
alreadyWorking = stopping = false

exports.start = (opts) ->
  unless redis? and not stopping
    redis = require('redis').createClient opts.port, opts.host
    intervalID = setInterval ->
      handleDelayedItems() unless alreadyWorking
    , 500

exports.stop = ->
  stopping = true
  if redis?
    clearInterval intervalID
    redis.quit()
    redis = intervalID = null
  
handleDelayedItems = ->
  unless stopping
    alreadyWorking = true
    nextDelayedTimestamp (err, timestamp) ->
      if timestamp?
        enqueueDelayedItemsForTimestamp timestamp, (err) ->
          if err? then alreadyWorking = false
          else
            # note the recursive looping...
            handleDelayedItems()
      else alreadyWorking = false
  return

nextDelayedTimestamp = (cb) ->
  try
    redis.zrangebyscore 'delayed_queue_schedule',
      '-inf', Date.now()
      'limit', 0, 1
      (err, timestamps) ->
        cb err, if timestamps? then timestamps[0]
  catch e
    throw e unless stopping

enqueueDelayedItemsForTimestamp = (timestamp, cb) ->
  nextItemForTimestamp timestamp, (err, item) ->
    if item?
      enqueueScheduledTask item
      # note the recursive looping to anonymous function...
      nextItemForTimestamp timestamp, arguments.callee
    else cb err

nextItemForTimestamp = (timestamp, cb) ->
  key = "delayed:#{timestamp}"
  redis.lpop key, (err, item) ->
    cleanUpTimestamp key, timestamp
    cb err, if item? then JSON.parse item

cleanUpTimestamp = (key, timestamp) ->
  # If the list is empty, remove it.
  redis.llen key, (err, length) ->
    if length is 0
      redis.del key
      redis.zrem 'delayed_queue_schedule', timestamp

enqueueScheduledTask = (item) ->
  require('coffee-resque')
    .connect({redis})
    .enqueue item.queue, item.class, item.args
  