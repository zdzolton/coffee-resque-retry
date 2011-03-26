{puts,inspect} = require 'sys'

redis = intervalID = null

exports.start = (opts) ->
  redis = require('redis').createClient opts.port, opts.host
  intervalID = setInterval handleDelayedItems, 1000

exports.stop = ->
  clearInterval intervalID
  redis.quit()
  
handleDelayedItems = ->
  nextDelayedTimestamp (err, timestamp) ->
    if timestamp?
      enqueueDelayedItemsForTimestamp timestamp, (err) ->
        # note the recursive looping...
        nextDelayedTimestamp arguments.callee unless err?
  return

nextDelayedTimestamp = (cb) ->
  redis.zrangebyscore 'delayed_queue_schedule',
    '-inf', Date.now()
    'limit', 0, 1
    (err, timestamps) -> cb err, if timestamps? then timestamps[0] else null

enqueueDelayedItemsForTimestamp = (timestamp, cb) ->
  nextItemForTimestamp timestamp, (err, item) ->
    if item?
      enqueueScheduledTask item
      # note the recursive looping...
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
  