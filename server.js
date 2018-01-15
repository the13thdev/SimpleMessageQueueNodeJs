//Importing modules
var express = require('express');
var morgan = require('morgan');
var mysql = require('mysql');

var sqs = require('./sqs.js');

//data Variables
const DATABASE_HOST = 'localhost';
const DATABASE_USER = 'root';
const DATABASE_PASSWORD = '';
const DATABASE_NAME = 'wonderview_test';

/**
The database should contain the following two tables named queues and messages.

SQL Create statement for queues table:
 CREATE TABLE `queues` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `name` varchar(100) NOT NULL,
 PRIMARY KEY (`id`),
 UNIQUE KEY `name` (`name`)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;

SQL Create statement for messages table:
CREATE TABLE `messages` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `queue_id` int(11) NOT NULL,
 `value` varchar(1000) NOT NULL,
 `arrival_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
    FOREIGN KEY (queue_id) REFERENCES queues(id)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1
 */

//Array that will store ids of all messages being currently processed.
var message_ids_being_processed = [];

//initializing express
var app = express();

//setting port to listen on
app.set('port', (process.env.PORT || 5000));

//middleware for logging requests
app.use(morgan('dev'));

app.get('/', function (req, res, next) {
  res.send("Wonder Test");
});

/**
 * Creates a Queue. Requires a queue_name parameter to be set in the url query string.
 * If a que with specified queue_name already exisits, then returns an error.
 * Returns a success object {success:true, data:null} if queue created successfuly.  
 */
app.get('/createQueue', function (req, res, next) {
  if (req.query.queue_name == undefined || req.query.queue_name.length == 0) {
    res.json(createErrorResponse("INVALID_QUERY_VAR", "Queue name not defined."));
  } else {
    sqs.createQueue(req.query.queue_name, function (result) {
      res.json(result);
    });
  }
});

/**
 * Writes a message to a queue. queue_name and message_value must be specified as query parameters 
 * in url query string.
 * If the queue_name specified does not exist, return error.
 * Returns a success object {success:true, data:{message_id: [message_id]}} 
 * if message successfuly added to queue.
 */
app.get('/writeMessage', function (req, res, next) {
  if ((req.query.queue_name == undefined || req.query.queue_name.length == 0) || (req.query.message_value == undefined)) {
    res.json(createErrorResponse("INVALID_QUERY_VAR", "Query variables in url not defined properly."));
  } else {
    sqs.writeMessage(req.query.queue_name, req.query.message_value, function (result) {
      res.json(result);
    });
  }
});

/**
 * Returns the first message from queue in FIFO order that is currently not being processed by 
 * any other request.
 * queue_name must be specified in the url query string.
 * If no free messages exist in the queue specified, then returns error.
 * If a message is successfuly polled from queue, then it returns a success object
 * of type {"success":true,"data":{"message_id":14,"value":"test33"}}, and it also
 * adds the message_id to the list message_ids_being_processed for 30 seconds.
 * You can delete the message within this span of 30 seconds.
 * After 30 seconds, the message is again available for polling by any request.
 */
app.get('/pollQueue', function (req, res, next) {
  if (req.query.queue_name == undefined || req.query.queue_name.length == 0) {
    res.json(createErrorResponse("INVALID_QUERY_VAR", "Queue name not defined."));
  } else {
    sqs.pollQueue(req.query.queue_name, function (result) {
      res.json(result)
    })
  }
});

/**
 * Deletes a message from the queue.
 * message_id must be specified in url query string as a parameter.
 * If message_id is not in the list message_ids_being_processed, then returns an error.
 * A message can only be delted in the 30 second span after it has been polled from a queue.
 * On successful deletion, returns a success object of form {"success":true,"data":null}.
 */
app.get('/deleteMessage', function (req, res, next) {
  if (req.query.message_id == undefined) {
    res.json(createErrorResponse("INVALID_QUERY_VAR", "Message id not defined."));
  } else {
    sqs.deleteMessage(req.query.message_id, function (result) {
      res.json(result);
    });
  }
});

/**
 * Purges the queue. Deletes all queues and messages.
 */
app.get('/purgeQueue', function (req, res, next) {
  sqs.purgeQueue(function (result) {
    res.json(result);
  });
});

/**
 * Middleware to be used at last to hande invalid requests made to server
 * If a request is made to the server for which an endpoint has not been defined, then this middleware displays an error text.
 */
app.use(function (req, res, next) {
  res.send("Error.....!!!!");
});

//Start server
app.listen(app.get('port'), function () {
  console.log('Node app is running on port', app.get('port'));
});

/**
 * Creates and returns a success response object to be sent in case of success.
 */
function createSuccessResponse(resp_data) {
  var success_respone = {
    success: true,
    data: resp_data
  };
  return success_respone;
}

/**
 * Creates and returns an error respone object to be sent in case of error.
 */
function createErrorResponse(error_code, error_message) {
  var error_respone = {
    success: false,
    error: {
      code: error_code,
      message: error_message
    }
  };
  return error_respone;
}