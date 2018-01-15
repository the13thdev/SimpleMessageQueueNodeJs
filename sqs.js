
/**
The database that will be used with this should contain the following two tables named queues and messages.

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


//Importing modules
var mysql = require('mysql');

//database config vars
const DATABASE_HOST = 'localhost';
const DATABASE_USER = 'root';
const DATABASE_PASSWORD = '';
const DATABASE_NAME = 'wonderview_test';

//other config constants
const TIMEOUT_DURATION_IN_SECONDS = 30;

//Array that will store ids of all messages being currently processed.
var message_ids_being_processed = [];

/**
 * Creates a Queue. 
 * Requires a queue_name parameter and callback function.
 * If a queue with specified queue_name already exisits, then returns an error.
 * Returns a success object {success:true, data:null} if queue created successfuly.  
 */
exports.createQueue = function (queue_name, callback) {
    let connection = getMySqlConnectionObject();
    connection.connect();
    //Adding a row to queues table.
    connection.query('INSERT INTO `queues` (`name`) VALUES ("' + queue_name + '");', function (error, results, fields) {
        if (error) {
            callback(createErrorResponse("MYSQL_ERROR", error));
        } else {
            callback(createSuccessResponse(null));
        }
    });
    connection.end();
};

/**
 * Writes a message to a queue. 
 * queue_name and message_value  and callback function must be specified as parameters. 
 * If the queue_name specified does not exist, return error.
 * Returns a success object {success:true, data:{message_id: [message_id]}} 
 * if message successfuly added to queue.
 */
exports.writeMessage = function (queue_name, message_value, callback) {
    var connection = getMySqlConnectionObject();
    connection.connect();
    connection.query('INSERT INTO `messages` (`queue_id`, `value`) SELECT id AS `queue_id`, "' + message_value + '" AS `value` from queues where name = "' + queue_name + '"; SELECT LAST_INSERT_ID() as message_id;', function (error, results, fields) {
        if (error) {
            callback(createErrorResponse("MYSQL_ERROR", error));
        } else if (results[0].affectedRows < 1) {
            callback(createErrorResponse("INVALID_QUEUE_NAME", "Queue name specified does not exist."));
        } else {
            callback(createSuccessResponse({ message_id: results[1][0].message_id }));
        }
    });
    connection.end();
};

/**
 * Returns the first message from queue in FIFO order that is currently not being processed by 
 * any other request.
 * queue_name  and callback function must be specified as parameters.
 * If no free messages exist in the queue specified, then returns error.
 * If a message is successfuly polled from queue, then it returns a success object
 * of type {"success":true,"data":{"message_id":14,"value":"test33"}}, and it also
 * adds the message_id to the list message_ids_being_processed for 30 seconds.
 * You can delete the message within this span of 30 seconds.
 * After 30 seconds, the message is again available for polling by any request.
 */
exports.pollQueue = function (queue_name, callback) {
    var connection = getMySqlConnectionObject();
    connection.connect();

    //creating not_in_list for query to exclude messages that are currently being processed.
    let not_in_list = "(-1";
    message_ids_being_processed.forEach(function (element) {
        not_in_list += "," + element;
    });
    not_in_list += ")";

    connection.query('SELECT messages.id as message_id, messages.value FROM messages INNER JOIN queues ON messages.queue_id = queues.id WHERE queues.name = "' + queue_name + '" AND messages.id NOT IN ' + not_in_list + ' ORDER BY messages.arrival_time LIMIT 1;', function (error, results, fields) {
        if (error) {
            callback(createErrorResponse("MYSQL_ERROR", error));
        } else if (results.length == 0) {
            callback(createErrorResponse("NO_MESSAGES_FOUND", "Either the queue does not contain any free messages, or no such queue exists."));
        } else {
            message_ids_being_processed.push(results[0].message_id);
            //timeout for 30 seconds
            setTimeout(function (m_id) {
                //console.log("...set timeout called with "+m_id);
                //removing message with given id from list of messages being currently processed.
                message_ids_being_processed.splice(message_ids_being_processed.indexOf(m_id), 1);
            }, TIMEOUT_DURATION_IN_SECONDS * 1000, results[0].message_id);
            callback(createSuccessResponse(results[0]));
        }
    });

    connection.end();
};

/**
 * Deletes a message from the queue.
 * message_id  and callback function must be specified as a parameters.
 * If message_id is not in the list message_ids_being_processed, then returns an error.
 * A message can only be delted in the 30 second span after it has been polled from a queue.
 * On successful deletion, returns a success object of form {"success":true,"data":null}.
 */
exports.deleteMessage = function (message_id, callback) {
    var connection = getMySqlConnectionObject();
    connection.connect();

    //console.log(message_ids_being_processed);
    if (message_ids_being_processed.indexOf(parseInt(message_id)) != -1) {
        connection.query('DELETE FROM `messages` WHERE id=' + message_id + ';', function (error, results, fields) {
            if (error) {
                callback(createErrorResponse("MYSQL_ERROR", error));
            } else if (results.affectedRows < 1) {
                callback(createErrorResponse("UNEXPECTED", "Message ID does not exist in database."));
            } else {
                message_ids_being_processed.splice(message_ids_being_processed.indexOf(message_id), 1);
                callback(createSuccessResponse(null));
            }
        });
    } else {
        callback(createErrorResponse("MESSAGE_NOT_POLLED", "The message id is not in the list of messages being processed."));
    }

    connection.end();
};

/**
 * Purges the queue.
 * Requires callback function as a parameter.
 * Deletes all queues and messages.
 * Returns a success object {success:true, data:null} if queue created successfuly.  
 */
exports.purgeQueue = function (callback) {
    let connection = getMySqlConnectionObject();
    connection.connect();
    //Adding a row to queues table.
    connection.query('TRUNCATE TABLE messages; TRUNCATE TABLE queues;', function (error, results, fields) {
        if (error) {
            callback(createErrorResponse("MYSQL_ERROR", error));
        } else {
            callback(createSuccessResponse(null));
        }
    });
    message_ids_being_processed = [];
    connection.end();
};


/**
 * Creates and returns a MySql conection object based on const DATABASE parameters defined at the top.
 */
function getMySqlConnectionObject() {
    return mysql.createConnection({
        host: DATABASE_HOST,
        user: DATABASE_USER,
        password: DATABASE_PASSWORD,
        database: DATABASE_NAME,
        multipleStatements: true
    });
}

/**
 * Creates and returns a success response object of type {
        success: true,
        data: resp_data
    };
 */
function createSuccessResponse(resp_data) {
    var success_respone = {
        success: true,
        data: resp_data
    };
    return success_respone;
}

/**
 * Creates and returns an error respone object of type {
        success: false,
        error: {
            code: error_code,
            message: error_message
        }
    }
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