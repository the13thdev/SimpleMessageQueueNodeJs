//importing modules
var assert = require("assert");
var sqs = require('./sqs.js');

describe("purgeQueue", function () {
    it('should purge queue i.e. delete all queues and messages.', function (done) {
        sqs.purgeQueue(function (result) {
            sqs.pollQueue("a", function (result) {
                if (result.success == false) {
                    done();
                } else {
                    done(new Error());
                }
            });
        });
    });
});

describe("createQueue", function () {
    it('should create queue if a queue with same name does not exist previously.', function (done) {
        sqs.createQueue("a", function (result) {
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }
        });
    });

    it('should create another queue if a queue with same name does not exist previously.', function (done) {
        sqs.createQueue("b", function (result) {
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }
        });
    });

    it('should not create queue if a queue with same name exists already.', function (done) {
        sqs.createQueue("a", function (result) {
            if (result.success == false) {
                done();
            } else {
                done(new Error());
            }
        });
    });
});

describe("writeMessage", function () {
    it('should write message to an already exsting queue', function (done) {
        sqs.writeMessage("a", "msg1", function (result) {
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }
        });
    });
    it('should write another message to the same queue', function (done) {
        sqs.writeMessage("a", "msg2", function (result) {
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }
        });
    });

    it('should write message to an already exsting queue that is different from the first', function (done) {
        sqs.writeMessage("b", "msg3", function (result) {
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }
        });
    });

    it('should not write message to a queue that does not exist', function (done) {
        sqs.writeMessage("a2", "msg4", function (result) {
            if (result.success == false) {
                done();
            } else {
                done(new Error());
            }
        });
    });
});

describe("pollQueue", function () {
    it("should poll the first correct msg in FIFO order from an existing queue", function (done) {
        sqs.pollQueue("a", function (result) {
            if (result.success == true && result.data.message_id == 1 && result.data.value == "msg1") {
                done();
            } else {
                done(new Error());
            }
        });
    });
    it("should poll the second correct msg in FIFO order from an existing queue if the first one is being processed", function (done) {
        sqs.pollQueue("a", function (result) {
            if (result.success == true && result.data.message_id == 2 && result.data.value == "msg2") {
                done();
            } else {
                done(new Error());
            }
        });
    });
    it("should not poll the msg in FIFO order from an existing queue if no free messages are in queue.", function (done) {
        sqs.pollQueue("a", function (result) {
            if (result.success == false) {
                done();
            } else {
                done(new Error());
            }
        });
    });

    it("should poll the first message in FIFO order from exisiting queue after the 30 second span since it is free again.", function (done) {
        console.log('waiting 31 seconds');
        this.timeout(40000);
        setTimeout(function () {
            sqs.pollQueue("a", function (result) {
                if (result.success == true && result.data.message_id == 1 && result.data.value == "msg1") {
                    done();
                } else {
                    done(new Error());
                }
            });
        }, 31000);
    });
});

describe("deleteMessage",function(){    
    
    it("should delete a message that was recently polled (less than 30 s before) and is currently being processed.", function(done){
        sqs.deleteMessage(1,function(result){
            if (result.success == true) {
                done();
            } else {
                done(new Error());
            }            
        });
    });

    it("should not delete message that was not recently polled", function(done){
        sqs.deleteMessage(2,function(result){
            if (result.success == false) {
                done();
            } else {
                done(new Error());
            }            
        });
    });

});

