var amqp = require('amqplib/callback_api');
var exchange = '';




var channel;
function connect(config,cb)
{
    exchange = config.exchange;
    amqp.connect(config.url, function (err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(connect.bind(this,config,cb), 1000);
        }
        conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
                return setTimeout(connect.bind(this,config,cb), 1000);
            }
        });
        conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(connect.bind(this,config,cb), 1000);
        });

        console.log("[AMQP] connected");
        conn.createConfirmChannel(function (err, ch) {
            ch.assertExchange(exchange, 'direct', {durable: true});
            console.log("Connected to rabbit");
            channel = ch;
            if(cb)
                cb(ch);

        });
    });
}
function publish(msg,key) {
    return new Promise(function(resolve, reject) {
        try {
            channel.publish(exchange, key, new Buffer(msg), {persistent: true}, function (err, ok) {
                    if (err !== null) {
                        reject(err);
                        console.warn(' [*] Message nacked');
                    } else {
                        console.log(' [*] Message acked');
                        resolve("Message sendt");
                    }
                }
            );
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] publish", e.message);
        }
    });
}




function RPC(queue,service,cb,ready){
        channel.assertQueue('', {exclusive: true},  (err, q) => {
            channel.consume(q.queue,  (msg) => {
                if (msg.properties.correlationId == service) {
                    cb(msg.content.toString());
                }
            }, {noAck: true});

            ready((sendMsg)=>{
                if (typeof sendMsg !== 'string') {
                    sendMsg = JSON.stringify(sendMsg);
                }
                channel.sendToQueue("RPC."+queue,
                new Buffer(sendMsg),
                {correlationId: service, replyTo: q.queue});
            });
        });
}

function RPCListen(queue,services) {
    "use strict";
    channel.assertQueue("RPC." + queue, {durable: false}, function (err, q) {
        channel.prefetch(1);
        console.log(' [x] Awaiting RPC requests');
        channel.consume(q.queue, function reply(msg) {
            new Promise((resolve, reject) => {
                if(services[msg.properties.correlationId] == undefined)
                {
                    channel.sendToQueue(msg.properties.replyTo,
                        new Buffer("Unknown service"),
                        {correlationId: msg.properties.correlationId});
                    channel.ack(msg);
                }
                else {
                    services[msg.properties.correlationId](msg.content.toString(), resolve, reject);
                }
            }).then((sendMsg) => {
                if (typeof sendMsg !== 'string') {
                    sendMsg = JSON.stringify(sendMsg);
                }

                channel.sendToQueue(msg.properties.replyTo,
                    new Buffer(sendMsg),
                    {correlationId: msg.properties.correlationId});
                channel.ack(msg);
            }).catch((msg) => {
            });
        });

    });
}
function listen(queue,key,cb){
    channel.assertQueue(queue, {durable:true},function(err, q) {
        console.log(' [*] Waiting for logs. To exit press CTRL+C'+q.queue);
        channel.bindQueue(q.queue, exchange, key);
        //Fetch 5 messages in a time and wait for ack on those
        channel.prefetch(5);
        channel.consume(q.queue, function(msg) {
            cb(channel,msg);
        }, {noAck: false});
    });
}
module.exports = {
    connect:connect,
    publish:publish,
    listen:listen,
    RPCListen:RPCListen,
    RPC:RPC
};