var sys 		= require('sys');
var amqp 		= require('amqp');
var restler = require('restler');
var fs			= require('fs');

var bunnyhop = module.exports = function() {
	var exchangeName;
	var queueName;
	var routingKey;
	var restEndpoint;
	var maxRetry;
	var retryDelay;
	
	var connection;
	
	function init(config) {
		exchangeName 	= config.exchange;
		queueName 		= config.queue;
		routingKey 		= config.routingKey;
		restEndpoint 	= config.restEndpoint;
		maxRetry			= parseInt(config.retryCount);
		retryDelay 		= parseInt(config.retryDelay);
		console.log(config);
	
		console.log('starting up listener...');
		connection = amqp.createConnection( { host: config.host } );
		console.log ('connection created!');

		//error listener on connection
		connection.addListener('error', function (e) {
			console.log(e);
		})

		//close listener on connection
		connection.addListener('close', function (e) {
			console.log('connection closed.');
		});
	}
	
	function listen() {
		//ready listener on connection
		connection.addListener('ready', function() {
			/*******
			EXCHANGE
			*******/
			var e = connection.exchange(exchangeName, { type: 'topic', durable: true });	//create exchange
		
			/*****
			QUEUES
			******/
			var q = connection.queue(queueName, { 
				passive: false,
				durable: true, 
				exclusive: false,
				autoDelete: false,
				endpoint: restEndpoint 
			});	//create queue 
	
			var qRetry = connection.queue(queueName + '.retry', {
				passive: false,
				durable: true,
				exclusive: false,
				autoDelete: false,
				endpoint: restEndpoint 
			}); //create associated retry queue
	
			var qPoison = connection.queue(queueName + '.poison', {
				passive: false,
				durable: true,
				exclusive: false,
				autoDelete: false,
				endpoint: restEndpoint 
			}); //create associated deadletter queue
	
			/*******************
			BINDINGS FOR QUEUES
			*******************/
			q.bind(e, routingKey);
			qRetry.bind(e, routingKey + '.retry');
			qPoison.bind(e, routingKey + '.poison');
	
			/*******************
			LISTEN ON MAIN QUEUE
			*******************/
			console.log('listening to main queue...');
			q.subscribeRaw(function(m) {
				//sys.p(q);
				console.log('======Message Received=====');
				try {	
					m.addListener('data', function (d) { 
						console.log('======Reading Data======');
					
						var json = d.toString();
					
						console.log('======Http Posting Data======');
						console.log(m.headers['retryCount'].toString());
						console.log(json);
					
						restler.post(q.options['endpoint'], json).on('complete', function(data) {
							console.log('message sent');
					
							m.acknowledge();
						}).on('error', function(data) {
							console.log('message failed');
					
							e.publish(routingKey + '.retry', "TEST", { durable: true,
								contentType: 'application/json',
								exchange: 'testexchange',
								headers: { retryCount: m.headers['retryCount'].toString() } 
							}); 
						
							m.acknowledge();
						});
				
				
					});		  
				} catch(e) {
					console.log('EXCEPTION!!');
					sys.p(e);
				}
			})
	
			/********************
			LISTEN ON RETRY QUEUE
			********************/
			console.log('listening to retry queue...');
			qRetry.subscribeRaw(function(retry) {
				console.log('----Starting Message Retry-----');
		
				retry.addListener('data', function (d) { 
					var retryCount = parseInt(retry.headers['retryCount'].toString());
					var delay = retryDelay;
			
					for(var a = 0; a <= retryCount; a++) {
						delay = delay + delay;
					}
			
					console.log('executing retry count ' + retryCount + ' with ' + delay + ' delay');
		
					setTimeout(function() {					
							var json = d.toString();
					
							console.log("POST: " + qRetry.options['endpoint']);
					
							restler.post(qRetry.options['endpoint'], json).on('complete', function(data) {
								console.log('message sent');
								retry.acknowledge();	
							}).on('error', function(data) {
								console.log('message failed');
				
								if(retryCount < maxRetry) {
									e.publish(routingKey + '.retry', json, { 
										durable: true,
										contentType: 'application/json',
										exchange: 'testexchange',
										headers: { retryCount: (retryCount+1).toString() } 
									}); 
								} else {
									e.publish(routingKey + '.poison', json, { 
										durable: true,
										contentType: 'application/json',
										exchange: 'testexchange',
										headers: { retryCount: retryCount.toString() } 
									}); 
								}
						
								retry.acknowledge();	
							});
					
					}, delay );
				});
			});
	
			/*********************
			LISTEN ON POISON QUEUE
			*********************/
			console.log('listening to poison queue...');
			qPoison.subscribeRaw(function(dead) {
				console.log('----Starting Message Poisoning----');
				console.log('Message Poisoned');
				dead.acknowledge();
			});
		});
	}
	
	return {
		start: function() {
			var configFile = process.env.BUNNYHOP_CONFIG || "/etc/bunnyhop.cfg";
			var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));

			init(config);
			listen();
		}
	}
}
