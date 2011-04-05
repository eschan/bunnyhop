var sys = require('sys');
var amqp = require('amqp');
var restler = require('restler');
var fs = require('fs');
var RestClient = require('./httpclient')

module.exports = Bunnyhop;

Bunnyhop.prototype = new process.EventEmitter;

function Bunnyhop (config) {
		this.exchangeName 	= config.exchange;
		this.queueName 			= config.queue;
		this.routingKey 		= config.routingKey;
		this.restEndpoint 	= config.restEndpoint;
		this.maxRetry				= parseInt(config.retryCount);
		this.retryDelay 		= parseInt(config.retryDelay);
		console.log(config);
	
		connection = amqp.createConnection( { host: config.host } );

		//error listener on connection
		connection.addListener('error', function (e) {
			console.log(e);
		})

		//close listener on connection
		connection.addListener('close', function (e) {
			console.log('connection closed.');
		});
} 

Bunnyhop.prototype.listen = function() {
	var self = this;
	
	//ready listener on connection
	connection.addListener('ready', function() {
		/*******
		EXCHANGE
		*******/
		self.exchange = connection.exchange(self.exchangeName, { type: 'topic', durable: true });	//create exchange
		
		/*****
		QUEUES
		******/
		var q = connection.queue(self.queueName, { 
			passive: false,
			durable: true, 
			exclusive: false,
			autoDelete: false,
			endpoint: self.restEndpoint 
		});	//create queue 

		var qRetry = connection.queue(self.queueName + '.retry', {
			passive: false,
			durable: true,
			exclusive: false,
			autoDelete: false,
			endpoint: self.restEndpoint 
		}); //create associated retry queue

		var qPoison = connection.queue(self.queueName + '.poison', {
			passive: false,
			durable: true,
			exclusive: false,
			autoDelete: false,
			endpoint: self.restEndpoint 
		}); //create associated deadletter queue

		/*******************
		BINDINGS FOR QUEUES
		*******************/
		q.bind(self.exchange, self.routingKey);
		qRetry.bind(self.exchange, self.routingKey + '.retry');
		qPoison.bind(self.exchange, self.routingKey + '.poison');

		/*******************
		LISTEN ON MAIN QUEUE
		*******************/
		console.log('listening to main queue...');
		q.subscribeRaw(function(m) {
			try {	
				m.addListener('data', function (d) { 
					//TODO: Change to logging
					console.log('======Http Posting Data======');
					var client = new RestClient();
					var json = d.toString();
					
					console.log('content-type: ' + m.contentType); 
					console.log(json);
				
					var rest_client = client.post(q.options['endpoint'], {'content-type': m.contentType }, json ); //NOTE: Maybe remove hard coded content-type
					rest_client.on('success', function(statusCode, data) {
						console.log("SUCCESS!!"); //TODO: Change to logging
						console.log(statusCode);
						
						//TODO: check status code
						
						m.acknowledge();	
					}).on('fail', function(err) {
						console.log('FAILED!!');	//TODO: Change to loggin
						console.log(err);
						
						self.retryOrPoison(m, d);
						
						m.acknowledge();	
					});				
				});		  
			} catch(e) {
				//TODO: Change to loggin
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
				var delay = self.retryDelay;
		
				for(var a = 0; a <= retryCount; a++) {
					delay = delay + delay;
				}
		
				console.log('retry count ' + retryCount + ' sleeping for ' + delay + ' ms');
	
				setTimeout(function() {					
						console.log(retryCount + " POST: " + qRetry.options['endpoint']);
						
						var client = new RestClient();
						var json = d.toString();
				
						var rest_client = client.post(qRetry.options['endpoint'], {'content-type': retry.contentType }, json ); //NOTE: Maybe remove hard coded content-type
						rest_client.on('success', function(statusCode, data) {
							
							console.log("SUCCESS!!"); //TODO: change to logging
							console.log(statusCode);
							
							//TODO: check status code
							
							retry.acknowledge();	
						}).on('fail', function(err) {
							
							console.log('FAILED!!');	//TODO: change to logging
							console.log(err);
						
							self.retryOrPoison(retry, d);
													
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

Bunnyhop.prototype.retryOrPoison = function(msg, data) {
	var self = this;
	
	var retryCount = parseInt(msg.headers['retryCount'].toString());
	var json = data.toString();
	
	if(retryCount < self.maxRetry) {
		self.exchange.publish(self.routingKey + '.retry', json, { 
			durable: true,
			contentType: msg.contentType,
			exchange: msg.exchange,
			headers: { retryCount: (retryCount+1).toString() } 
		}); 
	} else {
		self.exchange.publish(self.routingKey + '.poison', json, { 
			durable: true,
			contentType: msg.contentType,
			exchange: msg.exchange,
			headers: { retryCount: retryCount.toString() } 
		}); 
	} 
}

