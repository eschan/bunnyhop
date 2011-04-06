var sys = require('sys');
var amqp = require('amqp');
var fs = require('fs');
var RestClient = require('./httpclient')

module.exports = Bunnyhop;

Bunnyhop.prototype = new process.EventEmitter;

function Bunnyhop () {
	var configFile = process.env.BUNNYHOP_CONFIG || "/etc/bunnyhop.cfg";
	var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
	
	this.exchangeName = config.exchange;
	this.exchangeType	= config.exchangeType;
	this.queueName = config.queue;
	this.routingKey = config.routingKey;
	this.restEndpoint = config.restEndpoint;
	this.maxRetry = parseInt(config.retryCount);
	this.retryDelay = parseInt(config.retryDelay);
	console.log(config);

	this.connection = amqp.createConnection( { host: config.host } );

	//error listener on connection
	this.connection.addListener('error', function (e) {
		console.log(e);
	})

	//close listener on connection
	this.connection.addListener('close', function (e) {
		console.log('connection closed.');
	});
} 

Bunnyhop.prototype.listen = function() {
	var self = this;
	
	//ready listener on connection
	self.connection.addListener('ready', function() {
		/*******
		EXCHANGE
		*******/
		self.exchange = self.connection.exchange(self.exchangeName, { type: self.exchangeType, durable: true });	//create exchange
		
		/*****
		QUEUES
		******/
		var q = self.connection.queue(self.queueName, { 
			passive: false,
			durable: true, 
			exclusive: false,
			autoDelete: false,
			endpoint: self.restEndpoint 
		});	//create queue 

		var qRetry = self.connection.queue(self.queueName + '.retry', {
			passive: false,
			durable: true,
			exclusive: false,
			autoDelete: false,
			endpoint: self.restEndpoint 
		}); //create associated retry queue

		var qPoison = self.connection.queue(self.queueName + '.poison', {
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
		q.subscribeRaw(function(msg) {
			try {	
				msg.addListener('data', function (data) { 
					self.post(q.options['endpoint'], msg, data, function() {
						msg.acknowledge();
					});
				});		  
			} catch(e) {
				//TODO: Change to loging
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
	
			retry.addListener('data', function (data) { 
				var retryCount = parseInt(retry.headers['retryCount'].toString());
				var delay = self.retryDelay;
		
				for(var a = 0; a <= retryCount; a++) {
					delay = delay + delay;
				}
		
				console.log('retry count ' + retryCount + ' sleeping for ' + delay + ' ms');
	
				//TIMEOUT LOGIC	
				setTimeout(function() {					
					console.log(retryCount + " POST: " + qRetry.options['endpoint']);
					
					self.post(q.options['endpoint'], retry, data, function() {
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
			console.log('Message Poisoned');
			dead.acknowledge();
		});
	});
}

Bunnyhop.prototype.post = function(endpoint, msg, msgData, callback) {
	var self = this;
	var client = new RestClient();
	var json = msgData.toString();

	var rest_client = client.post(endpoint, {'content-type': msg.contentType }, json ); //NOTE: Maybe remove hard coded content-type
	rest_client.on('success', function(statusCode, postData) {
		switch(statusCode) {
			case 200:
				console.log("SUCCESS!!"); //TODO: Change to logging
				console.log(statusCode);
				callback();
				break;
			default:
				self.retryOrPoison(msg, msgData, function(){
					callback();
				});
		}	
	}).on('fail', function(err) {
		console.log('FAILED!!');	//TODO: change to logging
		console.log(err);
	
		self.retryOrPoison(msg, msgData, function(){
			callback();
		});
	});
}

Bunnyhop.prototype.retryOrPoison = function(msg, data, callback) {
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
	
	callback();
}

