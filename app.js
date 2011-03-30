var bunnyhop = require('./lib/bunnyhop')
var fs = require('fs')

var configFile = process.env.BUNNYHOP_CONFIG || "bunnyhop.cfg.example";
var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));

if (module == process.mainModule) {
	var bunhop = bunnyhop(config);
	bunhop.start();
  console.log("bunnyhop started!!!");
}
