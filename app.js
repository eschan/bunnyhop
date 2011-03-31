var bunnyhop = require('./lib/bunnyhop')

if (module == process.mainModule) {
	var bunhop = bunnyhop();
	bunhop.start();
  console.log("bunnyhop started!!!");
}
