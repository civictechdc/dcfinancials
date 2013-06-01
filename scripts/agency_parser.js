//Okay, so this thing is a mess.  TODO: Beautify, and restructure to match current goals.

var fs = require('graceful-fs');
var csv = require('csv');
var _ = require('underscore');
// _.string = require('underscore.string');
var Q = require('q');
var stream = require('stream');
var path = require('path');
var json2csv = require('json2csv');
var moment = require('moment');


var csv_options = {
	trim: true,
	columns: true
}

var agencies = [];
var pos = [];
var suppliers = [];

var data_path = path.join(__dirname,'..','/data/');
var po_files = fs.readdirSync(path.join(data_path,'pos/'));
var po_reads = [];
console.log(po_files);

po_files.forEach(function(po_file){
	if(po_file.substr(-3) == 'csv'){
		var deferred = Q.defer();
		po_reads.push(deferred.promise);

		//Here's where we deal with the fact that DC doesn't escape quotation marks inside fields.  No, really.
		function CSVFixer(options) {
		  if (!(this instanceof CSVFixer))
		    return new CSVFixer(options);
		  stream.Transform.call(this, options);
		}
		CSVFixer.prototype = Object.create(
		  stream.Transform.prototype, { constructor: { value: CSVFixer }});
		CSVFixer.prototype._transform = function(chunk, encoding, done) {
			var str = chunk.toString();
			var i = 0;
			while(i<str.length){
				if(str[i] == '"' && str[i-1] != "," && str[i+1] !== "," && str[i+1] != undefined){
					var snippet = str.substr(i-5,10);
					str = str.substr(0,i) + "'" + str.substr(i+1);
					console.log("Replaced \"" + snippet + "\" with \"" + str.substr(i-5,10) + "\" in file " + po_file);
				}
				i++;
			}
		  	this.push(new Buffer(str));
		  	done();
		  	return;
		};
		var file = fs.createReadStream(data_path + 'pos/' + po_file);
		var fixer = new CSVFixer();
		file.pipe(fixer);

		csv()
			.from.stream(fixer,csv_options)
			.on('record', function(row,index){
				var date = new moment(row['ORDER_DATE'],'MM/DD/YYYY');
				row['ORDER_DATE'] = date.format('YYYY-MM-DD')
				pos.push(row);
				/*var agency = _.where(agencies,{ name: row['AGENCY_NAME'] })[0];
				if(!agency){
					agency = {
						'name': row['AGENCY_NAME'],
						'pos': []
					}
					agencies.push(agency);
				}
				// delete row['AGENCY_NAME'];
				agency.pos.push(row);

				var supplier = _.where(suppliers,{ name: row['SUPPLIER'] })[0];
				if(!supplier){
					supplier = {
						'name': row['SUPPLIER'],
						'pos': []
					}
					suppliers.push(supplier);
				}
				// delete row['SUPPLIER'];
				supplier.pos.push(row);*/
			})
			.on('end',function(count){
				console.log("Read " + count + " lines from " + po_file);
				deferred.resolve("OK");
			})
			.on('error',function(error){
				console.log(po_file,error);
				deferred.reject();
			});
	}
}); 

Q.allResolved(po_reads)
	.then(function(promises){
		console.log("Done reading files.");
		json2csv({data: pos, fields: ['PO_NUMBER','AGENCY_NAME','NIGP_DESCRIPTION','PO_TOTAL_AMOUNT','ORDER_DATE','SUPPLIER','SUPPLIER_FULL_ADDRESS','SUPPLIER_CITY','SUPPLIER_STATE']}, function(err, csv) {
		  if (err) console.log(err);
	  	  var filename = path.join(__dirname ,'..','data','pos','all','all.csv');
		  fs.writeFile(filename, csv, function(err) {
		    if (err) throw err;
		    console.log('file saved');
		  });
		});
		/*agencies.forEach(function(agency){
			var filename = path.join(__dirname ,'..','data','pos','by_agency',_.string.slugify(agency.name) + ".json");
			fs.writeFile(filename,JSON.stringify(agency),function(){
				console.log("Saved " + filename);
			});
		});
		var filename = path.join(__dirname ,'..','data','pos','by_agency','all.json');
		fs.writeFile(filename,JSON.stringify(agencies),function(){
			console.log("Saved " + filename);
		});
		suppliers.forEach(function(supplier){
			var filename = path.join(__dirname ,'..','data','pos','by_supplier',_.string.slugify(supplier.name) + ".json");
			fs.writeFile(filename,JSON.stringify(supplier),function(){
				console.log("Saved " + filename);
			});
		});
		var filename = path.join(__dirname ,'..','data','pos','by_supplier','all.json');
		fs.writeFile(filename,JSON.stringify(suppliers),function(err){
			if(err) throw err;
			console.log("Saved " + filename);
		});*/
	});