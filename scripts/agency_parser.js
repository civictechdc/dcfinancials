var fs = require('fs');
var csv = require('csv');
var _ = require('underscore');
_.string = require('underscore.string');
var Q = require('q');
var stream = require('stream');
var path = require('path');

var csv_options = {
	trim: true,
	columns: true
}

var agencies = [];
var pos = [];

var data_path = path.join(__dirname,'..','/data/');
var po_files = fs.readdirSync(path.join(data_path,'pos/'));
var po_reads = [];

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
				var agency = _.where(agencies,{ name: row['AGENCY_NAME'] })[0];
				if(!agency){
					agency = {
						'name': row['AGENCY_NAME'],
						'pos': []
					}
					agencies.push(agency);
				}
				delete row['AGENCY_NAME'];
				agency.pos.push(row);
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
		agencies.forEach(function(agency){
			var filename = path.join(__dirname ,'..','data','pos','by_agency',_.string.slugify(agency.name) + ".json");
			console.log(filename);
			fs.writeFile(filename,JSON.stringify(agency),function(){
				console.log("Saved " + filename);
			});
		});
		var filename = path.join(__dirname ,'..','data','pos','by_agency','all_by_agency.json');
		fs.writeFile(filename,JSON.stringify(agencies),function(){
			console.log("Saved " + filename);
		});



	});