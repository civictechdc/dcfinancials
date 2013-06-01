//Okay, so this thing is a mess.  TODO: Beautify, and restructure to match current goals.

var fs = require('graceful-fs');
var csv = require('csv');
var path = require('path');
var moment = require('moment');


var csv_options = {
	trim: true,
	columns: true
}

var data_path = path.join(__dirname,'..','/data/');
var cbe_file = path.join(data_path,'cbe/','cbe_snapshot.csv');
var out_file = path.join(data_path,'cbe','cbe_snapshot.json');
var out_fd = fs.openSync(out_file,'w+');

var fields = null;

var file = fs.createReadStream(cbe_file);
var current_company = null;
var companies = [];

csv()
	.from.stream(file,csv_options)
	.on('record', function(row,index){
		if(row['Company Name'] != ''){
			//write current company
			if(current_company != null){
				companies.push(current_company);
				// var buff = new Buffer(JSON.stringify(current_company));
				// fs.writeSync(out_fd,buff,0,buff.length);
			}
			//create new company
			current_company = row;
			current_company['Expiration Date'] = new moment(current_company['Expiration Date'],'MM/DD/YY').format('YYYY-MM-DD');
			current_company['NIGP Codes'] = [current_company['NIGP Codes']];
		}
		else{
			current_company['NIGP Codes'].push(row['NIGP Codes'].slice(0,-1));
		}
	})
	.on('end',function(count){
		companies.push(current_company);
		console.log(companies);
		var buff = new Buffer(JSON.stringify(companies));
		fs.writeSync(out_fd,buff,0,buff.length);

		console.log("Read " + count + " lines from " + cbe_file);
		fs.closeSync(out_fd);
	})
	.on('error',function(error){
		console.log(cbe_file,error);
	});