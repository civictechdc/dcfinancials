//Okay, so this thing is a mess.  TODO: Beautify, and restructure to match current goals.

var fs = require('graceful-fs');
var csv = require('csv');
var Q = require('q');
var path = require('path');
var json2csv = require('json2csv');
var moment = require('moment');


var csv_options = {
	trim: true,
	columns: true
}

var data_path = path.join(__dirname,'..','/data/');
var pay_files = fs.readdirSync(path.join(data_path,'payments/'));
var out_file = path.join(__dirname ,'..','data','payments','all','allPayments.csv');
var out_fd = fs.openSync(out_file,'w+');
var pay_reads = [];

var fields = null;

pay_files.forEach(function(pay_file){
	if(pay_file.substr(-3) == 'csv'){
		console.log("Reading " + pay_file);
		var deferred = Q.defer();
		pay_reads.push(deferred.promise);

		var file = fs.createReadStream(data_path + 'payments/' + pay_file);
		var cbe_fields = ['LBE_SPEND','SBE_SPEND','DBE_SPEND','ROB_SPEND','LRB_SPEND','ZBE_SPEND'];

		csv()
			.from.stream(file,csv_options)
			.on('record', function(row,index){
				//Here's where we deal with the fact that the header rows aren't consistent.
				renameProperty(row,'Agency','AGENCY');
				renameProperty(row,'Vendor','VENDOR');
				renameProperty(row,'CBECert','CBECERT');
				renameProperty(row,'CBEIndicator','CBEINDICATOR');
				renameProperty(row,'VoucherNumber','VOUCHERNUMBER');
				renameProperty(row,'InvoiceNumber','INVOICENUMBER');
				renameProperty(row,'PONumber','PONUMBER');
				renameProperty(row,'ObjectTitle','OBJECTTITLE');
				renameProperty(row,'FundNumber','FUNDNUMBER');
				renameProperty(row,'FundDescription','FUNDDESCRIPTION');
				renameProperty(row,'LineAmount','LINEAMOUNT');
				renameProperty(row,'ApprovedDate','APPROVEDDATE');
				renameProperty(row,'LBE Spend','LBE_SPEND');
				renameProperty(row,'SBE Spend','SBE_SPEND');
				renameProperty(row,'DBESpend','DBE_SPEND'); //What.
				renameProperty(row,'ROB Spend','ROB_SPEND');
				renameProperty(row,'LRB Spend','LRB_SPEND');
				renameProperty(row,'ZBE Spend','ZBE_SPEND');

				row['LINEAMOUNT'] = row['LINEAMOUNT'].replace('$','');
				row['LINEAMOUNT'] = parseFloat(row['LINEAMOUNT']);

				row['CBEINDICATOR'] = row['CBEINDICATOR'].toUpperCase();

				var date = new moment(row['APPROVEDDATE'],'ddd MMM DD HH:mm:ss ddd YYYY');
				row['APPROVEDDATE'] = date.format('YYYY-MM-DD')

				//Harmonize use of CBE fields.  Duplicate the LINEAMOUNT field if CBE status is true.
				//This is technically unnecessary, but makes some computations a bit easier later on.
				cbe_fields.forEach(function(field){
					row[field] = row[field].replace('$','');
					row[field] = row[field].replace(',','');
					switch(row[field]){
						case 'null':
							row[field] = 0;
							break;
						case 'TRUE':
							row[field] = row['LINEAMOUNT'];
							break;
						case '-':
							row[field] = 0;
							break;
						case '1':
							row[field] = row['LINEAMOUNT'];
							break;
						default:
							row[field] = parseFloat(row[field]);
					}
				});

				//Write the header row
				if(fields == null){
					fields = [];
					for(field in row){
						fields.push(field);
					}
					var buff = new Buffer(fields.join(',') + '\n');
					fs.writeSync(out_fd,buff,0,buff.length);
				}

				var csvline = "";
				var val ="";
				for(var i=0;i<fields.length;i++){
					if(typeof fields[i] === 'string'){
						csvline+= "\"" + row[fields[i]] + "\"";
					}
					if(i != fields.length-1){
						csvline+= ",";
					}
					else{
						csvline+="\n";
					}
				}

				var buff = new Buffer(csvline);
				fs.writeSync(out_fd,buff,0,buff.length);
				
			})
			.on('end',function(count){
				console.log("Read " + count + " lines from " + pay_file);
				deferred.resolve("OK");
			})
			.on('error',function(error){
				console.log(pay_file,error);
				deferred.reject();
			});
	}
}); 

Q.allResolved(pay_reads)
	.then(function(promises){
		fs.closeSync(out_fd);
		console.log("Done.");
	});

function renameProperty(obj,oldName,newName){
	if(obj.hasOwnProperty(oldName)){
		obj[newName] = obj[oldName];
		delete obj[oldName];
	}
}