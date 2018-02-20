let AWS = require('aws-sdk');
const sns = new AWS.SNS();
const s3 = new AWS.S3();
exports.handler = function (event, context, callback) {

	console.log(`Batch process triggered at ${event.time}`);

	s3.listObjects({
		'Bucket': 'batch-process-bucket-udith',
		'MaxKeys': 100,
		'Prefix': ''
	}).promise()
		.then(data => {
			let numFiles = data.Contents.length;
			let successCount = 0;
			let failedCount = 0;

			console.log(`${numFiles} files found to process`);

			if (numFiles === 0) {
				// There are no files to process. So notify that.
				exports.sendNotification(
					'Processing Finished',
					'No files found to be processed',
					() => callback(null, "Processing finished without any files & Notification sent"),
					(err) => callback(err, "Processing finished without any files & Notification failed"));
			}

			// For each file, execute the processing
			data.Contents.forEach(file => {
				let fileName = file.Key;

				console.log(`Processing File : ${fileName}`);
				// CUSTOM PROCESSING LOGIC GOES HERE

				// After the processing, delete the file

				s3.deleteObject({
					'Bucket': "batch-process-bucket-udith",
					'Key': fileName
				}).promise()
					.then(data => {
						console.log(`Successfully deleted file ${fileName}`);
						successCount++;

						if ((successCount + failedCount) === numFiles) {
							// This is the last file. So send the notification.
							let message = `Processing finished. ${successCount} successful and ${failedCount} failed`;

							exports.sendNotification(
								'Processing Finished',
								message,
								() => callback(null, "Processing finished & Notification sent"),
								(err) => callback(err, "Processing finished & Notification failed"));
						}
					})
					.catch(err => {
						console.log(`Failed to delete file : ${fileName}`, err, err.stack);
						failedCount++;

						if ((successCount + failedCount) === numFiles) {
							// This is the last file. So send the notification.
							let message = `Processing finished. ${successCount} successful and ${failedCount} failed`;

							exports.sendNotification(
								'Processing Finished',
								message,
								() => callback(null, "Processing finished & Notification sent"),
								(err) => callback(err, "Processing finished & Notification failed"));
						}
					});

			});
		})
		.catch(err => {
			console.log("Failed to get file list", err, err.stack); // an error occurred
			let message = `Message processing failed due to : ${err}`;

			exports.sendNotification(
				'Processing Failed',
				message,
				() => callback(err, "Failed to get file list"),
				(err1) => callback(err, "Failed to get file list"));
		});
}

/* 
This function publishes the provided message with subject to the notification
topic and excute the provided onSuccess or onFailure callback handler 
*/
exports.sendNotification = (subject, message, onSuccess, onFailure) => {
	sns.publish({
		Message: message,
		Subject: subject,
		MessageAttributes: {},
		MessageStructure: 'String',
		TopicArn: 'arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:batch-process-notify'
	}).promise()
		.then(data => {
			console.log("Successfully published notification");
			onSuccess();
		})
		.catch(err => {
			console.log("Error occurred while publishing notification", err, err.stack);
			onFailure(err);
		});

}